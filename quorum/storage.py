"""Durable Raft state: the current term, who we voted for, the log and the snapshot.

Raft's safety depends on this being on disk *before* the node answers a
vote request or acknowledges entries. Raft calls these methods
synchronously, before it sends any message that depends on them.

MemoryStorage is what the simulator uses: it survives a simulated crash
because the simulator keeps the object while it throws the node away.
FileStorage is the real thing.
"""

import json
import os
import struct
import zlib
from dataclasses import dataclass


@dataclass(frozen=True)
class Entry:
    term: int
    command: object   # anything JSON-serialisable; None is a leader's no-op


@dataclass(frozen=True)
class Snapshot:
    index: int = 0
    term: int = 0
    data: object = None


class MemoryStorage:
    def __init__(self):
        self.term = 0
        self.voted_for = None
        self.snapshot = Snapshot()
        self.entries = []          # entries[0] is at index snapshot.index + 1
        self.writes = 0            # how many durable writes happened (for tests)
        self._durable = (0, None)

    def save_hard_state(self, term, voted_for):
        self.term, self.voted_for = term, voted_for
        self._durable = (term, voted_for)
        self.writes += 1

    def crash(self):
        """What a crash leaves behind: only what went through save_hard_state.

        A bug that sets storage.voted_for without saving it would survive a
        simulated crash otherwise, and the simulator would never notice it."""
        self.term, self.voted_for = self._durable

    def append(self, first_index, entries):
        """Store entries starting at first_index, dropping anything already at or after it."""
        offset = first_index - self.snapshot.index - 1
        assert 0 <= offset <= len(self.entries), (first_index, self.snapshot.index, len(self.entries))
        del self.entries[offset:]
        self.entries.extend(entries)
        self.writes += 1

    def save_snapshot(self, snapshot):
        """Store a snapshot and drop the log entries it covers."""
        old = self.snapshot.index
        keep = snapshot.index - old
        if 0 < keep <= len(self.entries) and self.entries[keep - 1].term == snapshot.term:
            self.entries = self.entries[keep:]
        else:
            self.entries = []
        self.snapshot = snapshot
        self.writes += 1


# --- on disk ------------------------------------------------------------------
#
#   <dir>/state.json      {"term": .., "voted_for": ..}, replaced atomically
#   <dir>/snapshot.json   {"index", "term", "data"}, replaced atomically
#   <dir>/log             records: length u32 | crc32 u32 | json [term, command]
#
# The log is append-only. A crash in the middle of an append leaves a torn
# record at the end; loading stops at the first record whose length or CRC
# doesn't check out and truncates the file there.

_RECORD = struct.Struct(">II")


def _write_atomic(path, payload):
    tmp = path + ".tmp"
    with open(tmp, "w") as f:
        json.dump(payload, f)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)
    _fsync_dir(os.path.dirname(path))


def _fsync_dir(path):
    try:
        fd = os.open(path, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(fd)
    except OSError:
        pass
    finally:
        os.close(fd)


def _encode(entry):
    data = json.dumps([entry.term, entry.command], separators=(",", ":")).encode()
    return _RECORD.pack(len(data), zlib.crc32(data)) + data


class FileStorage(MemoryStorage):
    def __init__(self, directory):
        super().__init__()
        self.dir = directory
        os.makedirs(directory, exist_ok=True)
        self.state_path = os.path.join(directory, "state.json")
        self.snapshot_path = os.path.join(directory, "snapshot.json")
        self.log_path = os.path.join(directory, "log")
        self.offsets = []          # file offset of each entry's record
        self._load()
        self.log = open(self.log_path, "ab")

    def _load(self):
        if os.path.exists(self.state_path):
            with open(self.state_path) as f:
                state = json.load(f)
            self.term, self.voted_for = state["term"], state["voted_for"]
        if os.path.exists(self.snapshot_path):
            with open(self.snapshot_path) as f:
                s = json.load(f)
            self.snapshot = Snapshot(s["index"], s["term"], s["data"])
        if not os.path.exists(self.log_path):
            return
        with open(self.log_path, "rb") as f:
            raw = f.read()
        pos = 0
        while pos + _RECORD.size <= len(raw):
            length, crc = _RECORD.unpack_from(raw, pos)
            body = raw[pos + _RECORD.size:pos + _RECORD.size + length]
            if len(body) < length or zlib.crc32(body) != crc:
                break
            term, command = json.loads(body)
            self.offsets.append(pos)
            self.entries.append(Entry(term, command))
            pos += _RECORD.size + length
        if pos < len(raw):
            # torn write from a crash: everything after the last good record goes
            with open(self.log_path, "r+b") as f:
                f.truncate(pos)
                f.flush()
                os.fsync(f.fileno())

    def save_hard_state(self, term, voted_for):
        super().save_hard_state(term, voted_for)
        _write_atomic(self.state_path, {"term": term, "voted_for": voted_for})

    def append(self, first_index, entries):
        offset = first_index - self.snapshot.index - 1
        if offset < len(self.offsets):
            # a conflicting suffix: cut the file back to where it starts
            self.log.flush()
            self.log.truncate(self.offsets[offset])
            del self.offsets[offset:]
        super().append(first_index, entries)
        pos = self.log.seek(0, os.SEEK_END)
        for e in entries:
            record = _encode(e)
            self.offsets.append(pos)
            self.log.write(record)
            pos += len(record)
        self.log.flush()
        os.fsync(self.log.fileno())

    def save_snapshot(self, snapshot):
        super().save_snapshot(snapshot)
        _write_atomic(self.snapshot_path, {"index": snapshot.index, "term": snapshot.term, "data": snapshot.data})
        # rewrite the log without the entries the snapshot now covers
        tmp = self.log_path + ".tmp"
        with open(tmp, "wb") as f:
            self.offsets = []
            pos = 0
            for e in self.entries:
                record = _encode(e)
                self.offsets.append(pos)
                f.write(record)
                pos += len(record)
            f.flush()
            os.fsync(f.fileno())
        self.log.close()
        os.replace(tmp, self.log_path)
        _fsync_dir(self.dir)
        self.log = open(self.log_path, "ab")

    def close(self):
        self.log.close()
