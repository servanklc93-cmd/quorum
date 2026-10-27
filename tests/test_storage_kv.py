import os

from quorum.kv import KVStore
from quorum.storage import Entry, FileStorage, MemoryStorage, Snapshot


def test_file_storage_round_trip(tmp_path):
    s = FileStorage(str(tmp_path))
    s.save_hard_state(3, 2)
    s.append(1, [Entry(1, {"op": "put"}), Entry(2, None), Entry(3, "x")])
    s.close()
    s = FileStorage(str(tmp_path))
    assert (s.term, s.voted_for) == (3, 2)
    assert s.entries == [Entry(1, {"op": "put"}), Entry(2, None), Entry(3, "x")]


def test_truncating_a_conflicting_suffix_survives_reopen(tmp_path):
    s = FileStorage(str(tmp_path))
    s.append(1, [Entry(1, "a"), Entry(1, "b"), Entry(1, "c")])
    s.append(2, [Entry(2, "B")])
    s.close()
    assert FileStorage(str(tmp_path)).entries == [Entry(1, "a"), Entry(2, "B")]


def test_a_torn_write_at_the_end_is_dropped(tmp_path):
    s = FileStorage(str(tmp_path))
    s.append(1, [Entry(1, "a"), Entry(1, "b")])
    s.close()
    log = os.path.join(str(tmp_path), "log")
    size = os.path.getsize(log)
    with open(log, "ab") as f:
        f.write(b"\x00\x00\x00\x40garbage")   # half a record, as if the power went out
    s = FileStorage(str(tmp_path))
    assert s.entries == [Entry(1, "a"), Entry(1, "b")]
    assert os.path.getsize(log) == size
    s.append(3, [Entry(1, "c")])               # appending still works after the repair
    s.close()
    assert [e.command for e in FileStorage(str(tmp_path)).entries] == ["a", "b", "c"]


def test_a_corrupted_record_stops_the_log_there(tmp_path):
    s = FileStorage(str(tmp_path))
    s.append(1, [Entry(1, "a"), Entry(1, "b"), Entry(1, "c")])
    s.close()
    log = os.path.join(str(tmp_path), "log")
    data = bytearray(open(log, "rb").read())
    data[s.offsets[1] + 10] ^= 0xFF            # flip a byte inside record 2
    open(log, "wb").write(bytes(data))
    assert [e.command for e in FileStorage(str(tmp_path)).entries] == ["a"]


def test_snapshot_compacts_the_log(tmp_path):
    s = FileStorage(str(tmp_path))
    s.append(1, [Entry(1, i) for i in range(10)])
    s.save_snapshot(Snapshot(7, 1, {"k": "v"}))
    assert [e.command for e in s.entries] == [7, 8, 9]
    s.close()
    s = FileStorage(str(tmp_path))
    assert s.snapshot == Snapshot(7, 1, {"k": "v"}) and [e.command for e in s.entries] == [7, 8, 9]


def test_memory_storage_forgets_what_wasnt_saved():
    s = MemoryStorage()
    s.save_hard_state(5, 1)
    s.voted_for = 2            # a bug: changed without saving
    s.crash()
    assert (s.term, s.voted_for) == (5, 1)


def test_kv_operations():
    kv = KVStore()
    assert kv.apply(1, {"op": "put", "key": "a", "value": "1"}) == {"ok": True}
    assert kv.apply(2, {"op": "get", "key": "a"}) == {"value": "1"}
    assert kv.apply(3, {"op": "cas", "key": "a", "expected": "0", "value": "2"}) == {"ok": False, "value": "1"}
    assert kv.apply(4, {"op": "cas", "key": "a", "expected": "1", "value": "2"}) == {"ok": True}
    assert kv.apply(5, {"op": "delete", "key": "a"}) == {"deleted": True}
    assert kv.apply(6, None) is None and kv.applied == 6


def test_retried_commands_run_once():
    kv = KVStore()
    put = {"op": "put", "key": "n", "value": "first", "client": "c", "seq": 1}
    kv.apply(1, put)
    kv.apply(2, {"op": "put", "key": "n", "value": "other", "client": "d", "seq": 1})
    # the client's retry of seq 1 lands in the log again: it must not overwrite
    assert kv.apply(3, put) == {"ok": True}
    assert kv.data["n"] == "other"


def test_snapshot_restore():
    kv = KVStore()
    kv.apply(1, {"op": "put", "key": "a", "value": "1", "client": "c", "seq": 1})
    snap = kv.snapshot()
    other = KVStore()
    other.restore(1, snap)
    assert other.data == {"a": "1"} and other.sessions == {"c": [1, {"ok": True}]}
    kv.data["a"] = "changed"
    assert other.data["a"] == "1"          # a real copy
