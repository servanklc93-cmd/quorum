"""A real quorum node: the same Raft core as the simulator, driven by asyncio.

    peers talk over TCP: one JSON message per line
    clients talk HTTP:
        GET    /kv/<key>          linearizable read (goes through the log)
        PUT    /kv/<key>          body is the value
        DELETE /kv/<key>
        POST   /kv/<key>/cas      {"expected": ..., "value": ...}
        GET    /status

A node that isn't the leader answers 307 with the leader's address. Clients
that send X-Client-Id and X-Seq headers get exactly-once retries.
"""

import asyncio
import itertools
import json
import logging
import os
import random
import uuid
from dataclasses import asdict
from urllib.parse import unquote

from .kv import KVStore
from .raft import (
    LEADER, AppendEntries, AppendResponse, Config, InstallSnapshot, Raft, RequestVote,
    SnapshotResponse, VoteResponse,
)
from .storage import Entry, FileStorage, Snapshot

log = logging.getLogger("quorum")

TICK_SECONDS = 0.02            # election timeout: 10-20 ticks, so 200-400 ms
REQUEST_TIMEOUT = 3.0
SNAPSHOT_EVERY = 1000
MAX_QUEUE = 1000               # per peer; past this, messages are dropped (Raft copes)

_TYPES = {cls.__name__: cls for cls in
          (RequestVote, VoteResponse, AppendEntries, AppendResponse, InstallSnapshot, SnapshotResponse)}


def encode_message(m):
    d = asdict(m)
    d["type"] = type(m).__name__
    if isinstance(m, AppendEntries):
        d["entries"] = [[e.term, e.command] for e in m.entries]
    return (json.dumps(d, separators=(",", ":")) + "\n").encode()


def decode_message(line):
    d = json.loads(line)
    cls = _TYPES[d.pop("type")]
    if cls is AppendEntries:
        d["entries"] = [Entry(t, c) for t, c in d["entries"]]
    if cls is InstallSnapshot:
        d["snapshot"] = Snapshot(**d["snapshot"])
    return cls(**d)


class Peer:
    """An outgoing connection to another node, reconnecting as needed."""

    def __init__(self, node_id, host, port):
        self.id, self.host, self.port = node_id, host, port
        self.queue = asyncio.Queue(MAX_QUEUE)
        self.task = None

    def send(self, data):
        try:
            self.queue.put_nowait(data)
        except asyncio.QueueFull:
            pass      # the peer is down or slow; Raft retransmits on its own

    async def run(self):
        while True:
            try:
                _, writer = await asyncio.open_connection(self.host, self.port)
            except OSError:
                await asyncio.sleep(0.1)
                continue
            try:
                while True:
                    data = await self.queue.get()
                    writer.write(data)
                    await writer.drain()
            except (ConnectionError, OSError):
                pass
            finally:
                writer.close()


class Node:
    def __init__(self, node_id, peers, http_port, data_dir, config=None):
        """peers: {id: (host, raft_port, http_port)} including this node."""
        self.id = node_id
        self.addresses = peers
        self.http_port = http_port
        self.storage = FileStorage(data_dir)
        self.raft = Raft(node_id, list(peers), self.storage, random.Random(), config or Config())
        self.kv = KVStore()
        self.waiting = {}          # index -> (term, future)
        self.peers = {i: Peer(i, h, rp) for i, (h, rp, _) in peers.items() if i != node_id}
        self._servers = []
        self._tasks = []
        self._request_ids = itertools.count()
        self._session = uuid.uuid4().hex[:8]
        self._drain()              # a restart may have a snapshot to restore

    # --- driving raft -------------------------------------------------------------------

    def _drain(self):
        for m in self.raft.take_messages():
            self.peers[m.dst].send(encode_message(m))
        for c in self.raft.take_committed():
            if c.snapshot is not None:
                self.kv.restore(c.index, c.snapshot.data)
                for index in [i for i in self.waiting if i <= c.index]:
                    _, fut = self.waiting.pop(index)
                    if not fut.done():
                        fut.set_exception(LostLeadership())
                continue
            result = self.kv.apply(c.index, c.command)
            waiting = self.waiting.pop(c.index, None)
            if waiting is not None:
                term, fut = waiting
                if fut.done():
                    pass
                elif term == c.term:
                    fut.set_result(result)
                else:
                    fut.set_exception(LostLeadership())   # someone else's entry won that slot
            if c.index - self.storage.snapshot.index >= SNAPSHOT_EVERY:
                self.raft.compact(c.index, self.kv.snapshot())

    async def _ticker(self):
        while True:
            await asyncio.sleep(TICK_SECONDS)
            was_leader = self.raft.role == LEADER
            self.raft.tick()
            self._drain()
            if was_leader and self.raft.role != LEADER:
                self._fail_waiting()

    def _fail_waiting(self):
        for _, fut in self.waiting.values():
            if not fut.done():
                fut.set_exception(LostLeadership())
        self.waiting.clear()

    async def _on_peer(self, reader, writer):
        try:
            while line := await reader.readline():
                try:
                    msg = decode_message(line)
                except (ValueError, KeyError, TypeError):
                    log.warning("node %s: dropping a malformed peer message", self.id)
                    continue
                if msg.dst != self.id or msg.src not in self.peers:
                    continue
                was_leader = self.raft.role == LEADER
                self.raft.step(msg)
                self._drain()
                if was_leader and self.raft.role != LEADER:
                    self._fail_waiting()
        except (ConnectionError, OSError):
            pass
        finally:
            writer.close()

    async def submit(self, command):
        proposed = self.raft.propose(command)
        if proposed is None:
            raise NotLeader(self.raft.leader)
        index, term = proposed
        fut = asyncio.get_running_loop().create_future()
        self.waiting[index] = (term, fut)
        self._drain()
        return await asyncio.wait_for(fut, REQUEST_TIMEOUT)

    # --- HTTP -----------------------------------------------------------------------------

    async def _on_http(self, reader, writer):
        try:
            while True:
                request = await _read_request(reader)
                if request is None:
                    break
                status, headers, body = await self._route(*request)
                _write_response(writer, status, headers, body)
                await writer.drain()
        except (ConnectionError, OSError, asyncio.IncompleteReadError):
            pass
        except _BadRequest as e:
            _write_response(writer, 400, {}, {"error": str(e)})
        finally:
            writer.close()

    async def _route(self, method, path, headers, body):
        if path == "/status" and method == "GET":
            return 200, {}, {**self.raft.status(), "keys": len(self.kv.data)}
        if not path.startswith("/kv/") or len(path) <= 4:
            return 404, {}, {"error": "not found"}

        key = unquote(path[4:])
        cas = key.endswith("/cas")
        if cas:
            key = key[:-4]
        try:
            command = _command(method, key, cas, body)
        except _BadRequest as e:
            return 400, {}, {"error": str(e)}
        if command is None:
            return 405, {}, {"error": "method not allowed"}

        client = headers.get("x-client-id")
        seq = headers.get("x-seq")
        if client and seq and seq.isdigit():
            command["client"], command["seq"] = client, int(seq)
        else:
            # no session: unique, so it's never mistaken for a retry
            command["client"], command["seq"] = f"anon-{self._session}-{next(self._request_ids)}", 1

        try:
            result = await self.submit(command)
        except NotLeader as e:
            if e.leader is None or e.leader not in self.addresses:
                return 503, {"Retry-After": "1"}, {"error": "no leader right now, try again"}
            host, _, http_port = self.addresses[e.leader]
            return 307, {"Location": f"http://{host}:{http_port}{path}"}, {"leader": e.leader}
        except (asyncio.TimeoutError, LostLeadership):
            # it may or may not have been committed: retry with the same seq to find out
            return 504, {}, {"error": "request outcome unknown, retry with the same X-Seq"}
        if "error" in result:
            return 409, {}, result
        return 200, {}, result

    # --- lifecycle --------------------------------------------------------------------------

    async def start(self):
        host, raft_port, _ = self.addresses[self.id]
        self._servers.append(await asyncio.start_server(self._on_peer, host, raft_port))
        self._servers.append(await asyncio.start_server(self._on_http, host, self.http_port))
        for p in self.peers.values():
            p.task = asyncio.create_task(p.run())
            self._tasks.append(p.task)
        self._tasks.append(asyncio.create_task(self._ticker()))
        log.info("node %s up: raft %s:%s, http %s:%s", self.id, host, raft_port, host, self.http_port)

    async def stop(self):
        for t in self._tasks:
            t.cancel()
        for s in self._servers:
            s.close()
            await s.wait_closed()
        self.storage.close()

    async def serve_forever(self):
        await self.start()
        await asyncio.Event().wait()


class NotLeader(Exception):
    def __init__(self, leader):
        super().__init__(f"not the leader (leader is {leader})")
        self.leader = leader


class LostLeadership(Exception):
    pass


class _BadRequest(Exception):
    pass


def _command(method, key, cas, body):
    if not key or len(key) > 1024:
        raise _BadRequest("key must be 1 to 1024 characters")
    if cas:
        if method != "POST":
            return None
        try:
            spec = json.loads(body or b"{}")
        except ValueError:
            raise _BadRequest("cas body must be JSON: {\"expected\": ..., \"value\": ...}") from None
        if not isinstance(spec, dict):
            raise _BadRequest("cas body must be a JSON object")
        return {"op": "cas", "key": key, "expected": spec.get("expected"), "value": spec.get("value")}
    if method == "GET":
        return {"op": "get", "key": key}
    if method == "PUT":
        try:
            return {"op": "put", "key": key, "value": body.decode()}
        except UnicodeDecodeError:
            raise _BadRequest("value must be UTF-8") from None
    if method == "DELETE":
        return {"op": "delete", "key": key}
    return None


MAX_BODY = 1024 * 1024


async def _read_request(reader):
    line = await reader.readline()
    if not line:
        return None
    try:
        method, target, _ = line.decode("latin-1").split(" ", 2)
    except ValueError:
        raise _BadRequest("malformed request line") from None
    headers = {}
    while True:
        h = await reader.readline()
        if h in (b"\r\n", b"\n", b""):
            break
        if len(headers) > 100:
            raise _BadRequest("too many headers")
        name, _, value = h.decode("latin-1").partition(":")
        headers[name.strip().lower()] = value.strip()
    length = headers.get("content-length", "0")
    if not length.isdigit() or int(length) > MAX_BODY:
        raise _BadRequest("bad or too large Content-Length")
    body = await reader.readexactly(int(length)) if int(length) else b""
    return method.upper(), target.split("?", 1)[0], headers, body


_REASONS = {200: "OK", 307: "Temporary Redirect", 400: "Bad Request", 404: "Not Found",
            405: "Method Not Allowed", 409: "Conflict", 503: "Service Unavailable", 504: "Gateway Timeout"}


def _write_response(writer, status, headers, body):
    payload = json.dumps(body).encode()
    head = [f"HTTP/1.1 {status} {_REASONS.get(status, '')}", "Content-Type: application/json",
            f"Content-Length: {len(payload)}"] + [f"{k}: {v}" for k, v in headers.items()]
    writer.write(("\r\n".join(head) + "\r\n\r\n").encode() + payload)


def parse_peers(spec):
    """"1=127.0.0.1:9001:8001,2=..." -> {1: ("127.0.0.1", 9001, 8001), ...}"""
    peers = {}
    for part in spec.split(","):
        node_id, _, addr = part.partition("=")
        host, raft_port, http_port = addr.rsplit(":", 2)
        peers[int(node_id)] = (host, int(raft_port), int(http_port))
    return peers


def run(node_id, peers, data_dir):
    logging.basicConfig(level=os.environ.get("QUORUM_LOG", "INFO"), format=f"%(asctime)s node{node_id} %(message)s")
    node = Node(node_id, peers, peers[node_id][2], data_dir)
    try:
        asyncio.run(node.serve_forever())
    except KeyboardInterrupt:
        pass

