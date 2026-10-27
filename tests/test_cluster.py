"""Real processes on localhost: SIGKILL the leader, restart nodes, and
check the history of concurrent clients for linearizability (a small
Jepsen-style test)."""

import json
import os
import random
import signal
import socket
import subprocess
import sys
import threading
import time
import urllib.request

import pytest

from quorum.client import Client, ClusterUnavailable
from quorum.linearizability import INFINITY, Op, check


def free_ports(n):
    socks = [socket.socket() for _ in range(n)]
    for s in socks:
        s.bind(("127.0.0.1", 0))
    ports = [s.getsockname()[1] for s in socks]
    for s in socks:
        s.close()
    return ports


class Cluster:
    def __init__(self, tmp_path, n=3):
        ports = free_ports(2 * n)
        self.peers = {i: ("127.0.0.1", ports[2 * (i - 1)], ports[2 * (i - 1) + 1]) for i in range(1, n + 1)}
        self.spec = ",".join(f"{i}={h}:{r}:{p}" for i, (h, r, p) in self.peers.items())
        self.dir = tmp_path
        self.procs = {}
        for i in self.peers:
            self.start(i)

    def url(self, i):
        return f"http://127.0.0.1:{self.peers[i][2]}"

    def urls(self):
        return [self.url(i) for i in self.peers]

    def start(self, i):
        env = dict(os.environ, QUORUM_LOG="WARNING")
        self.procs[i] = subprocess.Popen(
            [sys.executable, "-m", "quorum", "serve", "--id", str(i), "--peers", self.spec,
             "--data", str(self.dir / f"node{i}")], env=env)

    def kill(self, i):
        self.procs[i].send_signal(signal.SIGKILL)
        self.procs[i].wait()

    def status(self, i):
        with urllib.request.urlopen(self.url(i) + "/status", timeout=1) as r:
            return json.load(r)

    def wait_for_leader(self, among=None, timeout=10):
        deadline = time.time() + timeout
        while time.time() < deadline:
            for i in among or self.peers:
                try:
                    if self.status(i)["role"] == "leader":
                        return i
                except OSError:
                    pass
            time.sleep(0.05)
        seen = {}
        for i in self.peers:
            try:
                seen[i] = self.status(i)
            except OSError as e:
                seen[i] = f"unreachable: {e}"
        raise AssertionError(f"no leader after {timeout}s: {seen}")

    def stop(self):
        for p in self.procs.values():
            if p.poll() is None:
                p.kill()
                p.wait()


@pytest.fixture
def cluster(tmp_path):
    c = Cluster(tmp_path)
    yield c
    c.stop()


def test_writes_survive_killing_the_leader(cluster):
    leader = cluster.wait_for_leader()
    client = Client(cluster.urls())
    for i in range(20):
        client.put(f"k{i}", f"v{i}")
    cluster.kill(leader)

    new = cluster.wait_for_leader(among=[i for i in cluster.peers if i != leader])
    assert new != leader
    assert [client.get(f"k{i}") for i in range(20)] == [f"v{i}" for i in range(20)]
    client.put("after", "failover")

    # the dead node comes back from its own disk and catches up
    cluster.start(leader)
    deadline = time.time() + 10
    while time.time() < deadline:
        try:
            s = cluster.status(leader)
            if s["applied"] == cluster.status(new)["applied"] and s["keys"] == 21:
                break
        except OSError:
            pass
        time.sleep(0.1)
    assert cluster.status(leader)["keys"] == 21


def test_followers_redirect_to_the_leader(cluster):
    leader = cluster.wait_for_leader()
    follower = next(i for i in cluster.peers if i != leader)
    req = urllib.request.Request(cluster.url(follower) + "/kv/x", method="GET")
    opener = urllib.request.build_opener(type("NoRedirect", (urllib.request.HTTPRedirectHandler,),
                                              {"redirect_request": lambda *a: None}))
    try:
        opener.open(req, timeout=2)
        raise AssertionError("expected a redirect")
    except urllib.error.HTTPError as e:
        assert e.code == 307
        assert e.headers["Location"] == cluster.url(leader) + "/kv/x"


def test_bad_requests_get_400s(cluster):
    leader = cluster.wait_for_leader()
    for path, method, body in [("/kv/k/cas", "POST", b"not json"), ("/kv/k", "PUT", b"\xff\xfe")]:
        req = urllib.request.Request(cluster.url(leader) + path, data=body, method=method)
        try:
            urllib.request.urlopen(req, timeout=2)
            raise AssertionError("expected 400")
        except urllib.error.HTTPError as e:
            assert e.code == 400
    assert cluster.status(leader)["role"] == "leader"   # and the node is fine


def test_linearizable_while_the_leader_keeps_dying(cluster):
    """Four clients hammer three keys while the leader is killed twice.

    Every operation is recorded with real (monotonic) invoke and return
    times. An operation that gave up is recorded as not returned: it may or
    may not have happened. The checker then has to find one valid order.
    """
    cluster.wait_for_leader()
    history, lock = [], threading.Lock()
    stop = time.time() + 6

    def worker(name, seed):
        rng = random.Random(seed)
        client = Client(cluster.urls(), timeout=1.0, attempts=15)
        n = 0
        while time.time() < stop:
            n += 1
            # start each request at a random node, as behind a load balancer;
            # a node that answers reads from its own copy gets caught this way
            client.leader = rng.choice(cluster.urls())
            key = rng.choice("xyz")
            kind = rng.choice(["get", "get", "put", "cas"])
            value = f"{name}-{n}"
            expected = rng.choice([None, f"{name}-{n - 1}"])
            op = Op(name, kind, key, value if kind != "get" else None, expected if kind == "cas" else None,
                    invoke=time.monotonic())
            with lock:
                history.append(op)
            try:
                if kind == "get":
                    result = {"value": client.get(key)}
                elif kind == "put":
                    client.put(key, value)
                    result = {"ok": True}
                else:
                    result = client.cas(key, expected, value, detailed=True)
            except ClusterUnavailable:
                continue                                  # unknown outcome: leave it open-ended
            op.complete = time.monotonic()
            op.result = result

    threads = [threading.Thread(target=worker, args=(f"c{i}", i)) for i in range(4)]
    for t in threads:
        t.start()
    for _ in range(2):
        time.sleep(1.5)
        leader = cluster.wait_for_leader()
        cluster.kill(leader)
        time.sleep(0.5)
        cluster.start(leader)
    for t in threads:
        t.join()

    done = [op for op in history if op.complete != INFINITY]
    assert len(done) > 100, f"only {len(done)} operations completed"
    assert check(history) == []
