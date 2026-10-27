"""A small client for a quorum cluster.

    c = Client(["http://127.0.0.1:8001", "http://127.0.0.1:8002"])
    c.put("k", "v")
    c.get("k")          # -> "v"

It follows redirects to the leader, moves on to another node when one is
down, and retries timeouts with the same sequence number. The server
remembers the last (client, seq) it applied, so a retry of a write that
did land the first time isn't applied twice.
"""

import http.client
import json
import time
import uuid
from urllib.parse import quote, urlparse


class ClusterUnavailable(Exception):
    pass


class Client:
    def __init__(self, nodes, timeout=5.0, attempts=40):
        self.nodes = [n.rstrip("/") for n in nodes]
        self.leader = self.nodes[0]
        self.id = uuid.uuid4().hex
        self.seq = 0
        self.timeout = timeout
        self.attempts = attempts
        # one kept-alive connection per node. a fresh TCP connection per request
        # leaves thousands of sockets in TIME_WAIT under load, and macOS runs out
        # of ephemeral ports ("Can't assign requested address") within seconds
        self._conns = {}

    def get(self, key):
        return self._call("GET", key)["value"]

    def put(self, key, value):
        self._call("PUT", key, value.encode())

    def delete(self, key):
        return self._call("DELETE", key)["deleted"]

    def cas(self, key, expected, value, detailed=False):
        """Set key to value if it currently holds expected (None: doesn't exist).

        Returns True/False, or with detailed=True the server's whole answer,
        which on failure includes the value that was there instead."""
        reply = self._call("POST", key, json.dumps({"expected": expected, "value": value}).encode(), cas=True)
        return reply if detailed else reply["ok"]

    def status(self, node=None):
        status, body = self._request(node or self.leader, "GET", "/status", None, {})
        return body

    def _call(self, method, key, body=None, cas=False):
        self.seq += 1
        path = "/kv/" + quote(key, safe="") + ("/cas" if cas else "")
        headers = {"X-Client-Id": self.id, "X-Seq": str(self.seq)}
        target = self.leader
        tried = 0
        for attempt in range(self.attempts):
            try:
                status, reply = self._request(target, method, path, body, headers)
            except (OSError, http.client.HTTPException):
                status, reply = None, None
            if status == 200:
                self.leader = target
                return reply
            if status == 307 and reply:
                location = urlparse(reply.get("_location", ""))
                if location.netloc:
                    target = f"{location.scheme}://{location.netloc}"
                    continue
            if status in (400, 404, 405, 409):
                raise ValueError(reply.get("error", reply))
            # down, no leader yet, or outcome unknown (504): try the next node,
            # same seq, so a write that already happened isn't done again
            tried += 1
            target = self.nodes[tried % len(self.nodes)]
            time.sleep(min(0.05 * (attempt + 1), 0.5))
        raise ClusterUnavailable(f"{method} {key}: no answer after {self.attempts} attempts")

    def _request(self, base, method, path, body, headers):
        conn = self._conns.get(base)
        if conn is None:
            u = urlparse(base)
            conn = self._conns[base] = http.client.HTTPConnection(u.hostname, u.port, timeout=self.timeout)
        try:
            conn.request(method, path, body=body, headers=headers)
            resp = conn.getresponse()
            data = json.loads(resp.read() or b"{}")
        except BaseException:
            conn.close()
            self._conns.pop(base, None)
            raise
        if resp.status == 307:
            data["_location"] = resp.getheader("Location", "")
        return resp.status, data

    def close(self):
        for conn in self._conns.values():
            conn.close()
        self._conns.clear()
