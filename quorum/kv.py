"""The replicated state machine: a key-value map.

Commands are plain dicts so they serialise to JSON for the log:

    {"op": "put", "key": "k", "value": "v", "client": "c1", "seq": 7}
    {"op": "get", "key": "k", ...}
    {"op": "delete", "key": "k", ...}
    {"op": "cas", "key": "k", "expected": "old", "value": "new", ...}

Reads go through the log too, which makes them linearizable at the cost of
a round of replication.

A client that times out retries the same request with the same seq. If
the first attempt did get committed, the retry may get committed as well,
so the log can hold the command twice. The session table remembers each
client's last seq and result, and the duplicate gets the cached answer
instead of running again. That's what makes retries safe (§8 of the Raft
paper).
"""

import copy


class KVStore:
    def __init__(self):
        self.data = {}
        self.sessions = {}     # client -> [last seq, its result]
        self.applied = 0

    def apply(self, index, command):
        self.applied = index
        if command is None:
            return None                       # a leader's no-op entry
        client, seq = command.get("client"), command.get("seq")
        if client is not None:
            last = self.sessions.get(client)
            if last is not None and seq <= last[0]:
                return last[1] if seq == last[0] else {"error": "stale request"}
        result = self._run(command)
        if client is not None:
            self.sessions[client] = [seq, result]
        return result

    def _run(self, c):
        op, key = c["op"], c["key"]
        if op == "get":
            return {"value": self.data.get(key)}
        if op == "put":
            self.data[key] = c["value"]
            return {"ok": True}
        if op == "delete":
            return {"deleted": self.data.pop(key, None) is not None}
        if op == "cas":
            if self.data.get(key) == c.get("expected"):
                if c["value"] is None:
                    self.data.pop(key, None)
                else:
                    self.data[key] = c["value"]
                return {"ok": True}
            return {"ok": False, "value": self.data.get(key)}
        return {"error": f"unknown op {op!r}"}

    def snapshot(self):
        return {"data": copy.deepcopy(self.data), "sessions": copy.deepcopy(self.sessions)}

    def restore(self, index, snap):
        self.data = copy.deepcopy(snap["data"]) if snap else {}
        self.sessions = copy.deepcopy(snap["sessions"]) if snap else {}
        self.applied = index
