"""A linearizability checker for key-value histories.

A history is linearizable if every operation can be given a single instant,
somewhere between when it was invoked and when it returned, such that
running the operations one by one in that order produces exactly the
results the clients saw.

This is the Wing & Gong search with memoisation (the approach Knossos and
Porcupine take). It tries to pick the next operation to linearize, and
backtracks when a result doesn't fit. Linearizability is compositional, so
each key is checked on its own, which keeps the search small.

An operation that never returned (the client gave up, or the test ended)
may or may not have taken effect, so the search is free to linearize it
anywhere after its invocation, or not at all.
"""

import sys
from dataclasses import dataclass
from typing import Optional

INFINITY = float("inf")


@dataclass
class Op:
    client: str
    kind: str                  # get / put / delete / cas
    key: str
    value: Optional[str]       # for put and cas
    expected: Optional[str]    # for cas
    invoke: float
    complete: float = INFINITY
    result: Optional[dict] = None

    @property
    def done(self):
        return self.complete != INFINITY


def _step(state, op):
    """(new state, the result this op would have returned) on a single register."""
    if op.kind == "get":
        return state, {"value": state}
    if op.kind == "put":
        return op.value, {"ok": True}
    if op.kind == "delete":
        return None, {"deleted": state is not None}
    if op.kind == "cas":
        if state == op.expected:
            return op.value, {"ok": True}
        return state, {"ok": False, "value": state}
    raise ValueError(op.kind)


def check_key(ops, initial=None):
    """True if the operations on one key are linearizable."""
    ops = sorted(ops, key=lambda o: o.invoke)
    n = len(ops)
    must = sum(1 << i for i, o in enumerate(ops) if o.done)
    seen = set()

    old_limit = sys.getrecursionlimit()
    sys.setrecursionlimit(max(old_limit, 4 * n + 100))
    try:
        return _search(ops, 0, initial, must, seen)
    finally:
        sys.setrecursionlimit(old_limit)


def _search(ops, linearized, state, must, seen):
    if linearized & must == must:
        return True
    key = (linearized, state)
    if key in seen:
        return False
    seen.add(key)

    # an op that returned before another was invoked must come first, so the
    # next pick can't have been invoked after the earliest outstanding return.
    # (invoked in the same tick as that return: no real-time order, either way works)
    horizon = min(o.complete for i, o in enumerate(ops) if not linearized >> i & 1 and o.done)
    for i, op in enumerate(ops):
        if linearized >> i & 1:
            continue
        if op.invoke > horizon:
            break                     # sorted by invoke: nothing later qualifies
        new_state, result = _step(state, op)
        if op.done and result != op.result:
            continue
        if _search(ops, linearized | 1 << i, new_state, must, seen):
            return True
    return False


def check(history):
    """Check a whole history; returns the list of keys that fail (empty if fine)."""
    by_key = {}
    for op in history:
        by_key.setdefault(op.key, []).append(op)
    return [k for k, ops in sorted(by_key.items()) if not check_key(ops)]
