from quorum.linearizability import INFINITY, Op, check, check_key


def op(kind, invoke, complete, result=None, value=None, expected=None, key="k", client="c"):
    return Op(client, kind, key, value, expected, invoke, complete, result)


def test_sequential_history():
    h = [op("put", 0, 1, {"ok": True}, "a"), op("get", 2, 3, {"value": "a"})]
    assert check_key(h)


def test_stale_read_after_a_write_finished_is_not_linearizable():
    # the put returned at t=1; a read starting at t=2 must see it
    h = [op("put", 0, 1, {"ok": True}, "a"), op("get", 2, 3, {"value": None})]
    assert not check_key(h)


def test_concurrent_read_may_see_either_value():
    for seen in (None, "a"):
        h = [op("put", 0, 10, {"ok": True}, "a"), op("get", 2, 3, {"value": seen})]
        assert check_key(h)


def test_reads_cant_go_back_in_time():
    # two reads, one after the other, during a write: once a read saw the new
    # value, a later read can't see the old one again
    h = [op("put", 0, 20, {"ok": True}, "new"),
         op("get", 1, 2, {"value": "new"}, client="r1"),
         op("get", 3, 4, {"value": None}, client="r2")]
    assert not check_key(h)


def test_cas_outcomes_must_fit_one_order():
    # both CAS(None -> x) can't succeed
    h = [op("cas", 0, 5, {"ok": True}, "a", None, client="c1"),
         op("cas", 0, 5, {"ok": True}, "b", None, client="c2")]
    assert not check_key(h)
    h[1].result = {"ok": False, "value": "a"}
    assert check_key(h)


def test_an_unfinished_write_may_or_may_not_have_happened():
    for seen in (None, "a"):
        h = [op("put", 0, INFINITY, None, "a"), op("get", 5, 6, {"value": seen})]
        assert check_key(h)
    # ...but it can't be seen before it was invoked
    h = [op("get", 0, 1, {"value": "a"}), op("put", 2, INFINITY, None, "a")]
    assert not check_key(h)


def test_keys_are_checked_independently():
    good = [op("put", 0, 1, {"ok": True}, "a", key="x"), op("get", 2, 3, {"value": "a"}, key="x")]
    bad = [op("put", 0, 1, {"ok": True}, "a", key="y"), op("get", 2, 3, {"value": "zzz"}, key="y")]
    assert check(good + bad) == ["y"]


def test_long_histories_stay_fast():
    import time
    h, t = [], 0
    for i in range(400):
        h.append(op("put", t, t + 3, {"ok": True}, f"v{i}", client="w"))
        h.append(op("get", t + 1, t + 4, {"value": f"v{i}" if i % 2 else (f"v{i-1}" if i else None)}, client="r"))
        t += 5
    start = time.perf_counter()
    assert check_key(h)
    assert time.perf_counter() - start < 2
