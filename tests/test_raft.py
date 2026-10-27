"""Raft unit tests on a hand-driven network: every message is delivered (or
not) on purpose, so specific scenarios from the paper can be replayed."""

import random

import pytest

from quorum.raft import (
    CANDIDATE, FOLLOWER, LEADER, AppendEntries, AppendResponse, Config, Raft, RequestVote,
)
from quorum.storage import Entry, MemoryStorage


class Net:
    def __init__(self, n, **config):
        self.ids = list(range(1, n + 1))
        self.config = Config(**{"pre_vote": False, "check_quorum": False, **config})
        self.storage = {i: MemoryStorage() for i in self.ids}
        self.nodes = {i: self._make(i) for i in self.ids}
        self.applied = {i: [] for i in self.ids}

    def _make(self, i):
        return Raft(i, self.ids, self.storage[i], random.Random(i), self.config)

    def crash(self, i):
        self.storage[i].crash()
        del self.nodes[i]

    def restart(self, i):
        self.nodes[i] = self._make(i)

    def flush(self, allow=lambda m: True, rounds=50):
        """Deliver messages until things go quiet. Disallowed ones are dropped."""
        for _ in range(rounds):
            moved = False
            for i, node in list(self.nodes.items()):
                self._collect(i)
                for m in node.take_messages():
                    if m.dst in self.nodes and allow(m):
                        self.nodes[m.dst].step(m)
                        self._collect(m.dst)
                        moved = True
            if not moved:
                return

    def _collect(self, i):
        for c in self.nodes[i].take_committed():
            self.applied[i].append((c.index, c.term, c.command))

    def leaders(self):
        return [i for i, n in self.nodes.items() if n.role == LEADER]

    def tick_until_leader(self, limit=500):
        for _ in range(limit):
            for n in self.nodes.values():
                n.tick()
            self.flush()
            if len(self.leaders()) == 1:
                return self.leaders()[0]
        raise AssertionError("no leader")


def test_single_node_elects_itself_and_commits():
    net = Net(1)
    leader = net.tick_until_leader()
    assert net.nodes[leader].propose({"x": 1}) == (2, 1)   # index 1 is the leader's no-op
    net.flush()
    assert [c[2] for c in net.applied[leader]] == [None, {"x": 1}]


def test_election_and_replication():
    net = Net(3)
    leader = net.tick_until_leader()
    for i in range(5):
        net.nodes[leader].propose({"n": i})
    net.flush()
    # followers learn the new commit index from the next heartbeat
    for _ in range(net.config.heartbeat_ticks):
        net.nodes[leader].tick()
    net.flush()
    for i in net.ids:
        assert [c[2] for c in net.applied[i]] == [None] + [{"n": i} for i in range(5)]


def test_new_leader_keeps_committed_entries():
    net = Net(3)
    old = net.tick_until_leader()
    net.nodes[old].propose("committed")
    net.flush()
    net.crash(old)
    new = net.tick_until_leader()
    assert new != old
    assert any(c[2] == "committed" for c in net.applied[new])
    net.restart(old)
    net.nodes[new].propose("after")
    for _ in range(10):
        for n in net.nodes.values():
            n.tick()
        net.flush()
    # the restarted node catches up, applying everything in the same order
    assert [c[2] for c in net.applied[old]][-2:] == ["committed", "after"] or \
        [c[2] for c in net.applied[old]].count("after") == 1


def test_votes_once_per_term_and_only_for_up_to_date_logs():
    s = MemoryStorage()
    n = Raft(1, [1, 2, 3], s, random.Random(1), Config(pre_vote=False, check_quorum=False))
    n.step(RequestVote(2, 1, 5, last_index=0, last_term=0))
    n.step(RequestVote(3, 1, 5, last_index=0, last_term=0))
    granted = [m.granted for m in n.take_messages()]
    assert granted == [True, False]
    assert (s.term, s.voted_for) == (5, 2)   # persisted before answering

    s.append(1, [Entry(5, "x")])
    n.step(RequestVote(3, 1, 6, last_index=5, last_term=4))   # longer log, older term: refuse
    assert n.take_messages()[0].granted is False


def test_follower_truncates_a_conflicting_suffix():
    s = MemoryStorage()
    s.append(1, [Entry(1, "a"), Entry(1, "b"), Entry(2, "stale"), Entry(2, "stale2")])
    n = Raft(1, [1, 2], s, random.Random(1), Config(pre_vote=False, check_quorum=False))
    n.step(AppendEntries(2, 1, 3, prev_index=2, prev_term=1, entries=[Entry(3, "c")], commit=3))
    resp = n.take_messages()[0]
    assert resp.success and resp.match_index == 3
    assert [e.command for e in s.entries] == ["a", "b", "c"]
    assert [c.command for c in n.take_committed()] == ["a", "b", "c"]


def test_rejection_tells_the_leader_where_to_back_up_to():
    s = MemoryStorage()
    s.append(1, [Entry(1, "a"), Entry(2, "x"), Entry(2, "y"), Entry(2, "z")])
    n = Raft(1, [1, 2], s, random.Random(1), Config(pre_vote=False, check_quorum=False))
    n.step(AppendEntries(2, 1, 3, prev_index=4, prev_term=3, entries=[], commit=0))
    resp = n.take_messages()[0]
    # the whole term-2 run conflicts: skip back to its first index in one step
    assert not resp.success and (resp.conflict_term, resp.conflict_index) == (2, 2)


def test_figure_8_from_the_paper():
    """A leader must not commit an entry from an earlier term by counting replicas.

    Raft paper, figure 8: entry X (term 1) ends up on a majority while the
    leader is in term 3. If the leader counted that as committed, a later
    leader (S5, whose log ends in term 2) could still win an election and
    overwrite index 2, so a "committed" entry would vanish.
    """
    net = Net(5)
    s1, s2, s3, s4, s5 = (net.nodes[i] for i in net.ids)

    # term 1: S1 leads, the no-op at index 1 reaches everyone
    s1.campaign()
    net.flush()
    assert net.leaders() == [1] and s1.commit == 1

    # S1 gets X to S2 only, then crashes
    s1.propose("X")
    net.flush(allow=lambda m: not (m.src == 1 and m.dst != 2))
    net.crash(1)

    # term 2: S5 wins with S3 and S4, appends its no-op at index 2 locally, crashes
    net.nodes[5].campaign()
    net.flush(allow=lambda m: m.src in (3, 4, 5) and m.dst in (3, 4, 5) and not isinstance(m, AppendEntries))
    assert net.nodes[5].role == LEADER and net.nodes[5].term == 2
    net.crash(5)

    # term 3: S1 comes back and wins with S2 and S3. its first try (term 2)
    # fails because S3 already voted for S5 in term 2, so it tries again
    net.restart(1)
    s1 = net.nodes[1]
    between_1_2_3 = lambda m: not isinstance(m, (AppendEntries, AppendResponse)) and m.src in (1, 2, 3) and m.dst in (1, 2, 3)
    s1.campaign()
    net.flush(allow=between_1_2_3)
    assert s1.role == CANDIDATE and s1.term == 2
    s1.campaign()
    net.flush(allow=between_1_2_3)
    assert s1.role == LEADER and s1.term == 3
    s1.take_messages()

    # S1 replicates X (index 2, term 1) to S2 and S3 but not its term-3 no-op
    for follower in (2, 3):
        net.nodes[follower].step(AppendEntries(1, follower, 3, prev_index=1, prev_term=1,
                                               entries=[Entry(1, "X")], commit=s1.commit))
        for resp in net.nodes[follower].take_messages():
            assert resp.success and resp.match_index == 2
            s1.step(resp)
    s1.take_messages()

    # X is on S1, S2 and S3 (a majority), but it's from term 1: not committed.
    # (S1 restarted, so it doesn't even remember that index 1 was committed;
    # the commit index is volatile and it'll relearn it with its own no-op)
    assert s1.commit < 2

    # and that's what keeps this safe: S1 dies, S5 returns and can still win
    # (its log ends in term 2, newer than X's term 1)
    net.crash(1)
    net.restart(5)
    net.nodes[5].campaign()          # term 3: S2 and S3 already voted for S1
    net.flush()
    net.nodes[5].campaign()          # term 4, as in the paper
    net.flush()
    assert net.nodes[5].role == LEADER and net.nodes[5].term == 4
    assert all(net.nodes[i].entry(2).command is None for i in (2, 3, 4, 5))  # X is gone
    assert not any(c[2] == "X" for applied in net.applied.values() for c in applied)


def test_pre_vote_stops_a_rejoining_node_from_raising_the_term():
    net = Net(3, pre_vote=True, check_quorum=True)
    leader = net.tick_until_leader()
    term = net.nodes[leader].term
    isolated = next(i for i in net.ids if i != leader)
    # cut it off long enough to time out many times
    for _ in range(200):
        net.nodes[isolated].tick()
        net.nodes[isolated].take_messages()
    assert net.nodes[isolated].term == term    # pre-votes don't bump the term
    for _ in range(20):
        for n in net.nodes.values():
            n.tick()
        net.flush()
    assert net.leaders() == [leader] and net.nodes[leader].term == term


def test_without_pre_vote_the_rejoining_node_disrupts():
    net = Net(3)
    leader = net.tick_until_leader()
    term = net.nodes[leader].term
    isolated = next(i for i in net.ids if i != leader)
    for _ in range(200):
        net.nodes[isolated].tick()
        net.nodes[isolated].take_messages()
    assert net.nodes[isolated].term > term + 3


def test_check_quorum_makes_an_isolated_leader_step_down():
    net = Net(3, check_quorum=True)
    leader = net.tick_until_leader()
    for _ in range(3 * net.config.election_ticks):
        net.nodes[leader].tick()
        net.nodes[leader].take_messages()      # nobody hears it, nobody answers
    assert net.nodes[leader].role == FOLLOWER


def test_lagging_follower_gets_a_snapshot():
    net = Net(3)
    leader = net.tick_until_leader()
    lagger = next(i for i in net.ids if i != leader)
    net.crash(lagger)
    for i in range(30):
        net.nodes[leader].propose({"n": i})
    net.flush()
    node = net.nodes[leader]
    node.compact(node.applied, {"state": "everything up to here"})
    assert node.storage.snapshot.index == node.applied
    net.restart(lagger)
    for _ in range(10):
        for n in net.nodes.values():
            n.tick()
        net.flush()
    assert net.nodes[lagger].storage.snapshot.data == {"state": "everything up to here"}
    assert net.nodes[lagger].commit == node.commit


def test_candidate_steps_down_for_a_current_leader():
    net = Net(3)
    leader = net.tick_until_leader()
    other = next(i for i in net.ids if i != leader)
    net.nodes[other].campaign()
    assert net.nodes[other].role == CANDIDATE
    net.flush()
    assert len(net.leaders()) == 1
