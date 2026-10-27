"""Deterministic simulation of a Raft cluster under a hostile network.

    python -m quorum.sim --seeds 1000

Every source of nondeterminism (election timeouts, message delays, drops,
duplicates, partitions, crashes, client behaviour) comes from one seeded
random generator. A failing seed reproduces exactly, forever.

During the run the simulator checks Raft's safety properties after every
step:

  election safety       at most one leader per term
  log matching          same index and term means the same entry and the same history
  leader completeness   a new leader has every entry that was committed before it
  state machine safety  every node applies the same command at the same index

and at the end:

  linearizability       the clients' history checks out (linearizability.py)
  liveness              once the network heals, every request completes and
                        every node converges to the same state
"""

import argparse
import heapq
import json
import random
import sys
from dataclasses import dataclass, field

from .kv import KVStore
from .linearizability import Op, check
from .raft import LEADER, AppendEntries, AppendResponse, Config, Raft, VoteResponse
from .storage import MemoryStorage


class Violation(AssertionError):
    pass


@dataclass
class Faults:
    drop: float = 0.05            # chance a message is lost
    duplicate: float = 0.02       # chance it's delivered twice
    max_delay: int = 6            # ticks; delays vary, so messages get reordered
    crash: float = 0.004          # per tick: a running node dies (keeps its disk)
    restart: float = 0.03         # per tick: a dead node comes back
    partition: float = 0.006      # per tick: the network splits in two
    heal: float = 0.02            # per tick: the split goes away
    # "buggify": crash a node at a moment that matters (just after it voted,
    # just after it appended as leader, just after it acked entries) and bring
    # it back a few ticks later. random crashes almost never land in those
    # windows, and that's where the interesting bugs live.
    buggify: float = 0.0


@dataclass
class Client:
    name: str
    op: Op = None
    command: dict = None
    seq: int = 0
    target: int = None
    sent_at: int = 0
    done_ops: int = 0


@dataclass
class Stats:
    elections: int = 0
    crashes: int = 0
    partitions: int = 0
    messages: int = 0
    dropped: int = 0
    completed: int = 0
    retries: int = 0
    snapshots_installed: int = 0
    max_term: int = 0


def swarm(rng):
    """A random cluster configuration and fault profile for one run.

    "Swarm testing" (Groce et al., 2012): rather than one fixed setup, each
    seed gets its own mix of features and fault rates. Some bugs only show up
    with PreVote off, or with a 3-node cluster, or under a storm of crashes;
    a single configuration would keep missing them.
    """
    config = Config(
        election_ticks=rng.choice([5, 10, 20]),
        heartbeat_ticks=rng.randint(1, 3),
        pre_vote=rng.random() < 0.5,
        check_quorum=rng.random() < 0.5,
    )
    faults = Faults(
        drop=rng.choice([0.0, 0.02, 0.1, 0.25]),
        duplicate=rng.choice([0.0, 0.02, 0.1]),
        # Raft needs messages to be much faster than the election timeout; past
        # that no leader survives long enough to do anything, so it's not tested
        max_delay=rng.randint(1, config.election_ticks // 2),
        crash=rng.choice([0.0, 0.002, 0.01, 0.03]),
        restart=rng.choice([0.02, 0.1, 0.5]),
        partition=rng.choice([0.0, 0.005, 0.02]),
        heal=rng.choice([0.01, 0.05, 0.2]),
        buggify=rng.choice([0.0, 0.0, 0.05, 0.2]),
    )
    return config, faults, rng.choice([3, 5, 5, 7]), rng.choice([10, 40, 200])


class Simulation:
    def __init__(self, seed, nodes=None, clients=3, keys=("a", "b", "c"), faults=None,
                 config=None, snapshot_every=None):
        self.seed = seed
        self.rng = random.Random(seed)
        s_config, s_faults, s_nodes, s_snap = swarm(random.Random(seed * 7919 + 1))
        self.ids = list(range(1, (nodes or s_nodes) + 1))
        self.faults = faults or s_faults
        self.config = config or s_config
        self.snapshot_every = snapshot_every or s_snap
        self.keys = keys

        self.storage = {i: MemoryStorage() for i in self.ids}
        self.nodes = {}
        self.kv = {}
        self.pending = {}            # node -> {index: (term, client)}
        self.last_commit = {}
        for i in self.ids:
            self._boot(i)

        self.time = 0
        self.queue = []              # (deliver_at, seq, message)
        self._seq = 0
        self.groups = None           # node -> side of the partition, or None
        self.clients = [Client(f"c{i}") for i in range(clients)]
        self.history = []
        self.stats = Stats()
        self.chaos = True
        self.retry_after = 3 * self.config.election_ticks
        self.restart_at = {}

        # global record, for the safety checks
        self.leader_of_term = {}
        self.applied = {}            # index -> (term, command), first application seen
        self.state_at = {}           # index -> kv data after applying it

    # --- nodes ------------------------------------------------------------------------

    def _boot(self, i):
        self.nodes[i] = Raft(i, self.ids, self.storage[i], random.Random(self.rng.random()), self.config)
        self.kv[i] = KVStore()
        self.pending[i] = {}
        self.last_commit[i] = self.nodes[i].commit

    def up(self, i):
        return i in self.nodes

    def crash(self, i):
        # the process dies: memory and in-flight work are gone, the disk stays
        self.storage[i].crash()
        del self.nodes[i]
        del self.kv[i]
        del self.pending[i]
        self.stats.crashes += 1

    # --- network ----------------------------------------------------------------------

    def _connected(self, a, b):
        return self.groups is None or self.groups[a] == self.groups[b]

    def _send(self, msg):
        self.stats.messages += 1
        if self.chaos and self.rng.random() < self.faults.drop:
            self.stats.dropped += 1
            return
        copies = 2 if self.chaos and self.rng.random() < self.faults.duplicate else 1
        for _ in range(copies):
            delay = self.rng.randint(1, self.faults.max_delay if self.chaos else 2)
            self._seq += 1
            heapq.heappush(self.queue, (self.time + delay, self._seq, msg))

    def _deliver_due(self):
        due = []
        while self.queue and self.queue[0][0] <= self.time:
            due.append(heapq.heappop(self.queue)[2])
        for msg in due:
            if self.up(msg.dst) and self._connected(msg.src, msg.dst):
                self.nodes[msg.dst].step(msg)
                self._drain(msg.dst)

    # --- driving a node ---------------------------------------------------------------

    def _drain(self, i):
        node = self.nodes[i]
        risky = False
        for msg in node.take_messages():
            self._send(msg)
            if isinstance(msg, VoteResponse) and msg.granted and not msg.pre_vote:
                risky = True
            elif isinstance(msg, AppendEntries) and msg.entries:
                risky = True
            elif isinstance(msg, AppendResponse) and msg.success:
                risky = True
        for c in node.take_committed():
            if c.snapshot is not None:
                self._install(i, c)
            else:
                self._apply(i, c)
        self._check_node(i)
        if risky and self.chaos and self.rng.random() < self.faults.buggify:
            alive = sum(self.up(x) for x in self.ids)
            if alive > len(self.ids) // 2:
                self.crash(i)
                self.restart_at[i] = self.time + self.rng.randint(1, 5)

    def _install(self, i, c):
        expected = self.state_at.get(c.index)
        if expected is not None and expected != c.snapshot.data["data"]:
            raise Violation(f"node {i} installed a snapshot at {c.index} that differs from the applied state")
        self.kv[i].restore(c.index, c.snapshot.data)
        self.stats.snapshots_installed += 1
        # anything this node proposed and the snapshot covers: its fate is unknown here
        for index in [x for x in self.pending[i] if x <= c.index]:
            del self.pending[i][index]

    def _apply(self, i, c):
        seen = self.applied.get(c.index)
        if seen is None:
            self.applied[c.index] = (c.term, c.command)
        elif seen != (c.term, c.command):
            raise Violation(f"state machine safety: node {i} applied {(c.term, c.command)} at {c.index}, "
                            f"another node applied {seen}")
        kv = self.kv[i]
        if kv.applied != c.index - 1:
            raise Violation(f"node {i} applied {c.index} right after {kv.applied}")
        result = kv.apply(c.index, c.command)
        self.state_at.setdefault(c.index, json.loads(json.dumps(kv.data)))

        waiting = self.pending[i].pop(c.index, None)
        if waiting is not None:
            term, client = waiting
            if term == c.term:
                self._respond(client, c.command, result)

        node = self.nodes[i]
        if c.index - node.storage.snapshot.index >= self.snapshot_every:
            node.compact(c.index, kv.snapshot())

    def _check_node(self, i):
        node = self.nodes[i]
        if node.commit < self.last_commit[i]:
            raise Violation(f"node {i}'s commit index went backwards")
        self.last_commit[i] = node.commit
        self.stats.max_term = max(self.stats.max_term, node.term)
        if node.role != LEADER:
            return
        holder = self.leader_of_term.get(node.term)
        if holder is None:
            self.leader_of_term[node.term] = i
            self.stats.elections += 1
            self._check_leader_completeness(i)
        elif holder != i:
            raise Violation(f"election safety: nodes {holder} and {i} both lead term {node.term}")

    def _check_leader_completeness(self, i):
        node = self.nodes[i]
        for index, (term, _) in self.applied.items():
            if index <= node.storage.snapshot.index:
                continue
            if node.term_at(index) != term:
                raise Violation(f"leader completeness: new leader {i} (term {node.term}) is missing "
                                f"committed entry {index} from term {term}")

    def _check_log_matching(self):
        live = [i for i in self.ids if self.up(i)]
        for a_pos, a in enumerate(live):
            for b in live[a_pos + 1:]:
                na, nb = self.nodes[a], self.nodes[b]
                low = max(na.storage.snapshot.index, nb.storage.snapshot.index) + 1
                high = min(na.last_index, nb.last_index)
                # the highest index where the terms agree...
                agree = None
                for idx in range(high, low - 1, -1):
                    if na.term_at(idx) == nb.term_at(idx):
                        agree = idx
                        break
                if agree is None:
                    continue
                # ...must have identical logs up to it
                for idx in range(low, agree + 1):
                    if na.entry(idx) != nb.entry(idx):
                        raise Violation(f"log matching: nodes {a} and {b} agree on the term at {agree} "
                                        f"but differ at {idx}")

    # --- clients ----------------------------------------------------------------------

    def _new_op(self, cl):
        cl.seq += 1
        key = self.rng.choice(self.keys)
        kind = self.rng.choices(["get", "put", "cas", "delete"], [4, 4, 2, 1])[0]
        value = f"{cl.name}-{cl.seq}" if kind in ("put", "cas") else None
        expected = None
        if kind == "cas":
            expected = self.rng.choice([None] + [f"c{k}-{s}" for k in range(len(self.clients))
                                                 for s in range(max(1, cl.seq - 4), cl.seq)])
        cl.op = Op(cl.name, kind, key, value, expected, invoke=self.time)
        cl.command = {"op": kind, "key": key, "value": value, "expected": expected,
                      "client": cl.name, "seq": cl.seq}
        self.history.append(cl.op)
        self._submit(cl)

    def _submit(self, cl):
        live = [i for i in self.ids if self.up(i)]
        if not live:
            cl.target, cl.sent_at = None, self.time
            return
        # try the last node that worked, else anyone (a real client follows redirects)
        target = cl.target if cl.target in live and self.rng.random() < 0.8 else self.rng.choice(live)
        cl.target, cl.sent_at = target, self.time
        node = self.nodes[target]
        proposed = node.propose(cl.command)
        if proposed is not None:
            index, term = proposed
            self.pending[target][index] = (term, cl)
            self._drain(target)
        else:
            # "not the leader, try N": follow the hint after a short pause
            cl.target = node.leader
            cl.sent_at = self.time - self.retry_after + 2

    def _respond(self, cl, command, result):
        if cl.op is None or command["seq"] != cl.seq or command["client"] != cl.name:
            return       # an answer to an older attempt that already completed
        cl.op.complete = self.time
        cl.op.result = result
        cl.op = None
        cl.done_ops += 1
        self.stats.completed += 1

    def _drive_clients(self):
        for cl in self.clients:
            if cl.op is None:
                # once the network has healed, clients only finish what they started;
                # otherwise "everyone is done" would depend on luck at the last tick
                if self.chaos and self.rng.random() < 0.3:
                    self._new_op(cl)
            elif self.time - cl.sent_at > self.retry_after:
                self.stats.retries += 1
                self._submit(cl)      # same seq: a duplicate gets the cached result

    # --- faults -----------------------------------------------------------------------

    def _inject_faults(self):
        f, rng = self.faults, self.rng
        for i in self.ids:
            if self.up(i):
                # keep a majority alive most of the time, or nothing interesting happens
                alive = sum(self.up(x) for x in self.ids)
                if rng.random() < f.crash and alive > len(self.ids) // 2:
                    self.crash(i)
            elif self.restart_at.get(i, float("inf")) <= self.time or rng.random() < f.restart:
                self.restart_at.pop(i, None)
                self._boot(i)
                self._drain(i)
        if self.groups is None and rng.random() < f.partition:
            side = {i: rng.random() < 0.5 for i in self.ids}
            self.groups = side
            self.stats.partitions += 1
        elif self.groups is not None and rng.random() < f.heal:
            self.groups = None

    # --- main loop --------------------------------------------------------------------

    def step(self):
        self.time += 1
        if self.chaos:
            self._inject_faults()
        self._deliver_due()
        for i in self.ids:
            if self.up(i):
                self.nodes[i].tick()
                self._drain(i)
        self._drive_clients()
        if self.time % 25 == 0:
            self._check_log_matching()

    def run(self, chaos_ticks=2000, settle_ticks=800):
        for _ in range(chaos_ticks):
            self.step()

        # heal everything and check that the cluster recovers
        self.chaos = False
        self.groups = None
        for i in self.ids:
            if not self.up(i):
                self._boot(i)
                self._drain(i)
        for _ in range(settle_ticks):
            self.step()
            if all(cl.op is None for cl in self.clients) and self._converged():
                break
        self._check_log_matching()

        stuck = [cl.name for cl in self.clients if cl.op is not None]
        if stuck:
            raise Violation(f"liveness: clients {stuck} still waiting {settle_ticks} ticks after the network healed")
        if not self._converged():
            raise Violation("liveness: nodes didn't converge to the same state")
        bad = check(self.history)
        if bad:
            raise Violation(f"linearizability: the history for keys {bad} can't be linearized")
        return self.stats

    def _converged(self):
        states = [(self.kv[i].applied, json.dumps(self.kv[i].data, sort_keys=True)) for i in self.ids if self.up(i)]
        leader = [i for i in self.ids if self.up(i) and self.nodes[i].role == LEADER]
        return len(leader) == 1 and len(set(states)) == 1 and \
            states[0][0] == self.nodes[leader[0]].last_index


def main(argv=None):
    p = argparse.ArgumentParser(description="run the deterministic Raft simulation")
    p.add_argument("--seeds", type=int, default=200)
    p.add_argument("--start", type=int, default=0)
    p.add_argument("--ticks", type=int, default=2000)
    p.add_argument("--nodes", type=int, default=None, help="default: chosen per seed")
    args = p.parse_args(argv)

    totals = Stats()
    for seed in range(args.start, args.start + args.seeds):
        try:
            stats = Simulation(seed, nodes=args.nodes).run(chaos_ticks=args.ticks)
        except Violation as e:
            print(f"seed {seed}: FAILED: {e}")
            print(f"reproduce with: python -m quorum.sim --start {seed} --seeds 1")
            return 1
        for k in vars(totals):
            setattr(totals, k, max(getattr(totals, k), getattr(stats, k)) if k == "max_term"
                    else getattr(totals, k) + getattr(stats, k))
    print(f"{args.seeds} seeds passed")
    for k, v in vars(totals).items():
        print(f"  {k:20} {v}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
