"""The Raft consensus algorithm, as a pure state machine.

There is no networking, no clock and no threads in here. A node is driven
from outside:

    node.tick()            one unit of logical time passes
    node.step(message)     a message arrived
    node.propose(cmd)      a client wants a command replicated (leader only)
    node.compact(i, data)  the application took a snapshot up to index i

and after each call the driver collects what the node wants to happen:

    node.take_messages()   messages to send
    node.take_committed()  entries (or a snapshot) to apply, in order

Keeping IO out is what makes the deterministic simulator possible: the same
code runs over real sockets in node.py and over a fake, hostile network in
sim.py, where every run is reproducible from its seed.

Follows the Raft paper (Ongaro & Ousterhout, 2014), plus two extensions
etcd uses in production:

  PreVote      a node that was cut off asks "would you vote for me?" without
               bumping its term first, so rejoining can't depose a healthy leader
  CheckQuorum  a leader that can't hear from a majority steps down, and a
               follower that heard from a leader recently ignores vote requests
"""

from dataclasses import dataclass, field

from .storage import Entry, Snapshot

FOLLOWER, PRE_CANDIDATE, CANDIDATE, LEADER = "follower", "pre-candidate", "candidate", "leader"

MAX_BATCH = 64   # entries per AppendEntries message


# --- messages -------------------------------------------------------------------


@dataclass
class Message:
    src: int
    dst: int
    term: int


@dataclass
class RequestVote(Message):
    last_index: int
    last_term: int
    pre_vote: bool = False


@dataclass
class VoteResponse(Message):
    granted: bool
    pre_vote: bool = False


@dataclass
class AppendEntries(Message):
    prev_index: int
    prev_term: int
    entries: list
    commit: int


@dataclass
class AppendResponse(Message):
    success: bool
    match_index: int = 0
    # on failure, where the leader should back up to (the fast backup from §5.3)
    conflict_index: int = 0
    conflict_term: int = 0


@dataclass
class InstallSnapshot(Message):
    snapshot: Snapshot


@dataclass
class SnapshotResponse(Message):
    index: int


@dataclass
class Committed:
    """Delivered by take_committed(): either one log entry or a whole snapshot."""
    index: int
    term: int
    command: object = None
    snapshot: Snapshot = None


@dataclass
class Progress:
    """What a leader knows about one follower."""
    next_index: int
    match_index: int = 0
    active: bool = False   # heard from it during the current check-quorum window


@dataclass
class Config:
    election_ticks: int = 10      # timeout is random in [election_ticks, 2 * election_ticks)
    heartbeat_ticks: int = 2
    pre_vote: bool = True
    check_quorum: bool = True


class Raft:
    def __init__(self, node_id, peers, storage, rng, config=None):
        self.id = node_id
        self.peers = [p for p in peers if p != node_id]
        self.storage = storage
        self.rng = rng
        self.cfg = config or Config()

        # durable state lives in storage; these are the volatile parts
        self.role = FOLLOWER
        self.leader = None
        self.commit = storage.snapshot.index
        self.applied = storage.snapshot.index
        self.votes = set()
        self.progress = {}

        self.elapsed = 0             # ticks since the last reset of the election timer
        self.since_leader = 0        # ticks since we last heard from a leader
        self.heartbeat_elapsed = 0
        self.timeout = self._random_timeout()

        self.outbox = []
        self.ready = []
        # a node restarting from a snapshot hands it to the application first
        if storage.snapshot.index:
            self.ready.append(Committed(storage.snapshot.index, storage.snapshot.term, snapshot=storage.snapshot))

    # --- log helpers --------------------------------------------------------------

    @property
    def term(self):
        return self.storage.term

    @property
    def voted_for(self):
        return self.storage.voted_for

    @property
    def last_index(self):
        return self.storage.snapshot.index + len(self.storage.entries)

    @property
    def last_term(self):
        return self.term_at(self.last_index)

    def term_at(self, index):
        """Term of the entry at index; None if it's gone (compacted) or not there yet."""
        snap = self.storage.snapshot
        if index == snap.index:
            return snap.term
        if index < snap.index or index > self.last_index:
            return None
        return self.storage.entries[index - snap.index - 1].term

    def entry(self, index):
        return self.storage.entries[index - self.storage.snapshot.index - 1]

    def _entries_from(self, index, limit=MAX_BATCH):
        start = index - self.storage.snapshot.index - 1
        return self.storage.entries[start:start + limit]

    @property
    def quorum(self):
        return (len(self.peers) + 1) // 2 + 1

    def _random_timeout(self):
        return self.rng.randrange(self.cfg.election_ticks, 2 * self.cfg.election_ticks)

    def _send(self, msg):
        self.outbox.append(msg)

    def take_messages(self):
        out, self.outbox = self.outbox, []
        return out

    def take_committed(self):
        out, self.ready = self.ready, []
        return out

    # --- role changes -------------------------------------------------------------

    def _set_term(self, term, voted_for=None):
        if term != self.term or voted_for != self.voted_for:
            self.storage.save_hard_state(term, voted_for)

    def _become_follower(self, term, leader):
        if term > self.term:
            self._set_term(term, None)
        self.role = FOLLOWER
        self.leader = leader
        self.votes = set()
        self.progress = {}
        self.elapsed = 0
        self.timeout = self._random_timeout()

    def _start_election(self, pre_vote):
        self.elapsed = 0
        self.timeout = self._random_timeout()
        self.leader = None
        self.votes = {self.id}
        if pre_vote:
            self.role = PRE_CANDIDATE
            term = self.term + 1         # the term we *would* use; not stored
        else:
            self.role = CANDIDATE
            self._set_term(self.term + 1, self.id)
            term = self.term
        if len(self.votes) >= self.quorum:  # a single-node cluster
            self._won(pre_vote)
            return
        for p in self.peers:
            self._send(RequestVote(self.id, p, term, self.last_index, self.last_term, pre_vote))

    def _won(self, pre_vote):
        if pre_vote:
            self._start_election(pre_vote=False)
        else:
            self._become_leader()

    def _become_leader(self):
        self.role = LEADER
        self.leader = self.id
        self.heartbeat_elapsed = 0
        self.elapsed = 0
        self.progress = {p: Progress(self.last_index + 1) for p in self.peers}
        # a no-op in the new term: entries from earlier terms can only be
        # committed by committing something from the current term (§5.4.2)
        self._append_local([Entry(self.term, None)])
        self._broadcast_append()

    # --- time ---------------------------------------------------------------------

    def tick(self):
        self.since_leader += 1
        if self.role == LEADER:
            self.heartbeat_elapsed += 1
            self.elapsed += 1
            if self.cfg.check_quorum and self.elapsed >= self.cfg.election_ticks:
                self.elapsed = 0
                active = 1 + sum(p.active for p in self.progress.values())
                for p in self.progress.values():
                    p.active = False
                if active < self.quorum:
                    # can't reach a majority: someone else may be leading by now
                    self._become_follower(self.term, None)
                    return
            if self.heartbeat_elapsed >= self.cfg.heartbeat_ticks:
                self.heartbeat_elapsed = 0
                self._broadcast_append()
            return
        self.elapsed += 1
        if self.elapsed >= self.timeout:
            self._start_election(pre_vote=self.cfg.pre_vote)

    def campaign(self):
        """Start an election right now (tests, and leadership transfer)."""
        self._start_election(pre_vote=False)

    # --- client commands ----------------------------------------------------------

    def propose(self, command):
        """Append a command if we're the leader. Returns (index, term) or None."""
        if self.role != LEADER:
            return None
        self._append_local([Entry(self.term, command)])
        self._broadcast_append()
        return self.last_index, self.term

    def _append_local(self, entries):
        self.storage.append(self.last_index + 1, entries)
        if not self.peers:
            self._advance_commit()

    # --- replication (leader side) ------------------------------------------------

    def _broadcast_append(self):
        for p in self.peers:
            self._send_append(p)

    def _send_append(self, peer):
        pr = self.progress[peer]
        prev = pr.next_index - 1
        prev_term = self.term_at(prev)
        if prev_term is None:
            # the entries this follower needs are compacted away: send the snapshot
            self._send(InstallSnapshot(self.id, peer, self.term, self.storage.snapshot))
            return
        entries = self._entries_from(pr.next_index)
        self._send(AppendEntries(self.id, peer, self.term, prev, prev_term, list(entries), self.commit))

    def _advance_commit(self):
        # the highest index stored on a majority...
        matches = sorted([self.last_index] + [p.match_index for p in self.progress.values()], reverse=True)
        n = matches[self.quorum - 1]
        # ...but only entries from this term commit by counting (figure 8 in the paper)
        if n > self.commit and self.term_at(n) == self.term:
            self._commit_to(n)

    def _commit_to(self, index):
        if index <= self.commit:
            return
        self.commit = index
        while self.applied < self.commit:
            self.applied += 1
            e = self.entry(self.applied)
            self.ready.append(Committed(self.applied, e.term, e.command))

    # --- messages -----------------------------------------------------------------

    def step(self, m):
        # a vote request while a leader is known to be alive is ignored, so a
        # node that was partitioned away can't disrupt the cluster (§9.6)
        if isinstance(m, RequestVote) and m.term > self.term and self._in_lease():
            return

        if m.term > self.term:
            if isinstance(m, RequestVote) and m.pre_vote:
                pass                        # a pre-vote never changes our term
            elif isinstance(m, VoteResponse) and m.pre_vote and m.granted:
                pass                        # granted pre-votes carry the future term
            else:
                leader = m.src if isinstance(m, (AppendEntries, InstallSnapshot)) else None
                self._become_follower(m.term, leader)
        elif m.term < self.term:
            # stale: tell the sender about the newer term so it steps down
            if isinstance(m, (AppendEntries, InstallSnapshot)):
                self._send(AppendResponse(self.id, m.src, self.term, False))
            elif isinstance(m, RequestVote):
                self._send(VoteResponse(self.id, m.src, self.term, False, m.pre_vote))
            return

        handler = {
            RequestVote: self._on_request_vote,
            VoteResponse: self._on_vote_response,
            AppendEntries: self._on_append,
            AppendResponse: self._on_append_response,
            InstallSnapshot: self._on_snapshot,
            SnapshotResponse: self._on_snapshot_response,
        }[type(m)]
        handler(m)

    def _in_lease(self):
        # a leader keeps its lease as long as check-quorum keeps it leader; a
        # follower trusts its leader for one election timeout after hearing from it
        if not self.cfg.check_quorum:
            return False
        if self.role == LEADER:
            return True
        return self.leader is not None and self.since_leader < self.cfg.election_ticks

    def _log_ok(self, last_index, last_term):
        # §5.4.1: the candidate's log is at least as up to date as ours
        return (last_term, last_index) >= (self.last_term, self.last_index)

    def _on_request_vote(self, m):
        if m.pre_vote:
            granted = m.term > self.term and self._log_ok(m.last_index, m.last_term)
            # a grant carries the candidate's future term; a refusal carries ours,
            # so the candidate only learns of a newer term if there really is one
            self._send(VoteResponse(self.id, m.src, m.term if granted else self.term, granted, True))
            return
        can_vote = self.voted_for in (None, m.src)
        granted = can_vote and self._log_ok(m.last_index, m.last_term)
        if granted:
            self._set_term(self.term, m.src)    # durable before we answer
            self.elapsed = 0
        self._send(VoteResponse(self.id, m.src, self.term, granted))

    def _on_vote_response(self, m):
        if m.pre_vote:
            if self.role != PRE_CANDIDATE or m.term != self.term + 1:
                return
        elif self.role != CANDIDATE or m.term != self.term:
            return
        if m.granted:
            self.votes.add(m.src)
            if len(self.votes) >= self.quorum:
                self._won(m.pre_vote)

    def _on_append(self, m):
        # a valid leader for our term: follow it
        if self.role != FOLLOWER:
            self._become_follower(m.term, m.src)
        self.leader = m.src
        self.elapsed = 0
        self.since_leader = 0

        snap = self.storage.snapshot
        prev, entries = m.prev_index, m.entries
        if prev < snap.index:
            # everything up to the snapshot is committed and identical: skip it
            skip = snap.index - prev
            if skip > len(entries):
                self._send(AppendResponse(self.id, m.src, self.term, True, snap.index))
                return
            prev, entries = snap.index, entries[skip:]
        elif prev > self.last_index:
            self._send(AppendResponse(self.id, m.src, self.term, False, conflict_index=self.last_index + 1))
            return
        elif self.term_at(prev) != m.prev_term:
            conflict_term = self.term_at(prev)
            first = prev
            while first - 1 > snap.index and self.term_at(first - 1) == conflict_term:
                first -= 1
            self._send(AppendResponse(self.id, m.src, self.term, False,
                                      conflict_index=first, conflict_term=conflict_term))
            return

        # find the first entry we don't already have, truncate from there
        for i, e in enumerate(entries):
            index = prev + 1 + i
            existing = self.term_at(index)
            if existing is None or existing != e.term:
                if index <= self.commit:
                    raise AssertionError(f"node {self.id}: leader tried to overwrite committed entry {index}")
                self.storage.append(index, entries[i:])
                break
        match = prev + len(entries)
        self._commit_to(min(m.commit, match))
        self._send(AppendResponse(self.id, m.src, self.term, True, match))

    def _on_append_response(self, m):
        if self.role != LEADER:
            return
        pr = self.progress[m.src]
        pr.active = True
        # flow control: only a response that tells us something new triggers
        # an immediate send. otherwise every heartbeat starts another chain of
        # append -> response -> append that never dies out while a follower is
        # behind, and duplicated messages multiply those chains. the simulator
        # found this: one run went to 2,000 messages per tick.
        if m.success:
            if m.match_index <= pr.match_index:
                return                      # a duplicate or a stale ack
            pr.match_index = m.match_index
            pr.next_index = max(pr.next_index, pr.match_index + 1)
            self._advance_commit()
            if pr.next_index <= self.last_index:
                self._send_append(m.src)    # more to send
            return
        # back up next_index using the follower's hint (§5.3, the optimisation
        # at the end): skip a whole conflicting term per round trip
        nxt = m.conflict_index
        if m.conflict_term:
            last_of_term = None
            i = self.last_index
            while i > self.storage.snapshot.index:
                t = self.term_at(i)
                if t == m.conflict_term:
                    last_of_term = i
                    break
                if t < m.conflict_term:
                    break
                i -= 1
            if last_of_term is not None:
                nxt = last_of_term + 1
        nxt = max(min(nxt, self.last_index + 1), pr.match_index + 1, 1)
        if nxt >= pr.next_index:
            return                          # nothing learned; the next heartbeat retries
        pr.next_index = nxt
        self._send_append(m.src)

    def _on_snapshot(self, m):
        if self.role != FOLLOWER:
            self._become_follower(m.term, m.src)
        self.leader = m.src
        self.elapsed = 0
        self.since_leader = 0
        snap = m.snapshot
        if snap.index <= self.commit:
            # we already have all of it
            self._send(SnapshotResponse(self.id, m.src, self.term, self.commit))
            return
        self.storage.save_snapshot(snap)
        self.commit = self.applied = snap.index
        self.ready.append(Committed(snap.index, snap.term, snapshot=snap))
        self._send(SnapshotResponse(self.id, m.src, self.term, snap.index))

    def _on_snapshot_response(self, m):
        if self.role != LEADER:
            return
        pr = self.progress[m.src]
        pr.active = True
        if m.index <= pr.match_index:
            return
        pr.match_index = m.index
        pr.next_index = pr.match_index + 1
        self._advance_commit()
        if pr.next_index <= self.last_index:
            self._send_append(m.src)

    # --- snapshots ----------------------------------------------------------------

    def compact(self, index, data):
        """The application has a snapshot of its state after applying `index`."""
        if index <= self.storage.snapshot.index:
            return
        if index > self.applied:
            raise ValueError("can't snapshot past the applied index")
        self.storage.save_snapshot(Snapshot(index, self.term_at(index), data))

    # --- for tests and the status endpoint ------------------------------------------

    def status(self):
        return {
            "id": self.id, "role": self.role, "term": self.term, "leader": self.leader,
            "commit": self.commit, "applied": self.applied, "last_index": self.last_index,
            "snapshot_index": self.storage.snapshot.index,
        }
