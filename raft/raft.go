// Package raft is a deterministic implementation of the Raft consensus protocol.
//
// A Node never touches the network, the disk, or the clock. It is a pure state
// machine driven by three inputs:
//
//	Tick()      one unit of logical time has passed
//	Step(msg)   a message arrived from a peer
//	Propose(c)  a client wants command c added to the log
//
// and it produces two outputs, collected with Ready(): messages to send, and
// newly committed entries to apply. Everything else (transport, persistence,
// timers) lives outside. That separation is what makes the deterministic
// simulator possible: the same code runs in tests, under a seeded fault
// injector, and in the real networked cluster.
package raft

import (
	"fmt"
	"math/rand"
)

type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

func (r Role) String() string {
	return [...]string{"follower", "candidate", "leader"}[r]
}

// Entry is one slot in the replicated log. Cmd is nil for the no-op a new
// leader appends to commit entries from earlier terms (Raft §5.4.2, §8).
type Entry struct {
	Term  uint64 `json:"term"`
	Index uint64 `json:"index"`
	Cmd   []byte `json:"cmd,omitempty"`
}

type MsgType int

const (
	MsgVote MsgType = iota
	MsgVoteResp
	MsgAppend
	MsgAppendResp
)

func (t MsgType) String() string {
	return [...]string{"vote", "vote-resp", "append", "append-resp"}[t]
}

type Message struct {
	Type MsgType `json:"type"`
	From int     `json:"from"`
	To   int     `json:"to"`
	Term uint64  `json:"term"`

	// RequestVote
	LastIndex uint64 `json:"lastIndex,omitempty"`
	LastTerm  uint64 `json:"lastTerm,omitempty"`
	Granted   bool   `json:"granted,omitempty"`

	// AppendEntries
	PrevIndex uint64  `json:"prevIndex,omitempty"`
	PrevTerm  uint64  `json:"prevTerm,omitempty"`
	Entries   []Entry `json:"entries,omitempty"`
	Commit    uint64  `json:"commit,omitempty"`
	Success   bool    `json:"success,omitempty"`
	// In a response: the follower's match index on success, or a hint for
	// where the leader should retry from on failure.
	Match uint64 `json:"match,omitempty"`
}

// Storage is the state Raft requires to survive a crash. The caller must make
// it durable before sending any message produced in the same Ready batch.
type Storage struct {
	Term     uint64  `json:"term"`
	VotedFor int     `json:"votedFor"`
	Log      []Entry `json:"log"` // Log[0] is a sentinel {Term: 0, Index: 0}
}

func NewStorage() *Storage {
	return &Storage{VotedFor: -1, Log: []Entry{{}}}
}

func (s *Storage) Clone() *Storage {
	c := *s
	c.Log = append([]Entry(nil), s.Log...)
	return &c
}

func (s *Storage) lastIndex() uint64 { return uint64(len(s.Log) - 1) }
func (s *Storage) lastTerm() uint64  { return s.Log[len(s.Log)-1].Term }

// Bugs are deliberate, well-known Raft mistakes that can be switched on to
// prove the simulator and checkers catch them. All are off in production.
type Bugs struct {
	// Count replicas for entries from earlier terms and skip the leader's no-op:
	// the "Figure 8" bug from the Raft paper. Committed entries can be lost.
	CommitOldTerm bool `json:"commitOldTerm,omitempty"`
	// Do not persist votedFor, so a restarted node can vote twice in one term
	// and two leaders can be elected for the same term.
	ForgetVote bool `json:"forgetVote,omitempty"`
	// Serve reads from the leader's local state without confirming it is still
	// leader. A deposed leader on the wrong side of a partition returns stale data.
	StaleReads bool `json:"staleReads,omitempty"`
}

type Config struct {
	ID             int
	Peers          []int // every member, including ID
	ElectionTicks  int   // timeout is randomised in [ElectionTicks, 2*ElectionTicks)
	HeartbeatTicks int
	MaxBatch       int // max entries per AppendEntries
	Rand           *rand.Rand
	Bugs           Bugs
}

type Status struct {
	ID        int    `json:"id"`
	Role      string `json:"role"`
	Term      uint64 `json:"term"`
	Leader    int    `json:"leader"`
	Commit    uint64 `json:"commit"`
	Applied   uint64 `json:"applied"`
	LastIndex uint64 `json:"lastIndex"`
}

type Node struct {
	cfg Config
	st  *Storage

	role    Role
	leader  int
	commit  uint64
	applied uint64

	electionElapsed  int
	electionTimeout  int
	heartbeatElapsed int

	votes     map[int]bool
	responded map[int]bool
	next      map[int]uint64
	match     map[int]uint64

	outbox []Message
	dirty  bool // storage changed since the last Ready

	// Violations records protocol invariants this node saw broken. Always
	// empty unless a planted bug is enabled.
	Violations []string
}

// NewNode starts a node from durable state. On a restart, pass the same
// Storage that was persisted before the crash.
func NewNode(cfg Config, st *Storage) *Node {
	if cfg.ElectionTicks == 0 {
		cfg.ElectionTicks = 10
	}
	if cfg.HeartbeatTicks == 0 {
		cfg.HeartbeatTicks = 2
	}
	if cfg.MaxBatch == 0 {
		cfg.MaxBatch = 64
	}
	if cfg.Rand == nil {
		cfg.Rand = rand.New(rand.NewSource(int64(cfg.ID)))
	}
	if st == nil {
		st = NewStorage()
	}
	if cfg.Bugs.ForgetVote {
		st.VotedFor = -1
	}
	n := &Node{cfg: cfg, st: st, leader: -1}
	n.becomeFollower(st.Term, -1)
	return n
}

func (n *Node) quorum() int { return len(n.cfg.Peers)/2 + 1 }

func (n *Node) Status() Status {
	return Status{n.cfg.ID, n.role.String(), n.st.Term, n.leader, n.commit, n.applied, n.st.lastIndex()}
}

func (n *Node) Role() Role        { return n.role }
func (n *Node) Term() uint64      { return n.st.Term }
func (n *Node) Leader() int       { return n.leader }
func (n *Node) Storage() *Storage { return n.st }
func (n *Node) Bugs() Bugs        { return n.cfg.Bugs }
func (n *Node) IsLeader() bool    { return n.role == Leader }
func (n *Node) EntryAt(i uint64) (Entry, bool) {
	if i == 0 || i > n.st.lastIndex() {
		return Entry{}, false
	}
	return n.st.Log[i], true
}

// Ready drains outgoing messages and newly committed entries. persist reports
// whether Storage changed and must be made durable before the messages go out.
func (n *Node) Ready() (msgs []Message, committed []Entry, persist bool) {
	msgs, n.outbox = n.outbox, nil
	if n.commit > n.applied {
		committed = append([]Entry(nil), n.st.Log[n.applied+1:n.commit+1]...)
		n.applied = n.commit
	}
	persist, n.dirty = n.dirty, false
	return
}

// Propose appends a command if this node is leader. The command is committed
// only if the entry at the returned index still has the returned term when it
// is applied; otherwise a newer leader overwrote it and the client must retry.
func (n *Node) Propose(cmd []byte) (index, term uint64, ok bool) {
	if n.role != Leader {
		return 0, 0, false
	}
	n.appendEntry(cmd)
	n.broadcastAppend()
	n.maybeCommit() // a single-node cluster commits immediately
	return n.st.lastIndex(), n.st.Term, true
}

func (n *Node) Tick() {
	switch n.role {
	case Leader:
		n.heartbeatElapsed++
		if n.heartbeatElapsed >= n.cfg.HeartbeatTicks {
			n.heartbeatElapsed = 0
			n.broadcastAppend()
		}
	default:
		n.electionElapsed++
		if n.electionElapsed >= n.electionTimeout {
			n.campaign()
			return
		}
		if n.role == Candidate {
			// Raft retries RPCs that go unanswered (§5.1). Without this a
			// single dropped vote request can stall an election.
			n.heartbeatElapsed++
			if n.heartbeatElapsed >= n.cfg.HeartbeatTicks {
				n.heartbeatElapsed = 0
				n.requestVotes()
			}
		}
	}
}

func (n *Node) Step(m Message) {
	switch {
	case m.Term > n.st.Term:
		leader := -1
		if m.Type == MsgAppend {
			leader = m.From
		}
		n.becomeFollower(m.Term, leader)
	case m.Term < n.st.Term:
		// Stale sender. Answer requests so it learns the newer term; drop replies.
		switch m.Type {
		case MsgVote:
			n.send(Message{Type: MsgVoteResp, To: m.From})
		case MsgAppend:
			n.send(Message{Type: MsgAppendResp, To: m.From})
		}
		return
	}

	switch m.Type {
	case MsgVote:
		n.handleVote(m)
	case MsgVoteResp:
		n.handleVoteResp(m)
	case MsgAppend:
		n.handleAppend(m)
	case MsgAppendResp:
		n.handleAppendResp(m)
	}
}

// ---- roles ---------------------------------------------------------------

func (n *Node) resetElectionTimer() {
	n.electionElapsed = 0
	n.electionTimeout = n.cfg.ElectionTicks + n.cfg.Rand.Intn(n.cfg.ElectionTicks)
}

func (n *Node) becomeFollower(term uint64, leader int) {
	if term != n.st.Term {
		n.st.Term = term
		n.st.VotedFor = -1
		n.dirty = true
	}
	n.role = Follower
	n.leader = leader
	n.resetElectionTimer()
}

func (n *Node) campaign() {
	n.role = Candidate
	n.leader = -1
	n.st.Term++
	n.st.VotedFor = n.cfg.ID
	n.dirty = true
	n.votes = map[int]bool{n.cfg.ID: true}
	n.responded = map[int]bool{n.cfg.ID: true}
	n.heartbeatElapsed = 0
	n.resetElectionTimer()
	if len(n.votes) >= n.quorum() {
		n.becomeLeader()
		return
	}
	n.requestVotes()
}

func (n *Node) requestVotes() {
	for _, p := range n.cfg.Peers {
		if !n.responded[p] {
			n.send(Message{Type: MsgVote, To: p, LastIndex: n.st.lastIndex(), LastTerm: n.st.lastTerm()})
		}
	}
}

func (n *Node) becomeLeader() {
	n.role = Leader
	n.leader = n.cfg.ID
	n.heartbeatElapsed = 0
	n.next = map[int]uint64{}
	n.match = map[int]uint64{}
	for _, p := range n.cfg.Peers {
		n.next[p] = n.st.lastIndex() + 1
		n.match[p] = 0
	}
	if !n.cfg.Bugs.CommitOldTerm {
		// A leader may only commit entries from its own term by counting
		// replicas. The no-op gives it one immediately, so older entries
		// commit as soon as the no-op does.
		n.appendEntry(nil)
	}
	n.match[n.cfg.ID] = n.st.lastIndex()
	n.broadcastAppend()
	n.maybeCommit()
}

// ---- RequestVote ----------------------------------------------------------

func (n *Node) handleVote(m Message) {
	upToDate := m.LastTerm > n.st.lastTerm() ||
		(m.LastTerm == n.st.lastTerm() && m.LastIndex >= n.st.lastIndex())
	canVote := n.st.VotedFor == -1 || n.st.VotedFor == m.From
	grant := canVote && upToDate && n.role == Follower
	if grant {
		n.st.VotedFor = m.From
		n.dirty = true
		n.resetElectionTimer()
	}
	n.send(Message{Type: MsgVoteResp, To: m.From, Granted: grant})
}

func (n *Node) handleVoteResp(m Message) {
	if n.role != Candidate {
		return
	}
	n.responded[m.From] = true
	if !m.Granted {
		return
	}
	n.votes[m.From] = true
	if len(n.votes) >= n.quorum() {
		n.becomeLeader()
	}
}

// ---- AppendEntries --------------------------------------------------------

func (n *Node) appendEntry(cmd []byte) {
	n.st.Log = append(n.st.Log, Entry{Term: n.st.Term, Index: n.st.lastIndex() + 1, Cmd: cmd})
	n.dirty = true
	if n.role == Leader {
		n.match[n.cfg.ID] = n.st.lastIndex()
	}
}

func (n *Node) broadcastAppend() {
	for _, p := range n.cfg.Peers {
		if p != n.cfg.ID {
			n.sendAppend(p)
		}
	}
}

func (n *Node) sendAppend(to int) {
	next := n.next[to]
	if next < 1 {
		next = 1
	}
	prev := next - 1
	end := n.st.lastIndex()
	if end-prev > uint64(n.cfg.MaxBatch) {
		end = prev + uint64(n.cfg.MaxBatch)
	}
	var entries []Entry
	if next <= end {
		entries = append(entries, n.st.Log[next:end+1]...)
	}
	n.send(Message{
		Type:      MsgAppend,
		To:        to,
		PrevIndex: prev,
		PrevTerm:  n.st.Log[prev].Term,
		Entries:   entries,
		Commit:    n.commit,
	})
}

func (n *Node) handleAppend(m Message) {
	if n.role != Follower {
		n.becomeFollower(n.st.Term, m.From)
	}
	n.leader = m.From
	n.resetElectionTimer()

	if m.PrevIndex > n.st.lastIndex() {
		n.send(Message{Type: MsgAppendResp, To: m.From, Match: n.st.lastIndex()})
		return
	}
	if n.st.Log[m.PrevIndex].Term != m.PrevTerm {
		// Skip back over the whole conflicting term in one round trip.
		conflict := n.st.Log[m.PrevIndex].Term
		hint := m.PrevIndex - 1
		for hint > n.commit && n.st.Log[hint].Term == conflict {
			hint--
		}
		n.send(Message{Type: MsgAppendResp, To: m.From, Match: hint})
		return
	}

	for _, e := range m.Entries {
		if e.Index <= n.st.lastIndex() {
			if n.st.Log[e.Index].Term == e.Term {
				continue // already have it
			}
			if e.Index <= n.commit {
				// Only reachable with a planted bug. Record it instead of
				// crashing so the checkers can show what the bug costs.
				n.Violations = append(n.Violations, fmt.Sprintf(
					"node %d: leader %d overwrote committed index %d (term %d with term %d)",
					n.cfg.ID, m.From, e.Index, n.st.Log[e.Index].Term, e.Term))
				n.commit = e.Index - 1
				n.applied = min(n.applied, n.commit)
			}
			n.st.Log = n.st.Log[:e.Index] // drop the conflicting suffix
		}
		n.st.Log = append(n.st.Log, e)
		n.dirty = true
	}

	last := m.PrevIndex + uint64(len(m.Entries))
	if m.Commit > n.commit {
		n.commit = min(m.Commit, last)
	}
	n.send(Message{Type: MsgAppendResp, To: m.From, Success: true, Match: last})
}

func (n *Node) handleAppendResp(m Message) {
	if n.role != Leader {
		return
	}
	if m.Success {
		if m.Match > n.match[m.From] {
			n.match[m.From] = m.Match
		}
		n.next[m.From] = n.match[m.From] + 1
		n.maybeCommit()
		if n.next[m.From] <= n.st.lastIndex() {
			n.sendAppend(m.From)
		}
		return
	}
	retry := m.Match + 1
	if retry >= n.next[m.From] {
		retry = n.next[m.From] - 1
	}
	if retry < 1 {
		retry = 1
	}
	n.next[m.From] = retry
	n.sendAppend(m.From)
}

func (n *Node) maybeCommit() {
	for idx := n.st.lastIndex(); idx > n.commit; idx-- {
		if n.st.Log[idx].Term != n.st.Term && !n.cfg.Bugs.CommitOldTerm {
			break // never commit an older-term entry by counting replicas
		}
		count := 0
		for _, p := range n.cfg.Peers {
			if n.match[p] >= idx {
				count++
			}
		}
		if count >= n.quorum() {
			n.commit = idx
			return
		}
	}
}

func (n *Node) send(m Message) {
	m.From = n.cfg.ID
	m.Term = n.st.Term
	n.outbox = append(n.outbox, m)
}
