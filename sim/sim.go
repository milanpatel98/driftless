// Package sim runs a whole Raft cluster inside one deterministic loop.
//
// Every source of nondeterminism (message delay, loss, duplication and
// reordering, partitions, crashes, election timeouts, client timing) comes from
// one seeded random number generator. The same seed always produces the same
// run, so any failure found by a sweep over thousands of seeds can be replayed
// exactly, stepped through, and turned into a regression test.
package sim

import (
	"fmt"
	"math/rand"
	"sort"

	"github.com/milanpatel98/driftless/check"
	"github.com/milanpatel98/driftless/kv"
	"github.com/milanpatel98/driftless/raft"
)

type Config struct {
	Seed     int64
	Nodes    int
	Clients  int
	Keys     int
	Ticks    int // fault phase
	Recovery int // calm phase at the end: everything healed, liveness checked

	DropRate      float64
	DupRate       float64
	MaxDelay      int // ticks
	FaultEvery    int // mean ticks between fault events
	ClientTimeout int

	Bugs  raft.Bugs
	Trace bool
}

func Defaults(seed int64) Config {
	return Config{
		Seed: seed, Nodes: 5, Clients: 4, Keys: 3,
		Ticks: 1200, Recovery: 400,
		DropRate: 0.05, DupRate: 0.02, MaxDelay: 4,
		FaultEvery: 40, ClientTimeout: 40,
	}
}

// ---- results ------------------------------------------------------------

type Violation struct {
	Tick   int    `json:"tick"`
	Kind   string `json:"kind"` // election-safety | state-machine-safety | linearizability | liveness | log-overwrite
	Detail string `json:"detail"`
}

type Result struct {
	Seed       int64             `json:"seed"`
	Bugs       raft.Bugs         `json:"bugs"`
	Violations []Violation       `json:"violations"`
	Ops        int               `json:"ops"`
	Completed  int               `json:"completed"`
	Elections  int               `json:"elections"`
	Faults     int               `json:"faults"`
	Messages   int               `json:"messages"`
	Dropped    int               `json:"dropped"`
	Ticks      int               `json:"ticks"`
	History    []check.Op        `json:"history,omitempty"`
	Linear     check.Result      `json:"linearizability"`
	Final      map[string]string `json:"final,omitempty"`
	Trace      *Trace            `json:"trace,omitempty"`
}

func (r Result) OK() bool { return len(r.Violations) == 0 }

// ---- internals ------------------------------------------------------------

type inflight struct {
	msg     raft.Message
	deliver int
	seq     int // tiebreak so equal delivery times stay deterministic
}

type replica struct {
	id      int
	node    *raft.Node
	store   *kv.Store
	disk    *raft.Storage // what survives a crash
	up      bool
	applied map[uint64]raft.Entry
}

type pending struct {
	op       *check.Op
	cmd      kv.Command
	node     int
	index    uint64
	term     uint64
	deadline int
	waitTill int // client is backing off until this tick
}

type client struct {
	id   int
	seq  int
	cur  *pending
	hint int // node the client believes is leader
	idle int // ticks to wait before the next request
}

type world struct {
	cfg   Config
	rng   *rand.Rand
	now   int
	clock int64 // global event counter for history timestamps
	reps  []*replica
	group []int // partition group per node; nodes talk only within a group
	net   []inflight
	seq   int

	clients []*client
	history []*check.Op
	opID    int

	leaders map[uint64]int    // term -> leader id, for election safety
	votes   map[[2]uint64]int // (voter, term) -> candidate, for vote safety
	commits map[uint64]raft.Entry

	res   Result
	trace *Trace
}

func Run(cfg Config) Result {
	w := &world{
		cfg:     cfg,
		rng:     rand.New(rand.NewSource(cfg.Seed)),
		group:   make([]int, cfg.Nodes+1),
		leaders: map[uint64]int{},
		votes:   map[[2]uint64]int{},
		commits: map[uint64]raft.Entry{},
		res:     Result{Seed: cfg.Seed, Bugs: cfg.Bugs},
	}
	if cfg.Trace {
		w.trace = newTrace(cfg)
	}
	for id := 1; id <= cfg.Nodes; id++ {
		r := &replica{id: id, disk: raft.NewStorage(), up: true, applied: map[uint64]raft.Entry{}}
		w.reps = append(w.reps, r)
		w.boot(r)
	}
	for c := 1; c <= cfg.Clients; c++ {
		w.clients = append(w.clients, &client{id: c, hint: 1 + w.rng.Intn(cfg.Nodes), idle: w.rng.Intn(10)})
	}

	nextFault := w.rng.Intn(cfg.FaultEvery*2) + 1
	for ; w.now < cfg.Ticks; w.now++ {
		if w.now >= nextFault {
			w.fault()
			nextFault = w.now + 1 + w.rng.Intn(cfg.FaultEvery*2)
		}
		w.tick(true)
	}

	// Recovery: heal everything, restart everyone, stop dropping messages.
	w.healAll()
	for _, r := range w.reps {
		if !r.up {
			w.restart(r)
		}
	}
	calm := w.cfg
	calm.DropRate, calm.DupRate = 0, 0
	w.cfg = calm
	end := w.now + cfg.Recovery
	for ; w.now < end; w.now++ {
		w.tick(false)
		if w.allClientsIdle() && w.now > end-cfg.Recovery/2 {
			break
		}
	}
	w.finish()
	return w.res
}

// ---- lifecycle ----------------------------------------------------------

func (w *world) boot(r *replica) {
	peers := make([]int, w.cfg.Nodes)
	for i := range peers {
		peers[i] = i + 1
	}
	r.node = raft.NewNode(raft.Config{
		ID: r.id, Peers: peers, ElectionTicks: 10, HeartbeatTicks: 3, MaxBatch: 4,
		Rand: rand.New(rand.NewSource(w.rng.Int63())), Bugs: w.cfg.Bugs,
	}, r.disk.Clone())
	r.store = kv.New()
	r.applied = map[uint64]raft.Entry{}
}

func (w *world) crash(r *replica) {
	r.up = false
	w.res.Faults++
	w.emit(Event{Kind: "crash", Node: r.id})
}

func (w *world) restart(r *replica) {
	r.up = true
	w.boot(r)
	w.emit(Event{Kind: "restart", Node: r.id})
}

func (w *world) healAll() {
	for i := range w.group {
		w.group[i] = 0
	}
	w.emit(Event{Kind: "heal"})
}

func (w *world) fault() {
	switch k := w.rng.Intn(10); {
	case k < 4: // partition into two random sides
		for i := 1; i <= w.cfg.Nodes; i++ {
			w.group[i] = w.rng.Intn(2)
		}
		w.res.Faults++
		w.emit(Event{Kind: "partition", Groups: append([]int(nil), w.group[1:]...)})
	case k < 6: // isolate the current leader, the classic split-brain setup
		for _, r := range w.reps {
			if r.up && r.node.IsLeader() {
				for i := range w.group {
					w.group[i] = 0
				}
				w.group[r.id] = 1
				w.res.Faults++
				w.emit(Event{Kind: "partition", Groups: append([]int(nil), w.group[1:]...)})
				return
			}
		}
	case k < 7:
		w.healAll()
	case k < 8: // bounce: kill -9 and an immediate supervisor restart
		r := w.reps[w.rng.Intn(len(w.reps))]
		if r.up {
			w.crash(r)
			w.restart(r)
		}
	default:
		var up, down []*replica
		for _, r := range w.reps {
			if r.up {
				up = append(up, r)
			} else {
				down = append(down, r)
			}
		}
		if len(down) > 0 && (w.rng.Intn(2) == 0 || len(up) <= w.cfg.Nodes/2) {
			w.restart(down[w.rng.Intn(len(down))])
		} else if len(up) > 0 {
			w.crash(up[w.rng.Intn(len(up))])
		}
	}
}

// ---- one tick ------------------------------------------------------------

func (w *world) tick(faulty bool) {
	// 1. deliver due messages in a deterministic order
	sort.SliceStable(w.net, func(i, j int) bool {
		if w.net[i].deliver != w.net[j].deliver {
			return w.net[i].deliver < w.net[j].deliver
		}
		return w.net[i].seq < w.net[j].seq
	})
	// Split first: replies sent while delivering are appended to w.net and
	// must not be lost when the due batch is processed.
	var due, later []inflight
	for _, f := range w.net {
		if f.deliver > w.now {
			later = append(later, f)
		} else {
			due = append(due, f)
		}
	}
	w.net = later
	for _, f := range due {
		dst := w.reps[f.msg.To-1]
		if !dst.up || w.group[f.msg.From] != w.group[f.msg.To] {
			w.res.Dropped++
			w.emit(Event{Kind: "drop", Msg: summarize(f.msg)})
			continue
		}
		dst.node.Step(f.msg)
		w.emit(Event{Kind: "recv", Msg: summarize(f.msg)})
		w.drain(dst)
	}

	// 2. advance every live node's clock
	for _, r := range w.reps {
		if r.up {
			r.node.Tick()
			w.drain(r)
		}
	}

	// 3. clients
	for _, c := range w.clients {
		w.driveClient(c, faulty)
	}

	w.checkInvariants()
	w.snapshot()
}

// drain collects a node's output: sends messages and applies committed entries.
func (w *world) drain(r *replica) {
	msgs, committed, persist := r.node.Ready()
	if persist {
		r.disk = r.node.Storage().Clone() // durable before any message leaves
	}
	for _, v := range r.node.Violations {
		w.violate("log-overwrite", v)
	}
	r.node.Violations = nil
	for _, m := range msgs {
		w.res.Messages++
		if m.Type == raft.MsgVoteResp && m.Granted {
			key := [2]uint64{uint64(m.From), m.Term}
			if prev, ok := w.votes[key]; ok && prev != m.To {
				w.violate("vote-safety", fmt.Sprintf(
					"node %d voted for both %d and %d in term %d", m.From, prev, m.To, m.Term))
			}
			w.votes[key] = m.To
		}
		if w.rng.Float64() < w.cfg.DropRate {
			w.res.Dropped++
			w.emit(Event{Kind: "lost", Msg: summarize(m)})
			continue
		}
		copies := 1
		if w.rng.Float64() < w.cfg.DupRate {
			copies = 2
		}
		for i := 0; i < copies; i++ {
			w.seq++
			d := w.now + 1 + w.rng.Intn(w.cfg.MaxDelay)
			w.net = append(w.net, inflight{m, d, w.seq})
			w.emit(Event{Kind: "send", Msg: summarize(m), Deliver: d})
		}
	}
	for _, e := range committed {
		w.apply(r, e)
	}
}

func (w *world) apply(r *replica, e raft.Entry) {
	// State machine safety: every node applies the same entry at each index.
	if prev, ok := w.commits[e.Index]; ok {
		if prev.Term != e.Term || string(prev.Cmd) != string(e.Cmd) {
			w.violate("state-machine-safety", fmt.Sprintf(
				"node %d applied term-%d entry at index %d, but another node applied a term-%d entry there",
				r.id, e.Term, e.Index, prev.Term))
		}
	} else {
		w.commits[e.Index] = e
	}
	r.applied[e.Index] = e
	if e.Cmd == nil {
		return
	}
	cmd, err := kv.Decode(e.Cmd)
	if err != nil {
		w.violate("corruption", err.Error())
		return
	}
	res := r.store.Apply(cmd)
	w.emit(Event{Kind: "apply", Node: r.id, Index: e.Index, Term: e.Term, Op: string(cmd.Kind), Key: cmd.Key})

	// Complete the waiting client, if this node was the one it asked.
	for _, c := range w.clients {
		p := c.cur
		if p == nil || p.node != r.id || p.index != e.Index {
			continue
		}
		if p.term != e.Term {
			p.index = 0 // overwritten by a newer leader: retry
			continue
		}
		w.complete(c, res)
	}
}

// ---- clients --------------------------------------------------------------

func (w *world) allClientsIdle() bool {
	for _, c := range w.clients {
		if c.cur != nil {
			return false
		}
	}
	return true
}

func (w *world) driveClient(c *client, faulty bool) {
	if c.cur == nil {
		if !faulty {
			return // no new requests during recovery; just finish what's open
		}
		if c.idle > 0 {
			c.idle--
			return
		}
		c.seq++
		w.opID++
		kinds := []check.Kind{check.Put, check.Get, check.Append, check.Get, check.Append}
		op := &check.Op{
			ID: w.opID, Client: c.id, Kind: kinds[w.rng.Intn(len(kinds))],
			Key: fmt.Sprintf("k%d", w.rng.Intn(w.cfg.Keys)), Return: -1,
		}
		if op.Kind != check.Get {
			op.Value = fmt.Sprintf("%d.%d ", c.id, c.seq)
		}
		w.clock++
		op.Call = w.clock
		w.history = append(w.history, op)
		w.res.Ops++
		cmd := kv.Command{Client: c.id, Seq: c.seq, Kind: kv.OpKind(op.Kind), Key: op.Key, Value: op.Value}
		c.cur = &pending{op: op, cmd: cmd}
		w.emit(Event{Kind: "invoke", Client: c.id, OpID: op.ID, Op: string(op.Kind), Key: op.Key})
	}

	p := c.cur
	if p.index != 0 && w.now < p.deadline {
		return // waiting for the entry to commit
	}
	if w.now < p.waitTill {
		return
	}
	// (Re)submit to whichever node the client thinks is leader.
	r := w.reps[c.hint-1]
	if !r.up || !r.node.IsLeader() {
		if r.up && r.node.Leader() > 0 {
			c.hint = r.node.Leader()
		} else {
			c.hint = 1 + w.rng.Intn(w.cfg.Nodes)
		}
		p.index = 0
		p.waitTill = w.now + 2
		return
	}
	if w.cfg.Bugs.StaleReads && p.cmd.Kind == kv.Get {
		w.complete(c, r.store.Read(p.cmd.Key))
		return
	}
	idx, term, ok := r.node.Propose(p.cmd.Encode())
	if !ok {
		return
	}
	p.node, p.index, p.term = r.id, idx, term
	p.deadline = w.now + w.cfg.ClientTimeout
	w.drain(r)
}

func (w *world) complete(c *client, res kv.Result) {
	p := c.cur
	w.clock++
	p.op.Return = w.clock
	if p.op.Kind == check.Get {
		p.op.Out, p.op.Found = res.Value, res.Found
	}
	w.res.Completed++
	w.emit(Event{Kind: "return", Client: c.id, OpID: p.op.ID, Op: string(p.op.Kind), Key: p.op.Key, Value: res.Value})
	c.cur = nil
	c.idle = w.rng.Intn(6)
}

// ---- invariants -------------------------------------------------------------

func (w *world) checkInvariants() {
	for _, r := range w.reps {
		if !r.up || !r.node.IsLeader() {
			continue
		}
		t := r.node.Term()
		if prev, ok := w.leaders[t]; !ok {
			w.leaders[t] = r.id
			w.res.Elections++
			w.emit(Event{Kind: "elected", Node: r.id, Term: t})
		} else if prev != r.id {
			w.violate("election-safety", fmt.Sprintf("nodes %d and %d were both leader in term %d", prev, r.id, t))
		}
	}
}

func (w *world) violate(kind, detail string) {
	for _, v := range w.res.Violations {
		if v.Kind == kind && v.Detail == detail {
			return
		}
	}
	w.res.Violations = append(w.res.Violations, Violation{w.now, kind, detail})
	w.emit(Event{Kind: "violation", Detail: kind + ": " + detail})
}

func (w *world) finish() {
	w.res.Ticks = w.now
	for _, c := range w.clients {
		if c.cur != nil {
			w.violate("liveness", fmt.Sprintf(
				"client %d's request #%d never completed, even after %d calm ticks with every node up and connected",
				c.id, c.cur.op.ID, w.cfg.Recovery))
		}
	}
	ops := make([]check.Op, len(w.history))
	for i, op := range w.history {
		ops[i] = *op
	}
	w.res.History = ops
	w.res.Linear = check.Linearizable(ops)
	if !w.res.Linear.OK {
		w.violate("linearizability", w.res.Linear.Explanation)
	}
	for _, r := range w.reps {
		if r.up && r.node.IsLeader() {
			w.res.Final = r.store.Snapshot()
		}
	}
	if w.trace != nil {
		w.trace.finish(w)
		w.res.Trace = w.trace
	}
}
