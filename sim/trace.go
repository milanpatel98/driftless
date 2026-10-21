package sim

import (
	"github.com/milanpatel98/driftless/raft"
)

// Trace is the full replayable record of one simulated run, for the viewer.
type Trace struct {
	Seed   int64       `json:"seed"`
	Nodes  int         `json:"nodes"`
	Bugs   raft.Bugs   `json:"bugs"`
	Events []Event     `json:"events"`
	States [][]NodeRow `json:"states"` // per tick, one row per node
}

type MsgSummary struct {
	Type    string `json:"t"`
	From    int    `json:"f"`
	To      int    `json:"to"`
	Term    uint64 `json:"term"`
	Entries int    `json:"n,omitempty"`
	OK      bool   `json:"ok,omitempty"`
}

type Event struct {
	Tick    int         `json:"tick"`
	Kind    string      `json:"kind"`
	Node    int         `json:"node,omitempty"`
	Term    uint64      `json:"term,omitempty"`
	Index   uint64      `json:"index,omitempty"`
	Msg     *MsgSummary `json:"msg,omitempty"`
	Deliver int         `json:"deliver,omitempty"`
	Groups  []int       `json:"groups,omitempty"`
	Client  int         `json:"client,omitempty"`
	OpID    int         `json:"op,omitempty"`
	Op      string      `json:"opKind,omitempty"`
	Key     string      `json:"key,omitempty"`
	Value   string      `json:"value,omitempty"`
	Detail  string      `json:"detail,omitempty"`
}

// NodeRow is a compact per-tick snapshot: [up, role, term, commit, lastIndex, group].
type NodeRow [6]int

func newTrace(cfg Config) *Trace {
	return &Trace{Seed: cfg.Seed, Nodes: cfg.Nodes, Bugs: cfg.Bugs}
}

func summarize(m raft.Message) *MsgSummary {
	ok := m.Granted || m.Success
	return &MsgSummary{m.Type.String(), m.From, m.To, m.Term, len(m.Entries), ok}
}

func (w *world) emit(e Event) {
	if w.trace == nil {
		return
	}
	// Heartbeat traffic dominates; keep sends and receipts only when they carry
	// something (votes, entries, rejections) so traces stay small.
	if e.Msg != nil && (e.Kind == "send" || e.Kind == "recv") {
		quiet := (e.Msg.Type == "append" && e.Msg.Entries == 0) || (e.Msg.Type == "append-resp" && e.Msg.OK)
		if quiet && e.Kind == "recv" {
			return
		}
		if quiet && e.Kind == "send" && w.now%3 != 0 {
			return
		}
	}
	e.Tick = w.now
	w.trace.Events = append(w.trace.Events, e)
}

func (w *world) snapshot() {
	if w.trace == nil {
		return
	}
	rows := make([]NodeRow, len(w.reps))
	for i, r := range w.reps {
		up := 0
		if r.up {
			up = 1
		}
		s := r.node.Status()
		role := int(r.node.Role())
		rows[i] = NodeRow{up, role, int(s.Term), int(s.Commit), int(s.LastIndex), w.group[r.id]}
	}
	w.trace.States = append(w.trace.States, rows)
}

func (t *Trace) finish(w *world) {}
