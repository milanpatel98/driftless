// Package check decides whether a history of client operations is linearizable:
// whether there is one order of the operations, consistent with real time,
// in which every result matches what a single correct key-value store returns.
//
// The search is the Wing & Gong algorithm with Lowe's memoisation (the same
// approach as Porcupine and Knossos), written from scratch. Keys are checked
// independently, which is sound for put/get/append because each operation
// touches exactly one key.
package check

import (
	"fmt"
	"sort"
	"strings"
)

type Kind string

const (
	Put    Kind = "put"
	Get    Kind = "get"
	Append Kind = "append"
)

// Op is one client request. Call and Return are positions in a single global
// sequence of events, so "A returned before B was called" is exact.
// Return < 0 means the client never heard back: the operation may or may not
// have taken effect, and the checker must allow both.
type Op struct {
	ID     int    `json:"id"`
	Client int    `json:"client"`
	Kind   Kind   `json:"kind"`
	Key    string `json:"key"`
	Value  string `json:"value,omitempty"` // argument for put/append
	Out    string `json:"out,omitempty"`   // observed value for get
	Found  bool   `json:"found,omitempty"` // observed presence for get
	Call   int64  `json:"call"`
	Return int64  `json:"return"`
}

type state struct {
	value string
	found bool
}

func step(s state, op *Op) (state, bool) {
	switch op.Kind {
	case Put:
		return state{op.Value, true}, true
	case Append:
		return state{s.value + op.Value, true}, true
	case Get:
		if op.Return < 0 {
			return s, true // nobody saw the answer, so any answer is fine
		}
		return s, op.Found == s.found && (!s.found || op.Out == s.value)
	}
	return s, false
}

type Result struct {
	OK bool `json:"ok"`
	// For a failure: the key whose history cannot be linearised, and the
	// longest prefix the search managed to explain.
	Key         string `json:"key,omitempty"`
	Explained   []int  `json:"explained,omitempty"`
	Unexplained []int  `json:"unexplained,omitempty"`
	Explanation string `json:"explanation,omitempty"`
}

func Linearizable(ops []Op) Result {
	byKey := map[string][]*Op{}
	var keys []string
	for i := range ops {
		op := &ops[i]
		if op.Kind == Get && op.Return < 0 {
			continue // an unanswered read constrains nothing
		}
		if _, ok := byKey[op.Key]; !ok {
			keys = append(keys, op.Key)
		}
		byKey[op.Key] = append(byKey[op.Key], op)
	}
	sort.Strings(keys)
	for _, k := range keys {
		if r := checkKey(k, byKey[k]); !r.OK {
			return r
		}
	}
	return Result{OK: true}
}

// ---- the search -----------------------------------------------------------

type node struct {
	op         *Op
	call       bool
	idx        int // index within this key's ops
	match      *node
	prev, next *node
}

const infinity = int64(1) << 62

func checkKey(key string, ops []*Op) Result {
	type event struct {
		t    int64
		call bool
		idx  int
	}
	events := make([]event, 0, 2*len(ops))
	for i, op := range ops {
		ret := op.Return
		if ret < 0 {
			ret = infinity
		}
		events = append(events, event{op.Call, true, i}, event{ret, false, i})
	}
	sort.SliceStable(events, func(a, b int) bool {
		if events[a].t != events[b].t {
			return events[a].t < events[b].t
		}
		return events[a].call && !events[b].call // calls first: be generous on ties
	})

	head := &node{}
	tail := head
	calls := make([]*node, len(ops))
	for _, e := range events {
		n := &node{op: ops[e.idx], call: e.call, idx: e.idx, prev: tail}
		tail.next = n
		tail = n
		if e.call {
			calls[e.idx] = n
		} else {
			calls[e.idx].match = n
			n.match = calls[e.idx]
		}
	}

	lift := func(n *node) {
		n.prev.next = n.next
		n.next.prev = n.prev
		m := n.match
		m.prev.next = m.next
		if m.next != nil {
			m.next.prev = m.prev
		}
	}
	unlift := func(n *node) {
		m := n.match
		m.prev.next = m
		if m.next != nil {
			m.next.prev = m
		}
		n.prev.next = n
		n.next.prev = n
	}

	type frame struct {
		n     *node
		state state
	}
	var stack []frame
	done := newBitset(len(ops))
	seen := map[string]bool{}
	cur := state{}
	best := 0
	var bestSet []int

	entry := head.next
	for head.next != nil {
		if entry.call {
			next, ok := step(cur, entry.op)
			if ok {
				done.set(entry.idx)
				k := done.key() + "|" + fmt.Sprint(next.found) + next.value
				if !seen[k] {
					seen[k] = true
					stack = append(stack, frame{entry, cur})
					cur = next
					lift(entry)
					if len(stack) > best {
						best = len(stack)
						bestSet = bestSet[:0]
						for _, f := range stack {
							bestSet = append(bestSet, f.n.op.ID)
						}
					}
					entry = head.next
					continue
				}
				done.clear(entry.idx)
			}
			entry = entry.next
			continue
		}
		// Reached a return whose call is still unplaced: backtrack.
		if len(stack) == 0 {
			return failure(key, ops, bestSet)
		}
		top := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		cur = top.state
		done.clear(top.n.idx)
		unlift(top.n)
		entry = top.n.next
	}
	return Result{OK: true}
}

func failure(key string, ops []*Op, explained []int) Result {
	in := map[int]bool{}
	for _, id := range explained {
		in[id] = true
	}
	var rest []*Op
	for _, op := range ops {
		if !in[op.ID] {
			rest = append(rest, op)
		}
	}
	// The earliest-finishing unplaceable operation is usually the culprit.
	sort.SliceStable(rest, func(i, j int) bool { return finish(rest[i]) < finish(rest[j]) })
	ids := make([]int, len(rest))
	var lines []string
	for i, op := range rest {
		ids[i] = op.ID
		if i < 2 {
			lines = append(lines, describe(op))
		}
	}
	more := ""
	if len(rest) > 2 {
		more = fmt.Sprintf(" (and %d later operations)", len(rest)-2)
	}
	return Result{
		Key:         key,
		Explained:   append([]int(nil), explained...),
		Unexplained: ids,
		Explanation: fmt.Sprintf("key %q: after %d operations that fit a valid order, no order can explain %s%s",
			key, len(explained), strings.Join(lines, ", then "), more),
	}
}

func finish(op *Op) int64 {
	if op.Return < 0 {
		return infinity
	}
	return op.Return
}

func describe(op *Op) string {
	switch op.Kind {
	case Get:
		if !op.Found {
			return fmt.Sprintf("#%d client %d read nothing", op.ID, op.Client)
		}
		return fmt.Sprintf("#%d client %d read %q", op.ID, op.Client, op.Out)
	default:
		return fmt.Sprintf("#%d client %d %s %q", op.ID, op.Client, op.Kind, op.Value)
	}
}

type bitset []uint64

func newBitset(n int) bitset { return make(bitset, (n+63)/64) }
func (b bitset) set(i int)   { b[i/64] |= 1 << (i % 64) }
func (b bitset) clear(i int) { b[i/64] &^= 1 << (i % 64) }
func (b bitset) key() string {
	var sb strings.Builder
	for _, w := range b {
		fmt.Fprintf(&sb, "%x.", w)
	}
	return sb.String()
}
