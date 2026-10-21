package raft

import (
	"fmt"
	"math/rand"
	"testing"
)

// cluster is a minimal lossless in-memory network for unit tests.
type cluster struct {
	nodes map[int]*Node
	cut   map[int]bool // isolated nodes
	log   map[int][]Entry
}

func newCluster(n int, bugs Bugs) *cluster {
	peers := make([]int, n)
	for i := range peers {
		peers[i] = i + 1
	}
	c := &cluster{nodes: map[int]*Node{}, cut: map[int]bool{}, log: map[int][]Entry{}}
	for _, id := range peers {
		c.nodes[id] = NewNode(Config{ID: id, Peers: peers, Rand: rand.New(rand.NewSource(int64(id) * 7)), Bugs: bugs}, nil)
	}
	return c
}

func (c *cluster) pump() {
	for moved := true; moved; {
		moved = false
		for id := 1; id <= len(c.nodes); id++ {
			msgs, committed, _ := c.nodes[id].Ready()
			c.log[id] = append(c.log[id], committed...)
			for _, m := range msgs {
				if c.cut[m.From] || c.cut[m.To] {
					continue
				}
				c.nodes[m.To].Step(m)
				moved = true
			}
		}
	}
}

func (c *cluster) tick(n int) {
	for i := 0; i < n; i++ {
		for id := 1; id <= len(c.nodes); id++ {
			c.nodes[id].Tick()
		}
		c.pump()
	}
}

func (c *cluster) leader() *Node {
	var found *Node
	for id := 1; id <= len(c.nodes); id++ {
		n := c.nodes[id]
		if n.IsLeader() && !c.cut[id] {
			if found == nil || n.Term() > found.Term() {
				found = n
			}
		}
	}
	return found
}

func TestElectsOneLeader(t *testing.T) {
	c := newCluster(5, Bugs{})
	c.tick(50)
	l := c.leader()
	if l == nil {
		t.Fatal("no leader after 50 ticks")
	}
	for id, n := range c.nodes {
		if id != l.cfg.ID && n.IsLeader() && n.Term() == l.Term() {
			t.Fatalf("two leaders in term %d", l.Term())
		}
		if n.Leader() != l.cfg.ID {
			t.Fatalf("node %d thinks leader is %d, want %d", id, n.Leader(), l.cfg.ID)
		}
	}
}

func TestReplicatesAndCommits(t *testing.T) {
	c := newCluster(3, Bugs{})
	c.tick(50)
	l := c.leader()
	for i := 0; i < 5; i++ {
		if _, _, ok := l.Propose([]byte(fmt.Sprint("cmd", i))); !ok {
			t.Fatal("leader refused proposal")
		}
	}
	c.tick(5)
	for id := range c.nodes {
		var cmds []string
		for _, e := range c.log[id] {
			if e.Cmd != nil {
				cmds = append(cmds, string(e.Cmd))
			}
		}
		if len(cmds) != 5 || cmds[0] != "cmd0" || cmds[4] != "cmd4" {
			t.Fatalf("node %d applied %v", id, cmds)
		}
	}
}

func TestFollowerRefusesProposal(t *testing.T) {
	c := newCluster(3, Bugs{})
	c.tick(50)
	for _, n := range c.nodes {
		if !n.IsLeader() {
			if _, _, ok := n.Propose([]byte("x")); ok {
				t.Fatal("follower accepted a proposal")
			}
		}
	}
}

func TestIsolatedLeaderIsReplaced(t *testing.T) {
	c := newCluster(5, Bugs{})
	c.tick(50)
	old := c.leader()
	c.cut[old.cfg.ID] = true
	c.tick(60)
	nl := c.leader()
	if nl == nil || nl == old || nl.Term() <= old.Term() {
		t.Fatal("majority did not elect a new leader")
	}
	// The old leader's uncommitted write must be discarded once it rejoins.
	old.Propose([]byte("lost"))
	nl.Propose([]byte("kept"))
	c.tick(5)
	delete(c.cut, old.cfg.ID)
	c.tick(20)
	for id, n := range c.nodes {
		for i := uint64(1); i <= n.commit; i++ {
			if string(n.st.Log[i].Cmd) == "lost" {
				t.Fatalf("node %d committed the deposed leader's write", id)
			}
		}
	}
	if nl.commit != old.commit {
		t.Fatalf("old leader did not catch up: commit %d vs %d", old.commit, nl.commit)
	}
}

func TestLaggingFollowerCatchesUp(t *testing.T) {
	c := newCluster(3, Bugs{})
	c.tick(50)
	l := c.leader()
	lag := l.cfg.ID%3 + 1
	c.cut[lag] = true
	for i := 0; i < 200; i++ {
		l.Propose([]byte{byte(i)})
		c.pump()
	}
	delete(c.cut, lag)
	c.tick(30)
	if c.nodes[lag].commit != l.commit {
		t.Fatalf("follower at %d, leader at %d", c.nodes[lag].commit, l.commit)
	}
}

func TestVoteSurvivesRestart(t *testing.T) {
	n := NewNode(Config{ID: 1, Peers: []int{1, 2, 3}}, nil)
	n.Step(Message{Type: MsgVote, From: 2, To: 1, Term: 5})
	msgs, _, persist := n.Ready()
	if !persist || len(msgs) != 1 || !msgs[0].Granted {
		t.Fatalf("expected a persisted grant, got %+v persist=%v", msgs, persist)
	}
	restarted := NewNode(Config{ID: 1, Peers: []int{1, 2, 3}}, n.Storage().Clone())
	restarted.Step(Message{Type: MsgVote, From: 3, To: 1, Term: 5})
	msgs, _, _ = restarted.Ready()
	if msgs[0].Granted {
		t.Fatal("voted twice in term 5 after a restart")
	}
}

func TestStaleLogLosesElection(t *testing.T) {
	n := NewNode(Config{ID: 1, Peers: []int{1, 2, 3}}, &Storage{Term: 3, VotedFor: -1, Log: []Entry{{}, {Term: 2, Index: 1}, {Term: 3, Index: 2}}})
	n.Step(Message{Type: MsgVote, From: 2, To: 1, Term: 4, LastIndex: 5, LastTerm: 2})
	msgs, _, _ := n.Ready()
	if msgs[0].Granted {
		t.Fatal("granted a vote to a candidate with an older last term")
	}
}
