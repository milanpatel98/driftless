package sim

import (
	"encoding/json"
	"testing"

	"github.com/milanpatel98/driftless/raft"
)

// The correct implementation must survive every seed. CI runs a wider sweep.
func TestCorrectRaftSurvivesFaults(t *testing.T) {
	for seed := int64(1); seed <= 150; seed++ {
		r := Run(Defaults(seed))
		if !r.OK() {
			t.Fatalf("seed %d: %+v", seed, r.Violations)
		}
		if r.Completed != r.Ops {
			t.Fatalf("seed %d: %d of %d ops completed", seed, r.Completed, r.Ops)
		}
	}
}

func TestSameSeedSameRun(t *testing.T) {
	a, _ := json.Marshal(Run(withTrace(Defaults(42))))
	b, _ := json.Marshal(Run(withTrace(Defaults(42))))
	if string(a) != string(b) {
		t.Fatal("seed 42 produced two different runs; the simulator is not deterministic")
	}
}

func withTrace(c Config) Config { c.Trace = true; return c }

// Each planted bug must be caught by the invariant it breaks.
func TestPlantedBugsAreCaught(t *testing.T) {
	cases := []struct {
		name string
		bugs raft.Bugs
		kind string
	}{
		{"stale reads", raft.Bugs{StaleReads: true}, "linearizability"},
		{"forgotten vote", raft.Bugs{ForgetVote: true}, "vote-safety"},
		{"figure 8", raft.Bugs{CommitOldTerm: true}, "state-machine-safety"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for seed := int64(1); seed <= 400; seed++ {
				c := Defaults(seed)
				c.Bugs = tc.bugs
				for _, v := range Run(c).Violations {
					if v.Kind == tc.kind {
						return
					}
				}
			}
			t.Fatalf("%s survived 400 seeds undetected", tc.name)
		})
	}
}
