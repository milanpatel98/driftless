package check

import "testing"

func put(id, client int, key, v string, call, ret int64) Op {
	return Op{ID: id, Client: client, Kind: Put, Key: key, Value: v, Call: call, Return: ret}
}
func app(id, client int, key, v string, call, ret int64) Op {
	return Op{ID: id, Client: client, Kind: Append, Key: key, Value: v, Call: call, Return: ret}
}
func get(id, client int, key, out string, call, ret int64) Op {
	return Op{ID: id, Client: client, Kind: Get, Key: key, Out: out, Found: out != "", Call: call, Return: ret}
}

func TestSequentialHistory(t *testing.T) {
	h := []Op{put(1, 1, "x", "a", 0, 1), get(2, 1, "x", "a", 2, 3), app(3, 1, "x", "b", 4, 5), get(4, 1, "x", "ab", 6, 7)}
	if r := Linearizable(h); !r.OK {
		t.Fatal(r.Explanation)
	}
}

func TestConcurrentWritesEitherOrder(t *testing.T) {
	// Two overlapping puts; a later read may see either one.
	for _, seen := range []string{"a", "b"} {
		h := []Op{put(1, 1, "x", "a", 0, 10), put(2, 2, "x", "b", 1, 9), get(3, 3, "x", seen, 11, 12)}
		if r := Linearizable(h); !r.OK {
			t.Fatalf("read %q should be allowed: %s", seen, r.Explanation)
		}
	}
}

func TestStaleReadRejected(t *testing.T) {
	// put a finishes, then put b finishes, then a read returns a: impossible.
	h := []Op{put(1, 1, "x", "a", 0, 1), put(2, 1, "x", "b", 2, 3), get(3, 2, "x", "a", 4, 5)}
	r := Linearizable(h)
	if r.OK {
		t.Fatal("stale read accepted")
	}
	if r.Key != "x" || len(r.Unexplained) == 0 {
		t.Fatalf("bad failure report: %+v", r)
	}
}

func TestLostAppendRejected(t *testing.T) {
	h := []Op{app(1, 1, "x", "a", 0, 1), app(2, 1, "x", "b", 2, 3), get(3, 2, "x", "b", 4, 5)}
	if Linearizable(h).OK {
		t.Fatal("lost append accepted")
	}
}

func TestReadsCannotGoBackInTime(t *testing.T) {
	// Concurrent with a put, two sequential reads see new then old: not allowed.
	h := []Op{put(1, 1, "x", "a", 0, 1), put(2, 1, "x", "b", 2, 20), get(3, 2, "x", "b", 3, 4), get(4, 2, "x", "a", 5, 6)}
	if Linearizable(h).OK {
		t.Fatal("read went back in time")
	}
}

func TestUnansweredWriteMayOrMayNotHappen(t *testing.T) {
	for _, seen := range []string{"a", "ab"} {
		h := []Op{app(1, 1, "x", "a", 0, 1), app(2, 1, "x", "b", 2, -1), get(3, 2, "x", seen, 3, 4)}
		if r := Linearizable(h); !r.OK {
			t.Fatalf("read %q after an unanswered append should be allowed: %s", seen, r.Explanation)
		}
	}
	// But it cannot take effect before it was called.
	h := []Op{get(1, 2, "x", "b", 0, 1), app(2, 1, "x", "b", 2, -1)}
	if Linearizable(h).OK {
		t.Fatal("effect observed before the call")
	}
}

func TestMissingKey(t *testing.T) {
	h := []Op{get(1, 1, "x", "", 0, 1), put(2, 1, "x", "a", 2, 3)}
	if r := Linearizable(h); !r.OK {
		t.Fatal(r.Explanation)
	}
	h = []Op{put(1, 1, "x", "a", 0, 1), get(2, 1, "x", "", 2, 3)}
	if Linearizable(h).OK {
		t.Fatal("read of a missing value after a completed put accepted")
	}
}

func TestKeysAreIndependent(t *testing.T) {
	h := []Op{put(1, 1, "x", "a", 0, 1), put(2, 1, "y", "b", 2, 3), get(3, 2, "y", "b", 4, 5), get(4, 2, "x", "a", 6, 7)}
	if r := Linearizable(h); !r.OK {
		t.Fatal(r.Explanation)
	}
}

func TestManyConcurrentOpsStayFast(t *testing.T) {
	var h []Op
	id := 0
	for round := int64(0); round < 60; round++ {
		base := round * 10
		for c := 1; c <= 5; c++ {
			id++
			h = append(h, app(id, c, "x", string(rune('a'+c)), base, base+8))
		}
	}
	if r := Linearizable(h); !r.OK {
		t.Fatal(r.Explanation)
	}
}
