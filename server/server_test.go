package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"
)

func freePort(t *testing.T) string {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().String()
}

type harness struct {
	t      *testing.T
	addrs  map[int]string
	dir    string
	cancel map[int]context.CancelFunc
}

func (h *harness) start(id int) {
	s, err := New(Config{ID: id, Addrs: h.addrs, DataDir: h.dir, Tick: 10 * time.Millisecond})
	if err != nil {
		h.t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	h.cancel[id] = cancel
	go s.Run(ctx)
}

func (h *harness) do(id int, method, key, body string) (int, map[string]any) {
	req, _ := http.NewRequest(method, "http://"+h.addrs[id]+"/kv/"+key, strings.NewReader(body))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, nil
	}
	defer resp.Body.Close()
	var out map[string]any
	json.NewDecoder(resp.Body).Decode(&out)
	return resp.StatusCode, out
}

// retry until a leader exists and the request commits
func (h *harness) must(id int, method, key, body string) map[string]any {
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		code, out := h.do(id, method, key, body)
		if code == 200 || (code == 404 && method == "GET") {
			return out
		}
		time.Sleep(50 * time.Millisecond)
	}
	h.t.Fatalf("%s %s on node %d never succeeded", method, key, id)
	return nil
}

func TestClusterSurvivesLeaderCrash(t *testing.T) {
	h := &harness{t: t, addrs: map[int]string{}, dir: t.TempDir(), cancel: map[int]context.CancelFunc{}}
	for id := 1; id <= 3; id++ {
		h.addrs[id] = freePort(t)
	}
	for id := 1; id <= 3; id++ {
		h.start(id)
	}
	defer func() {
		for _, c := range h.cancel {
			c()
		}
	}()

	h.must(2, "PUT", "guest", "Milan")   // forwarded if 2 is not leader
	h.must(3, "POST", "guest", " Patel") // append
	out := h.must(1, "GET", "guest", "")
	if out["value"] != "Milan Patel" {
		t.Fatalf("got %v", out)
	}
	leader := int(out["leader"].(float64))

	h.cancel[leader]()
	other := leader%3 + 1
	h.must(other, "PUT", "room", "204")
	if out := h.must(other, "GET", "guest", ""); out["value"] != "Milan Patel" {
		t.Fatalf("data lost after leader crash: %v", out)
	}
	if out := h.must(other, "GET", "room", ""); int(out["leader"].(float64)) == leader {
		t.Fatal("crashed node still reported as leader")
	}

	// The old leader restarts from disk and catches up.
	time.Sleep(100 * time.Millisecond)
	h.start(leader)
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := http.Get("http://" + h.addrs[leader] + "/status")
		if err == nil {
			var st struct{ Data map[string]string }
			json.NewDecoder(resp.Body).Decode(&st)
			resp.Body.Close()
			if st.Data["room"] == "204" {
				return
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal(fmt.Sprintf("restarted node %d never caught up", leader))
}
