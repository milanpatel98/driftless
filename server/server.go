// Package server runs one Raft node as a real process: HTTP transport between
// peers, an fsync'd file for durable state, a wall-clock ticker, and a small
// key-value HTTP API for clients. The consensus logic is the same raft.Node
// the simulator tests.
package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/milanpatel98/driftless/kv"
	"github.com/milanpatel98/driftless/raft"
)

type Config struct {
	ID      int
	Addrs   map[int]string // id -> host:port, including this node
	DataDir string
	Tick    time.Duration
}

type waiter struct {
	term uint64
	done chan result
}

type result struct {
	res kv.Result
	err error
}

type Server struct {
	cfg      Config
	mu       sync.Mutex
	node     *raft.Node
	store    *kv.Store
	wait     map[uint64]waiter
	inbox    chan raft.Message
	client   *http.Client
	logger   *log.Logger
	seq      int
	lastRole raft.Role
}

func New(cfg Config) (*Server, error) {
	if cfg.Tick == 0 {
		cfg.Tick = 50 * time.Millisecond
	}
	if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
		return nil, err
	}
	st, err := load(cfg.statePath())
	if err != nil {
		return nil, err
	}
	var peers []int
	for id := range cfg.Addrs {
		peers = append(peers, id)
	}
	s := &Server{
		cfg:    cfg,
		store:  kv.New(),
		wait:   map[uint64]waiter{},
		inbox:  make(chan raft.Message, 4096),
		client: &http.Client{Timeout: 2 * time.Second},
		logger: log.New(os.Stderr, fmt.Sprintf("[node %d] ", cfg.ID), log.Ltime),
	}
	s.node = raft.NewNode(raft.Config{
		ID: cfg.ID, Peers: peers, ElectionTicks: 10, HeartbeatTicks: 2,
		Rand: rand.New(rand.NewSource(time.Now().UnixNano())),
	}, st)
	s.logger.Printf("started in term %d with %d log entries", st.Term, len(st.Log)-1)
	return s, nil
}

func (c Config) statePath() string {
	return filepath.Join(c.DataDir, fmt.Sprintf("node-%d.json", c.ID))
}

func (s *Server) Run(ctx context.Context) error {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /raft", s.handleRaft)
	mux.HandleFunc("GET /status", s.handleStatus)
	mux.HandleFunc("GET /kv/{key}", s.handleKV)
	mux.HandleFunc("PUT /kv/{key}", s.handleKV)
	mux.HandleFunc("POST /kv/{key}", s.handleKV) // append
	srv := &http.Server{Addr: s.cfg.Addrs[s.cfg.ID], Handler: mux}
	go func() {
		<-ctx.Done()
		srv.Close()
	}()
	go s.loop(ctx)
	s.logger.Printf("listening on %s", srv.Addr)
	if err := srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

func (s *Server) loop(ctx context.Context) {
	ticker := time.NewTicker(s.cfg.Tick)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.mu.Lock()
			s.node.Tick()
			s.flush()
			s.mu.Unlock()
		case m := <-s.inbox:
			s.mu.Lock()
			s.node.Step(m)
			s.flush()
			s.mu.Unlock()
		}
	}
}

// flush persists, sends and applies, in that order. Must hold s.mu.
func (s *Server) flush() {
	msgs, committed, persist := s.node.Ready()
	if persist {
		if err := save(s.cfg.statePath(), s.node.Storage()); err != nil {
			s.logger.Fatalf("cannot persist state: %v", err) // never send without durability
		}
	}
	for _, m := range msgs {
		go s.send(m)
	}
	for _, e := range committed {
		var res kv.Result
		if e.Cmd != nil {
			if cmd, err := kv.Decode(e.Cmd); err == nil {
				res = s.store.Apply(cmd)
			}
		}
		if w, ok := s.wait[e.Index]; ok {
			delete(s.wait, e.Index)
			if w.term == e.Term {
				w.done <- result{res: res}
			} else {
				w.done <- result{err: errors.New("leadership changed before the write committed; retry")}
			}
		}
	}
	if r := s.node.Role(); r != s.lastRole {
		s.logger.Printf("now %s in term %d", r, s.node.Term())
		s.lastRole = r
		if r != raft.Leader {
			for idx, w := range s.wait {
				w.done <- result{err: errors.New("no longer leader; retry")}
				delete(s.wait, idx)
			}
		}
	}
}

func (s *Server) send(m raft.Message) {
	body, _ := json.Marshal(m)
	resp, err := s.client.Post("http://"+s.cfg.Addrs[m.To]+"/raft", "application/json", bytes.NewReader(body))
	if err != nil {
		return // the peer is down or unreachable; Raft retries on its own
	}
	resp.Body.Close()
}

// ---- HTTP handlers --------------------------------------------------------

func (s *Server) handleRaft(w http.ResponseWriter, r *http.Request) {
	var m raft.Message
	if err := json.NewDecoder(r.Body).Decode(&m); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	select {
	case s.inbox <- m:
	default: // overloaded: dropping is safe, Raft tolerates message loss
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	st := s.node.Status()
	snap := s.store.Snapshot()
	s.mu.Unlock()
	writeJSON(w, http.StatusOK, map[string]any{"node": st, "data": snap})
}

func (s *Server) handleKV(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	cmd := kv.Command{Key: key}
	body, _ := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	switch r.Method {
	case http.MethodGet:
		cmd.Kind = kv.Get
	case http.MethodPut, http.MethodPost:
		cmd.Value = strings.TrimRight(string(body), "\n")
		cmd.Kind = kv.Put
		if r.Method == http.MethodPost {
			cmd.Kind = kv.Append
		}
	}

	s.mu.Lock()
	if !s.node.IsLeader() {
		leader := s.node.Leader()
		s.mu.Unlock()
		if leader <= 0 {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "no leader yet; retry in a moment"})
			return
		}
		s.forward(w, r, body, leader)
		return
	}
	// Reads go through the log too: that is what makes them linearizable.
	idx, term, _ := s.node.Propose(cmd.Encode())
	done := make(chan result, 1)
	s.wait[idx] = waiter{term: term, done: done}
	s.flush()
	s.mu.Unlock()

	select {
	case res := <-done:
		if res.err != nil {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": res.err.Error()})
			return
		}
		if cmd.Kind == kv.Get && !res.res.Found {
			writeJSON(w, http.StatusNotFound, map[string]any{"key": key, "found": false, "leader": s.cfg.ID})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"key": key, "value": res.res.Value, "index": idx, "leader": s.cfg.ID})
	case <-time.After(3 * time.Second):
		writeJSON(w, http.StatusGatewayTimeout, map[string]string{"error": "not committed in time (is a majority up?)"})
	case <-r.Context().Done():
	}
}

func (s *Server) forward(w http.ResponseWriter, r *http.Request, body []byte, leader int) {
	req, _ := http.NewRequestWithContext(r.Context(), r.Method, "http://"+s.cfg.Addrs[leader]+r.URL.Path, bytes.NewReader(body))
	resp, err := (&http.Client{Timeout: 5 * time.Second}).Do(req)
	if err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": fmt.Sprintf("leader %d unreachable: %v", leader, err)})
		return
	}
	defer resp.Body.Close()
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Forwarded-To", fmt.Sprint(leader))
	w.WriteHeader(resp.StatusCode)
	io.Copy(w, resp.Body)
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}

// ---- durable state -----------------------------------------------------------

func load(path string) (*raft.Storage, error) {
	b, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return raft.NewStorage(), nil
	}
	if err != nil {
		return nil, err
	}
	var st raft.Storage
	if err := json.Unmarshal(b, &st); err != nil {
		return nil, fmt.Errorf("corrupt state file %s: %w", path, err)
	}
	return &st, nil
}

// save writes atomically: temp file, fsync, rename, fsync the directory.
func save(path string, st *raft.Storage) error {
	b, err := json.Marshal(st)
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	if _, err := f.Write(b); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	f.Close()
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	if d, err := os.Open(filepath.Dir(path)); err == nil {
		d.Sync()
		d.Close()
	}
	return nil
}
