// Package kv is the state machine replicated by Raft: a string key-value store
// with put, get and append, plus exactly-once semantics for retried requests.
package kv

import "encoding/json"

type OpKind string

const (
	Put    OpKind = "put"
	Get    OpKind = "get"
	Append OpKind = "append"
)

// Command is what clients submit and what lands in the Raft log.
// Client and Seq identify a request so a retry is applied at most once.
type Command struct {
	Client int    `json:"c"`
	Seq    int    `json:"s"`
	Kind   OpKind `json:"k"`
	Key    string `json:"key"`
	Value  string `json:"v,omitempty"`
}

func (c Command) Encode() []byte {
	b, _ := json.Marshal(c)
	return b
}

func Decode(b []byte) (Command, error) {
	var c Command
	err := json.Unmarshal(b, &c)
	return c, err
}

type Result struct {
	Value string `json:"value"`
	Found bool   `json:"found"`
}

type session struct {
	seq    int
	result Result
}

type Store struct {
	data     map[string]string
	sessions map[int]session
}

func New() *Store {
	return &Store{data: map[string]string{}, sessions: map[int]session{}}
}

// Apply executes a committed command. A command whose (client, seq) was
// already applied returns the cached result instead of running again: this
// is what makes client retries safe after a timeout or a leader change.
func (s *Store) Apply(c Command) Result {
	if sess, ok := s.sessions[c.Client]; ok && c.Client != 0 && c.Seq <= sess.seq {
		return sess.result
	}
	var r Result
	switch c.Kind {
	case Put:
		s.data[c.Key] = c.Value
		r = Result{Value: c.Value, Found: true}
	case Append:
		s.data[c.Key] += c.Value
		r = Result{Value: s.data[c.Key], Found: true}
	case Get:
		v, ok := s.data[c.Key]
		r = Result{Value: v, Found: ok}
	}
	if c.Client != 0 {
		s.sessions[c.Client] = session{seq: c.Seq, result: r}
	}
	return r
}

// Read answers a get from local state without consensus. Only the StaleReads
// bug uses this path; correct reads go through the log.
func (s *Store) Read(key string) Result {
	v, ok := s.data[key]
	return Result{Value: v, Found: ok}
}

func (s *Store) Snapshot() map[string]string {
	out := make(map[string]string, len(s.data))
	for k, v := range s.data {
		out[k] = v
	}
	return out
}
