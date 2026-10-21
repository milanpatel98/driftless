// Command driftless: hunt for consensus bugs with a deterministic simulator,
// replay any run exactly, or run a real cluster.
//
//	driftless hunt   -seeds 2000 [-bug stale-reads|forget-vote|figure-8]
//	driftless replay -seed 42 [-bug ...] -out trace.json
//	driftless node   -id 1 -cluster 1=127.0.0.1:7001,2=127.0.0.1:7002,3=127.0.0.1:7003
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/milanpatel98/driftless/raft"
	"github.com/milanpatel98/driftless/server"
	"github.com/milanpatel98/driftless/sim"
)

var bugNames = map[string]raft.Bugs{
	"":            {},
	"none":        {},
	"stale-reads": {StaleReads: true},
	"forget-vote": {ForgetVote: true},
	"figure-8":    {CommitOldTerm: true},
}

func main() {
	if len(os.Args) < 2 {
		usage()
	}
	switch os.Args[1] {
	case "hunt":
		hunt(os.Args[2:])
	case "replay":
		replay(os.Args[2:])
	case "node":
		node(os.Args[2:])
	default:
		usage()
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, `driftless: Raft, checked by a deterministic simulator

  hunt    run many seeded simulations in parallel and report any violation
  replay  re-run one seed exactly and write its full trace for the viewer
  node    run one node of a real cluster

Run "driftless <command> -h" for flags.`)
	os.Exit(2)
}

func parseBug(name string) raft.Bugs {
	b, ok := bugNames[name]
	if !ok {
		fmt.Fprintf(os.Stderr, "unknown bug %q; choose stale-reads, forget-vote or figure-8\n", name)
		os.Exit(2)
	}
	return b
}

// ---- hunt -------------------------------------------------------------------

type huntReport struct {
	Bug        string          `json:"bug"`
	Seeds      int             `json:"seeds"`
	Failed     int             `json:"failed"`
	FirstSeed  int64           `json:"firstFailingSeed"`
	ByKind     map[string]int  `json:"byKind"`
	Ops        int             `json:"ops"`
	Messages   int             `json:"messages"`
	Faults     int             `json:"faults"`
	Elections  int             `json:"elections"`
	Seconds    float64         `json:"seconds"`
	Failures   []sim.Violation `json:"sampleViolations,omitempty"`
	FailedSeed []int64         `json:"failedSeeds,omitempty"`
}

func hunt(args []string) {
	fs := flag.NewFlagSet("hunt", flag.ExitOnError)
	seeds := fs.Int("seeds", 1000, "number of seeds to try")
	start := fs.Int64("from", 1, "first seed")
	bug := fs.String("bug", "", "plant a bug: stale-reads, forget-vote, figure-8")
	ticks := fs.Int("ticks", 1200, "ticks of faults per run")
	out := fs.String("out", "", "write a JSON report here")
	fs.Parse(args)
	bugs := parseBug(*bug)

	label := *bug
	if label == "" {
		label = "none"
	}
	fmt.Printf("hunting: %d seeds from %d, bug=%s, %d workers\n", *seeds, *start, label, runtime.NumCPU())

	t0 := time.Now()
	results := make([]sim.Result, *seeds)
	var wg sync.WaitGroup
	jobs := make(chan int)
	for w := 0; w < runtime.NumCPU(); w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range jobs {
				c := sim.Defaults(*start + int64(i))
				c.Bugs, c.Ticks = bugs, *ticks
				r := sim.Run(c)
				r.History = nil
				results[i] = r
			}
		}()
	}
	for i := 0; i < *seeds; i++ {
		jobs <- i
	}
	close(jobs)
	wg.Wait()

	rep := huntReport{Bug: label, Seeds: *seeds, FirstSeed: -1, ByKind: map[string]int{}}
	for _, r := range results {
		rep.Ops += r.Ops
		rep.Messages += r.Messages
		rep.Faults += r.Faults
		rep.Elections += r.Elections
		if r.OK() {
			continue
		}
		rep.Failed++
		rep.FailedSeed = append(rep.FailedSeed, r.Seed)
		if rep.FirstSeed < 0 {
			rep.FirstSeed = r.Seed
			rep.Failures = r.Violations
		}
		seen := map[string]bool{}
		for _, v := range r.Violations {
			if !seen[v.Kind] {
				rep.ByKind[v.Kind]++
				seen[v.Kind] = true
			}
		}
	}
	rep.Seconds = time.Since(t0).Seconds()

	fmt.Printf("\n%d runs · %s client ops · %s messages · %s injected faults · %s elections · %.1fs\n",
		rep.Seeds, comma(rep.Ops), comma(rep.Messages), comma(rep.Faults), comma(rep.Elections), rep.Seconds)
	if rep.Failed == 0 {
		fmt.Println("no violations: every history linearizable, every invariant held, every request completed")
	} else {
		var kinds []string
		for k, n := range rep.ByKind {
			kinds = append(kinds, fmt.Sprintf("%s in %d", k, n))
		}
		sort.Strings(kinds)
		fmt.Printf("%d of %d runs failed (%s)\n", rep.Failed, rep.Seeds, strings.Join(kinds, ", "))
		fmt.Printf("first failing seed: %d\n", rep.FirstSeed)
		for _, v := range rep.Failures {
			fmt.Printf("  tick %d  %s: %s\n", v.Tick, v.Kind, v.Detail)
		}
		flagBug := ""
		if *bug != "" {
			flagBug = " -bug " + *bug
		}
		fmt.Printf("\nreplay it: driftless replay -seed %d%s -out seed-%d.json\n", rep.FirstSeed, flagBug, rep.FirstSeed)
	}
	if *out != "" {
		writeFile(*out, rep)
	}
	if rep.Failed > 0 && *bug == "" {
		os.Exit(1)
	}
}

// ---- replay -------------------------------------------------------------------

func replay(args []string) {
	fs := flag.NewFlagSet("replay", flag.ExitOnError)
	seed := fs.Int64("seed", 1, "seed to replay")
	bug := fs.String("bug", "", "plant a bug: stale-reads, forget-vote, figure-8")
	out := fs.String("out", "", "write the full trace JSON here")
	fs.Parse(args)
	c := sim.Defaults(*seed)
	c.Bugs = parseBug(*bug)
	c.Trace = true
	r := sim.Run(c)
	fmt.Printf("seed %d: %d ops, %d elections, %d faults, %d messages (%d lost or blocked)\n",
		r.Seed, r.Ops, r.Elections, r.Faults, r.Messages, r.Dropped)
	if r.OK() {
		fmt.Println("no violations")
	}
	for _, v := range r.Violations {
		fmt.Printf("  tick %d  %s: %s\n", v.Tick, v.Kind, v.Detail)
	}
	if *out != "" {
		writeFile(*out, r)
	}
}

// ---- node ----------------------------------------------------------------------

func node(args []string) {
	fs := flag.NewFlagSet("node", flag.ExitOnError)
	id := fs.Int("id", 1, "this node's id")
	cluster := fs.String("cluster", "1=127.0.0.1:7001,2=127.0.0.1:7002,3=127.0.0.1:7003", "id=host:port for every member")
	data := fs.String("data", "data", "directory for durable state")
	fs.Parse(args)

	addrs := map[int]string{}
	for _, part := range strings.Split(*cluster, ",") {
		k, v, ok := strings.Cut(part, "=")
		n, err := strconv.Atoi(k)
		if !ok || err != nil {
			fmt.Fprintf(os.Stderr, "bad -cluster entry %q; want id=host:port\n", part)
			os.Exit(2)
		}
		addrs[n] = v
	}
	if _, ok := addrs[*id]; !ok {
		fmt.Fprintf(os.Stderr, "node %d is not in -cluster\n", *id)
		os.Exit(2)
	}
	s, err := server.New(server.Config{ID: *id, Addrs: addrs, DataDir: *data})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	if err := s.Run(ctx); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// ---- helpers ----------------------------------------------------------------------

func writeFile(path string, v any) {
	b, err := json.Marshal(v)
	if err == nil {
		err = os.WriteFile(path, b, 0o644)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println("wrote", path)
}

func comma(n int) string {
	s := strconv.Itoa(n)
	for i := len(s) - 3; i > 0; i -= 3 {
		s = s[:i] + "," + s[i:]
	}
	return s
}
