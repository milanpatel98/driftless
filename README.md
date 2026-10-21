# Driftless

**Raft consensus in Go, checked by a deterministic simulator that tries to break it.**

Driftless is a replicated key-value store built on a from-scratch Raft implementation. The interesting part is how it is tested. A seeded simulator runs a five-node cluster through partitions, crashes, restarts, lost, duplicated and reordered messages, while clients read and write. Then a hand-written linearizability checker and a set of safety invariants decide whether anything went wrong.

Every run is reproducible from its seed. When something breaks, `driftless replay -seed N` reproduces it exactly, and the viewer shows it message by message.

**[Replay viewer](https://milanpatel98.github.io/driftless/)**

---

## Results

2,000 seeded runs per build. Each run lasts 1,600 ticks: about 22 injected faults, then a calm period where everything must recover.

| Build | Runs failed | First failing seed | Caught by | Client ops checked |
| --- | --- | --- | --- | --- |
| Correct Raft | **0** of 2,000 | none | | 373,675 |
| Stale reads (planted bug) | 291 of 2,000 | 10 | Linearizability | 479,198 |
| Forgotten vote (planted bug) | 20 of 2,000 | 20 | One vote per term | 373,174 |
| Figure 8 (planted bug) | 40 of 2,000 | 80 | Same entry at every index; committed entries overwritten | 374,530 |

Across the correct-build sweep: 27.6 million messages, 43,410 injected faults and 16,540 elections, with every history linearizable and every request completed. The sweep takes about a minute on a 2-core machine.

The planted bugs are the point. A test suite that has never caught anything proves nothing, so each bug is a real, well-known Raft mistake that can be switched on, and CI fails if the simulator ever stops catching one.

| Bug | What goes wrong | How it is caught |
| --- | --- | --- |
| `stale-reads` | The leader answers reads from memory. Cut off by a partition, a deposed leader keeps serving old values. | The history is not linearizable: a read returns a value that was already overwritten. |
| `forget-vote` | `votedFor` is not persisted. After a crash and restart a node votes twice in one term. | A node grants votes to two candidates in the same term. |
| `figure-8` | A new leader commits an older-term entry by counting replicas, the scenario in Figure 8 of the Raft paper. | A later leader overwrites a committed entry, and nodes apply different commands at the same index. |

## How it works

```
                 seed
                  │
        ┌─────────▼──────────┐   partitions, crashes, restarts,
        │  simulator (sim/)  │◄─ loss, duplication, delay, reordering,
        │  one RNG, one loop │   clients reading and writing
        └─────────┬──────────┘
                  │ Tick / Step / Propose        Ready → messages, committed entries
        ┌─────────▼──────────┐
        │  raft.Node × 5     │   pure state machines: no clocks, sockets or disks
        └─────────┬──────────┘
                  │ committed commands
        ┌─────────▼──────────┐
        │  kv.Store × 5      │   put / get / append, exactly-once retries
        └─────────┬──────────┘
                  │ client history, per-tick state
        ┌─────────▼──────────┐
        │  checkers          │   linearizability, one leader per term, one vote
        └────────────────────┘   per term, same entry at every index, liveness
```

**The Raft node is a pure state machine.** `raft.Node` never touches the network, disk or clock. It takes `Tick()`, `Step(msg)` and `Propose(cmd)`, and returns messages to send and entries to apply. The same code runs under the simulator and in the real networked cluster, so the simulator tests the code that ships.

**Determinism.** Every random choice (message delay, loss, duplication, partitions, crashes, election timeouts, client timing) comes from one seeded generator, and events at the same tick are delivered in a fixed order. A test asserts that the same seed produces byte-identical runs.

**The linearizability checker** is the Wing & Gong search with Lowe's memoisation, the algorithm behind Porcupine and Knossos, written from scratch in about 250 lines. Keys are checked independently. Requests that never got a reply are allowed to have taken effect, or not, at any point after they were sent. When a history fails, it reports the operations no valid order can explain.

**Faults are realistic, not random noise.** The injector partitions the cluster randomly, isolates the current leader (the classic split-brain setup), crashes nodes, restarts them from their last durable state, and bounces nodes (kill and immediate restart). After the fault phase, everything heals and the cluster must finish every open request, which catches liveness bugs.

**Exactly-once client semantics.** Clients retry on timeout or leader change with the same `(client, seq)`. The state machine remembers each client's last result, so a retried write is applied once.

### Implementation details worth noting

- A new leader appends a no-op entry, and only counts replicas for entries from its own term (§5.4.2). The `figure-8` bug removes exactly this.
- Followers reject an inconsistent append with a hint, so the leader skips back over a whole conflicting term in one round trip instead of one entry at a time.
- Candidates retransmit vote requests to peers that have not answered (§5.1), so one lost message does not stall an election.
- Reads go through the log. That costs a round of replication, but it is what makes them linearizable. The `stale-reads` bug shows the cost of the shortcut.
- In cluster mode, state is written with temp file, `fsync`, rename and directory `fsync` before any message from the same step is sent.

## Run it

Requires Go 1.22+. For the viewer, Node 20+.

```bash
git clone https://github.com/milanpatel98/driftless.git
cd driftless
go test ./...

# Hunt for bugs: 2,000 seeds in parallel
go run ./cmd/driftless hunt -seeds 2000
go run ./cmd/driftless hunt -seeds 2000 -bug stale-reads

# Replay one seed exactly and write its trace
go run ./cmd/driftless replay -seed 10 -bug stale-reads -out web/public/runs/stale-reads.json

# Watch it
cd web && npm install && npm run dev        # http://localhost:5179
```

### A real cluster

```bash
./scripts/cluster.sh                         # three nodes on :7001 :7002 :7003

curl -X PUT  localhost:7001/kv/guest -d "Milan"     # any node; followers forward to the leader
curl -X POST localhost:7002/kv/guest -d " Patel"    # append
curl         localhost:7003/kv/guest                # linearizable read
curl         localhost:7001/status                  # role, term, commit index, data
```

Kill the leader (its terminal line shows `now leader`) and keep writing: the other two elect a new leader within about a second. Restart it and it catches up from disk.

## Project layout

```
raft/      the protocol: elections, replication, commit; pure and deterministic
kv/        the replicated state machine with exactly-once retries
check/     linearizability checker
sim/       deterministic cluster simulator, fault injector, invariants, traces
server/    real node: HTTP transport, fsync'd state, client API
cmd/       the driftless command: hunt, replay, node
web/       replay viewer (React + Vite)
```

## Limitations

- No log compaction or snapshots: the log grows without bound. Fine for a simulator and a demo cluster, not for production.
- Membership is fixed at startup; no joint consensus.
- Cluster mode uses JSON over HTTP for clarity, not efficiency.
- A clean sweep is evidence, not proof. The planted bugs show the checks have teeth; they do not show that no other bug exists.

## License

MIT
