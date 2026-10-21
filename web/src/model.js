// Turns a simulator trace into what the chart draws.

export const SCENARIOS = [
  {
    id: "healthy",
    title: "Correct Raft",
    blurb: "No planted bug. Partitions, crashes, lost and duplicated messages, and every history still checks out.",
  },
  {
    id: "stale-reads",
    title: "Stale reads",
    blurb: "The leader answers reads from memory without checking it is still leader. Cut off by a partition, it keeps serving old data.",
  },
  {
    id: "forget-vote",
    title: "Forgotten vote",
    blurb: "A node does not save who it voted for. After a crash and restart it votes a second time in the same term.",
  },
  {
    id: "figure-8",
    title: "Figure 8",
    blurb: "A new leader commits an old entry by counting copies, the mistake from Figure 8 of the Raft paper. A later leader overwrites it.",
  },
];

export const ROLE = ["follower", "candidate", "leader"];

export const KIND_TEXT = {
  "linearizability": "Linearizability",
  "vote-safety": "One vote per term",
  "election-safety": "One leader per term",
  "state-machine-safety": "Same entry at every index",
  "log-overwrite": "Committed entries are permanent",
  "liveness": "Every request finishes",
};

const msg = (m) => ({ type: m.t, from: m.f, to: m.to, term: m.term, entries: m.n || 0, ok: !!m.ok });

export function buildModel(result) {
  const trace = result.trace;
  const nodes = trace.nodes;
  const states = trace.states;
  const ticks = states.length;

  const sends = [];
  const losses = [];
  const faults = [];
  const elections = [];
  const ops = new Map();

  for (const e of trace.events) {
    switch (e.kind) {
      case "send":
        sends.push({ tick: e.tick, deliver: e.deliver, ...msg(e.msg) });
        break;
      case "lost":
      case "drop":
        losses.push({ tick: e.tick, kind: e.kind, ...msg(e.msg) });
        break;
      case "crash":
      case "restart":
        faults.push({ t: e.tick, kind: e.kind, node: e.node });
        break;
      case "partition":
        faults.push({ t: e.tick, kind: "partition", groups: e.groups });
        break;
      case "heal":
        faults.push({ t: e.tick, kind: "heal" });
        break;
      case "elected":
        elections.push({ t: e.tick, node: e.node, term: e.term });
        break;
      case "invoke":
        ops.set(e.op, { id: e.op, client: e.client, kind: e.opKind, key: e.key, start: e.tick, end: null, value: "" });
        break;
      case "return": {
        const op = ops.get(e.op);
        if (op) {
          op.end = e.tick;
          op.value = e.value || "";
        }
        break;
      }
    }
  }

  const culprits = new Set((result.linearizability?.unexplained || []).slice(0, 2));
  const opList = [...ops.values()];
  for (const op of opList) op.culprit = culprits.has(op.id);

  const violations = (result.violations || []).map((v) => {
    let t = v.tick;
    if (v.kind === "linearizability") {
      const first = opList.find((o) => o.culprit && o.end != null);
      if (first) t = first.end;
    }
    return { ...v, at: Math.min(t, ticks - 1) };
  });

  return {
    seed: result.seed,
    nodes,
    ticks,
    states,
    sends,
    losses,
    faults,
    elections,
    ops: opList,
    clients: Math.max(0, ...opList.map((o) => o.client)),
    violations,
    summary: {
      ops: result.ops,
      completed: result.completed,
      elections: result.elections,
      faults: result.faults,
      messages: result.messages,
      dropped: result.dropped,
    },
  };
}

// Runs of a per-tick value for one node, clipped to [from, to).
export function segments(states, node, from, to, pick) {
  const out = [];
  let cur = null;
  for (let t = from; t < to; t++) {
    const v = pick(states[t][node]);
    if (cur && cur.v === v) {
      cur.to = t + 1;
    } else {
      cur = { v, from: t, to: t + 1 };
      out.push(cur);
    }
  }
  return out;
}

export const fmt = (n) => (n ?? 0).toLocaleString("en-US");
