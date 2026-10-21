import { useEffect, useLayoutEffect, useMemo, useRef, useState } from "react";
import Chart from "./Chart.jsx";
import Minimap from "./Minimap.jsx";
import { KIND_TEXT, SCENARIOS, buildModel, fmt } from "./model.js";

const ZOOMS = [
  { span: 40, label: "40 ticks" },
  { span: 100, label: "100 ticks" },
  { span: 300, label: "300 ticks" },
];

function useWidth() {
  const ref = useRef(null);
  const [width, setWidth] = useState(960);
  useLayoutEffect(() => {
    const el = ref.current;
    const ro = new ResizeObserver(([e]) => setWidth(Math.floor(e.contentRect.width)));
    ro.observe(el);
    return () => ro.disconnect();
  }, []);
  return [ref, width];
}

const fromHash = () => {
  const id = window.location.hash.replace(/^#\/?/, "");
  return SCENARIOS.some((s) => s.id === id) ? id : "healthy";
};

export default function App() {
  const [scenario, setScenario] = useState(fromHash);
  const [runs, setRuns] = useState({});
  const [hunts, setHunts] = useState({});
  const [error, setError] = useState("");

  useEffect(() => {
    const onHash = () => setScenario(fromHash());
    window.addEventListener("hashchange", onHash);
    return () => window.removeEventListener("hashchange", onHash);
  }, []);

  useEffect(() => {
    if (runs[scenario]) return;
    fetch(`runs/${scenario}.json`)
      .then((r) => (r.ok ? r.json() : Promise.reject(r.status)))
      .then((data) => setRuns((prev) => ({ ...prev, [scenario]: buildModel(data) })))
      .catch(() => setError(`Could not load runs/${scenario}.json. Generate it with "driftless replay -out".`));
  }, [scenario, runs]);

  useEffect(() => {
    for (const name of ["none", "stale-reads", "forget-vote", "figure-8"]) {
      fetch(`runs/hunt-${name}.json`)
        .then((r) => (r.ok ? r.json() : null))
        .then((h) => h && setHunts((prev) => ({ ...prev, [name]: h })));
    }
  }, []);

  const meta = SCENARIOS.find((s) => s.id === scenario);
  const model = runs[scenario];

  return (
    <div className="shell">
      <header className="hero">
        <div className="bar">
          <span>(Driftless – 02)</span>
          <a href="https://github.com/milanpatel98/driftless">Source</a>
        </div>
        <h1 className="word" aria-label="Driftless">
          <span aria-hidden="true">Driftless</span>
        </h1>
        <p className="hero-line">
          Raft consensus, checked by a
          <br />
          deterministic simulator that tries to break it
        </p>
      </header>

      <div className="page">
      <section className="lead">
        <p className="lead-text">
          {hunts.none ? (
            <>
              {fmt(hunts.none.seeds)} seeded runs. {fmt(hunts.none.faults)} injected crashes and partitions.{" "}
              <span className="lead-strong">{hunts.none.failed === 0 ? "Zero" : fmt(hunts.none.failed)} broken invariants.</span>
            </>
          ) : (
            "Five nodes, one seed, every message on the record."
          )}
        </p>
      </section>

      <nav className="scenarios" aria-label="Scenarios">
        <h2 className="scenarios-title">Replays</h2>
        <ul>
          {SCENARIOS.map((s, i) => (
            <li key={s.id}>
              <a href={`#/${s.id}`} aria-current={s.id === scenario ? "page" : undefined}>
                <span className="scenario-title">{s.title}</span>
                <span className="scenario-note">{s.id === "healthy" ? "No bug" : "Planted bug"}</span>
              </a>
            </li>
          ))}
        </ul>
      </nav>

      <p className="blurb">{meta.blurb}</p>

      {error && <p className="notice">{error}</p>}
      {model ? <Replay key={scenario} model={model} /> : !error && <p className="loading">Loading seed…</p>}

      <Hunts hunts={hunts} />

      <footer className="colophon">
        <p>Built by Milan Patel in Go, standard library only.</p>
        <p>
          Reproduce this run: <code>driftless replay -seed {model?.seed ?? "N"}{scenario !== "healthy" ? ` -bug ${scenario}` : ""}</code>
        </p>
      </footer>
      </div>
    </div>
  );
}

function Replay({ model }) {
  const [zoom, setZoom] = useState(1);
  const [heartbeats, setHeartbeats] = useState(false);
  const span = Math.min(ZOOMS[zoom].span, model.ticks);
  const first = model.violations[0];
  const [from, setFrom] = useState(() => (first ? Math.max(0, first.at - Math.round(span * 0.7)) : 0));
  const [ref, width] = useWidth();
  const clamp = (t) => Math.max(0, Math.min(model.ticks - span, t));
  const s = model.summary;

  const verdict = useMemo(() => {
    if (!model.violations.length) return null;
    const kinds = [...new Set(model.violations.map((v) => v.kind))];
    return kinds.map((k) => KIND_TEXT[k] || k);
  }, [model]);

  return (
    <section className="replay" aria-label={`Seed ${model.seed}`}>
      <div className="facts">
        <p>
          Seed <strong>{model.seed}</strong>: {fmt(s.ops)} client operations, {fmt(s.elections)} elections, {fmt(s.faults)} injected faults,{" "}
          {fmt(s.messages)} messages of which {fmt(s.dropped)} were lost or blocked.
        </p>
        {verdict ? (
          <div className="verdict verdict-broken">
            <p>
              Broken invariant: <strong>{verdict.join(", ")}</strong>.
            </p>
            <p className="verdict-detail">{model.violations[0].detail}</p>
            <button type="button" onClick={() => setFrom(clamp(first.at - Math.round(span * 0.7)))}>
              Show the moment it broke (tick {first.at})
            </button>
          </div>
        ) : (
          <div className="verdict verdict-held">
            <p>
              Every invariant held, every request completed, and the history of {fmt(s.ops)} operations is linearizable.
            </p>
          </div>
        )}
      </div>

      <div className="controls">
        <div className="zoom" role="group" aria-label="Zoom">
          {ZOOMS.map((z, i) => (
            <button key={z.span} type="button" aria-pressed={i === zoom} onClick={() => setZoom(i)}>
              {z.label}
            </button>
          ))}
        </div>
        <label className="toggle">
          <input type="checkbox" checked={heartbeats} onChange={(e) => setHeartbeats(e.target.checked)} />
          Show heartbeats
        </label>
        <div className="pan">
          <button type="button" onClick={() => setFrom(clamp(from - Math.round(span / 2)))} aria-label="Earlier">
            Earlier
          </button>
          <span className="window">
            ticks {from}–{Math.min(model.ticks, from + span)}
          </span>
          <button type="button" onClick={() => setFrom(clamp(from + Math.round(span / 2)))} aria-label="Later">
            Later
          </button>
        </div>
      </div>

      <div className="chart-wrap" ref={ref}>
        <Chart model={model} from={from} span={span} width={width} heartbeats={heartbeats} />
        <Minimap model={model} from={from} span={span} width={width} onSeek={setFrom} />
      </div>

      <Legend />
    </section>
  );
}

function Legend() {
  return (
    <dl className="legend">
      <div>
        <dt><svg width="34" height="10"><line className="lane lane-2" x1="2" x2="32" y1="5" y2="5" /></svg></dt>
        <dd>Leader</dd>
      </div>
      <div>
        <dt><svg width="34" height="10"><line className="lane lane-1" x1="2" x2="32" y1="5" y2="5" /></svg></dt>
        <dd>Candidate</dd>
      </div>
      <div>
        <dt><svg width="34" height="10"><line className="msg msg-vote" x1="2" x2="32" y1="9" y2="1" /></svg></dt>
        <dd>Vote request</dd>
      </div>
      <div>
        <dt><svg width="34" height="10"><line className="msg msg-append" x1="2" x2="32" y1="9" y2="1" /></svg></dt>
        <dd>Log entries</dd>
      </div>
      <div>
        <dt><svg width="14" height="10"><path className="loss" d="M4 2 l6 6 M10 2 l-6 6" /></svg></dt>
        <dd>Message lost</dd>
      </div>
      <div>
        <dt><svg width="34" height="10"><rect x="2" y="0" width="30" height="10" fill="url(#split)" /></svg></dt>
        <dd>Cut off by a partition</dd>
      </div>
      <div>
        <dt><svg width="14" height="12"><circle className="fault fault-crash" cx="7" cy="6" r="5" /></svg></dt>
        <dd>Crash</dd>
      </div>
      <div>
        <dt><svg width="34" height="10"><g className="op op-put"><rect x="2" y="0.5" width="30" height="9" rx="2" /></g></svg></dt>
        <dd>Client request, call to reply</dd>
      </div>
    </dl>
  );
}

function Hunts({ hunts }) {
  const rows = [
    ["none", "Correct Raft"],
    ["stale-reads", "Stale reads"],
    ["forget-vote", "Forgotten vote"],
    ["figure-8", "Figure 8"],
  ].filter(([k]) => hunts[k]);
  if (!rows.length) return null;
  return (
    <section className="hunts" aria-labelledby="hunts-title">
      <h2 id="hunts-title">Bug hunts</h2>
      <p className="hunts-intro">
        Each line is {fmt(hunts.none?.seeds)} independent seeded runs: random partitions, crashes, restarts, lost and duplicated
        messages, with four clients writing and reading the whole time.
      </p>
      <div className="table-scroll">
        <table>
          <thead>
            <tr>
              <th scope="col">Build</th>
              <th scope="col" className="num">Runs failed</th>
              <th scope="col" className="num">First failing seed</th>
              <th scope="col">Caught by</th>
              <th scope="col" className="num">Client ops checked</th>
            </tr>
          </thead>
          <tbody>
            {rows.map(([k, label]) => {
              const h = hunts[k];
              const caught = Object.keys(h.byKind || {}).map((kind) => KIND_TEXT[kind] || kind);
              return (
                <tr key={k} className={h.failed ? "row-failed" : "row-clean"}>
                  <th scope="row">{label}</th>
                  <td className="num">
                    {fmt(h.failed)} of {fmt(h.seeds)}
                  </td>
                  <td className="num">{h.firstFailingSeed > 0 ? h.firstFailingSeed : "none"}</td>
                  <td>{caught.length ? caught.join(", ") : "nothing to catch"}</td>
                  <td className="num">{fmt(h.ops)}</td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </section>
  );
}
