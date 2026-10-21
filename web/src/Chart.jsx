import { useMemo } from "react";
import { segments } from "./model.js";

const LANE = 54; // vertical distance between node lanes
const TOP = 40;
const CLIENT_ROW = 16;
const GAP = 46; // between the last lane and the client strip
const LEFT = 72;
const RIGHT = 14;

// Heartbeats and their acknowledgements: most of the traffic, least of the story.
const routine = (m) => (m.type === "append" && m.entries === 0) || (m.type === "append-resp" && m.ok);

export function chartHeight(model) {
  return TOP + LANE * (model.nodes - 1) + GAP + CLIENT_ROW * model.clients + 30;
}

export default function Chart({ model, from, span, width, heartbeats }) {
  const to = Math.min(model.ticks, from + span);
  const plot = Math.max(240, width - LEFT - RIGHT);
  const x = (t) => LEFT + ((t - from) / span) * plot;
  const y = (node) => TOP + (node - 1) * LANE;
  const clientTop = y(model.nodes) + GAP;
  const cy = (client) => clientTop + (client - 1) * CLIENT_ROW;
  const height = chartHeight(model);
  const inWindow = (t) => t >= from && t < to;

  const lanes = useMemo(() => {
    const out = [];
    for (let n = 0; n < model.nodes; n++) {
      out.push({
        node: n + 1,
        roles: segments(model.states, n, from, to, (r) => (r[0] ? r[1] : -1)),
        split: segments(model.states, n, from, to, (r) => r[5]),
      });
    }
    return out;
  }, [model, from, to]);

  const view = useMemo(
    () => ({
      sends: model.sends.filter((m) => m.deliver >= from && m.tick < to && (heartbeats || !routine(m))),
      losses: model.losses.filter((l) => inWindow(l.tick) && (heartbeats || !routine(l))),
      ops: model.ops.filter((o) => (o.end ?? model.ticks) >= from && o.start < to),
      violations: model.violations.filter((v) => inWindow(v.at)),
      faults: model.faults.filter((f) => inWindow(f.t) && (f.kind === "crash" || f.kind === "restart")),
      elections: model.elections.filter((e) => inWindow(e.t)),
    }),
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [model, from, to, heartbeats],
  );

  const step = span <= 90 ? 10 : span <= 220 ? 20 : 50;
  const rulers = [];
  for (let t = Math.ceil(from / step) * step; t < to; t += step) rulers.push(t);

  return (
    <svg className="chart" width={width} height={height} role="img" aria-label={`Ticks ${from} to ${to} of seed ${model.seed}`}>
      <defs>
        <pattern id="split" width="5" height="5" patternUnits="userSpaceOnUse" patternTransform="rotate(45)">
          <line x1="0" y1="0" x2="0" y2="5" className="hatch" />
        </pattern>
        <clipPath id="plot">
          <rect x={LEFT} y={0} width={plot} height={height} />
        </clipPath>
      </defs>

      {rulers.map((t) => (
        <g key={t} className="ruler">
          <line x1={x(t)} x2={x(t)} y1={TOP - 18} y2={height - 20} />
          <text x={x(t)} y={TOP - 24}>
            {t}
          </text>
        </g>
      ))}

      {/* station names */}
      {lanes.map((l) => (
        <text key={l.node} className="lane-label" x={LEFT - 14} y={y(l.node) + 4}>
          Node {l.node}
        </text>
      ))}
      <text className="lane-label strip-label" x={LEFT - 14} y={clientTop - 14}>
        Clients
      </text>

      <g clipPath="url(#plot)">
        {/* the cut-off side of a partition */}
        {lanes.map((l) =>
          l.split
            .filter((s) => s.v === 1)
            .map((s) => (
              <rect key={`${l.node}-${s.from}`} x={x(s.from)} y={y(l.node) - LANE / 2 + 7} width={x(s.to) - x(s.from)} height={LANE - 14} fill="url(#split)" />
            )),
        )}

        {/* messages, sloped like trains running between stations */}
        {view.sends.map((m, i) => {
          const quiet = m.type === "append" && m.entries === 0;
          const cls = `msg msg-${m.type}${quiet ? " msg-quiet" : ""}`;
          return <line key={i} className={cls} x1={x(m.tick)} y1={y(m.from)} x2={x(m.deliver)} y2={y(m.to)} />;
        })}

        {/* node lanes: thick for leader, dashed for candidate, gone while crashed */}
        {lanes.map((l) =>
          l.roles
            .filter((s) => s.v >= 0)
            .map((s) => (
              <line key={`${l.node}-${s.from}`} className={`lane lane-${s.v}`} x1={x(s.from)} x2={x(s.to)} y1={y(l.node)} y2={y(l.node)} />
            )),
        )}

        {view.losses.map((l, i) => {
          const at = l.kind === "drop" ? y(l.to) : y(l.from);
          const cx = x(l.tick) + (l.kind === "lost" ? 3 : 0);
          return <path key={i} className="loss" d={`M${cx - 3} ${at - 3} l6 6 M${cx + 3} ${at - 3} l-6 6`} />;
        })}

        {view.elections.map((e) => (
          <text key={`${e.t}-${e.node}`} className="term" x={x(e.t) + 4} y={y(e.node) - 9}>
            term {e.term}
          </text>
        ))}

        {view.faults.map((f, i) => (
          <g key={i} className={`fault fault-${f.kind}`}>
            <circle cx={x(f.t)} cy={y(f.node)} r={f.kind === "crash" ? 5 : 4} />
          </g>
        ))}

        {/* client operations: each bar runs from request to reply */}
        {view.ops.map((o) => {
          const end = o.end ?? model.ticks;
          const x1 = x(Math.max(o.start, from - 1));
          const x2 = Math.max(x1 + 2, x(Math.min(end, to + 1)));
          const label = o.kind === "get" ? `get ${o.key}` : `${o.kind} ${o.key}`;
          return (
            <g key={o.id} className={`op op-${o.kind}${o.culprit ? " op-culprit" : ""}${o.end == null ? " op-open" : ""}`}>
              <rect x={x1} y={cy(o.client) - 5} width={x2 - x1} height={10} rx={2} />
              {x2 - Math.max(x1, LEFT) > 58 && (
                <text x={Math.max(x1, LEFT) + 5} y={cy(o.client) + 3.5}>
                  {label}
                </text>
              )}
              <title>{`#${o.id} client ${o.client}: ${o.kind} ${o.key}${o.value ? ` → "${o.value.trim()}"` : ""}${o.end == null ? " (no reply)" : ""}`}</title>
            </g>
          );
        })}

        {view.violations.map((v, i) => (
          <g key={i} className="violation">
            <line x1={x(v.at)} x2={x(v.at)} y1={TOP - 18} y2={height - 20} />
          </g>
        ))}
      </g>
    </svg>
  );
}
