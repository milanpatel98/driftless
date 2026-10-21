import { useMemo, useRef } from "react";

// The whole run at a glance: who led when, where the faults hit, where it broke.
// Click or drag to move the chart's window.
export default function Minimap({ model, from, span, width, onSeek }) {
  const H = 8;
  const height = model.nodes * H + 18;
  const scale = width / model.ticks;
  const ref = useRef(null);

  const bands = useMemo(() => {
    const out = [];
    for (let n = 0; n < model.nodes; n++) {
      let cur = null;
      for (let t = 0; t < model.ticks; t++) {
        const r = model.states[t][n];
        const v = !r[0] ? "down" : r[1] === 2 ? "lead" : r[5] === 1 ? "cut" : null;
        if (cur && cur.v === v) {
          cur.to = t + 1;
          continue;
        }
        cur = { v, n, from: t, to: t + 1 };
        out.push(cur);
      }
    }
    return out.filter((b) => b.v);
  }, [model]);

  const seek = (clientX) => {
    const rect = ref.current.getBoundingClientRect();
    const t = Math.round((clientX - rect.left) / scale - span / 2);
    onSeek(Math.max(0, Math.min(model.ticks - span, t)));
  };

  return (
    <svg
      ref={ref}
      className="minimap"
      width={width}
      height={height}
      onPointerDown={(e) => {
        e.currentTarget.setPointerCapture(e.pointerId);
        seek(e.clientX);
      }}
      onPointerMove={(e) => e.buttons && seek(e.clientX)}
      role="slider"
      aria-label="Position in the run"
      aria-valuemin={0}
      aria-valuemax={model.ticks}
      aria-valuenow={from}
      tabIndex={0}
      onKeyDown={(e) => {
        const by = e.shiftKey ? span : Math.round(span / 4);
        if (e.key === "ArrowRight") onSeek(Math.min(model.ticks - span, from + by));
        if (e.key === "ArrowLeft") onSeek(Math.max(0, from - by));
      }}
    >
      {Array.from({ length: model.nodes }, (_, n) => (
        <rect key={n} className="mm-lane" x={0} y={n * H + 2} width={width} height={H - 3} />
      ))}
      {bands.map((b, i) => (
        <rect key={i} className={`mm-${b.v}`} x={b.from * scale} y={b.n * H + 2} width={Math.max(1, (b.to - b.from) * scale)} height={H - 3} />
      ))}
      {model.violations.map((v, i) => (
        <rect key={i} className="mm-violation" x={v.at * scale - 1} y={0} width={2.5} height={height - 14} />
      ))}
      <rect className="mm-window" x={from * scale} y={0.5} width={span * scale} height={height - 14} />
      <text className="mm-axis" x={0} y={height - 2}>
        tick 0
      </text>
      <text className="mm-axis" x={width} y={height - 2} textAnchor="end">
        {model.ticks}
      </text>
    </svg>
  );
}
