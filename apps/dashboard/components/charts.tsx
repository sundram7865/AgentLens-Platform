/**
 * Hand-written SVG charts.
 *
 * Three charts do not justify a charting library: ~200 KB of bundle, a dozen
 * transitive packages in `npm audit`, and a client-side runtime for something
 * that renders perfectly well as static SVG from a Server Component. These
 * render on the server, ship no JavaScript, and use `<title>` for hover
 * readouts.
 */

import type { TimeBucket } from "@/lib/types";

interface Series {
  label: string;
  values: (number | null)[];
  color: string;
}

function niceMax(value: number): number {
  if (value <= 0) return 1;
  const magnitude = 10 ** Math.floor(Math.log10(value));
  return Math.ceil(value / magnitude) * magnitude;
}

/** Multi-series line chart over time buckets. */
export function LineChart({
  buckets,
  series,
  height = 160,
  format = (v: number) => String(Math.round(v)),
  emptyLabel = "No data in this window",
}: {
  buckets: TimeBucket[];
  series: Series[];
  height?: number;
  format?: (value: number) => string;
  emptyLabel?: string;
}) {
  if (buckets.length === 0) {
    return <div className="py-10 text-center text-xs text-ink-faint">{emptyLabel}</div>;
  }

  const width = 720;
  const padding = { top: 8, right: 8, bottom: 20, left: 44 };
  const plotWidth = width - padding.left - padding.right;
  const plotHeight = height - padding.top - padding.bottom;

  const allValues = series.flatMap((s) => s.values.filter((v): v is number => v !== null));
  const max = niceMax(Math.max(...allValues, 0));
  // A single bucket has no span to interpolate across, so pin it mid-plot.
  const x = (index: number) =>
    padding.left + (buckets.length === 1 ? plotWidth / 2 : (index / (buckets.length - 1)) * plotWidth);
  const y = (value: number) => padding.top + plotHeight - (value / max) * plotHeight;

  const gridLines = [0, 0.25, 0.5, 0.75, 1];

  return (
    <svg
      viewBox={`0 0 ${width} ${height}`}
      className="h-auto w-full"
      role="img"
      aria-label={series.map((s) => s.label).join(", ")}
    >
      {gridLines.map((fraction) => {
        const gy = padding.top + plotHeight - fraction * plotHeight;
        return (
          <g key={fraction}>
            <line
              x1={padding.left}
              x2={width - padding.right}
              y1={gy}
              y2={gy}
              stroke="var(--color-border)"
              strokeWidth={1}
            />
            <text x={padding.left - 6} y={gy + 3} textAnchor="end" className="fill-ink-faint text-[9px]">
              {format(max * fraction)}
            </text>
          </g>
        );
      })}

      {series.map((s) => {
        const points = s.values
          .map((value, index) => (value === null ? null : `${x(index)},${y(value)}`))
          .filter((p): p is string => p !== null);
        if (points.length === 0) return null;
        return (
          <g key={s.label}>
            <polyline
              points={points.join(" ")}
              fill="none"
              stroke={s.color}
              strokeWidth={1.75}
              strokeLinejoin="round"
              strokeLinecap="round"
            />
            {s.values.map((value, index) =>
              value === null ? null : (
                <circle key={index} cx={x(index)} cy={y(value)} r={2} fill={s.color}>
                  <title>{`${s.label}: ${format(value)}\n${new Date(
                    buckets[index]!.bucket,
                  ).toISOString().slice(0, 16).replace("T", " ")} UTC`}</title>
                </circle>
              ),
            )}
          </g>
        );
      })}

      <text x={padding.left} y={height - 6} className="fill-ink-faint text-[9px]">
        {new Date(buckets[0]!.bucket).toISOString().slice(5, 16).replace("T", " ")}
      </text>
      <text
        x={width - padding.right}
        y={height - 6}
        textAnchor="end"
        className="fill-ink-faint text-[9px]"
      >
        {new Date(buckets[buckets.length - 1]!.bucket)
          .toISOString()
          .slice(5, 16)
          .replace("T", " ")}
      </text>
    </svg>
  );
}

export function ChartLegend({ series }: { series: { label: string; color: string }[] }) {
  return (
    <div className="flex flex-wrap gap-4 pt-2">
      {series.map((s) => (
        <span key={s.label} className="flex items-center gap-1.5 text-[11px] text-ink-muted">
          <span className="inline-block h-2 w-2 rounded-full" style={{ background: s.color }} />
          {s.label}
        </span>
      ))}
    </div>
  );
}

/** Volume bars with errors stacked on top, so a spike in either is obvious. */
export function VolumeChart({ buckets, height = 110 }: { buckets: TimeBucket[]; height?: number }) {
  if (buckets.length === 0) {
    return <div className="py-8 text-center text-xs text-ink-faint">No traffic in this window</div>;
  }
  const width = 720;
  const padding = { top: 6, right: 8, bottom: 16, left: 44 };
  const plotWidth = width - padding.left - padding.right;
  const plotHeight = height - padding.top - padding.bottom;
  const max = niceMax(Math.max(...buckets.map((b) => b.traces), 1));
  const barWidth = Math.max(2, (plotWidth / buckets.length) * 0.7);

  return (
    <svg viewBox={`0 0 ${width} ${height}`} className="h-auto w-full" role="img" aria-label="Trace volume">
      <line
        x1={padding.left}
        x2={width - padding.right}
        y1={padding.top + plotHeight}
        y2={padding.top + plotHeight}
        stroke="var(--color-border)"
      />
      <text x={padding.left - 6} y={padding.top + 8} textAnchor="end" className="fill-ink-faint text-[9px]">
        {max}
      </text>
      {buckets.map((bucket, index) => {
        const cx = padding.left + (index + 0.5) * (plotWidth / buckets.length) - barWidth / 2;
        const total = (bucket.traces / max) * plotHeight;
        const errors = (bucket.errors / max) * plotHeight;
        return (
          <g key={bucket.bucket}>
            <rect
              x={cx}
              y={padding.top + plotHeight - total}
              width={barWidth}
              height={Math.max(total, bucket.traces > 0 ? 1 : 0)}
              fill="var(--color-accent)"
              opacity={0.55}
              rx={1}
            >
              <title>{`${bucket.traces} traces, ${bucket.errors} errors\n${new Date(
                bucket.bucket,
              ).toISOString().slice(0, 16).replace("T", " ")} UTC`}</title>
            </rect>
            {bucket.errors > 0 && (
              <rect
                x={cx}
                y={padding.top + plotHeight - errors}
                width={barWidth}
                height={Math.max(errors, 1)}
                fill="var(--color-danger)"
                rx={1}
              />
            )}
          </g>
        );
      })}
    </svg>
  );
}

/** Horizontal 0..1 meter for an eval score. */
export function ScoreBar({ label, value }: { label: string; value: number }) {
  const percent = Math.round(value * 100);
  const color =
    value >= 0.8 ? "var(--color-ok)" : value >= 0.6 ? "var(--color-warn)" : "var(--color-danger)";
  return (
    <div>
      <div className="flex items-baseline justify-between text-xs">
        <span className="text-ink-muted">{label.replace(/_/g, " ")}</span>
        <span className="font-mono text-ink">{value.toFixed(3)}</span>
      </div>
      <div className="mt-1 h-1.5 w-full overflow-hidden rounded-full bg-surface-2">
        <div className="h-full rounded-full" style={{ width: `${percent}%`, background: color }} />
      </div>
    </div>
  );
}
