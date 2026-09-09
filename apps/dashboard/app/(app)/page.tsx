import Link from "next/link";
import { ChartLegend, LineChart, ScoreBar, VolumeChart } from "@/components/charts";
import {
  ApiUnavailable,
  Badge,
  Card,
  Cost,
  EmptyState,
  Stat,
  Timestamp,
  TraceLink,
} from "@/components/ui";
import { ApiError, api, requireSession } from "@/lib/api";

export const dynamic = "force-dynamic";

const WINDOWS = [
  { hours: 1, label: "1h" },
  { hours: 24, label: "24h" },
  { hours: 168, label: "7d" },
];

export default async function OverviewPage({
  searchParams,
}: {
  searchParams: Promise<{ hours?: string; tenant_id?: string }>;
}) {
  const params = await searchParams;
  const hours = String(Number(params.hours) || 24);
  const tenantId = params.tenant_id;

  try {
    const [overview, series, alerts, recent, drift] = await Promise.all([
      requireSession(() => api.overview({ hours, tenant_id: tenantId })),
      api.timeseries({ hours, tenant_id: tenantId, bucket: Number(hours) <= 2 ? "minute" : "hour" }),
      api.alerts({ status: "open", limit: "5", tenant_id: tenantId }),
      api.traces({ limit: "8", tenant_id: tenantId }),
      api.drift({ hours, tenant_id: tenantId, limit: "40" }),
    ]);

    const latencySeries = [
      { label: "p50", values: series.map((b) => b.p50_latency_ms), color: "var(--color-accent)" },
      { label: "p95", values: series.map((b) => b.p95_latency_ms), color: "var(--color-warn)" },
    ];
    const costSeries = [
      { label: "cost (USD)", values: series.map((b) => b.cost_usd), color: "var(--color-ok)" },
    ];
    const drifted = drift.filter((point) => point.drifted);

    return (
      <div className="space-y-5">
        <div className="flex flex-wrap items-center justify-between gap-3">
          <h1 className="text-base font-semibold">Overview</h1>
          <div className="flex gap-1 rounded-md border border-border bg-surface p-0.5">
            {WINDOWS.map((option) => (
              <Link
                key={option.hours}
                href={`/?hours=${option.hours}${tenantId ? `&tenant_id=${tenantId}` : ""}`}
                className={`rounded px-2.5 py-1 text-xs transition ${
                  Number(hours) === option.hours
                    ? "bg-accent-dim text-ink"
                    : "text-ink-muted hover:text-ink"
                }`}
              >
                {option.label}
              </Link>
            ))}
          </div>
        </div>

        <div className="grid grid-cols-2 gap-3 lg:grid-cols-6">
          <Stat label="Traces" value={overview.traces.toLocaleString()} sub={`last ${hours}h`} />
          <Stat
            label="Error rate"
            value={`${(overview.error_rate * 100).toFixed(1)}%`}
            sub={`${overview.errors} failed`}
            tone={overview.error_rate > 0.05 ? "danger" : overview.errors ? "warn" : "ok"}
          />
          <Stat
            label="p95 latency"
            value={overview.p95_latency_ms ? `${Math.round(overview.p95_latency_ms)}ms` : "—"}
            sub={overview.p99_latency_ms ? `p99 ${Math.round(overview.p99_latency_ms)}ms` : undefined}
            tone={(overview.p95_latency_ms ?? 0) > 5000 ? "warn" : "neutral"}
          />
          <Stat
            label="Flagged"
            value={overview.flagged.toLocaleString()}
            sub={`${(overview.flagged_rate * 100).toFixed(1)}% of traffic`}
            tone={overview.flagged ? "warn" : "ok"}
          />
          <Stat
            label="Agent cost"
            value={`$${overview.cost_usd.toFixed(4)}`}
            sub={`${overview.total_tokens.toLocaleString()} tokens`}
          />
          <Stat
            label="Open alerts"
            value={String(overview.open_alerts)}
            tone={overview.open_alerts ? "danger" : "ok"}
          />
        </div>

        <div className="grid gap-5 lg:grid-cols-3">
          <Card title="Latency" className="lg:col-span-2">
            <LineChart buckets={series} series={latencySeries} format={(v) => `${Math.round(v)}ms`} />
            <ChartLegend series={latencySeries} />
          </Card>

          <Card title="Quality">
            {Object.keys(overview.eval_scores).length === 0 ? (
              <p className="py-6 text-center text-xs text-ink-faint">
                No evaluation scores in this window. Only a sampled percentage of traces is scored,
                see <span className="font-mono">OBS_EVAL_SAMPLE_RATE</span>.
              </p>
            ) : (
              <div className="space-y-4">
                {Object.entries(overview.eval_scores).map(([metric, value]) => (
                  <ScoreBar key={metric} label={metric} value={value} />
                ))}
                {drifted.length > 0 && (
                  <p className="rounded border border-danger/30 bg-danger/10 px-3 py-2 text-xs text-danger">
                    Drift detected on {drifted.map((d) => d.metric).join(", ")} in this window.
                  </p>
                )}
              </div>
            )}
          </Card>

          <Card title="Volume" className="lg:col-span-2">
            <VolumeChart buckets={series} />
            <ChartLegend
              series={[
                { label: "traces", color: "var(--color-accent)" },
                { label: "errors", color: "var(--color-danger)" },
              ]}
            />
          </Card>

          <Card title="Cost over time">
            <LineChart
              buckets={series}
              series={costSeries}
              height={140}
              format={(v) => `$${v.toFixed(4)}`}
            />
          </Card>
        </div>

        <div className="grid gap-5 lg:grid-cols-2">
          <Card
            title="Open alerts"
            action={
              <Link href="/alerts" className="text-xs text-accent hover:underline">
                View all
              </Link>
            }
          >
            {alerts.items.length === 0 ? (
              <EmptyState title="No open alerts" hint="Guardrail, drift and budget alerts land here." />
            ) : (
              <ul className="divide-y divide-border">
                {alerts.items.map((alert) => (
                  <li key={alert.id} className="flex items-start gap-3 py-2.5 first:pt-0 last:pb-0">
                    <Badge tone={alert.severity}>{alert.severity}</Badge>
                    <div className="min-w-0 flex-1">
                      <p className="truncate text-sm">{alert.title}</p>
                      <p className="mt-0.5 text-xs text-ink-faint">
                        {alert.kind} · <Timestamp value={alert.created_at} />
                      </p>
                    </div>
                    {alert.trace_id && <TraceLink traceId={alert.trace_id} />}
                  </li>
                ))}
              </ul>
            )}
          </Card>

          <Card
            title="Recent traces"
            action={
              <Link href="/traces" className="text-xs text-accent hover:underline">
                View all
              </Link>
            }
          >
            {recent.items.length === 0 ? (
              <EmptyState
                title="No traces yet"
                hint="Wire the SDK into your agent, or run scripts/traffic_sim.py to generate some."
              />
            ) : (
              <ul className="divide-y divide-border">
                {recent.items.map((trace) => (
                  <li key={trace.trace_id} className="flex items-center gap-3 py-2 first:pt-0 last:pb-0">
                    <Badge tone={trace.status}>{trace.status}</Badge>
                    <TraceLink traceId={trace.trace_id} />
                    <span className="min-w-0 flex-1 truncate text-xs text-ink-faint">
                      {trace.input_preview || trace.name}
                    </span>
                    {trace.guardrail_status === "flagged" && <Badge tone="critical">flagged</Badge>}
                    <Cost usd={trace.usage.cost_usd} />
                  </li>
                ))}
              </ul>
            )}
          </Card>
        </div>
      </div>
    );
  } catch (error) {
    if (error instanceof ApiError) {
      return <ApiUnavailable message={error.message} coldStart={error.coldStart} />;
    }
    throw error;
  }
}
