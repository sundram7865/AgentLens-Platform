import Link from "next/link";
import { notFound } from "next/navigation";
import { ScoreBar } from "@/components/charts";
import {
  ApiUnavailable,
  Badge,
  Card,
  Cost,
  Duration,
  RedactionNotice,
  Timestamp,
} from "@/components/ui";
import { ApiError, api, requireSession } from "@/lib/api";
import type { Span } from "@/lib/types";

export const dynamic = "force-dynamic";

const KIND_COLORS: Record<string, string> = {
  llm: "text-accent",
  tool: "text-warn",
  retriever: "text-ok",
  chain: "text-ink-muted",
  agent: "text-ink",
  guardrail: "text-critical",
};

export default async function TraceDetailPage({
  params,
}: {
  params: Promise<{ traceId: string }>;
}) {
  const { traceId } = await params;

  try {
    const trace = await requireSession(() => api.trace(traceId));

    return (
      <div className="space-y-5">
        <div>
          <Link href="/traces" className="text-xs text-ink-faint hover:text-ink">
            ← Traces
          </Link>
          <div className="mt-1 flex flex-wrap items-center gap-3">
            <h1 className="font-mono text-base">{trace.trace_id}</h1>
            <Badge tone={trace.status}>{trace.status}</Badge>
            {trace.guardrail_status === "flagged" && (
              <Badge tone={trace.max_severity ?? "medium"}>
                {trace.max_severity ?? "flagged"} · risk {trace.risk_score}
              </Badge>
            )}
            <span className="text-xs text-ink-faint">
              {trace.service} · {trace.tenant_id} · {trace.environment}
            </span>
          </div>
        </div>

        {trace.redacted && <RedactionNotice />}

        <div className="grid grid-cols-2 gap-3 lg:grid-cols-6">
          <Metric label="Latency" value={<Duration ms={trace.latency_ms} />} />
          <Metric label="Spans" value={<span className="font-mono">{trace.span_count}</span>} />
          <Metric label="LLM calls" value={<span className="font-mono">{trace.llm_calls}</span>} />
          <Metric label="Tool calls" value={<span className="font-mono">{trace.tool_calls}</span>} />
          <Metric
            label="Tokens"
            value={<span className="font-mono">{trace.usage.total_tokens.toLocaleString()}</span>}
          />
          <Metric label="Cost" value={<Cost usd={trace.usage.cost_usd} />} />
        </div>

        <div className="grid gap-5 lg:grid-cols-3">
          <div className="space-y-5 lg:col-span-2">
            <Card title="Request">
              <Payload value={trace.input} />
            </Card>
            <Card title="Response">
              <Payload value={trace.output} />
            </Card>

            <Card title={`Call tree (${trace.spans.length} spans)`}>
              {trace.spans.length === 0 ? (
                <p className="py-6 text-center text-xs text-ink-faint">
                  No spans recorded for this trace.
                </p>
              ) : (
                <ol className="space-y-1">
                  {trace.spans.map((span) => (
                    <SpanRow key={span.span_id} span={span} />
                  ))}
                </ol>
              )}
            </Card>
          </div>

          <div className="space-y-5">
            <Card title="Guardrails">
              {trace.findings.length === 0 ? (
                <p className="text-xs text-ink-faint">
                  {trace.guardrail_status === "pending"
                    ? "Not scanned yet: the guardrail consumer runs on its own group and may be a moment behind."
                    : "No findings. Scanned clean."}
                </p>
              ) : (
                <ul className="space-y-2.5">
                  {trace.findings.map((finding) => (
                    <li key={finding.id} className="rounded border border-border bg-surface-2 p-2.5">
                      <div className="flex items-center gap-2">
                        <Badge tone={finding.severity}>{finding.severity}</Badge>
                        <span className="font-mono text-xs">{finding.finding_type}</span>
                        <span className="ml-auto text-[11px] text-ink-faint">
                          {finding.detector}
                        </span>
                      </div>
                      <p className="mt-1.5 font-mono text-[11px] break-all text-ink-muted">
                        {finding.excerpt}
                      </p>
                      <p className="mt-1 text-[11px] text-ink-faint">
                        in <span className="font-mono">{finding.field}</span> · score{" "}
                        {finding.score.toFixed(2)}
                      </p>
                    </li>
                  ))}
                </ul>
              )}
            </Card>

            <Card title="Evaluation">
              {trace.scores.length === 0 ? (
                <p className="text-xs text-ink-faint">
                  {trace.eval_status === "not_sampled"
                    ? "Not in the evaluation sample. Sampling is deterministic on the trace id, so this decision is stable across retries."
                    : trace.eval_status === "skipped_budget"
                      ? "Skipped: this tenant reached its judge budget cap for the period."
                      : "Awaiting scoring."}
                </p>
              ) : (
                <div className="space-y-4">
                  {trace.scores.map((score) => (
                    <div key={score.metric}>
                      <ScoreBar label={score.metric} value={score.score} />
                      {score.reason && (
                        <p className="mt-1 text-[11px] leading-relaxed text-ink-faint">
                          {score.reason}
                        </p>
                      )}
                    </div>
                  ))}
                  <p className="border-t border-border pt-2 text-[11px] text-ink-faint">
                    Judged by <span className="font-mono">{trace.scores[0]!.backend}</span>
                    {trace.scores[0]!.judge_model && ` (${trace.scores[0]!.judge_model})`} · judging
                    cost <Cost usd={trace.scores.reduce((sum, s) => sum + s.cost_usd, 0)} />
                  </p>
                </div>
              )}
            </Card>

            <Card title="Attributes">
              <dl className="space-y-1.5 text-xs">
                <Row label="Started" value={<Timestamp value={trace.started_at} relative={false} />} />
                <Row label="Model" value={<span className="font-mono">{trace.model ?? "—"}</span>} />
                <Row label="Eval status" value={<span className="font-mono">{trace.eval_status}</span>} />
                {Object.entries(trace.attributes).map(([key, value]) => (
                  <Row
                    key={key}
                    label={key}
                    value={<span className="font-mono break-all">{format(value)}</span>}
                  />
                ))}
              </dl>
            </Card>
          </div>
        </div>
      </div>
    );
  } catch (error) {
    if (error instanceof ApiError) {
      if (error.status === 404) notFound();
      return <ApiUnavailable message={error.message} coldStart={error.coldStart} />;
    }
    throw error;
  }
}

function SpanRow({ span }: { span: Span }) {
  const hasPayload = Object.keys(span.input).length > 0 || Object.keys(span.output).length > 0;
  return (
    <li style={{ marginLeft: `${span.depth * 16}px` }}>
      <details className="group rounded border border-border bg-surface-2/50 open:bg-surface-2">
        <summary className="flex cursor-pointer list-none items-center gap-2 px-2.5 py-1.5 text-xs">
          <span className={`font-mono ${KIND_COLORS[span.kind] ?? "text-ink-muted"}`}>
            {span.kind}
          </span>
          <span className="truncate font-medium">{span.name}</span>
          {span.status === "error" && <Badge tone="high">error</Badge>}
          <span className="ml-auto flex items-center gap-3 text-ink-faint">
            {span.usage.total_tokens > 0 && (
              <span className="font-mono">{span.usage.total_tokens.toLocaleString()}t</span>
            )}
            <Duration ms={span.latency_ms} />
          </span>
        </summary>
        <div className="space-y-2 border-t border-border px-2.5 py-2">
          {span.error && (
            <div className="rounded border border-danger/30 bg-danger/10 p-2">
              <p className="font-mono text-[11px] text-danger">
                {String(span.error.type)}: {String(span.error.message)}
              </p>
            </div>
          )}
          {hasPayload ? (
            <div className="grid gap-2 md:grid-cols-2">
              <Payload label="input" value={span.input} compact />
              <Payload label="output" value={span.output} compact />
            </div>
          ) : (
            <p className="text-[11px] text-ink-faint">No payload captured for this span.</p>
          )}
          <p className="font-mono text-[10px] text-ink-faint">
            {span.span_id}
            {span.parent_span_id && ` ← ${span.parent_span_id}`}
          </p>
        </div>
      </details>
    </li>
  );
}

function Payload({
  value,
  label,
  compact = false,
}: {
  value: Record<string, unknown>;
  label?: string;
  compact?: boolean;
}) {
  const entries = Object.entries(value);
  if (entries.length === 0) {
    return <p className="text-xs text-ink-faint">{label ? `No ${label}` : "Empty"}</p>;
  }
  return (
    <div>
      {label && <p className="mb-1 text-[10px] tracking-wider text-ink-faint uppercase">{label}</p>}
      <pre
        className={`overflow-x-auto rounded border border-border bg-canvas p-2.5 font-mono whitespace-pre-wrap ${
          compact ? "max-h-56 text-[11px]" : "max-h-80 text-xs"
        } text-ink-muted`}
      >
        {JSON.stringify(value, null, 2)}
      </pre>
    </div>
  );
}

function Metric({ label, value }: { label: string; value: React.ReactNode }) {
  return (
    <div className="rounded-lg border border-border bg-surface px-3 py-2">
      <div className="text-[10px] tracking-wider text-ink-faint uppercase">{label}</div>
      <div className="mt-0.5 text-sm">{value}</div>
    </div>
  );
}

function Row({ label, value }: { label: string; value: React.ReactNode }) {
  return (
    <div className="flex justify-between gap-3">
      <dt className="text-ink-faint">{label}</dt>
      <dd className="text-right text-ink-muted">{value}</dd>
    </div>
  );
}

function format(value: unknown): string {
  if (value === null || value === undefined) return "—";
  if (typeof value === "object") return JSON.stringify(value);
  return String(value);
}
