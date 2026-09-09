import Link from "next/link";
import { revalidatePath } from "next/cache";
import { ApiUnavailable, Badge, EmptyState, Timestamp, TraceLink } from "@/components/ui";
import { ApiError, api, requireSession } from "@/lib/api";

export const dynamic = "force-dynamic";

/**
 * The only write the dashboard performs, and it touches nothing but an alert's
 * status. Trace data stays read-only from this side by construction.
 */
async function resolveAlert(formData: FormData): Promise<void> {
  "use server";
  const id = Number(formData.get("id"));
  if (Number.isFinite(id)) {
    await api.acknowledgeAlert(id, true);
    revalidatePath("/alerts");
  }
}

const KINDS = ["all", "guardrail", "drift", "budget", "dead_letter"] as const;

export default async function AlertsPage({
  searchParams,
}: {
  searchParams: Promise<{ status?: string; kind?: string; cursor?: string }>;
}) {
  const params = await searchParams;
  const status = params.status ?? "open";
  const kind = params.kind && params.kind !== "all" ? params.kind : undefined;

  try {
    const page = await requireSession(() =>
      api.alerts({ status, kind, cursor: params.cursor, limit: "50" }),
    );

    return (
      <div className="space-y-4">
        <div className="flex flex-wrap items-center justify-between gap-3">
          <h1 className="text-base font-semibold">Alerts</h1>
          <div className="flex gap-1 rounded-md border border-border bg-surface p-0.5 text-xs">
            {["open", "resolved", "all"].map((option) => (
              <Link
                key={option}
                href={`/alerts?status=${option}${kind ? `&kind=${kind}` : ""}`}
                className={`rounded px-2.5 py-1 transition ${
                  status === option ? "bg-accent-dim text-ink" : "text-ink-muted hover:text-ink"
                }`}
              >
                {option}
              </Link>
            ))}
          </div>
        </div>

        <div className="flex flex-wrap gap-2 text-xs">
          {KINDS.map((option) => (
            <Link
              key={option}
              href={`/alerts?status=${status}&kind=${option}`}
              className={`rounded-full border px-2.5 py-1 transition ${
                (params.kind ?? "all") === option
                  ? "border-accent/40 bg-accent-dim text-ink"
                  : "border-border bg-surface text-ink-muted hover:text-ink"
              }`}
            >
              {option.replace("_", " ")}
            </Link>
          ))}
        </div>

        {page.items.length === 0 ? (
          <EmptyState
            title={status === "open" ? "No open alerts" : "Nothing here"}
            hint="Alerts are raised by the guardrail scanner (severity at or above the configured floor), the drift monitor, the per-tenant budget cap, and the dead-letter watch."
          />
        ) : (
          <ul className="space-y-2">
            {page.items.map((alert) => (
              <li
                key={alert.id}
                className="rounded-lg border border-border bg-surface px-4 py-3"
              >
                <div className="flex flex-wrap items-center gap-2.5">
                  <Badge tone={alert.severity}>{alert.severity}</Badge>
                  <Badge tone="info">{alert.kind}</Badge>
                  <span className="text-sm">{alert.title}</span>
                  <span className="ml-auto flex items-center gap-3 text-xs text-ink-faint">
                    {alert.trace_id && <TraceLink traceId={alert.trace_id} />}
                    <Timestamp value={alert.created_at} />
                    {alert.status === "open" ? (
                      <form action={resolveAlert}>
                        <input type="hidden" name="id" value={alert.id} />
                        <button
                          type="submit"
                          className="rounded border border-border px-2 py-0.5 text-ink-muted transition hover:border-border-strong hover:text-ink"
                        >
                          Resolve
                        </button>
                      </form>
                    ) : (
                      <span className="text-ok">
                        resolved{alert.acknowledged_by ? ` by ${alert.acknowledged_by}` : ""}
                      </span>
                    )}
                  </span>
                </div>

                {Object.keys(alert.detail).length > 0 && (
                  <details className="mt-2">
                    <summary className="cursor-pointer text-[11px] text-ink-faint hover:text-ink-muted">
                      Detail
                    </summary>
                    <pre className="mt-1.5 overflow-x-auto rounded border border-border bg-canvas p-2.5 font-mono text-[11px] whitespace-pre-wrap text-ink-muted">
                      {JSON.stringify(alert.detail, null, 2)}
                    </pre>
                  </details>
                )}
              </li>
            ))}
          </ul>
        )}

        {page.next_cursor && (
          <div className="flex justify-end">
            <Link
              href={`/alerts?status=${status}${kind ? `&kind=${kind}` : ""}&cursor=${page.next_cursor}`}
              className="rounded-md border border-border bg-surface px-3 py-1.5 text-xs text-ink-muted hover:text-ink"
            >
              Next page →
            </Link>
          </div>
        )}
      </div>
    );
  } catch (error) {
    if (error instanceof ApiError) {
      return <ApiUnavailable message={error.message} coldStart={error.coldStart} />;
    }
    throw error;
  }
}
