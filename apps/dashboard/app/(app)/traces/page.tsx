import Link from "next/link";
import {
  ApiUnavailable,
  Badge,
  Cost,
  Duration,
  EmptyState,
  RedactionNotice,
  Timestamp,
  TraceLink,
} from "@/components/ui";
import { ApiError, api, requireSession } from "@/lib/api";

export const dynamic = "force-dynamic";

interface Search {
  cursor?: string;
  tenant_id?: string;
  status?: string;
  flagged?: string;
  has_errors?: string;
  ticket_id?: string;
  search?: string;
  since_hours?: string;
}

function href(params: Search, overrides: Partial<Search>): string {
  const merged = { ...params, ...overrides };
  const search = new URLSearchParams();
  for (const [key, value] of Object.entries(merged)) {
    if (value) search.set(key, value);
  }
  const encoded = search.toString();
  return `/traces${encoded ? `?${encoded}` : ""}`;
}

export default async function TracesPage({ searchParams }: { searchParams: Promise<Search> }) {
  const params = await searchParams;

  try {
    const [page, tenants] = await Promise.all([
      requireSession(() =>
        api.traces({
          limit: "25",
          cursor: params.cursor,
          tenant_id: params.tenant_id,
          status: params.status,
          flagged: params.flagged,
          has_errors: params.has_errors,
          ticket_id: params.ticket_id,
          search: params.search,
          since_hours: params.since_hours,
        }),
      ),
      api.tenants(),
    ]);

    const redacted = page.items.some((trace) => trace.redacted);

    return (
      <div className="space-y-4">
        <div className="flex flex-wrap items-center justify-between gap-3">
          <h1 className="text-base font-semibold">Traces</h1>
          <form action="/traces" className="flex gap-2">
            <input
              type="search"
              name="search"
              defaultValue={params.search}
              placeholder="Trace id or name"
              className="w-56 rounded-md border border-border bg-surface px-3 py-1.5 text-sm outline-none focus:border-accent"
            />
            <input
              type="search"
              name="ticket_id"
              defaultValue={params.ticket_id}
              placeholder="Ticket id"
              className="w-32 rounded-md border border-border bg-surface px-3 py-1.5 text-sm outline-none focus:border-accent"
            />
            <button
              type="submit"
              className="rounded-md border border-border bg-surface px-3 py-1.5 text-sm text-ink-muted hover:text-ink"
            >
              Search
            </button>
          </form>
        </div>

        {redacted && <RedactionNotice />}

        <div className="flex flex-wrap items-center gap-2 text-xs">
          <FilterChip label="All" active={!params.status && !params.flagged && !params.has_errors} target={href({}, {})} />
          <FilterChip label="Errors" active={params.has_errors === "true"} target={href(params, { has_errors: "true", cursor: undefined })} />
          <FilterChip label="Flagged" active={params.flagged === "true"} target={href(params, { flagged: "true", cursor: undefined })} />
          <FilterChip label="Last hour" active={params.since_hours === "1"} target={href(params, { since_hours: "1", cursor: undefined })} />
          {tenants.length > 1 && (
            <>
              <span className="ml-2 text-ink-faint">tenant:</span>
              {tenants.slice(0, 6).map((tenant) => (
                <FilterChip
                  key={tenant.tenant_id}
                  label={`${tenant.tenant_id} (${tenant.traces})`}
                  active={params.tenant_id === tenant.tenant_id}
                  target={href(params, { tenant_id: tenant.tenant_id, cursor: undefined })}
                />
              ))}
            </>
          )}
        </div>

        {page.items.length === 0 ? (
          <EmptyState
            title="No traces match these filters"
            hint="Traces arrive through Redis Streams from the SDK. Try scripts/traffic_sim.py to generate a realistic batch."
          />
        ) : (
          <div className="overflow-x-auto rounded-lg border border-border bg-surface">
            <table className="w-full min-w-[900px] text-sm">
              <thead>
                <tr className="border-b border-border text-left text-[11px] tracking-wider text-ink-faint uppercase">
                  <th className="px-3 py-2 font-medium">Trace</th>
                  <th className="px-3 py-2 font-medium">Request</th>
                  <th className="px-3 py-2 font-medium">Status</th>
                  <th className="px-3 py-2 font-medium">Guardrails</th>
                  <th className="px-3 py-2 font-medium text-right">Latency</th>
                  <th className="px-3 py-2 font-medium text-right">Tokens</th>
                  <th className="px-3 py-2 font-medium text-right">Cost</th>
                  <th className="px-3 py-2 font-medium text-right">When</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-border">
                {page.items.map((trace) => (
                  <tr key={trace.trace_id} className="transition hover:bg-surface-2">
                    <td className="px-3 py-2">
                      <TraceLink traceId={trace.trace_id} />
                      <div className="text-[11px] text-ink-faint">{trace.tenant_id}</div>
                    </td>
                    <td className="max-w-[320px] px-3 py-2">
                      <div className="truncate text-ink-muted">{trace.input_preview || trace.name}</div>
                      {typeof trace.attributes.ticket_id === "string" && (
                        <div className="text-[11px] text-ink-faint">
                          ticket {trace.attributes.ticket_id}
                        </div>
                      )}
                    </td>
                    <td className="px-3 py-2">
                      <Badge tone={trace.status}>{trace.status}</Badge>
                    </td>
                    <td className="px-3 py-2">
                      {trace.guardrail_status === "flagged" ? (
                        <Badge tone={trace.max_severity ?? "medium"} title={`risk ${trace.risk_score}`}>
                          {trace.max_severity ?? "flagged"}
                        </Badge>
                      ) : (
                        <Badge tone={trace.guardrail_status}>{trace.guardrail_status}</Badge>
                      )}
                    </td>
                    <td className="px-3 py-2 text-right">
                      <Duration ms={trace.latency_ms} />
                    </td>
                    <td className="px-3 py-2 text-right font-mono text-ink-muted">
                      {trace.usage.total_tokens.toLocaleString()}
                    </td>
                    <td className="px-3 py-2 text-right">
                      <Cost usd={trace.usage.cost_usd} />
                    </td>
                    <td className="px-3 py-2 text-right">
                      <Timestamp value={trace.started_at} />
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}

        {/*
          Next-page only. The API pages by keyset cursor, which is what keeps
          page 500 as cheap as page 1 and stops rows shifting under an offset --
          the cost is that there is no "jump to page N", which nobody scanning a
          trace list actually wants.
        */}
        <div className="flex items-center justify-between text-xs text-ink-faint">
          <span>
            {page.items.length} shown{page.has_more ? " · more available" : ""}
          </span>
          {page.next_cursor && (
            <Link
              href={href(params, { cursor: page.next_cursor })}
              className="rounded-md border border-border bg-surface px-3 py-1.5 text-ink-muted transition hover:text-ink"
            >
              Next page →
            </Link>
          )}
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

function FilterChip({ label, active, target }: { label: string; active: boolean; target: string }) {
  return (
    <Link
      href={target}
      className={`rounded-full border px-2.5 py-1 transition ${
        active
          ? "border-accent/40 bg-accent-dim text-ink"
          : "border-border bg-surface text-ink-muted hover:text-ink"
      }`}
    >
      {label}
    </Link>
  );
}
