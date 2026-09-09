/** Shared presentational pieces. Server components -- none of these need state. */

import Link from "next/link";

const SEVERITY_STYLES: Record<string, string> = {
  critical: "bg-critical/15 text-critical border-critical/30",
  high: "bg-danger/15 text-danger border-danger/30",
  medium: "bg-warn/15 text-warn border-warn/30",
  low: "bg-info/15 text-info border-info/30",
  info: "bg-ink-faint/15 text-ink-muted border-border-strong",
};

const STATUS_STYLES: Record<string, string> = {
  ok: "bg-ok/15 text-ok border-ok/30",
  error: "bg-danger/15 text-danger border-danger/30",
  running: "bg-info/15 text-info border-info/30",
  flagged: "bg-critical/15 text-critical border-critical/30",
  clean: "bg-ok/15 text-ok border-ok/30",
  pending: "bg-ink-faint/15 text-ink-muted border-border-strong",
};

export function Badge({
  children,
  tone = "info",
  title,
}: {
  children: React.ReactNode;
  tone?: string;
  title?: string;
}) {
  const style =
    SEVERITY_STYLES[tone] ?? STATUS_STYLES[tone] ?? "bg-surface-2 text-ink-muted border-border";
  return (
    <span
      title={title}
      className={`inline-flex items-center gap-1 rounded border px-1.5 py-0.5 text-[11px] font-medium tracking-wide uppercase ${style}`}
    >
      {children}
    </span>
  );
}

export function Card({
  title,
  action,
  children,
  className = "",
}: {
  title?: string;
  action?: React.ReactNode;
  children: React.ReactNode;
  className?: string;
}) {
  return (
    <section className={`rounded-lg border border-border bg-surface ${className}`}>
      {(title || action) && (
        <header className="flex items-center justify-between border-b border-border px-4 py-2.5">
          {title && (
            <h2 className="text-[13px] font-semibold tracking-wide text-ink-muted uppercase">
              {title}
            </h2>
          )}
          {action}
        </header>
      )}
      <div className="p-4">{children}</div>
    </section>
  );
}

export function Stat({
  label,
  value,
  sub,
  tone,
}: {
  label: string;
  value: string;
  sub?: string;
  tone?: "ok" | "warn" | "danger" | "neutral";
}) {
  const valueTone =
    tone === "ok"
      ? "text-ok"
      : tone === "warn"
        ? "text-warn"
        : tone === "danger"
          ? "text-danger"
          : "text-ink";
  return (
    <div className="rounded-lg border border-border bg-surface px-4 py-3">
      <div className="text-[11px] font-medium tracking-wider text-ink-faint uppercase">{label}</div>
      <div className={`mt-1 font-mono text-2xl leading-tight ${valueTone}`}>{value}</div>
      {sub && <div className="mt-0.5 text-xs text-ink-faint">{sub}</div>}
    </div>
  );
}

export function EmptyState({
  title,
  hint,
  action,
}: {
  title: string;
  hint?: string;
  action?: React.ReactNode;
}) {
  return (
    <div className="flex flex-col items-center justify-center gap-2 rounded-lg border border-dashed border-border-strong px-6 py-14 text-center">
      <p className="text-sm text-ink-muted">{title}</p>
      {hint && <p className="max-w-md text-xs text-ink-faint">{hint}</p>}
      {action}
    </div>
  );
}

/**
 * Shown when the API is unreachable. Distinguishes a sleeping free-tier
 * instance from a genuine failure, because those need different reactions from
 * whoever is looking at the screen.
 */
export function ApiUnavailable({ message, coldStart }: { message: string; coldStart: boolean }) {
  return (
    <div className="rounded-lg border border-warn/30 bg-warn/5 px-5 py-4">
      <h2 className="text-sm font-semibold text-warn">
        {coldStart ? "Waking the API up" : "Cannot reach the API"}
      </h2>
      <p className="mt-1 text-sm text-ink-muted">{message}</p>
      {coldStart && (
        <p className="mt-2 text-xs text-ink-faint">
          Render&apos;s free tier stops the service after 15 minutes of inactivity. The first
          request after that wakes it, which takes about a minute. Reload in a moment.
        </p>
      )}
    </div>
  );
}

export function Duration({ ms }: { ms: number | null }) {
  if (ms === null) return <span className="text-ink-faint">—</span>;
  const tone = ms > 5000 ? "text-danger" : ms > 2000 ? "text-warn" : "text-ink";
  return <span className={`font-mono ${tone}`}>{ms < 1000 ? `${ms}ms` : `${(ms / 1000).toFixed(2)}s`}</span>;
}

export function Cost({ usd }: { usd: number }) {
  if (!usd) return <span className="font-mono text-ink-faint">$0</span>;
  // Sub-cent costs are the norm here; two decimals would show every trace as $0.00.
  return <span className="font-mono">{usd < 0.01 ? `$${usd.toFixed(5)}` : `$${usd.toFixed(3)}`}</span>;
}

export function Timestamp({ value, relative = true }: { value: string | null; relative?: boolean }) {
  if (!value) return <span className="text-ink-faint">—</span>;
  const date = new Date(value);
  const iso = date.toISOString().replace("T", " ").slice(0, 19);
  if (!relative) return <span className="font-mono text-xs">{iso}</span>;
  return (
    <span title={`${iso} UTC`} className="font-mono text-xs text-ink-muted">
      {formatRelative(date)}
    </span>
  );
}

function formatRelative(date: Date): string {
  const seconds = Math.round((Date.now() - date.getTime()) / 1000);
  if (seconds < 60) return `${Math.max(0, seconds)}s ago`;
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m ago`;
  if (seconds < 86400) return `${Math.floor(seconds / 3600)}h ago`;
  return `${Math.floor(seconds / 86400)}d ago`;
}

export function TraceLink({ traceId, children }: { traceId: string; children?: React.ReactNode }) {
  return (
    <Link
      href={`/traces/${encodeURIComponent(traceId)}`}
      className="font-mono text-accent hover:underline"
    >
      {children ?? `${traceId.slice(0, 14)}…`}
    </Link>
  );
}

/** Banner telling a viewer that what they are looking at has been masked. */
export function RedactionNotice() {
  return (
    <div className="rounded-lg border border-accent-dim bg-accent/5 px-4 py-2.5 text-xs text-ink-muted">
      <span className="font-semibold text-accent">Redacted view.</span> Your role is{" "}
      <span className="font-mono">viewer</span>, so personal data was masked by the API before this
      response was built, not hidden here in the browser. The same request with{" "}
      <span className="font-mono">curl</span> returns the same masked bytes.
    </div>
  );
}
