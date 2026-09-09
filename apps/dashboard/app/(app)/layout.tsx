import Link from "next/link";
import { signOut } from "@/app/login/actions";
import { api, requireSession } from "@/lib/api";
import { Badge } from "@/components/ui";

const NAV = [
  { href: "/", label: "Overview" },
  { href: "/traces", label: "Traces" },
  { href: "/alerts", label: "Alerts" },
];

export default async function AppLayout({ children }: { children: React.ReactNode }) {
  // Resolving the session here means every page under this layout is behind
  // auth by construction -- a new page cannot forget to check.
  const me = await requireSession(() => api.me());

  return (
    <div className="min-h-screen">
      <header className="sticky top-0 z-10 border-b border-border bg-canvas/95 backdrop-blur">
        <div className="mx-auto flex max-w-[1400px] items-center gap-6 px-5 py-3">
          <Link href="/" className="flex items-center gap-2 text-sm font-semibold">
            <span className="inline-block h-2 w-2 rounded-full bg-accent" />
            Observability
          </Link>

          <nav className="flex items-center gap-1">
            {NAV.map((item) => (
              <Link
                key={item.href}
                href={item.href}
                className="rounded px-2.5 py-1.5 text-sm text-ink-muted transition hover:bg-surface-2 hover:text-ink"
              >
                {item.label}
              </Link>
            ))}
          </nav>

          <div className="ml-auto flex items-center gap-3 text-xs">
            {me.tenant_id && <Badge tone="info">tenant: {me.tenant_id}</Badge>}
            <Badge tone={me.can_view_raw ? "medium" : "low"}>
              {me.role}
              {me.can_view_raw ? " · raw" : " · redacted"}
            </Badge>
            <span className="hidden text-ink-faint sm:inline">{me.email}</span>
            <form action={signOut}>
              <button
                type="submit"
                className="rounded border border-border px-2 py-1 text-ink-muted transition hover:border-border-strong hover:text-ink"
              >
                Sign out
              </button>
            </form>
          </div>
        </div>
      </header>

      <main className="mx-auto max-w-[1400px] px-5 py-6">{children}</main>
    </div>
  );
}
