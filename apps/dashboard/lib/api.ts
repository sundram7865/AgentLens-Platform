/**
 * Server-side API client.
 *
 * Every function here runs in a Server Component or Route Handler -- never in
 * the browser. That is what keeps the session token in an httpOnly cookie that
 * client JavaScript cannot read, and it means the browser never talks to the
 * API directly, so there is no second place where auth could be got wrong.
 *
 * Cold starts are handled explicitly. Render's free tier spins down after 15
 * minutes idle and takes ~50 seconds to wake, so a recruiter opening a stale
 * link would otherwise see a fetch failure. The first request gets a long
 * timeout and one retry, and `ApiError.coldStart` lets the page say "waking up"
 * instead of "something went wrong".
 */

import { cookies } from "next/headers";
import { redirect } from "next/navigation";
import type {
  Alert,
  DriftPoint,
  Me,
  MetricsOverview,
  Page,
  TimeBucket,
  Tenant,
  TraceDetail,
  TraceSummary,
} from "./types";

export const API_URL =
  process.env.OBS_API_URL ?? process.env.NEXT_PUBLIC_OBS_API_URL ?? "http://localhost:8000";

export const SESSION_COOKIE = "obs_token";

/** Generous, because a sleeping free-tier instance takes ~50s to answer. */
const COLD_START_TIMEOUT_MS = 60_000;
const WARM_TIMEOUT_MS = 15_000;

export class ApiError extends Error {
  constructor(
    readonly status: number,
    message: string,
    readonly coldStart = false,
  ) {
    super(message);
    this.name = "ApiError";
  }
}

async function token(): Promise<string | undefined> {
  return (await cookies()).get(SESSION_COOKIE)?.value;
}

interface RequestOptions {
  /** Send the caller's session cookie. Off only for the login round trip. */
  authenticated?: boolean;
  method?: string;
  body?: unknown;
  /** Seconds of Next.js data cache. 0 disables -- the default for trace data. */
  revalidate?: number;
}

async function request<T>(path: string, options: RequestOptions = {}): Promise<T> {
  const { authenticated = true, method = "GET", body, revalidate = 0 } = options;

  const headers: Record<string, string> = { Accept: "application/json" };
  if (body !== undefined) headers["Content-Type"] = "application/json";
  if (authenticated) {
    const session = await token();
    if (!session) throw new ApiError(401, "Not signed in");
    headers.Authorization = `Bearer ${session}`;
  }

  let lastError: unknown;
  // Two attempts: the first may be paying for a cold start, and a single retry
  // turns "the demo link is broken" into "the demo link took a moment".
  for (let attempt = 0; attempt < 2; attempt++) {
    const controller = new AbortController();
    const timeout = setTimeout(
      () => controller.abort(),
      attempt === 0 ? WARM_TIMEOUT_MS : COLD_START_TIMEOUT_MS,
    );
    try {
      const response = await fetch(`${API_URL}${path}`, {
        method,
        headers,
        body: body === undefined ? undefined : JSON.stringify(body),
        signal: controller.signal,
        next: revalidate > 0 ? { revalidate } : { revalidate: 0 },
      });

      if (response.status === 401) throw new ApiError(401, "Session expired");
      if (!response.ok) {
        const detail = await response.text();
        let message = detail.slice(0, 300);
        try {
          message = JSON.parse(detail)?.error?.message ?? message;
        } catch {
          /* the body was not JSON; the raw text is the best available message */
        }
        throw new ApiError(response.status, message);
      }
      return (await response.json()) as T;
    } catch (error) {
      lastError = error;
      // A 4xx is the server's considered answer. Retrying it just wastes time.
      if (error instanceof ApiError && error.status < 500) throw error;
    } finally {
      clearTimeout(timeout);
    }
  }

  const aborted = lastError instanceof Error && lastError.name === "AbortError";
  throw new ApiError(
    503,
    aborted
      ? "The API did not respond. On the free tier it sleeps after 15 minutes idle and takes about a minute to wake."
      : `Could not reach the API at ${API_URL}`,
    true,
  );
}

/** Redirects to /login when the session is missing or expired. */
export async function requireSession<T>(loader: () => Promise<T>): Promise<T> {
  try {
    return await loader();
  } catch (error) {
    if (error instanceof ApiError && error.status === 401) redirect("/login");
    throw error;
  }
}

export const api = {
  me: () => request<Me>("/v1/auth/me"),

  login: (email: string, password: string) =>
    request<{ access_token: string; expires_in: number; role: string; email: string }>(
      "/v1/auth/login",
      { authenticated: false, method: "POST", body: { email, password } },
    ),

  traces: (params: Record<string, string | undefined>) =>
    request<Page<TraceSummary>>(`/v1/traces${query(params)}`),

  trace: (traceId: string) => request<TraceDetail>(`/v1/traces/${encodeURIComponent(traceId)}`),

  tenants: () => request<Tenant[]>("/v1/traces/tenants", { revalidate: 60 }),

  overview: (params: Record<string, string | undefined>) =>
    request<MetricsOverview>(`/v1/metrics/overview${query(params)}`),

  timeseries: (params: Record<string, string | undefined>) =>
    request<TimeBucket[]>(`/v1/metrics/timeseries${query(params)}`),

  alerts: (params: Record<string, string | undefined>) =>
    request<Page<Alert>>(`/v1/alerts${query(params)}`),

  acknowledgeAlert: (id: number, resolve: boolean) =>
    request<Alert>(`/v1/alerts/${id}/acknowledge?resolve=${resolve}`, { method: "POST" }),

  drift: (params: Record<string, string | undefined>) =>
    request<DriftPoint[]>(`/v1/drift${query(params)}`),

  health: () => request<Record<string, unknown>>("/health/meta", { authenticated: false }),
};

function query(params: Record<string, string | undefined>): string {
  const search = new URLSearchParams();
  for (const [key, value] of Object.entries(params)) {
    if (value !== undefined && value !== "") search.set(key, value);
  }
  const encoded = search.toString();
  return encoded ? `?${encoded}` : "";
}
