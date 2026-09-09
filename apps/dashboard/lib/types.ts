/**
 * Response shapes mirrored from the API's pydantic models.
 *
 * Hand-written rather than generated from the OpenAPI schema: the surface is
 * small, and a generator would add a build step and a dependency for a dozen
 * interfaces. If this grows past a couple of screens, generate it instead.
 */

export type Role = "admin" | "viewer";

export interface Usage {
  prompt_tokens: number;
  completion_tokens: number;
  total_tokens: number;
  cost_usd: number;
}

export interface TraceSummary {
  trace_id: string;
  tenant_id: string;
  name: string;
  service: string;
  environment: string;
  status: "ok" | "error" | "running";
  started_at: string | null;
  ended_at: string | null;
  latency_ms: number | null;
  model: string | null;
  usage: Usage;
  span_count: number;
  llm_calls: number;
  tool_calls: number;
  error_count: number;
  guardrail_status: "pending" | "clean" | "flagged";
  risk_score: number;
  max_severity: string | null;
  eval_status: string;
  input_preview: string;
  output_preview: string;
  attributes: Record<string, unknown>;
  /** True when PII was masked for this viewer's role. */
  redacted: boolean;
}

export interface Span {
  span_id: string;
  parent_span_id: string | null;
  kind: string;
  name: string;
  status: string;
  started_at: string | null;
  ended_at: string | null;
  latency_ms: number | null;
  model: string | null;
  usage: Usage;
  input: Record<string, unknown>;
  output: Record<string, unknown>;
  error: Record<string, unknown> | null;
  attributes: Record<string, unknown>;
  /** Nesting level, precomputed server-side so the client does no tree walking. */
  depth: number;
}

export interface GuardrailFinding {
  id: number;
  span_id: string;
  detector: string;
  finding_type: string;
  severity: string;
  score: number;
  field: string;
  /** Already masked when stored -- never the raw value. */
  excerpt: string;
  created_at: string;
}

export interface EvalScore {
  metric: string;
  score: number;
  reason: string;
  backend: string;
  judge_model: string;
  cost_usd: number;
  created_at: string;
}

export interface TraceDetail extends TraceSummary {
  input: Record<string, unknown>;
  output: Record<string, unknown>;
  guardrail_flags: Record<string, unknown>;
  spans: Span[];
  findings: GuardrailFinding[];
  scores: EvalScore[];
}

export interface Page<T> {
  items: T[];
  next_cursor: string | null;
  has_more: boolean;
  limit: number;
}

export interface Alert {
  id: number;
  tenant_id: string;
  trace_id: string | null;
  kind: string;
  severity: string;
  title: string;
  detail: Record<string, unknown>;
  status: string;
  acknowledged_by: string | null;
  acknowledged_at: string | null;
  created_at: string;
}

export interface MetricsOverview {
  window_hours: number;
  tenant_id: string | null;
  traces: number;
  errors: number;
  error_rate: number;
  flagged: number;
  flagged_rate: number;
  p50_latency_ms: number | null;
  p95_latency_ms: number | null;
  p99_latency_ms: number | null;
  total_tokens: number;
  cost_usd: number;
  eval_scores: Record<string, number>;
  open_alerts: number;
}

export interface TimeBucket {
  bucket: string;
  traces: number;
  errors: number;
  p50_latency_ms: number | null;
  p95_latency_ms: number | null;
  total_tokens: number;
  cost_usd: number;
}

export interface DriftPoint {
  metric: string;
  window_start: string;
  window_end: string;
  mean: number;
  sample_count: number;
  baseline_mean: number | null;
  z_score: number | null;
  drifted: boolean;
  created_at: string;
}

export interface Me {
  id: string;
  email: string;
  role: Role;
  tenant_id: string | null;
  can_view_raw: boolean;
}

export interface Tenant {
  tenant_id: string;
  traces: number;
  last_seen_at: string | null;
}
