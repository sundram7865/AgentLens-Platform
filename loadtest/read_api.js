// k6 load test for the read API (the path the dashboard uses).
//
//   k6 run -e API=http://localhost:8000 -e EMAIL=admin@example.com -e PASSWORD=... loadtest/read_api.js
//
// The ingestion path is the interesting one and is measured separately by
// loadtest/ingest_load.py -- it needs the SDK in-process, which k6 cannot do.
// This covers the other half: what happens when several operators are scanning
// traces at once, which is where an unpaginated list or a missing index shows up.
//
// Thresholds are assertions, not decoration: the run fails if they are missed.

import http from "k6/http";
import { check, group, sleep } from "k6";
import { Rate, Trend } from "k6/metrics";

const API = __ENV.API || "http://localhost:8000";
const EMAIL = __ENV.EMAIL || "admin@example.com";
const PASSWORD = __ENV.PASSWORD || "admin-local-password";

const errorRate = new Rate("errors");
const listLatency = new Trend("trace_list_ms");
const detailLatency = new Trend("trace_detail_ms");
const deepPageLatency = new Trend("deep_page_ms");

export const options = {
  scenarios: {
    operators: {
      executor: "ramping-vus",
      startVUs: 1,
      stages: [
        { duration: "20s", target: 10 },
        { duration: "40s", target: 10 },
        { duration: "10s", target: 0 },
      ],
    },
  },
  thresholds: {
    errors: ["rate<0.01"],
    http_req_failed: ["rate<0.01"],
    // Generous, because the free tier is a shared CPU and the point of this
    // test is catching a missing index, not benchmarking Render's hardware.
    trace_list_ms: ["p(95)<1500"],
    trace_detail_ms: ["p(95)<2000"],
    // The keyset-pagination claim, as an assertion: page 20 must cost roughly
    // what page 1 costs. With OFFSET this threshold is what would break first.
    deep_page_ms: ["p(95)<1500"],
  },
};

export function setup() {
  const response = http.post(
    `${API}/v1/auth/login`,
    JSON.stringify({ email: EMAIL, password: PASSWORD }),
    { headers: { "Content-Type": "application/json" }, timeout: "60s" }, // cold start
  );
  if (response.status !== 200) {
    throw new Error(`login failed: ${response.status} ${response.body}`);
  }
  return { token: response.json("access_token") };
}

export default function (data) {
  const params = {
    headers: { Authorization: `Bearer ${data.token}` },
    timeout: "30s",
  };

  group("trace list", () => {
    const response = http.get(`${API}/v1/traces?limit=25`, params);
    listLatency.add(response.timings.duration);
    const ok = check(response, {
      "list 200": (r) => r.status === 200,
      "list is bounded": (r) => (r.json("items") || []).length <= 25,
      "list is a keyset page": (r) => r.json("next_cursor") !== undefined,
    });
    errorRate.add(!ok);

    // Walk deep with the cursor. This is the assertion that page 20 is as cheap
    // as page 1 -- the property OFFSET pagination does not have.
    let cursor = response.json("next_cursor");
    for (let page = 0; page < 20 && cursor; page++) {
      const next = http.get(
        `${API}/v1/traces?limit=25&cursor=${encodeURIComponent(cursor)}`,
        params,
      );
      if (page >= 5) deepPageLatency.add(next.timings.duration);
      errorRate.add(next.status !== 200);
      cursor = next.json("next_cursor");
    }

    const items = response.json("items") || [];
    if (items.length > 0) {
      const traceId = items[Math.floor(Math.random() * items.length)].trace_id;
      const detail = http.get(`${API}/v1/traces/${traceId}`, params);
      detailLatency.add(detail.timings.duration);
      errorRate.add(
        !check(detail, {
          "detail 200": (r) => r.status === 200,
          "detail has spans": (r) => Array.isArray(r.json("spans")),
          // Every response must declare whether it was masked.
          "detail declares redaction": (r) => typeof r.json("redacted") === "boolean",
        }),
      );
    }
  });

  group("dashboard widgets", () => {
    const responses = http.batch([
      ["GET", `${API}/v1/metrics/overview?hours=24`, null, params],
      ["GET", `${API}/v1/metrics/timeseries?hours=24&bucket=hour`, null, params],
      ["GET", `${API}/v1/alerts?status=open&limit=20`, null, params],
      ["GET", `${API}/v1/traces?flagged=true&limit=25`, null, params],
    ]);
    for (const response of responses) {
      errorRate.add(response.status !== 200);
    }
  });

  group("unauthenticated is rejected", () => {
    // Cheap to assert on every iteration, and it is the one regression that
    // would be catastrophic and silent.
    const response = http.get(`${API}/v1/traces`, { timeout: "10s" });
    errorRate.add(response.status !== 401);
  });

  sleep(1);
}
