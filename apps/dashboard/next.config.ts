import type { NextConfig } from "next";

const config: NextConfig = {
  reactStrictMode: true,
  // Pin the tracing root to this app. Next infers it from the nearest lockfile
  // and would otherwise walk up past the monorepo into an unrelated parent
  // directory, silently changing which files end up in the standalone bundle.
  outputFileTracingRoot: __dirname,
  // Standalone output keeps the Docker image small; Vercel ignores it.
  output: process.env.NEXT_OUTPUT === "standalone" ? "standalone" : undefined,
  poweredByHeader: false,
  async headers() {
    return [
      {
        source: "/:path*",
        headers: [
          { key: "X-Content-Type-Options", value: "nosniff" },
          { key: "X-Frame-Options", value: "DENY" },
          { key: "Referrer-Policy", value: "no-referrer" },
          // Trace payloads can contain customer data even after redaction.
          // Caching them in a shared proxy puts that data somewhere the
          // redaction layer has no say over.
          { key: "Cache-Control", value: "no-store" },
        ],
      },
    ];
  },
};

export default config;
