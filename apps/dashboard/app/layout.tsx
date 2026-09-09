import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "AI Observability & Guardrails",
  description:
    "Traces, PII and prompt-injection guardrails, evaluation scores and drift for LLM agents.",
  robots: { index: false, follow: false },
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en">
      <body className="min-h-screen bg-canvas text-ink antialiased">{children}</body>
    </html>
  );
}
