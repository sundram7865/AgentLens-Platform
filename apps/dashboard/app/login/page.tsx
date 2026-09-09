"use client";

import { useActionState } from "react";
import { useFormStatus } from "react-dom";
import { type LoginState, signIn } from "./actions";

function SubmitButton() {
  const { pending } = useFormStatus();
  return (
    <button
      type="submit"
      disabled={pending}
      className="w-full rounded-md bg-accent px-4 py-2 text-sm font-medium text-canvas transition hover:opacity-90 disabled:opacity-50"
    >
      {/* The first login of the day may be paying for a free-tier cold start. */}
      {pending ? "Signing in…" : "Sign in"}
    </button>
  );
}

export default function LoginPage() {
  const [state, formAction] = useActionState<LoginState, FormData>(signIn, {});

  return (
    <main className="flex min-h-screen items-center justify-center px-4">
      <div className="w-full max-w-sm">
        <div className="mb-6">
          <h1 className="text-lg font-semibold">AI Observability &amp; Guardrails</h1>
          <p className="mt-1 text-sm text-ink-faint">
            Traces, guardrails and evaluation scores for LLM agents.
          </p>
        </div>

        <form action={formAction} className="space-y-3 rounded-lg border border-border bg-surface p-5">
          <div>
            <label htmlFor="email" className="block text-xs font-medium text-ink-muted">
              Email
            </label>
            <input
              id="email"
              name="email"
              type="email"
              autoComplete="username"
              required
              className="mt-1 w-full rounded-md border border-border bg-canvas px-3 py-2 text-sm outline-none focus:border-accent"
            />
          </div>

          <div>
            <label htmlFor="password" className="block text-xs font-medium text-ink-muted">
              Password
            </label>
            <input
              id="password"
              name="password"
              type="password"
              autoComplete="current-password"
              required
              className="mt-1 w-full rounded-md border border-border bg-canvas px-3 py-2 text-sm outline-none focus:border-accent"
            />
          </div>

          {state.error && (
            <p role="alert" className="rounded-md border border-danger/30 bg-danger/10 px-3 py-2 text-xs text-danger">
              {state.error}
            </p>
          )}

          <SubmitButton />
        </form>

        <p className="mt-4 text-center text-xs text-ink-faint">
          Two roles: <span className="font-mono text-ink-muted">admin</span> sees raw trace
          payloads, <span className="font-mono text-ink-muted">viewer</span> sees them with personal
          data masked by the API.
        </p>
      </div>
    </main>
  );
}
