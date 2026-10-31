# Evidence

Raw output behind every number the README and the docs quote. Each file says
what produced it; every command is in this repository and can be re-run.

Measured 2026-10-31 to 2026-11-01 on one Windows 11 laptop (8 logical CPUs,
7.9 GB RAM), Python 3.12.6, Docker Desktop with Postgres 16 and Redis 7 in Linux
containers. That machine was also running an editor and other containers, and
was short of memory for much of the session, so treat timings as a floor for
this hardware, not a ceiling for the design.

| File | What it proves | Produced by |
|---|---|---|
| `tests.txt` | Every test layer and its result (342 tests) | `pytest`, `pytest -m integration`, `pytest -m e2e` |
| `loadtest_paced.txt` / `.json` | SDK overhead in the caller, a 12,000-event paced burst with every span reconciled in Postgres, per-group drain, write throughput | `loadtest/ingest_load.py --traces 1500 --spans 6 --concurrency 30 --rate 4` |
| `loadtest_paced_before_lock_fix.txt` | The same run before the lock-order fix: 245 spans still pending, the deadlocks behind it | same command, previous code |
| `loadtest_burst.txt` / `.json` | Producers outrunning the bounded queue: 37.2% shed, nothing blocks | `loadtest/ingest_load.py --traces 2000 --spans 6 --concurrency 200 --rate 0` |
| `injection_threshold.txt` / `.json` | The injection threshold sweep: 20 held-out attacks, 40 benign tickets, 39 signatures | `scripts/tune_injection_threshold.py` |
| `pii_memory.txt` | Resident memory of the built-in PII engine (39 MB) and Presidio (163 MB), on Linux | `scripts/measure_pii_memory.py` in the platform image |
| `redis_idle_commands.txt` | Idle Redis command rate, which the Upstash budget in `docs/DEPLOY.md` is built on | `redis-cli INFO commandstats`, 120 s apart |
| `dashboard_build.txt` | Per-route and first-load JavaScript of the dashboard | `npm run build` in `apps/dashboard` |
| `supportpilot_real_run.txt` | The real SupportPilot agent, patched with the integration, recording into AgentLens; SupportPilot's own suite (598 tests) passing with it enabled | see the file |
