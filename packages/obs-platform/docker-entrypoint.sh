#!/usr/bin/env bash
# Entrypoint for both roles.
#
# `exec` matters: it makes uvicorn/the worker PID 1, so the SIGTERM the platform
# sends on every redeploy reaches the process that knows how to finish its
# in-flight message. Without exec, the shell is PID 1, swallows the signal, and
# the container is SIGKILLed 10 seconds later mid-write.
set -euo pipefail

ROLE="${1:-api}"

run_migrations() {
  if [ "${OBS_RUN_MIGRATIONS:-true}" = "true" ]; then
    echo "[entrypoint] alembic upgrade head"
    (cd /app/packages/obs-platform && python -m alembic upgrade head)
  else
    echo "[entrypoint] migrations skipped (OBS_RUN_MIGRATIONS=false)"
  fi
}

wait_for_deps() {
  python - <<'PY'
import os, socket, sys, time
from urllib.parse import urlsplit

def wait(url, default_port, label):
    if not url:
        return
    parts = urlsplit(url)
    host, port = parts.hostname, parts.port or default_port
    if not host or host in {"localhost", "127.0.0.1"} and os.environ.get("OBS_SKIP_WAIT"):
        return
    deadline = time.time() + 60
    while time.time() < deadline:
        try:
            with socket.create_connection((host, port), timeout=2):
                print(f"[entrypoint] {label} reachable at {host}:{port}")
                return
        except OSError:
            time.sleep(1)
    print(f"[entrypoint] WARNING: {label} not reachable at {host}:{port} after 60s", file=sys.stderr)

wait(os.environ.get("OBS_DATABASE_URL") or os.environ.get("DATABASE_URL"), 5432, "postgres")
wait(os.environ.get("OBS_REDIS_URL") or os.environ.get("REDIS_URL"), 6379, "redis")
PY
}

case "$ROLE" in
  api)
    wait_for_deps
    run_migrations
    exec uvicorn obs_platform.api.main:app \
      --host 0.0.0.0 --port "${PORT:-8000}" \
      --workers "${OBS_WEB_CONCURRENCY:-1}" \
      --no-access-log
    ;;
  worker)
    wait_for_deps
    shift || true
    exec python -m obs_platform.workers.cli "$@"
    ;;
  migrate)
    run_migrations
    ;;
  *)
    exec "$@"
    ;;
esac
