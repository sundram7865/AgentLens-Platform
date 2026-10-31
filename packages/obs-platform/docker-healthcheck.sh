#!/usr/bin/env bash
# Container HEALTHCHECK for both roles of the one image.
#
# HEALTHCHECK cannot see the container's command, so the entrypoint records the
# role it started in /tmp/obs-role. The API answers HTTP; a worker does not, so
# probing :8000 marked every worker container permanently unhealthy. A worker is
# healthy when every role it runs has heartbeated recently.
set -euo pipefail

role="$(cat /tmp/obs-role 2>/dev/null || echo api)"
case "$role" in
  worker) exec python -m obs_platform.workers.cli --healthcheck ;;
  *)      exec curl -fsS "http://localhost:${PORT:-8000}/health" -o /dev/null ;;
esac
