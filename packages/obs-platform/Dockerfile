# One image, two roles. The API and the workers run the same code with the same
# dependencies; only the command differs. Two images would guarantee they drift.
FROM python:3.12-slim AS base

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

RUN apt-get update \
 && apt-get install -y --no-install-recommends curl \
 && rm -rf /var/lib/apt/lists/*

# Dependencies first: this layer is cached across every source-only change.
COPY packages/obs-platform/requirements.txt /app/requirements.txt
RUN pip install -r /app/requirements.txt

COPY packages/obs-sdk /app/packages/obs-sdk
COPY packages/obs-platform /app/packages/obs-platform

# --no-deps for both: every third-party pin already came from requirements.txt,
# and obs-sdk is a workspace path, not a PyPI package.
RUN pip install --no-deps -e /app/packages/obs-sdk \
 && pip install --no-deps -e /app/packages/obs-platform

COPY scripts /app/scripts

# Non-root. A container that writes traces containing customer data has no
# business running as uid 0.
RUN useradd --create-home --uid 10001 obs && chown -R obs:obs /app
USER obs

ENV PYTHONPATH=/app/packages/obs-sdk/src:/app/packages/obs-platform/src
EXPOSE 8000

COPY --chown=obs:obs --chmod=0755 packages/obs-platform/docker-healthcheck.sh /app/healthcheck.sh
HEALTHCHECK --interval=30s --timeout=10s --start-period=30s --retries=3 \
  CMD /app/healthcheck.sh

# --chmod, not the git file mode. A checkout made on Windows (core.filemode=false)
# commits shell scripts as 100644; Linux builders (CI, Render) then copy a
# non-executable entrypoint and the container dies with "permission denied"
# before printing a line. Docker Desktop on Windows marks every file executable,
# which is why that never reproduces locally.
COPY --chown=obs:obs --chmod=0755 packages/obs-platform/docker-entrypoint.sh /app/entrypoint.sh
ENTRYPOINT ["/app/entrypoint.sh"]
CMD ["api"]
