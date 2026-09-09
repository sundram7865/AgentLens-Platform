#!/usr/bin/env bash
#
# Weekly logical backup, sized honestly to the free tier.
#
# Neon's free plan gives point-in-time recovery automatically, at no cost, with
# a **6-hour** history window. That is real protection against "I fat-fingered a
# DELETE five minutes ago". It is no protection at all against "nobody noticed
# for two days", which is the failure mode that actually loses data.
#
# So: a weekly pg_dump to local disk (or any free object store), retained for a
# configurable number of weeks. A few lines of shell, no paid backup product.
#
#   ./scripts/backup.sh
#   BACKUP_DIR=/mnt/backups RETAIN_WEEKS=8 ./scripts/backup.sh
#
# Restore is documented at the bottom of this file AND in docs/RUNBOOK.md, and
# `--verify` actually performs one into a scratch database. An untested backup
# is a hope, not a plan.
set -euo pipefail

DATABASE_URL="${OBS_DATABASE_DIRECT_URL:-${OBS_DATABASE_URL:-}}"
BACKUP_DIR="${BACKUP_DIR:-./backups}"
RETAIN_WEEKS="${RETAIN_WEEKS:-4}"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
ARCHIVE="${BACKUP_DIR}/obs-${STAMP}.dump"

if [ -z "$DATABASE_URL" ]; then
  echo "error: set OBS_DATABASE_DIRECT_URL (preferred) or OBS_DATABASE_URL" >&2
  exit 2
fi

# pg_dump speaks libpq, not SQLAlchemy. Strip the driver prefix the app uses.
PGURL="${DATABASE_URL/postgresql+asyncpg:/postgresql:}"
PGURL="${PGURL/postgresql+psycopg:/postgresql:}"

if ! command -v pg_dump >/dev/null 2>&1; then
  echo "error: pg_dump not found. Install postgresql-client (Debian/Ubuntu:" >&2
  echo "       apt-get install -y postgresql-client)" >&2
  exit 2
fi

mkdir -p "$BACKUP_DIR"

echo "[backup] dumping to ${ARCHIVE}"
# -Fc: custom format. Compressed, and restorable selectively with pg_restore -t,
# which plain SQL is not.
# --no-owner / --no-privileges: the restore target is a different Neon project
# with different role names, and ownership statements would fail there.
pg_dump "$PGURL" \
  --format=custom \
  --no-owner \
  --no-privileges \
  --file="$ARCHIVE"

SIZE="$(du -h "$ARCHIVE" | cut -f1)"
echo "[backup] wrote ${ARCHIVE} (${SIZE})"

if [ "${1:-}" = "--verify" ]; then
  # Restoring into a scratch database is the only way to know the dump is good.
  SCRATCH="obs_restore_check_${STAMP}"
  echo "[backup] verifying by restoring into ${SCRATCH}"
  BASE="${PGURL%/*}"
  createdb "${BASE}/postgres" "$SCRATCH" 2>/dev/null || psql "${BASE}/postgres" -c "CREATE DATABASE ${SCRATCH};"
  pg_restore --dbname="${BASE}/${SCRATCH}" --no-owner --no-privileges "$ARCHIVE"
  COUNT="$(psql "${BASE}/${SCRATCH}" -tAc 'SELECT count(*) FROM traces;')"
  echo "[backup] verified: ${COUNT} traces restored"
  psql "${BASE}/postgres" -c "DROP DATABASE ${SCRATCH};"
fi

echo "[backup] pruning archives older than ${RETAIN_WEEKS} weeks"
find "$BACKUP_DIR" -name 'obs-*.dump' -type f -mtime "+$((RETAIN_WEEKS * 7))" -print -delete

echo "[backup] done. ${BACKUP_DIR} now holds:"
ls -1sh "$BACKUP_DIR"/obs-*.dump 2>/dev/null | tail -5

# -----------------------------------------------------------------------------
# RESTORE (also in docs/RUNBOOK.md)
#
#   # 1. Full restore into a fresh database:
#   createdb obs_restored
#   pg_restore --dbname="postgresql://.../obs_restored" \
#              --no-owner --no-privileges backups/obs-<stamp>.dump
#
#   # 2. One table only (e.g. traces clobbered, everything else fine):
#   pg_restore --dbname="$PGURL" --data-only --table=traces \
#              --no-owner backups/obs-<stamp>.dump
#
#   # 3. Inspect what is in an archive without restoring it:
#   pg_restore --list backups/obs-<stamp>.dump
#
# SCHEDULING
#
#   # crontab -e   (Sunday 03:00 UTC)
#   0 3 * * 0 cd /path/to/obs-platform && OBS_DATABASE_DIRECT_URL='...' ./scripts/backup.sh
#
# On a machine that is not always on, use GitHub Actions with a schedule and a
# repository secret for the connection string; upload the dump as an artifact
# (90-day retention on the free plan) or push it to a free object store.
# -----------------------------------------------------------------------------
