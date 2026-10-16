#!/bin/sh
# backup.sh — Nightly backup: pg_dump + enterprise.db copy to /data/backups/<YYYY-MM-DD>/
# Runs inside the `backup` compose service (postgres:16-alpine image).
# Env vars provided by compose: PGHOST, PGUSER, PGDATABASE, POSTGRES_PASSWORD.
#
# Schedule: this script sleeps until the next 02:00 UTC, then loops every 24 h.
#           On Saturdays (ISO weekday 6) it waits another 90 min then copies
#           the latest backup folder off-host (item 1).
# Data volume /data is shared with the api service.
#
# Weekly off-host copy (item 1):
#   Set BACKUP_REMOTE to enable. Examples:
#     BACKUP_REMOTE="s3:mybucket/voicebot-backups"        (S3/S3-compatible via rclone)
#     BACKUP_REMOTE="gdrive:voicebot/backups"             (Google Drive via rclone)
#     BACKUP_REMOTE="sftp:user@host.example.com:/backups" (SFTP via rclone)
#     BACKUP_REMOTE="user@host.example.com:/data/backups" (raw SSH, falls back to rsync)
#   If BACKUP_REMOTE is unset the step is skipped silently.

set -e

export PGPASSWORD="${POSTGRES_PASSWORD}"

log() { echo "[backup] $(date -u +%Y-%m-%dT%H:%M:%SZ) $*"; }

wait_until_next_2am() {
    now=$(date -u +%s)
    today_2am=$(date -u -d "today 02:00" +%s 2>/dev/null || date -u -j -f "%Y-%m-%d %H:%M" "$(date -u +%Y-%m-%d) 02:00" +%s)
    if [ "$now" -ge "$today_2am" ]; then
        target=$((today_2am + 86400))
    else
        target=$today_2am
    fi
    sleep_secs=$((target - now))
    log "Next backup in ${sleep_secs}s (at $(date -u -d "@${target}" +%H:%M 2>/dev/null || date -u +%H:%M))"
    sleep "$sleep_secs"
}

run_backup() {
    DATE=$(date -u +%Y-%m-%d)
    DEST="/data/backups/${DATE}"
    mkdir -p "$DEST"

    # 1. PostgreSQL logical dump (compressed, roles excluded — restore via pg_restore)
    log "pg_dump -> ${DEST}/postgres.dump"
    pg_dump \
        --host="$PGHOST" \
        --username="$PGUSER" \
        --dbname="$PGDATABASE" \
        --format=custom \
        --compress=9 \
        --no-privileges \
        --no-owner \
        --file="${DEST}/postgres.dump"
    log "pg_dump done ($(du -sh "${DEST}/postgres.dump" | cut -f1))"

    # 2. Enterprise SQLite copy (if exists)
    ENTERPRISE_DB="/data/enterprise.db"
    if [ -f "$ENTERPRISE_DB" ]; then
        log "Copying enterprise.db"
        cp "$ENTERPRISE_DB" "${DEST}/enterprise.db"
        log "enterprise.db done ($(du -sh "${DEST}/enterprise.db" | cut -f1))"
    else
        log "enterprise.db not found at ${ENTERPRISE_DB}, skipping"
    fi

    # 3. Keep only the last 14 daily backups to avoid disk fill
    ls -1d /data/backups/20* 2>/dev/null \
        | sort \
        | head -n -14 \
        | xargs -r rm -rf
    log "Backup complete: ${DEST}"
}

# ---------------------------------------------------------------------------
# Item 1 — Weekly off-host copy. Runs every Saturday after the nightly backup.
# ---------------------------------------------------------------------------
weekly_offsite() {
    if [ -z "${BACKUP_REMOTE:-}" ]; then
        log "[offsite] BACKUP_REMOTE not set — skipping weekly offsite copy"
        return 0
    fi

    LATEST=$(ls -1d /data/backups/20* 2>/dev/null | sort | tail -n 1)
    if [ -z "$LATEST" ]; then
        log "[offsite] No local backup folders found — nothing to copy"
        return 0
    fi

    log "[offsite] Copying ${LATEST} -> ${BACKUP_REMOTE}/$(basename "${LATEST}")"

    # Prefer rclone (handles S3, GCS, GDrive, SFTP, Azure, etc.).
    # Fall back to rsync for plain SSH destinations.
    if command -v rclone >/dev/null 2>&1; then
        log "[offsite] Using rclone"
        rclone copy "${LATEST}" "${BACKUP_REMOTE}/$(basename "${LATEST}")" \
            --transfers=4 \
            --retries=3 \
            --log-level=INFO \
            2>&1 | while IFS= read -r line; do log "[rclone] $line"; done
        log "[offsite] rclone copy complete"
    elif command -v rsync >/dev/null 2>&1; then
        log "[offsite] rclone not found, falling back to rsync (SSH/local paths only)"
        rsync -az --delete "${LATEST}/" "${BACKUP_REMOTE}/$(basename "${LATEST}")/" \
            2>&1 | while IFS= read -r line; do log "[rsync] $line"; done
        log "[offsite] rsync copy complete"
    else
        log "[offsite] ERROR: neither rclone nor rsync available — install rclone in the backup container"
        return 1
    fi
}

# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

log "Backup service started"

# Immediate backup on first run so the first night is never skipped.
run_backup

# Loop: wait until next 02:00 UTC, back up, then check if it is Saturday.
while true; do
    wait_until_next_2am
    run_backup

    # ISO weekday: 6 = Saturday
    DOW=$(date -u +%u)
    if [ "$DOW" = "6" ]; then
        log "Saturday — waiting 90 min then running weekly offsite copy"
        sleep 5400
        weekly_offsite
    fi
done
