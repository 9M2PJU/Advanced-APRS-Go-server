#!/bin/sh
# docker-entrypoint.sh — ensure runtime config files exist in /data, then exec.
set -e

DATA_DIR="${DATA_DIR:-/data}"
cd "$DATA_DIR"

# Runtime state files that must live on the mounted volume so they persist
# across container restarts. Touch them into existence if missing so the
# server can write to them.
RUNTIME_FILES="
creds.json
server_config.json
members.json
webhooks.json
apikeys.json
bans.json
motd.json
audit.log
igate_history.json
performance_history.jsonl
vapid.json
"

for f in $RUNTIME_FILES; do
  if [ ! -e "$DATA_DIR/$f" ]; then
    touch "$DATA_DIR/$f" 2>/dev/null || true
  fi
done

# Hand off to the main process (aprs_server by default)
exec "$@"
