#!/bin/sh
# docker-entrypoint.sh — prepare /data, then exec the server.
set -e

DATA_DIR="${DATA_DIR:-/data}"
cd "$DATA_DIR"

# Ensure the data directory is writable by the runtime user. When a host
# directory is bind-mounted (instead of a named volume) it may be owned by
# root, which would prevent the non-root aprs user from creating state files.
# We only chown if we are root; under the default named-volume setup this is
# a no-op because the volume already inherits aprs ownership from the image.
if [ "$(id -u)" = "0" ]; then
  chown -R aprs:aprs "$DATA_DIR" 2>/dev/null || true
  exec su-exec aprs "$@"
fi

# Hand off to the main process (aprs_server by default)
exec "$@"
