#!/bin/sh
# docker-install.sh — one-line installer for Advanced APRS Go Server via Docker.
#
# Usage:
#   curl -fsSL https://raw.githubusercontent.com/9M2PJU/Advanced-APRS-Go-server/main/docker-install.sh | sh
#
# Requires Docker (and optionally docker compose). Pulls the multi-arch image
# from GHCR, so it runs on amd64 and arm64.
set -e

IMAGE="ghcr.io/9m2pju/advanced-aprs-go-server:latest"
CONTAINER="aprs-gateway"

if ! command -v docker >/dev/null 2>&1; then
  echo "Docker is not installed. Install it first: https://docs.docker.com/get-docker/"
  exit 1
fi

echo "Pulling $IMAGE ..."
docker pull "$IMAGE"

if docker ps -a --format '{{.Names}}' | grep -qx "$CONTAINER"; then
  echo "Existing container '$CONTAINER' found — stopping and removing."
  docker rm -f "$CONTAINER" >/dev/null
fi

echo "Starting $CONTAINER ..."
docker run -d \
  --name "$CONTAINER" \
  --restart unless-stopped \
  -p 8080:8080 \
  -p 14580:14580 \
  -p 14580:14580/udp \
  -p 1883:1883 \
  -v aprs-gateway-data:/data \
  -e TZ="${TZ:-UTC}" \
  "$IMAGE"

echo
echo "Advanced APRS Go Server is running."
echo "  Web dashboard : http://localhost:8080"
echo "  APRS-IS TCP   : localhost:14580"
echo "  APRS-IS UDP   : localhost:14580"
echo "  iGate MQTT    : localhost:1883"
echo
echo "Logs:   docker logs -f $CONTAINER"
echo "Stop:   docker stop $CONTAINER"
echo "Update: docker pull $IMAGE && docker rm -f $CONTAINER && sh $0"
