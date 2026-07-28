# syntax=docker/dockerfile:1

# ── Build stage ──────────────────────────────────────────────────────────────
# Multi-arch build targets linux/amd64 and linux/arm64.
FROM golang:1.22-alpine AS builder

WORKDIR /src

# Cache module downloads
COPY go.mod go.sum ./
RUN go mod download

# Copy source
COPY . .

# Static, CGO-free build for portable multi-arch binaries
ENV CGO_ENABLED=0 GOOS=linux
RUN go build -trimpath -ldflags="-s -w" -o /out/aprs_server .

# ── Runtime stage ────────────────────────────────────────────────────────────
FROM alpine:3.20 AS runtime

RUN apk add --no-cache ca-certificates tzdata wget \
    && addgroup -S aprs \
    && adduser -S -G aprs -h /data aprs

WORKDIR /data

# Copy binary
COPY --from=builder /out/aprs_server /usr/local/bin/aprs_server

# Copy static web assets and embedded data
COPY --chown=aprs:aprs \
    index.html mobile.html admin.html privacy.html \
    favicon.ico favicon.svg favicon-16.png favicon-32.png favicon-48.png \
    apple-touch-icon.png og-image.png robots.txt sitemap.xml \
    tocalls.json Caddyfile \
    /data/
COPY --chown=aprs:aprs symbols/ /data/symbols/
COPY --chown=aprs:aprs docs/ /data/docs/

# Entrypoint handles config persistence across container restarts
COPY docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
RUN chmod +x /usr/local/bin/docker-entrypoint.sh

# Ports:
#   8080  HTTP API + web dashboard
#   14580 APRS-IS TCP
#   14580/udp APRS-IS UDP
#   1883  iGate MQTT
EXPOSE 8080 14580 1883
EXPOSE 14580/udp

ENV DATA_DIR=/data
USER aprs

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD wget -qO- http://127.0.0.1:8080/api/tocalls >/dev/null 2>&1 || exit 1

ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]
CMD ["aprs_server"]
