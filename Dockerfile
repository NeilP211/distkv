# syntax=docker/dockerfile:1

# ---------------------------------------------------------------------------
# Stage 1: build
# Build both distkvd (daemon) and distkvctl (CLI) as static binaries.
# ---------------------------------------------------------------------------
FROM golang:1.26 AS builder

WORKDIR /src

# Cache dependency downloads separately from source changes.
COPY go.mod go.sum ./
RUN go mod download

# Copy the full source tree and build both commands statically.
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" \
      -o /out/distkvd  ./cmd/distkvd && \
    CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" \
      -o /out/distkvctl ./cmd/distkvctl

# ---------------------------------------------------------------------------
# Stage 2: runtime
# Distroless/static:nonroot — no shell, no libc, minimal attack surface.
# ---------------------------------------------------------------------------
FROM gcr.io/distroless/static:nonroot

COPY --from=builder /out/distkvd  /distkvd
COPY --from=builder /out/distkvctl /distkvctl

# gRPC / KV service port (configurable via --listen).
EXPOSE 9001
# Prometheus metrics port (configurable via --metrics-listen).
EXPOSE 9101

ENTRYPOINT ["/distkvd"]
