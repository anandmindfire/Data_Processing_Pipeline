# syntax=docker/dockerfile:1.7

# ---- Build stage ---------------------------------------------------------------------------
# Keep GO_VERSION in sync with the `toolchain` line in go.mod (Dependabot tracks both).
ARG GO_VERSION=1.26.10
FROM golang:${GO_VERSION}-alpine AS build

ARG VERSION=dev
ARG COMMIT=unknown
ARG BUILD_DATE=unknown

WORKDIR /src
# Use the toolchain from the image; never download one during the build.
ENV CGO_ENABLED=0 GOOS=linux GOTOOLCHAIN=local GOFLAGS=-mod=readonly

COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download && go mod verify

COPY . .
# Pure-Go SQLite driver: no CGO toolchain needed, the binary is fully static.
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    go build -trimpath \
      -ldflags="-s -w \
        -X github.com/anandmindfire/data-processing-pipeline/internal/buildinfo.Version=${VERSION} \
        -X github.com/anandmindfire/data-processing-pipeline/internal/buildinfo.Commit=${COMMIT} \
        -X github.com/anandmindfire/data-processing-pipeline/internal/buildinfo.Date=${BUILD_DATE}" \
      -o /out/pipeline-server ./cmd/server \
 && mkdir -p /out/data /out/exports /out/inputs

# ---- Runtime stage -------------------------------------------------------------------------
# Distroless: no shell or package manager; runs as the unprivileged "nonroot" user (65532).
FROM gcr.io/distroless/static-debian12:nonroot

ARG VERSION=dev
ARG COMMIT=unknown
ARG BUILD_DATE=unknown
LABEL org.opencontainers.image.title="data-processing-pipeline" \
      org.opencontainers.image.description="Concurrent data processing pipeline with a REST API" \
      org.opencontainers.image.source="https://github.com/anandmindfire/Data_Processing_Pipeline" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.revision="${COMMIT}" \
      org.opencontainers.image.created="${BUILD_DATE}"

WORKDIR /app
COPY --from=build /out/pipeline-server /pipeline-server
COPY --chown=65532:65532 samples /app/samples
# Pre-create writable directories owned by the nonroot user. Docker seeds new named volumes from
# these paths, so they inherit the ownership instead of being root-owned.
COPY --from=build --chown=65532:65532 /out/data /data
COPY --from=build --chown=65532:65532 /out/exports /app/exports
COPY --from=build --chown=65532:65532 /out/inputs /app/inputs

ENV ADDR=:8080 \
    DATA_DIR=/data \
    EXPORT_DIR=/app/exports \
    INPUT_DIRS=/app/samples,/app/inputs \
    LOG_FORMAT=json

USER 65532:65532
EXPOSE 8080
VOLUME ["/data", "/app/exports"]
HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 CMD ["/pipeline-server", "--healthcheck"]
ENTRYPOINT ["/pipeline-server"]
