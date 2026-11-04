# syntax=docker/dockerfile:1

FROM golang:1.23-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
# Pure-Go SQLite driver: no CGO toolchain needed, binary stays fully static.
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/pipeline-server ./cmd/server \
 && mkdir -p /out/data /out/exports

FROM gcr.io/distroless/static-debian12:nonroot
WORKDIR /app
COPY --from=build /out/pipeline-server /pipeline-server
COPY --chown=65532:65532 samples /app/samples
# Pre-create writable directories owned by the nonroot user (uid 65532). Docker seeds new named
# volumes from these paths, so they inherit the ownership instead of being root-owned.
COPY --from=build --chown=65532:65532 /out/data /data
COPY --from=build --chown=65532:65532 /out/exports /app/exports
ENV ADDR=:8080 \
    DATA_DIR=/data \
    EXPORT_DIR=/app/exports \
    INPUT_DIRS=/app/samples \
    LOG_FORMAT=json
USER 65532:65532
EXPOSE 8080
VOLUME ["/data", "/app/exports"]
HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 CMD ["/pipeline-server", "--healthcheck"]
ENTRYPOINT ["/pipeline-server"]
