FROM golang:1.23-alpine AS build
WORKDIR /src
COPY go.mod ./
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/pipeline-server ./cmd/server

FROM gcr.io/distroless/static-debian12:nonroot
WORKDIR /app
COPY --from=build /out/pipeline-server /pipeline-server
ENV ADDR=:8080 DATA_DIR=/data
EXPOSE 8080
VOLUME ["/data", "/app/exports"]
ENTRYPOINT ["/pipeline-server"]
