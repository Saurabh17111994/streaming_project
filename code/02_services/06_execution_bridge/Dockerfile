# Build context MUST be the Maven/Java reactor root `code/`.
# The patch-level tag matches versions.pin. Replace with the recorded digest
# when the pinned Go image is captured in the execution evidence bundle.
ARG GO_BUILDER_IMAGE=golang:1.24.5-alpine
FROM ${GO_BUILDER_IMAGE} AS go-builder
WORKDIR /src

# Keep the relative SDK replacement used by the standalone module valid in
# the image. No Arrow credentials are copied into the image.
COPY 02_services/06_execution_bridge/go-bridge/go.mod 02_services/06_execution_bridge/go-bridge/go.sum ./02_services/06_execution_bridge/go-bridge/
COPY 02_services/01_ingestion/go-bridge/third_party ./02_services/01_ingestion/go-bridge/third_party
WORKDIR /src/02_services/06_execution_bridge/go-bridge
RUN go mod download
COPY 02_services/06_execution_bridge/go-bridge/*.go ./
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /execution-bridge .

FROM alpine:3.22
RUN addgroup -S execution && adduser -S -G execution execution
WORKDIR /app
COPY --from=go-builder /execution-bridge /app/execution-bridge
USER execution
ENV EXECUTION_BRIDGE_LISTEN_ADDR=0.0.0.0:8787
EXPOSE 8787
ENTRYPOINT ["/app/execution-bridge"]
