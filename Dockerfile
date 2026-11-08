# syntax=docker/dockerfile:1.7
#
# DockFleet images. One file, two targets sharing one build:
#
#   docker build --target manager -t dockfleet-manager .
#   docker build --target agent   -t dockfleet-agent   .
#
# The build is reproducible from the repository alone: the generated protobuf
# code is not committed, so it is generated here with the same pinned tools as
# `make generate`. The pins below must equal the ones in the Makefile and go.mod
# (a test, deploy/artifacts_test.go, checks it).
#
# Both images contain one static binary and nothing else: no shell, no package
# manager, no curl. They run as an unprivileged user with a read-only root
# filesystem in the examples under deploy/examples. HEALTHCHECK is the binary
# asking itself (`<binary> healthcheck`), so the image needs no other tool.

ARG GO_VERSION=1.26.10
ARG BUF_VERSION=v1.47.2
ARG PROTOC_GEN_GO_VERSION=v1.35.2
ARG PROTOC_GEN_GO_GRPC_VERSION=v1.5.1
# Pin the runtime base by digest in your own build (`docker buildx imagetools
# inspect <image>` prints it), or mirror it into your registry and point this at the mirror.
ARG RUNTIME_IMAGE=gcr.io/distroless/static-debian12:nonroot

FROM --platform=$BUILDPLATFORM golang:${GO_VERSION}-bookworm AS build
ARG BUF_VERSION
ARG PROTOC_GEN_GO_VERSION
ARG PROTOC_GEN_GO_GRPC_VERSION
ARG TARGETOS
ARG TARGETARCH
ARG VERSION=dev
ENV CGO_ENABLED=0 GOTOOLCHAIN=local GOFLAGS=-trimpath
WORKDIR /src

# The generators are installed first so that they are cached until a pin changes.
RUN GOBIN=/tools go install github.com/bufbuild/buf/cmd/buf@${BUF_VERSION} \
 && GOBIN=/tools go install google.golang.org/protobuf/cmd/protoc-gen-go@${PROTOC_GEN_GO_VERSION} \
 && GOBIN=/tools go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@${PROTOC_GEN_GO_GRPC_VERSION}

COPY go.mod go.sum ./
RUN go mod download

COPY buf.gen.yaml ./
COPY proto ./proto
COPY cmd ./cmd
COPY internal ./internal
RUN PATH=/tools:$PATH buf generate proto

RUN GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build \
      -ldflags "-s -w -X github.com/dockfleet/dockfleet/internal/version.Version=${VERSION}" \
      -o /out/dockfleet-manager ./cmd/manager \
 && GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build \
      -ldflags "-s -w -X github.com/dockfleet/dockfleet/internal/version.Version=${VERSION}" \
      -o /out/dockfleet-agent ./cmd/agent

# ---- manager -----------------------------------------------------------------

FROM ${RUNTIME_IMAGE} AS manager
COPY --from=build /out/dockfleet-manager /usr/local/bin/dockfleet-manager
LABEL org.opencontainers.image.title="dockfleet-manager" \
      org.opencontainers.image.description="DockFleet control plane: admin API and agent gRPC service" \
      org.opencontainers.image.source="https://github.com/dockfleet/dockfleet" \
      org.opencontainers.image.licenses="MIT"
# In a container the manager has to listen on all interfaces, and DockFleet
# refuses a non-loopback address without TLS: the image does not start without
# DOCKFLEET_TLS_CERT_FILE, DOCKFLEET_TLS_KEY_FILE and DOCKFLEET_TLS_CLIENT_CA_FILE.
ENV DOCKFLEET_API_ADDR=0.0.0.0:21280 \
    DOCKFLEET_GRPC_ADDR=0.0.0.0:21281 \
    DOCKFLEET_LOG_FORMAT=json
USER 65532:65532
EXPOSE 21280 21281
HEALTHCHECK --interval=10s --timeout=5s --start-period=20s --retries=3 \
  CMD ["/usr/local/bin/dockfleet-manager", "healthcheck"]
STOPSIGNAL SIGTERM
ENTRYPOINT ["/usr/local/bin/dockfleet-manager"]

# ---- agent -------------------------------------------------------------------

FROM ${RUNTIME_IMAGE} AS agent
COPY --from=build /out/dockfleet-agent /usr/local/bin/dockfleet-agent
LABEL org.opencontainers.image.title="dockfleet-agent" \
      org.opencontainers.image.description="DockFleet worker agent: runs the containers the manager assigns to this node" \
      org.opencontainers.image.source="https://github.com/dockfleet/dockfleet" \
      org.opencontainers.image.licenses="MIT"
ENV DOCKFLEET_LOG_FORMAT=json
# The agent needs the node's Docker Engine socket, which is access to the node.
# Running the agent as a service on the host is the recommended way (see
# docs/deployment.md); in a container, add the group that owns the socket with
# `--group-add` (the group id of the socket), rather than running as root.
USER 65532:65532
HEALTHCHECK --interval=10s --timeout=5s --start-period=20s --retries=3 \
  CMD ["/usr/local/bin/dockfleet-agent", "healthcheck"]
STOPSIGNAL SIGTERM
ENTRYPOINT ["/usr/local/bin/dockfleet-agent"]
