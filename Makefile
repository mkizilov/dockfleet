SHELL := /bin/bash
.DEFAULT_GOAL := help

GO      ?= go
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS := -X github.com/dockfleet/dockfleet/internal/version.Version=$(VERSION)

# Pinned code-generation tools are installed into .tools/bin (project-local).
TOOLS_BIN := $(CURDIR)/.tools/bin
export PATH := $(TOOLS_BIN):$(PATH)
BUF_VERSION                := v1.47.2
PROTOC_GEN_GO_VERSION      := v1.35.2
PROTOC_GEN_GO_GRPC_VERSION := v1.5.1

COMPOSE := docker compose --env-file config/dev.env -f deploy/docker-compose.yml
PKGS    := ./...

# Development credentials live in PKI_DIR, which is git-ignored.
PKI_DIR  ?= .dev-pki
DEV_NODE ?= dev-node-1
SCHEME   ?= http

.PHONY: help
help: ## List targets
	@grep -E '^[a-zA-Z_-]+:.*## ' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-18s %s\n", $$1, $$2}'

.PHONY: tools
tools: ## Install pinned buf and protoc plugins into .tools/bin
	GOBIN=$(TOOLS_BIN) $(GO) install github.com/bufbuild/buf/cmd/buf@$(BUF_VERSION)
	GOBIN=$(TOOLS_BIN) $(GO) install google.golang.org/protobuf/cmd/protoc-gen-go@$(PROTOC_GEN_GO_VERSION)
	GOBIN=$(TOOLS_BIN) $(GO) install google.golang.org/grpc/cmd/protoc-gen-go-grpc@$(PROTOC_GEN_GO_GRPC_VERSION)

.PHONY: proto
proto: ## Generate Go code from proto/ (required before build and test)
	buf generate proto

.PHONY: generate
generate: tools proto-lint proto ## One-shot reproducible bindings: pinned tools, lint, generate

.PHONY: proto-lint
proto-lint: ## Lint the protobuf definitions
	buf lint proto

.PHONY: tidy
tidy: ## Resolve dependencies and write go.sum (needs network)
	$(GO) mod tidy

.PHONY: fmt
fmt: ## Format Go sources
	gofmt -w cmd internal

.PHONY: fmt-check
fmt-check: ## Fail if any Go source is not gofmt-formatted
	@out=$$(gofmt -l cmd internal); if [ -n "$$out" ]; then echo "Unformatted files:"; echo "$$out"; exit 1; fi

.PHONY: vet
vet: ## Run go vet
	$(GO) vet $(PKGS)

.PHONY: build
build: ## Build manager and agent into bin/
	mkdir -p bin
	$(GO) build -ldflags "$(LDFLAGS)" -o bin/dockfleet-manager ./cmd/manager
	$(GO) build -ldflags "$(LDFLAGS)" -o bin/dockfleet-agent ./cmd/agent
	$(GO) build -ldflags "$(LDFLAGS)" -o bin/dockfleet-pki ./cmd/pki

.PHONY: test
test: ## Run unit tests with the race detector
	$(GO) test -race -count=1 $(PKGS)

.PHONY: cover
cover: ## Run unit tests and print per-package coverage
	$(GO) test -count=1 -cover $(PKGS)

.PHONY: test-integration
test-integration: ## Run PostgreSQL integration tests (needs `make compose-up`)
	set -a; . config/dev.env; set +a; \
	DOCKFLEET_TEST_DATABASE_URL="$$DOCKFLEET_DATABASE_URL" $(GO) test -race -count=1 -tags integration ./internal/store/...

.PHONY: check-secrets
check-secrets: ## Fail if a private key is tracked by git
	@if git grep -qE -e '-----BEGIN [A-Z ]*PRIVATE KEY-----'; then \
		echo "A PEM private key is committed:"; git grep -lE -e '-----BEGIN [A-Z ]*PRIVATE KEY-----'; exit 1; fi
	@if [ -n "$$(git ls-files '*.key' '*.csr' '$(PKI_DIR)')" ]; then \
		echo "Key material is tracked by git:"; git ls-files '*.key' '*.csr' '$(PKI_DIR)'; exit 1; fi

.PHONY: check
check: fmt-check vet check-secrets test ## Formatting, vet, secret scan and unit tests

.PHONY: run-manager
run-manager: ## Run the manager with config/dev.env (needs `make compose-up`)
	set -a; . config/dev.env; set +a; $(GO) run ./cmd/manager

.PHONY: run-manager-memory
run-manager-memory: ## Run the manager against the in-memory store (no PostgreSQL)
	set -a; . config/dev.env; set +a; DOCKFLEET_STORE=memory $(GO) run ./cmd/manager

.PHONY: run-agent
run-agent: ## Run an agent with config/dev.env
	set -a; . config/dev.env; set +a; $(GO) run ./cmd/agent

.PHONY: dev-pki
dev-pki: ## Create a development CA plus manager and agent certificates in $(PKI_DIR)
	@test -f $(PKI_DIR)/ca.pem || $(GO) run ./cmd/pki init-ca -dir $(PKI_DIR)
	@test -f $(PKI_DIR)/manager.pem || $(GO) run ./cmd/pki issue-manager -dir $(PKI_DIR)
	@test -f $(PKI_DIR)/agents/$(DEV_NODE)/agent.pem || $(GO) run ./cmd/pki issue-agent -dir $(PKI_DIR) -node-id $(DEV_NODE)

.PHONY: run-manager-mtls
run-manager-mtls: dev-pki ## Run the manager with TLS: HTTPS admin API and mutual TLS for agents
	set -a; . config/dev.env; . config/dev-mtls-manager.env; set +a; $(GO) run ./cmd/manager

.PHONY: run-agent-mtls
run-agent-mtls: dev-pki ## Run an agent authenticating with its client certificate
	set -a; . config/dev.env; . config/dev-mtls-agent.env; set +a; $(GO) run ./cmd/agent

.PHONY: dev-enroll
dev-enroll: ## Enroll DEV_NODE through the admin API (SCHEME=https when the manager runs with TLS)
	set -a; . config/dev.env; set +a; \
	curl --fail-with-body -sS --cacert $(PKI_DIR)/ca.pem -H "Authorization: Bearer $$DOCKFLEET_API_TOKEN" \
		-d '{"node_id":"$(DEV_NODE)","description":"development node"}' $(SCHEME)://$$DOCKFLEET_API_ADDR/v1/enrollments

.PHONY: compose-up
compose-up: ## Start PostgreSQL for development (127.0.0.1 only)
	$(COMPOSE) up -d --wait

.PHONY: compose-down
compose-down: ## Stop development dependencies (keeps the data volume)
	$(COMPOSE) down

.PHONY: compose-reset
compose-reset: ## Stop development dependencies and delete their data
	$(COMPOSE) down --volumes

.PHONY: clean
clean: ## Remove build output and generated protobuf code (development credentials stay)
	rm -rf bin internal/protocol/dockfleet

.PHONY: clean-pki
clean-pki: ## Delete the development CA and every development certificate and key
	rm -rf $(PKI_DIR)
