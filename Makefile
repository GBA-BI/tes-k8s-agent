# The old school Makefile, following are required targets. The Makefile is written
# to allow building multiple binaries. You are free to add more targets or change
# existing implementations, as long as the semantics are preserved.
#
#   make                - default to 'build' target
#   make lint           - code analysis
#   make test           - run unit test (or plus integration test)
#   make build          - alias to build-local target
#   make build-local    - build local binary targets
#   make build-linux    - build linux binary targets
#   make container      - build containers
#   $ docker login registry -u username -p xxxxx
#   make push           - push containers
#   make clean          - clean up targets
#
# Not included but recommended targets:
#   make e2e-test
#
# The makefile is also responsible to populate project version information.
#

#
# Tweak the variables based on your project.
#

# This repo's root import path (under GOPATH).
ROOT := code.byted.org/epscp/vetes-k8s-agent

# Module name.
NAME := vetes-k8s-agent

# Container image prefix and suffix added to targets.
# The final built images are:
#   $[REGISTRY]/$[IMAGE_PREFIX]$[TARGET]$[IMAGE_SUFFIX]:$[VERSION]
# $[REGISTRY] is an item from $[REGISTRIES], $[TARGET] is an item from $[TARGETS].
IMAGE_PREFIX ?= $(strip )
IMAGE_SUFFIX ?= $(strip )

# Container registries.
REGISTRY ?= hub.byted.org/infcprelease

# Helm chart repo
CHART_REPO ?= charts

#
# These variables should not need tweaking.
#

# It's necessary to set this because some environments don't link sh -> bash.
export SHELL := /bin/bash

# It's necessary to set the errexit flags for the bash shell.
export SHELLOPTS := errexit

# Project main package location.
CMD_DIR := ./cmd

# Project output directory.
OUTPUT_DIR := ./bin

# Build directory.
BUILD_DIR := ./build

IMAGE_NAME := $(IMAGE_PREFIX)$(NAME)$(IMAGE_SUFFIX)

# Current version of the project.
VERSION      ?= $(shell git describe --tags --always --dirty)
BRANCH       ?= $(shell git branch | grep \* | cut -d ' ' -f2)
GITCOMMIT    ?= $(shell git rev-parse HEAD)
GITTREESTATE ?= $(if $(shell git status --porcelain),dirty,clean)
BUILDTIME    ?= $(shell date -u +"%Y-%m-%dT%H:%M:%SZ")

# Track code version with Docker Label.
DOCKER_LABELS ?= git-describe="$(shell date -u +v%Y%m%d)-$(shell git describe --tags --always --dirty)"

# Golang standard bin directory.
GOPATH ?= $(shell go env GOPATH)
BIN_DIR := $(GOPATH)/bin
GOLANGCI_LINT := $(BIN_DIR)/golangci-lint
HELM_LINT := $(BIN_DIR)/helm
GOMOCK := $(BIN_DIR)/mockgen

# Default golang flags used in build and test
# -count: run each test and benchmark 1 times. Set this flag to disable test cache
GOFLAGS += -count=1
GOLDFLAGS += -s -w -X  $(ROOT)/pkg/version.module=$(NAME) \
	-X $(ROOT)/pkg/version.version=$(VERSION)             \
	-X $(ROOT)/pkg/version.branch=$(BRANCH)               \
	-X $(ROOT)/pkg/version.gitCommit=$(GITCOMMIT)         \
	-X $(ROOT)/pkg/version.gitTreeState=$(GITTREESTATE)   \
	-X $(ROOT)/pkg/version.buildTime=$(BUILDTIME)

#
# Define all targets. At least the following commands are required:
#

# All targets.
.PHONY: lint test build container push

build: build-local

# more info about `GOGC` env: https://github.com/golangci/golangci-lint#memory-usage-of-golangci-lint
lint: $(GOLANGCI_LINT) $(HELM_LINT)
	@$(GOLANGCI_LINT) run
	@$(HELM_LINT) lint manifests/vetes-k8s-agent

$(GOLANGCI_LINT):
	go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest

$(HELM_LINT):
	go install helm.sh/helm/v3/cmd/helm@v3.13.3

test:
	@go test -race -coverpkg=$(ROOT)/... -coverprofile=coverage.out $(ROOT)/...
	@go tool cover -func coverage.out | tail -n 1 | awk '{ print "Total coverage: " $$3 }'

build-local:
	@go build -v -o $(OUTPUT_DIR)/$(NAME) -ldflags "$(GOLDFLAGS)" $(CMD_DIR);

build-linux:
	@GOOS=linux GOARCH=amd64 GOFLAGS="$(GOFLAGS)" go build -v -o $(OUTPUT_DIR)/$(NAME) -ldflags "$(GOLDFLAGS)" $(CMD_DIR)

container:
	@docker build -t $(REGISTRY)/$(IMAGE_NAME):$(VERSION) \
	  --label $(DOCKER_LABELS)                            \
	  -f $(BUILD_DIR)/Dockerfile .;

push: container
	@docker push $(REGISTRY)/$(IMAGE_NAME):$(VERSION);

.PHONY: clean
clean:
	@-rm -vrf ${OUTPUT_DIR}

.PHONY: images-list check-images

images-list:
	@find manifests -type f | sed -n 's/Chart.yaml//p' \
	| xargs -L1 helm template --set platformConfig.imageRegistry=REGISTRY_DOMAIN \
	--set platformConfig.imageRepositoryRelease=release \
	--set platformConfig.imageRepositoryLibrary=library \
	| grep -Eo "REGISTRY_DOMAIN/.*:.*" | sed "s#^REGISTRY_DOMAIN/##g" \
	| tr -d "[:blank:],']{}\[\]\`\"" | sort -u | tee /tmp/images.list
	@echo "" >> /tmp/images.list
	@find manifests -type f -name "images*.list" \
	| xargs -L1 grep -E '^library|^release' >> /tmp/images.list || true

check-images: images-list
	@grep -Ev 'TagOfRepo|imageTagFromGitTag' /tmp/images.list | grep -E '^library|^release' \
	| sed -n 's|^|hub.byted.org/infcp|p' | xargs -L1 -P 16 -I {} skopeo inspect --raw docker://{} > /dev/null

$(GOMOCK):
	go install github.com/golang/mock/mockgen@v1.6.0

.PHONY: mock
mock: $(GOMOCK)
	@$(GOMOCK) -source pkg/vetesclient/client.go -destination pkg/vetesclient/fake/client.go -package fake -mock_names=Client=FakeClient
	@$(GOMOCK) -source pkg/offload/offload.go -destination pkg/offload/fake/offload.go -package fake -mock_names=Helper=FakeHelper
	@$(GOMOCK) -source pkg/localstore/localstore.go -destination pkg/localstore/fake/localstore.go -package fake -mock_names=Helper=FakeHelper
	@$(GOMOCK) -source pkg/accelerate/accelerate.go -destination pkg/accelerate/fake/accelerate.go -package fake -mock_names=Accelerator=FakeAccelerator
