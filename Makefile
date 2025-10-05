.PHONY: build install clean test test-verbose test-coverage lint fmt fmt-check check release

BINARY_NAME=cometbft-log-etl
GOBIN=$(shell go env GOPATH)/bin

build:
	go build -o $(BINARY_NAME) .

install: build
	cp $(BINARY_NAME) $(GOBIN)/$(BINARY_NAME)

clean:
	rm -f $(BINARY_NAME)
	
test:
	go test ./...

lint:
	@which golangci-lint > /dev/null 2>&1 || (echo "golangci-lint not installed. Install with: go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest" && exit 1)
	golangci-lint run --timeout=5m ./...

fmt:
	go fmt ./...
	gofmt -s -w .

run:
	@[ -n "$(DIR)" ] || DIR=example-logs/normal; \
	[ -n "$(SIM)" ] || SIM=demo-sim; \
	go run . -dir $$DIR -simulation $$SIM

# Release: create an annotated git tag for manual builds
# Usage:
#   make release VERSION=v0.1.0           # creates tag v0.1.0
#   make release TAG_PREFIX=etl- VERSION=v0.1.0  # creates tag etl-v0.1.0
#   make release-push VERSION=v0.1.0      # pushes the tag to origin

VERSION ?=
TAG_PREFIX ?=
TAG := $(if $(TAG_PREFIX),$(TAG_PREFIX)$(VERSION),$(VERSION))

release:
	@test -n "$(VERSION)" || (echo "VERSION required, e.g.: make release VERSION=v0.1.0" && exit 1)
	@git diff --quiet || (echo "Working tree not clean; commit or stash changes first" && exit 1)
	@if git rev-parse -q --verify "refs/tags/$(TAG)" >/dev/null; then \
		echo "Tag '$(TAG)' already exists"; \
		exit 1; \
	else \
		git tag -a "$(TAG)" -m "Release $(TAG)"; \
		git push origin "$(TAG)"; \
		echo "Tagged and pushed $(TAG)"; \
	fi
