BINARY ?= bin/notcrawl

.PHONY: build test run fmt release-notes release-snapshot release-check

build:
	go build -o $(BINARY) ./cmd/notcrawl

test:
	go test ./...

run:
	go run ./cmd/notcrawl $(ARGS)

fmt:
	gofmt -w $$(find . -name '*.go' -not -path './.git/*')

release-notes:
	@test -n "$(TAG)" || (echo "usage: make release-notes TAG=v0.1.0" >&2; exit 2)
	scripts/release-notes.sh "$(TAG)"

release-check:
	goreleaser check

release-snapshot:
	goreleaser release --snapshot --clean
