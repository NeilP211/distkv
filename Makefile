.PHONY: test lint proto build bench docker

test:
	go test -race ./...

lint:
	golangci-lint run

proto:
	@echo "see Phase 3"

build:
	go build ./...

bench:
	@echo "see Phase 10"

docker:
	@echo "see Phase 11"
