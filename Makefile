.PHONY: test lint proto build bench docker

test:
	go test -race ./...

lint:
	golangci-lint run

proto:
	PATH="$(shell go env GOPATH)/bin:$(PATH)" protoc \
		--go_out=. --go_opt=paths=source_relative \
		--go-grpc_out=. --go-grpc_opt=paths=source_relative \
		api/distkv.proto

build:
	go build ./...

bench:
	@echo "see Phase 10"

docker:
	@echo "see Phase 11"
