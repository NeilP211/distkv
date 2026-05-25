.PHONY: test lint proto build bench docker demo-web

test:
	go test -race ./...

# demo-web builds and launches the live web showcase: a single binary that runs
# an in-process Raft cluster and serves a terminal-styled browser console for it.
demo-web:
	go build -o bin/distkv-web ./cmd/distkv-web
	@echo "DistKV web showcase → http://localhost:8080"
	bin/distkv-web

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
	go build -o bin/distkv-bench ./cmd/distkv-bench
	./scripts/cluster-up.sh
	@echo "warming up..."
	@sleep 2
	bin/distkv-bench --workload write-only --duration 10s
	bin/distkv-bench --workload read-only --duration 10s
	bin/distkv-bench --workload mixed --duration 10s
	./scripts/cluster-down.sh --clean

docker:
	@echo "see Phase 11"
