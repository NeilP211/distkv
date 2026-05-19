#!/usr/bin/env bash
# cluster-up.sh — build distkvd and launch a local 3-node DistKV cluster.
#
# Nodes n1/n2/n3 listen on 127.0.0.1:9001/9002/9003, each with its own data
# directory under ./data.  Each daemon is backgrounded and its pid written to
# ./data/<id>.pid so cluster-down.sh can stop it.
set -euo pipefail

cd "$(dirname "$0")/.."

PEERS="n1=127.0.0.1:9001,n2=127.0.0.1:9002,n3=127.0.0.1:9003"

echo "building distkvd..."
go build -o bin/distkvd ./cmd/distkvd

mkdir -p data

start_node() {
  local id="$1" listen="$2"
  local dir="./data/${id}"
  mkdir -p "${dir}"
  echo "starting ${id} on ${listen} (data: ${dir})"
  ./bin/distkvd \
    --id "${id}" \
    --listen "${listen}" \
    --peers "${PEERS}" \
    --data-dir "${dir}" \
    >"./data/${id}.log" 2>&1 &
  echo $! >"./data/${id}.pid"
}

start_node n1 127.0.0.1:9001
start_node n2 127.0.0.1:9002
start_node n3 127.0.0.1:9003

echo "cluster up: 3 nodes started; logs in ./data/<id>.log"
