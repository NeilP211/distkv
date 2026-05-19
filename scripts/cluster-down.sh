#!/usr/bin/env bash
# cluster-down.sh — stop the local DistKV cluster started by cluster-up.sh.
#
# Kills each pid recorded in ./data/<id>.pid.  Pass --clean to also delete the
# ./data directory (pids, logs, and bbolt databases).
set -euo pipefail

cd "$(dirname "$0")/.."

CLEAN=0
if [[ "${1:-}" == "--clean" ]]; then
  CLEAN=1
fi

if [[ -d data ]]; then
  for pidfile in data/*.pid; do
    [[ -e "${pidfile}" ]] || continue
    pid="$(cat "${pidfile}")"
    if kill -0 "${pid}" 2>/dev/null; then
      echo "stopping pid ${pid} (${pidfile})"
      kill "${pid}" 2>/dev/null || true
    fi
    rm -f "${pidfile}"
  done
fi

echo "cluster down."

if [[ "${CLEAN}" == "1" ]]; then
  echo "cleaning ./data"
  rm -rf data
fi
