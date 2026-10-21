#!/usr/bin/env bash
# Start a local 3-node cluster. Ctrl-C stops all three.
set -euo pipefail
cd "$(dirname "$0")/.."
go build -o bin/driftless ./cmd/driftless
mkdir -p data
trap 'kill $(jobs -p) 2>/dev/null' EXIT
for id in 1 2 3; do
  bin/driftless node -id "$id" -data data 2>&1 | sed "s/^/  /" &
done
echo "cluster up on :7001 :7002 :7003  (data in ./data, Ctrl-C to stop)"
wait
