#!/usr/bin/env bash

# Configuration
RPC_URL="http://127.0.0.1:8545"
LOGFILE="/var/log/geth/memstats.log"

# Ensure log directory exists
mkdir -p "$(dirname "$LOGFILE")"

while true; do
  ts=$(date -Iseconds)
  # Fetch debug_memStats over HTTP
  stats=$(
    curl -s -X POST "${RPC_URL}" \
      -H "Content-Type: application/json" \
      -d '{"jsonrpc":"2.0","id":1,"method":"debug_memStats","params":[]}' \
    | jq '.result | {Alloc, HeapAlloc, Sys, NumGC}'
  )
  # Append timestamped JSON to log
  echo "${ts} ${stats}" >> "${LOGFILE}"
  sleep 10
done