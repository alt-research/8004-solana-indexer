#!/usr/bin/env bash
set -euo pipefail

PID_FILE="/tmp/8004-solana-test-validator.pid"

if [[ -f "$PID_FILE" ]]; then
  pid="$(cat "$PID_FILE")"
  if [[ -n "${pid:-}" ]]; then
    kill "$pid" >/dev/null 2>&1 || true
  fi
  rm -f "$PID_FILE"
fi

pkill -f "solana-test-validator" >/dev/null 2>&1 || true
