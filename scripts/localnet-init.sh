#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INDEXER_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
SOLANA_ROOT="$(cd "$INDEXER_ROOT/../8004-solana" && pwd)"

echo "[localnet] initializing on-chain state from $SOLANA_ROOT"
(
  cd "$SOLANA_ROOT"
  ANCHOR_PROVIDER_URL="${ANCHOR_PROVIDER_URL:-http://127.0.0.1:8899}" \
  ANCHOR_WALLET="${ANCHOR_WALLET:-$HOME/.config/solana/id.json}" \
  npx ts-mocha -p ./tsconfig.json -t 1000000 tests/init-localnet.ts
)
