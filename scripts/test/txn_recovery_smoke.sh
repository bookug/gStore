#!/bin/bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
WAL_FILE="$ROOT_DIR/data/txn_recovery_smoke.db/update.log"

echo "[txn-recovery-smoke] root: $ROOT_DIR"
echo "[txn-recovery-smoke] this is a manual smoke helper."
echo "[txn-recovery-smoke] steps:"
echo "  1) start gserver and create/load a test db"
echo "  2) run begin -> tquery(update) -> commit"
echo "  3) stop server and restart server"
echo "  4) verify committed data is visible"
echo "  5) check WAL archive file: update.log.recovered.*"

if [ -f "$WAL_FILE" ]; then
  echo "[txn-recovery-smoke] current wal file exists: $WAL_FILE"
else
  echo "[txn-recovery-smoke] wal file not found yet: $WAL_FILE"
fi

echo "[txn-recovery-smoke] done."
