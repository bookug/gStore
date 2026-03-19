#!/bin/bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
DB_NAME="${1:-txn_recovery_smoke}"
DB_DIR="$ROOT_DIR/data/${DB_NAME}.db"
WAL_FILE="$DB_DIR/update.log"

echo "[txn-recovery-assert] db=${DB_NAME}"
echo "[txn-recovery-assert] db_dir=${DB_DIR}"

if [ ! -d "$DB_DIR" ]; then
  echo "[txn-recovery-assert] skip: db dir does not exist."
  exit 0
fi

if [ ! -f "$WAL_FILE" ]; then
  echo "[txn-recovery-assert] fail: wal file missing: $WAL_FILE"
  exit 1
fi

ARCHIVE_COUNT=$(ls "$WAL_FILE".recovered.* 2>/dev/null | wc -l | tr -d ' ')
if [ "$ARCHIVE_COUNT" -lt 1 ]; then
  echo "[txn-recovery-assert] fail: no recovered wal archive found."
  echo "[txn-recovery-assert] expected: $WAL_FILE.recovered.*"
  exit 1
fi

echo "[txn-recovery-assert] ok: found ${ARCHIVE_COUNT} recovered wal archive files."
