#!/usr/bin/env bash
# Sequoia-X：定时任务入口（日常模式）。由 crontab 在 16:00 / 20:00 调用。
set -euo pipefail
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"
LOG_DIR="${LOG_DIR:-$REPO_ROOT/logs}"
mkdir -p "$LOG_DIR"
{
  echo "===== $(date -Is) ====="
  exec "$REPO_ROOT/.venv/bin/python" "$REPO_ROOT/main.py" "$@"
} >>"$LOG_DIR/cron_main.log" 2>&1
