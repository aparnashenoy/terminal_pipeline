#!/usr/bin/env bash
# Anchor relative input, output and log paths regardless of cron's working directory.
set -euo pipefail
PROJECT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
cd -- "$PROJECT_DIR"
# Set PYTHON_BIN to an absolute virtualenv interpreter path when running under cron.
exec "${PYTHON_BIN:-python3}" "$PROJECT_DIR/weekly_report.py"
