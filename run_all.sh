#!/usr/bin/env bash
# run_all.sh — One-click full pipeline: generate data, benchmark 1M, scalability benchmark.
# Works on Linux, macOS, and Windows (Git Bash / WSL).
#
# Usage:
#   bash run_all.sh            # full run (all phases)
#   bash run_all.sh --skip-gen # skip data generation (datasets already exist)

set -euo pipefail

SKIP_GEN=false
for arg in "$@"; do
  [[ "$arg" == "--skip-gen" ]] && SKIP_GEN=true
done

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# ── Helpers ──────────────────────────────────────────────────────────────────

log()  { echo; echo "══════════════════════════════════════════════════════"; echo "  $*"; echo "══════════════════════════════════════════════════════"; }
step() { echo "  ▶ $*"; }

wait_for_trino() {
  local url="http://localhost:8081/v1/info"
  local timeout=180
  local elapsed=0
  step "Waiting for Trino to be ready (up to ${timeout}s)..."
  until curl -sf "$url" | python3 -c "import sys,json; d=json.load(sys.stdin); sys.exit(0 if not d.get('starting',True) else 1)" 2>/dev/null; do
    sleep 5
    elapsed=$((elapsed + 5))
    if [[ $elapsed -ge $timeout ]]; then
      echo "  [ERROR] Trino did not become ready within ${timeout}s" >&2
      return 1
    fi
    echo "    ... ${elapsed}s elapsed"
  done
  echo "  [OK] Trino is ready"
}

# ── Preflight checks ─────────────────────────────────────────────────────────

log "Preflight checks"

if ! command -v docker &>/dev/null; then
  echo "  [ERROR] Docker is not installed or not in PATH."
  echo "          Install Docker Desktop from https://www.docker.com/products/docker-desktop/"
  echo "          On Linux: https://docs.docker.com/engine/install/"
  exit 1
fi

if ! docker info &>/dev/null; then
  echo "  [ERROR] Docker daemon is not running. Please start Docker Desktop."
  exit 1
fi

if ! command -v python3 &>/dev/null; then
  echo "  [ERROR] python3 is not installed or not in PATH."
  exit 1
fi

echo "  [OK] Docker and Python found"

# ── Phase 0: Python virtualenv + dependencies ────────────────────────────────

log "PHASE 0 — Python virtualenv + dependencies"

VENV_DIR=".venv"

if [[ ! -d "$VENV_DIR" ]]; then
  step "Creating virtualenv at .venv..."
  python3 -m venv "$VENV_DIR"
fi

step "Activating virtualenv..."
# shellcheck disable=SC1091
source "$VENV_DIR/bin/activate"

step "Installing dependencies from requirements.txt..."
pip install -q -r requirements.txt
echo "  [OK] Virtualenv ready and dependencies installed"

# ── Phase 1: Generate datasets ───────────────────────────────────────────────

if [[ "$SKIP_GEN" == "false" ]]; then
  log "PHASE 1 — Data generation"

  step "Generating standard 1M-row dataset (seed=42)..."
  python3 Generator/Generator.py --type large --scale 1 --partitions 12 --seed 42

  step "Generating all scalability datasets (1K → 10M)..."
  python3 Generator/generate_all_scales.py

  echo "  [OK] All datasets ready"
else
  log "PHASE 1 — Skipped (--skip-gen)"
fi

# ── Phase 2: Standard benchmark on 1M rows ───────────────────────────────────

log "PHASE 2 — Standard benchmark (1M rows, docker-compose.yml)"

step "Starting standard Docker stack (Trino + PostgreSQL)..."
docker compose down -v --remove-orphans 2>/dev/null || true
docker compose up -d

wait_for_trino

step "Running benchmark.py..."
TRINO_PORT=8081 python3 benchmark.py

step "Generating plots and report..."
python3 plot_results.py

echo "  [OK] Standard benchmark done — results in output/ and figures/"

step "Tearing down standard stack..."
docker compose down -v

# ── Phase 3: Scalability benchmark ───────────────────────────────────────────

log "PHASE 3 — Scalability benchmark (1K → 10M rows)"

step "Running run_scalability.py (manages its own docker-compose lifecycle)..."
python3 run_scalability.py

step "Generating scalability plots..."
python3 plot_scalability.py

echo "  [OK] Scalability benchmark done — results in output/scalability/"

# ── Done ──────────────────────────────────────────────────────────────────────

log "ALL DONE"
echo "  Standard results  : output/results.csv, figures/"
echo "  Scalability results: output/scalability/"
echo
