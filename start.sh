#!/bin/bash
# ============================================================
#  City Pulse — Auto Setup & Run Script (Mac / Linux)
#  Usage: bash start.sh
# ============================================================

set -e   # stop on any error

# ── Colors for output ─────────────────────────────────────
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

log()  { echo -e "${GREEN}[✔]${NC} $1"; }
warn() { echo -e "${YELLOW}[!]${NC} $1"; }
err()  { echo -e "${RED}[✘]${NC} $1"; exit 1; }

echo ""
echo "╔══════════════════════════════════════════════════════════╗"
echo "║   🏙  City Pulse — NYC ETL Pipeline Setup               ║"
echo "╚══════════════════════════════════════════════════════════╝"
echo ""

# ── Step 1: Check Python ──────────────────────────────────
log "Checking Python version..."
if ! command -v python3 &>/dev/null; then
    err "Python 3 not found. Install it from https://python.org"
fi
PYTHON_VERSION=$(python3 -c "import sys; print(sys.version_info.minor)")
if [ "$PYTHON_VERSION" -lt 10 ]; then
    err "Python 3.10+ required. Found: $(python3 --version)"
fi
log "Python OK — $(python3 --version)"

# ── Step 2: Create dags/__init__.py if missing ────────────
log "Checking dags/ folder..."
if [ ! -d "dags" ]; then
    warn "dags/ folder not found — creating it..."
    mkdir -p dags
fi
if [ ! -f "dags/__init__.py" ]; then
    warn "dags/__init__.py missing — creating it..."
    touch dags/__init__.py
fi
log "dags/ folder OK"

# ── Step 3: Virtual environment ───────────────────────────
if [ ! -d "venv" ]; then
    log "Creating virtual environment..."
    python3 -m venv venv
else
    log "Virtual environment already exists — skipping"
fi

log "Activating virtual environment..."
source venv/bin/activate

# ── Step 4: Install dependencies ─────────────────────────
log "Installing dependencies..."
pip install --quiet --upgrade pip
pip install --quiet flask pandas requests numpy
log "Dependencies installed"

# ── Step 5: Check required files ─────────────────────────
log "Checking required project files..."
REQUIRED=("run.py" "config.py" "database.py" "transforms.py"
          "data_generator.py" "dashboard.py" "scheduler.py"
          "dags/city_pulse_dag.py")
for f in "${REQUIRED[@]}"; do
    if [ ! -f "$f" ]; then
        err "Missing required file: $f — make sure all project files are in this folder"
    fi
done
log "All required files found"

# ── Step 6: Launch ────────────────────────────────────────
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  Setup complete! Launching City Pulse..."
echo ""
echo "  Dashboard will be available at: http://localhost:5050"
echo "  Running backfill (90 days of data) — takes ~1-2 minutes"
echo "  Press Ctrl+C at any time to stop"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

python3 run.py --backfill --rows 100000
