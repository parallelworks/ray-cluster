#!/bin/bash
# diagnose.sh — Run cluster diagnostics on an active ray-cluster-demo run
#
# Usage: bash scripts/diagnose.sh [RUN_SLUG]
#   e.g.: bash scripts/diagnose.sh ray-cluster-demo-00011
#
# Uses pw ssh to connect to the on-prem head node and run diagnostics.

set -e

SLUG="${1:-}"
if [ -z "$SLUG" ]; then
    echo "Usage: bash scripts/diagnose.sh <run-slug>"
    echo "  e.g.: bash scripts/diagnose.sh ray-cluster-demo-00011"
    echo ""
    echo "Recent runs:"
    pw workflows runs list ray_cluster_demo -o json 2>/dev/null | \
        python3 -c "import json,sys; runs=json.load(sys.stdin)['runs']; [print(f'  {r[\"slug\"]}  {r[\"status\"]}') for r in runs[:5]]" 2>/dev/null || \
        echo "  (could not list runs)"
    exit 1
fi

echo "=========================================="
echo "Ray Cluster Diagnostics: ${SLUG}"
echo "=========================================="

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Find the on-prem resource (a30gpuserver)
ONPREM_RESOURCE=""
PW_USER="${PW_USER:-$(pw auth status -o json 2>/dev/null | python3 -c 'import json,sys; print(json.load(sys.stdin).get("user",""))' 2>/dev/null || echo '')}"

# Detect resources from pw cluster list
echo "Detecting resources..."
while IFS=$'\t' read -r uri status type rest; do
    name="${uri##*/}"
    if [[ "$type" == *"existing"* ]]; then
        ONPREM_RESOURCE="${name}"
        echo "  On-prem: ${name} (${status})"
    else
        echo "  Cloud:   ${name} (${status}, ${type})"
    fi
done < <(pw cluster list 2>/dev/null | grep "^pw://" || true)

if [ -z "${ONPREM_RESOURCE}" ]; then
    echo "[ERROR] Could not find on-prem resource"
    exit 1
fi

echo ""
echo "Connecting to ${ONPREM_RESOURCE} via pw ssh..."
echo ""

# Find the job directory on the remote machine
JOB_NUM=$(echo "$SLUG" | grep -oP '\d+$')
JOB_DIR_PATTERN="ray_cluster_demo"

# Run diagnostics remotely
pw ssh "${ONPREM_RESOURCE}" bash -s <<'REMOTE_SCRIPT'
set -e
echo "=== Remote host: $(hostname) ==="
echo "=== Date: $(date) ==="

# Find the latest job directory
JOB_DIR=""
for d in ~/pw/jobs/ray_cluster_demo/*/; do
    if [ -f "$d/.venv/bin/python" ] && [ -f "$d/scripts/diagnose_cluster.py" ]; then
        JOB_DIR="$d"
    fi
done

if [ -z "$JOB_DIR" ]; then
    echo "[ERROR] No job directory found with diagnose_cluster.py"
    echo "Available job dirs:"
    ls -la ~/pw/jobs/ray_cluster_demo/ 2>/dev/null || echo "  (none)"
    exit 1
fi

echo "Using job dir: ${JOB_DIR}"
cd "$JOB_DIR"

# Activate venv
if [ -f .venv/bin/activate ]; then
    source .venv/bin/activate
fi

# Check Ray is running
echo ""
echo "=== Ray Status ==="
ray status 2>&1 || echo "Ray not running or not accessible"

echo ""
echo "=== Running Diagnostics ==="
python3 scripts/diagnose_cluster.py --num-tasks 20

REMOTE_SCRIPT

echo ""
echo "=========================================="
echo "Diagnostics complete"
echo "=========================================="
