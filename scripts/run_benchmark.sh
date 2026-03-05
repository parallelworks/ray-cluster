#!/bin/bash
# run_benchmark.sh — Submit Ray benchmark and stream results to dashboard
#
# Environment variables:
#   DASHBOARD_URL - URL of the dashboard
#   RAY_HEAD_IP   - IP of the Ray head node (for site detection)
#   NUM_TASKS     - Number of benchmark tasks (default: 500)
#   MATRIX_SIZE   - Matrix size for CPU compute (default: 500)

set -e

echo "=========================================="
echo "Ray Benchmark Starting: $(date)"
echo "=========================================="
echo "Dashboard URL: ${DASHBOARD_URL}"
echo "Ray head IP:   ${RAY_HEAD_IP}"
echo "Num tasks:     ${NUM_TASKS:-500}"
echo "Matrix size:   ${MATRIX_SIZE:-500}"

JOB_DIR="${PW_PARENT_JOB_DIR%/}"
SCRIPT_DIR="${JOB_DIR}/scripts"

# Activate venv if available
VENV_DIR="${JOB_DIR}/.venv"
if [ -f "${VENV_DIR}/bin/python" ]; then
    source "${VENV_DIR}/bin/activate"
    PYTHON_CMD="${VENV_DIR}/bin/python"
else
    PYTHON_CMD="python3"
fi

# Wait for workers to join the cluster (at least 2 nodes = head + worker)
echo "Waiting for Ray workers to join..."
attempt=1
MAX_ATTEMPTS=90
while [ ${attempt} -le ${MAX_ATTEMPTS} ]; do
    NODE_COUNT=$(ray status 2>/dev/null | grep -c "node_" || echo "0")
    echo "[${attempt}/${MAX_ATTEMPTS}] Ray nodes: ${NODE_COUNT}"
    if [ "${NODE_COUNT}" -ge 2 ]; then
        echo "Workers joined! ${NODE_COUNT} nodes in cluster."
        break
    fi
    sleep 5
    ((attempt++))
done

if [ ${attempt} -gt ${MAX_ATTEMPTS} ]; then
    echo "[WARN] Timeout waiting for workers. Running with available nodes."
fi

ray status

# Auto-detect cluster name for on-prem (site-1)
CLUSTER_NAME=""
SCHEDULER_TYPE="ssh"
if command -v pw &>/dev/null || [ -x "${HOME}/pw/pw" ]; then
    PW_CMD=$(command -v pw 2>/dev/null || echo "${HOME}/pw/pw")
    while IFS= read -r uri; do
        name="${uri##*/}"
        CLUSTER_NAME="${name}"
        break
    done < <(${PW_CMD} cluster list 2>/dev/null | awk '/^pw:\/\/'"${PW_USER}"'/ && /active/ && /existing/ {print $1}')
fi

echo "On-prem cluster name: ${CLUSTER_NAME:-unknown}"

# Run benchmark
${PYTHON_CMD} "${SCRIPT_DIR}/benchmark.py" \
    --dashboard-url "${DASHBOARD_URL}" \
    --ray-head-ip "${RAY_HEAD_IP}" \
    --num-tasks "${NUM_TASKS:-500}" \
    --matrix-size "${MATRIX_SIZE:-500}" \
    --onprem-cluster-name "${CLUSTER_NAME}"

echo "=========================================="
echo "Benchmark Complete!"
echo "=========================================="
