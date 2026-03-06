#!/bin/bash
# run_benchmark.sh — Submit Ray benchmark and stream results to dashboard
#
# Environment variables:
#   DASHBOARD_URL  - URL of the dashboard
#   RAY_HEAD_IP    - IP of the Ray head node (for site detection)
#   WORKLOAD_TYPE  - "benchmark" or "fractal" (default: benchmark)
#   NUM_TASKS      - Number of benchmark tasks (default: 500)
#   MATRIX_SIZE    - Matrix size for CPU compute (default: 500)
#   GRID_SIZE      - Fractal grid size per side (default: 16)
#   IMAGE_SIZE     - Fractal tile pixel size (default: 256)
#   PALETTE        - Fractal color palette (default: electric)

set -e

WORKLOAD_TYPE="${WORKLOAD_TYPE:-benchmark}"
GRID_SIZE="${GRID_SIZE:-16}"
IMAGE_SIZE="${IMAGE_SIZE:-256}"
PALETTE="${PALETTE:-electric}"

echo "=========================================="
echo "Ray Workload Starting: $(date)"
echo "=========================================="
echo "Dashboard URL:  ${DASHBOARD_URL}"
echo "Ray head IP:    ${RAY_HEAD_IP}"
echo "Workload type:  ${WORKLOAD_TYPE}"
echo "Num tasks:      ${NUM_TASKS:-500}"
echo "Matrix size:    ${MATRIX_SIZE:-500}"
echo "Grid size:      ${GRID_SIZE}"
echo "Image size:     ${IMAGE_SIZE}"
echo "Palette:        ${PALETTE}"

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

# Point ray CLI at the head node (needed when running on login node with SLURM)
export RAY_ADDRESS="${RAY_HEAD_IP}:6379"
echo "Ray address:    ${RAY_ADDRESS}"

# Fix DASHBOARD_URL when benchmark runs on a different host than the dashboard
# (e.g., login node in SLURM while dashboard is on compute node)
MY_IP=$(hostname -I 2>/dev/null | awk '{print $1}')
if [[ "${DASHBOARD_URL}" == *"localhost"* || "${DASHBOARD_URL}" == *"127.0.0.1"* ]] && [ "${MY_IP}" != "${RAY_HEAD_IP}" ]; then
    DASH_PORT=$(echo "${DASHBOARD_URL}" | grep -oP ':\K[0-9]+')
    if [ -n "${DASH_PORT}" ]; then
        DASHBOARD_URL="http://${RAY_HEAD_IP}:${DASH_PORT}"
        echo "Adjusted Dashboard URL: ${DASHBOARD_URL} (benchmark on different host than dashboard)"
    fi
fi

# Wait for workers to join the cluster (at least 2 nodes = head + 1 worker)
# In head-only mode (1 site), skip waiting for workers
NUM_WORKER_SITES="${NUM_WORKER_SITES:-0}"
MIN_NODES=$((NUM_WORKER_SITES + 1))
echo "Expected minimum nodes: ${MIN_NODES} (1 head + ${NUM_WORKER_SITES} worker sites)"

if [ "${MIN_NODES}" -gt 1 ]; then
    echo "Waiting for Ray workers to join..."
    attempt=1
    MAX_ATTEMPTS=90
    while [ ${attempt} -le ${MAX_ATTEMPTS} ]; do
        NODE_COUNT=$(ray status 2>/dev/null | grep -c "node_" || echo "0")
        echo "[${attempt}/${MAX_ATTEMPTS}] Ray nodes: ${NODE_COUNT}"
        if [ "${NODE_COUNT}" -ge "${MIN_NODES}" ]; then
            echo "Workers joined! ${NODE_COUNT} nodes in cluster."
            break
        fi
        sleep 5
        ((attempt++))
    done

    if [ ${attempt} -gt ${MAX_ATTEMPTS} ]; then
        echo "[WARN] Timeout waiting for workers. Running with available nodes."
    fi
else
    echo "Head-only mode — no remote workers to wait for."
fi

ray status || echo "[WARN] ray status failed (head may not be reachable from login node)"

# Auto-detect cluster name and scheduler type for head node (site-1)
# Uses pw cluster list and hostname matching, same pattern as burst-render-demo
CLUSTER_NAME=""
SCHEDULER_TYPE=""
PW_CMD=""
for cmd in pw ~/pw/pw; do
    command -v $cmd &>/dev/null && { PW_CMD=$cmd; break; }
    [ -x "$cmd" ] && { PW_CMD=$cmd; break; }
done

if [ -n "${PW_CMD}" ]; then
    MY_HOST=$(hostname -s)
    while IFS= read -r line; do
        uri=$(echo "$line" | awk '{print $1}')
        ctype=$(echo "$line" | awk '{print $3}')
        name="${uri##*/}"
        # Match hostname containing the cluster name
        if echo "${MY_HOST}" | grep -qi "${name}"; then
            CLUSTER_NAME="${name}"
            case "${ctype}" in
                *slurm*) SCHEDULER_TYPE="slurm" ;;
                *pbs*)   SCHEDULER_TYPE="pbs" ;;
                existing) SCHEDULER_TYPE="ssh" ;;
                *)       SCHEDULER_TYPE="${ctype}" ;;
            esac
            break
        fi
    done < <(${PW_CMD} cluster list 2>/dev/null | grep "^pw://${PW_USER}/" | grep "active")
fi
[ -z "${CLUSTER_NAME}" ] && CLUSTER_NAME="$(hostname -s)"
[ -z "${SCHEDULER_TYPE}" ] && SCHEDULER_TYPE="ssh"

echo "Cluster name:   ${CLUSTER_NAME}"
echo "Scheduler type: ${SCHEDULER_TYPE}"

# Run benchmark
${PYTHON_CMD} "${SCRIPT_DIR}/benchmark.py" \
    --dashboard-url "${DASHBOARD_URL}" \
    --ray-head-ip "${RAY_HEAD_IP}" \
    --workload-type "${WORKLOAD_TYPE}" \
    --num-tasks "${NUM_TASKS:-500}" \
    --matrix-size "${MATRIX_SIZE:-500}" \
    --grid-size "${GRID_SIZE}" \
    --image-size "${IMAGE_SIZE}" \
    --palette "${PALETTE}" \
    --onprem-cluster-name "${CLUSTER_NAME}" \
    --onprem-scheduler-type "${SCHEDULER_TYPE}"

echo "=========================================="
echo "Workload Complete!"
echo "=========================================="
