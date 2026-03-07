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
VENV_DIR="$(cat "${JOB_DIR}/RAY_VENV_DIR" 2>/dev/null || echo "${JOB_DIR}/.venv")"
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

# Wait for at least 1 worker CPU before submitting tasks.
# Without this, early-submitted tasks can timeout before any worker joins.
NUM_WORKER_SITES="${NUM_WORKER_SITES:-0}"
echo "Expected worker sites: ${NUM_WORKER_SITES}"
echo "Waiting for at least 1 worker CPU..."

MAX_WAIT=600
WAITED=0
while true; do
    CPUS=$(python3 -c "import ray; ray.init(address='${RAY_HEAD_IP}:6379', ignore_reinit_error=True); print(int(ray.cluster_resources().get('CPU', 0)))" 2>/dev/null || echo "0")
    if [ "${CPUS}" -gt 0 ] 2>/dev/null; then
        echo "Cluster has ${CPUS} CPUs available — starting workload"
        break
    fi
    if [ ${WAITED} -ge ${MAX_WAIT} ]; then
        echo "WARNING: No workers after ${MAX_WAIT}s — starting anyway (tasks will queue)"
        break
    fi
    sleep 5
    WAITED=$((WAITED + 5))
    [ $((WAITED % 30)) -eq 0 ] && echo "  Still waiting for workers... (${WAITED}s elapsed)"
done

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
