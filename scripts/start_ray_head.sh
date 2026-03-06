#!/bin/bash
# start_ray_head.sh — Start Ray head node + custom dashboard
#
# The head is a pure coordinator (--num-cpus=0): no compute tasks run here.
# Workers are dispatched separately by dispatch_workers.sh.
#
# Creates coordination files:
#   - HOSTNAME      — Dashboard hostname
#   - SESSION_PORT  — Dashboard port
#   - RAY_HEAD_IP   — Ray head node IP
#   - job.started   — Signals job has started
#
# Environment variables:
#   RAY_VERSION  - Ray version to install (default: 2.40.0)

set -e

JOB_DIR="${PW_PARENT_JOB_DIR%/}"
cd "${JOB_DIR}"

SCRIPT_DIR="${JOB_DIR}/scripts"
RAY_VERSION="${RAY_VERSION:-2.40.0}"
RAY_PORT=6379

echo "=========================================="
echo "Ray Head + Dashboard Starting: $(date)"
echo "=========================================="
echo "Hostname: $(hostname)"
echo "Job dir:  ${PW_PARENT_JOB_DIR}"

# Verify scripts were checked out
if [ ! -f "${SCRIPT_DIR}/dashboard.py" ]; then
    echo "[ERROR] dashboard.py not found at ${SCRIPT_DIR}/dashboard.py"
    ls -la "${JOB_DIR}" 2>&1
    exit 1
fi

# =============================================================================
# Install Ray + dependencies
# =============================================================================
bash "${SCRIPT_DIR}/setup.sh"

# Determine Python from venv
VENV_DIR="${JOB_DIR}/.venv"
if [ -f "${VENV_DIR}/bin/python" ]; then
    PYTHON_CMD="${VENV_DIR}/bin/python"
    source "${VENV_DIR}/bin/activate"
else
    PYTHON_CMD="python3"
fi
echo "Python: ${PYTHON_CMD}"

# Install dashboard dependencies
${PYTHON_CMD} -c "import fastapi" 2>/dev/null || {
    echo "Installing dashboard dependencies..."
    UV_CMD=""
    for uv_path in "${JOB_DIR}/.uv/uv" "$HOME/.local/bin/uv" "$HOME/.cargo/bin/uv"; do
        if [ -x "${uv_path}" ]; then UV_CMD="${uv_path}"; break; fi
    done
    if [ -z "${UV_CMD}" ]; then command -v uv &>/dev/null && UV_CMD="uv"; fi

    if [ -n "${UV_CMD}" ]; then
        ${UV_CMD} pip install --python "${PYTHON_CMD}" fastapi uvicorn websockets httpx 2>&1 || {
            echo "[ERROR] Failed to install dashboard dependencies via uv"
            exit 1
        }
    else
        ${PYTHON_CMD} -m pip install --quiet fastapi uvicorn websockets httpx 2>&1 || {
            echo "[ERROR] Failed to install dashboard dependencies"
            exit 1
        }
    fi
}

# =============================================================================
# Start Ray head node (coordinator only — no compute tasks)
# =============================================================================
echo "Stopping any existing Ray processes..."
ray stop --force 2>/dev/null || true

# Get real network IP (not loopback)
HEAD_IP=$(hostname -I 2>/dev/null | awk '{print $1}')
if [ -z "${HEAD_IP}" ] || [[ "${HEAD_IP}" == 127.* ]]; then
    HEAD_IP=$(ip -4 addr show | grep -oP '(?<=inet\s)\d+(\.\d+){3}' | grep -v '^127\.' | head -n 1)
fi
echo "Ray head IP: ${HEAD_IP}"

# Pin BLAS/OpenMP to 1 thread per process so Ray tasks don't oversubscribe cores.
export OMP_NUM_THREADS=1
export MKL_NUM_THREADS=1
export OPENBLAS_NUM_THREADS=1

echo "Starting Ray head node (coordinator only, --num-cpus=0)..."
ray start --head \
    --port=${RAY_PORT} \
    --node-ip-address=${HEAD_IP} \
    --num-cpus=0 \
    --dashboard-host=0.0.0.0 \
    --dashboard-port=8265

# Verify Ray GCS is listening
echo "Checking Ray GCS port binding..."
ss -tlnp 2>/dev/null | grep ":${RAY_PORT}" || netstat -tlnp 2>/dev/null | grep ":${RAY_PORT}" || echo "  (port check tools unavailable)"

echo "Ray head started on ${HEAD_IP}:${RAY_PORT}"
ray status

# Record exact Python version for worker matching
PYTHON_MICRO=$($PYTHON_CMD -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}')")
echo "${PYTHON_MICRO}" > PYTHON_VERSION
echo "Python version (for workers to match): ${PYTHON_MICRO}"

# =============================================================================
# Port allocation for custom dashboard
# =============================================================================
if command -v pw &>/dev/null; then
    service_port=$(pw agent open-port 2>/dev/null)
elif [ -x "${HOME}/pw/pw" ]; then
    service_port=$(~/pw/pw agent open-port 2>/dev/null)
else
    echo "[ERROR] pw CLI not found"
    exit 1
fi

if [ -z "${service_port}" ] || ! [[ "${service_port}" =~ ^[0-9]+$ ]]; then
    echo "[ERROR] Failed to allocate port (got: '${service_port}')"
    exit 1
fi
echo "Dashboard port: ${service_port}"

# =============================================================================
# Write coordination files
# =============================================================================
hostname > HOSTNAME
echo "${service_port}" > SESSION_PORT
echo "${HEAD_IP}" > RAY_HEAD_IP
touch job.started

echo "Coordination files written:"
echo "  HOSTNAME=$(cat HOSTNAME)"
echo "  SESSION_PORT=$(cat SESSION_PORT)"
echo "  RAY_HEAD_IP=$(cat RAY_HEAD_IP)"

# =============================================================================
# Start custom dashboard
# =============================================================================
mkdir -p logs

export DASHBOARD_PORT="${service_port}"
export RAY_HEAD_IP="${HEAD_IP}"

nohup ${PYTHON_CMD} -m uvicorn dashboard:app \
    --host 0.0.0.0 \
    --port "${service_port}" \
    --app-dir "${SCRIPT_DIR}" \
    > logs/dashboard.log 2>&1 &
disown
SERVER_PID=$!

echo "Dashboard PID: ${SERVER_PID}"
echo "${SERVER_PID}" > dashboard.pid

sleep 3

if kill -0 ${SERVER_PID} 2>/dev/null; then
    echo "=========================================="
    echo "Ray Head + Dashboard RUNNING"
    echo "  Ray: ${HEAD_IP}:${RAY_PORT}"
    echo "  Dashboard: port ${service_port}"
    echo "=========================================="

    # Keep SSH session alive
    while kill -0 ${SERVER_PID} 2>/dev/null; do
        # Periodic health check
        ray status 2>/dev/null || echo "Ray health check: $(date)"
        sleep 10
    done
    echo "Dashboard process exited"
else
    echo "[ERROR] Dashboard failed to start"
    cat logs/dashboard.log >&2
    exit 1
fi
