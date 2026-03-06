#!/bin/bash
# start_ray_head.sh — Start Ray head node + custom dashboard
#
# Creates coordination files:
#   - HOSTNAME      — Dashboard hostname
#   - SESSION_PORT  — Dashboard port
#   - RAY_HEAD_IP   — Ray head node IP
#   - job.started   — Signals job has started
#
# Environment variables:
#   RAY_VERSION  - Ray version to install (default: 2.40.0)
#   TARGETS_JSON - JSON array of target objects (for scheduler config)

set -e

JOB_DIR="${PW_PARENT_JOB_DIR%/}"
cd "${JOB_DIR}"

SCRIPT_DIR="${JOB_DIR}/scripts"
RAY_VERSION="${RAY_VERSION:-2.40.0}"
RAY_PORT=6379

# =============================================================================
# SLURM detection: if first target has scheduler enabled, wrap with srun
# =============================================================================
if [ -z "${_RAY_HEAD_INSIDE_SRUN}" ] && [ -n "${TARGETS_JSON}" ]; then
    # Parse scheduler config for first target (site 0)
    PYTHON_CMD=""
    for cmd in python3 python; do
        command -v $cmd &>/dev/null && { PYTHON_CMD=$cmd; break; }
    done

    if [ -n "${PYTHON_CMD}" ]; then
        SCHED_INFO=$(${PYTHON_CMD} -c "
import json, os
targets = json.loads(os.environ['TARGETS_JSON'])
t = targets[0]
res = t.get('resource', {})
if isinstance(res, str):
    res = {'name': res}
use_scheduler = t.get('scheduler', False)
if isinstance(use_scheduler, str):
    use_scheduler = use_scheduler.lower() == 'true'
scheduler_type = res.get('schedulerType', '')
if use_scheduler and not scheduler_type:
    scheduler_type = 'slurm'
slurm = t.get('slurm', {}) or {}
print('USE_SCHEDULER=' + str(use_scheduler).lower())
print('SCHEDULER_TYPE=' + scheduler_type)
print('SLURM_PARTITION=' + slurm.get('partition', ''))
print('SLURM_ACCOUNT=' + slurm.get('account', ''))
print('SLURM_QOS=' + slurm.get('qos', ''))
print('SLURM_TIME=' + slurm.get('time', '00:05:00'))
" 2>/dev/null) || true

        if [ -n "${SCHED_INFO}" ]; then
            eval "${SCHED_INFO}"
        fi

        if [ "${USE_SCHEDULER}" = "true" ] && [ "${SCHEDULER_TYPE}" = "slurm" ]; then
            echo "=========================================="
            echo "SLURM detected for site 0 — submitting via srun"
            echo "=========================================="

            # Run setup on login node first (installs Ray + dashboard deps)
            echo "Running setup on login node..."
            bash "${SCRIPT_DIR}/setup.sh"

            # Install dashboard dependencies on login node too
            VENV_DIR="${JOB_DIR}/.venv"
            if [ -f "${VENV_DIR}/bin/python" ]; then
                PY="${VENV_DIR}/bin/python"
                source "${VENV_DIR}/bin/activate"
            else
                PY="python3"
            fi
            ${PY} -c "import fastapi" 2>/dev/null || {
                echo "Installing dashboard dependencies..."
                UV_CMD=""
                for uv_path in "${JOB_DIR}/.uv/uv" "$HOME/.local/bin/uv" "$HOME/.cargo/bin/uv"; do
                    if [ -x "${uv_path}" ]; then UV_CMD="${uv_path}"; break; fi
                done
                if [ -z "${UV_CMD}" ]; then command -v uv &>/dev/null && UV_CMD="uv"; fi

                if [ -n "${UV_CMD}" ]; then
                    ${UV_CMD} pip install --python "${PY}" fastapi uvicorn websockets httpx 2>&1
                else
                    ${PY} -m pip install --quiet fastapi uvicorn websockets httpx 2>&1
                fi
            }

            # Build srun command
            srun_cmd="srun --nodes=1 --ntasks=1"
            [ -n "${SLURM_PARTITION}" ] && srun_cmd="${srun_cmd} --partition=${SLURM_PARTITION}"
            [ -n "${SLURM_ACCOUNT}" ] && srun_cmd="${srun_cmd} --account=${SLURM_ACCOUNT}"
            [ -n "${SLURM_QOS}" ] && srun_cmd="${srun_cmd} --qos=${SLURM_QOS}"
            [ -n "${SLURM_TIME}" ] && srun_cmd="${srun_cmd} --time=${SLURM_TIME}"

            echo "Submitting: ${srun_cmd} bash scripts/start_ray_head.sh"

            # Re-exec this script inside srun (skip the SLURM detection block)
            export _RAY_HEAD_INSIDE_SRUN=1
            export RAY_VERSION
            export TARGETS_JSON
            ${srun_cmd} bash "${SCRIPT_DIR}/start_ray_head.sh"
            exit $?
        fi
    fi
fi

# =============================================================================
# Main logic (runs on login node or compute node via srun)
# =============================================================================
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
# Start Ray head node
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
# Each Ray task uses 1 CPU slot; parallelism comes from Ray scheduling, not BLAS threading.
export OMP_NUM_THREADS=1
export MKL_NUM_THREADS=1
export OPENBLAS_NUM_THREADS=1

echo "Starting Ray head node..."
RAY_START_ARGS=(
    --head
    --port=${RAY_PORT}
    --node-ip-address=${HEAD_IP}
    --dashboard-host=0.0.0.0
    --dashboard-port=8265
)
if [ -n "${RAY_NUM_CPUS}" ]; then
    RAY_START_ARGS+=(--num-cpus=${RAY_NUM_CPUS})
    echo "Ray CPUs: ${RAY_NUM_CPUS} (user-configured)"
fi
ray start "${RAY_START_ARGS[@]}"

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
