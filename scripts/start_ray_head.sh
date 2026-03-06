#!/bin/bash
# start_ray_head.sh — Start Ray head node + custom dashboard
#
# In SLURM mode with N nodes, rank 0 becomes the head + dashboard and
# ranks 1..N-1 become Ray workers. All within a single srun allocation.
#
# Creates coordination files:
#   - HOSTNAME      — Dashboard hostname
#   - SESSION_PORT  — Dashboard port
#   - RAY_HEAD_IP   — Ray head node IP
#   - job.started   — Signals job has started
#
# Environment variables:
#   RAY_VERSION  - Ray version to install (default: 2.40.0)
#   RAY_NUM_CPUS - Optional CPU cap per node for Ray
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
print('SLURM_NODES=' + str(slurm.get('nodes', '1')))
" 2>/dev/null) || true

        if [ -n "${SCHED_INFO}" ]; then
            eval "${SCHED_INFO}"
        fi

        if [ "${USE_SCHEDULER}" = "true" ] && [ "${SCHEDULER_TYPE}" = "slurm" ]; then
            echo "=========================================="
            echo "SLURM detected for site 0 — submitting ${SLURM_NODES} node(s) via srun"
            echo "=========================================="

            # Run setup on login node first (shared filesystem — compute nodes reuse it)
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

            # Build srun command — request ALL nodes (rank 0 = head, rank 1+ = workers)
            srun_cmd="srun --nodes=${SLURM_NODES} --ntasks=${SLURM_NODES}"
            [ -n "${SLURM_PARTITION}" ] && srun_cmd="${srun_cmd} --partition=${SLURM_PARTITION}"
            [ -n "${SLURM_ACCOUNT}" ] && srun_cmd="${srun_cmd} --account=${SLURM_ACCOUNT}"
            [ -n "${SLURM_QOS}" ] && srun_cmd="${srun_cmd} --qos=${SLURM_QOS}"
            [ -n "${SLURM_TIME}" ] && srun_cmd="${srun_cmd} --time=${SLURM_TIME}"

            echo "Submitting: ${srun_cmd} bash scripts/start_ray_head.sh"
            echo "  Rank 0 = Ray head + dashboard"
            if [ "${SLURM_NODES}" -gt 1 ]; then
                echo "  Ranks 1-$((SLURM_NODES - 1)) = Ray workers"
            fi

            # Re-exec this script inside srun (skip the SLURM detection block)
            export _RAY_HEAD_INSIDE_SRUN=1
            export RAY_VERSION
            export RAY_NUM_CPUS
            export TARGETS_JSON
            ${srun_cmd} bash "${SCRIPT_DIR}/start_ray_head.sh"
            exit $?
        fi
    fi
fi

# =============================================================================
# Main logic (runs on compute node via srun, or login node without scheduler)
# Each srun task gets here; SLURM_PROCID determines head vs worker role.
# =============================================================================

# Pin BLAS/OpenMP to 1 thread per process so Ray tasks don't oversubscribe cores.
export OMP_NUM_THREADS=1
export MKL_NUM_THREADS=1
export OPENBLAS_NUM_THREADS=1

# Determine Python from venv (shared filesystem — already installed by login node)
VENV_DIR="${JOB_DIR}/.venv"
if [ -f "${VENV_DIR}/bin/python" ]; then
    PYTHON_CMD="${VENV_DIR}/bin/python"
    source "${VENV_DIR}/bin/activate"
else
    PYTHON_CMD="python3"
fi

# =============================================================================
# WORKER MODE: SLURM ranks 1+ wait for head and join as Ray workers
# =============================================================================
if [ -n "${SLURM_PROCID}" ] && [ "${SLURM_PROCID}" -gt 0 ]; then
    echo "=========================================="
    echo "Ray Worker (rank ${SLURM_PROCID}): $(date)"
    echo "=========================================="
    echo "Hostname: $(hostname)"

    # Wait for head coordination files (written by rank 0 to shared filesystem)
    echo "Waiting for Ray head to start..."
    attempt=0
    while [ ! -f "${JOB_DIR}/RAY_HEAD_IP" ]; do
        sleep 2
        attempt=$((attempt + 1))
        if [ ${attempt} -gt 120 ]; then
            echo "[ERROR] Timeout waiting for Ray head coordination files"
            exit 1
        fi
    done

    HEAD_IP=$(cat "${JOB_DIR}/RAY_HEAD_IP")
    echo "Ray head IP: ${HEAD_IP}"

    # Wait for Ray GCS to be reachable
    echo "Waiting for Ray GCS at ${HEAD_IP}:${RAY_PORT}..."
    attempt=0
    while [ ${attempt} -le 60 ]; do
        if ${PYTHON_CMD} -c "
import socket, sys
s = socket.socket()
s.settimeout(3)
try:
    s.connect(('${HEAD_IP}', ${RAY_PORT}))
    s.close()
    sys.exit(0)
except:
    sys.exit(1)
" 2>/dev/null; then
            echo "Ray head reachable!"
            break
        fi
        sleep 2
        attempt=$((attempt + 1))
    done

    ray stop --force 2>/dev/null || true

    WORKER_IP=$(hostname -I 2>/dev/null | awk '{print $1}')
    NUM_CPUS="${RAY_NUM_CPUS:-$(nproc 2>/dev/null || echo 1)}"

    echo "Starting Ray worker: address=${HEAD_IP}:${RAY_PORT}, CPUs=${NUM_CPUS}"
    RAY_WORKER_ARGS=(--address="${HEAD_IP}:${RAY_PORT}")
    if [ -n "${RAY_NUM_CPUS}" ]; then
        RAY_WORKER_ARGS+=(--num-cpus="${RAY_NUM_CPUS}")
    fi
    ray start "${RAY_WORKER_ARGS[@]}"

    # Register with dashboard (wait for SESSION_PORT to be written)
    attempt=0
    while [ ! -f "${JOB_DIR}/SESSION_PORT" ]; do
        sleep 1
        attempt=$((attempt + 1))
        if [ ${attempt} -gt 60 ]; then break; fi
    done
    DASH_PORT=$(cat "${JOB_DIR}/SESSION_PORT" 2>/dev/null || echo "")

    # Auto-detect cluster name
    CLUSTER_NAME=""
    SCHED_TYPE="slurm"
    PW_CMD=""
    for try_cmd in pw ~/pw/pw; do
        command -v ${try_cmd} &>/dev/null && { PW_CMD=${try_cmd}; break; }
        [ -x "${try_cmd}" ] && { PW_CMD=${try_cmd}; break; }
    done
    if [ -n "${PW_CMD}" ]; then
        MY_HOST=$(hostname -s)
        while IFS= read -r line; do
            uri=$(echo "${line}" | awk '{print $1}')
            cname="${uri##*/}"
            if echo "${MY_HOST}" | grep -qi "${cname}"; then
                CLUSTER_NAME="${cname}"
                break
            fi
        done < <(${PW_CMD} cluster list 2>/dev/null | grep "^pw://${PW_USER}/" | grep "active")
    fi
    [ -z "${CLUSTER_NAME}" ] && CLUSTER_NAME="$(hostname -s)"

    if [ -n "${DASH_PORT}" ]; then
        curl -s -X POST "http://${HEAD_IP}:${DASH_PORT}/api/worker" \
            -H "Content-Type: application/json" \
            -d "{
                \"site_id\": \"site-1\",
                \"worker_ip\": \"${WORKER_IP}\",
                \"num_cpus\": ${NUM_CPUS},
                \"cluster_name\": \"${CLUSTER_NAME}\",
                \"scheduler_type\": \"${SCHED_TYPE}\"
            }" 2>/dev/null || echo "Note: Could not notify dashboard"
    fi

    echo "=========================================="
    echo "Ray Worker RUNNING (rank ${SLURM_PROCID})"
    echo "  Head: ${HEAD_IP}:${RAY_PORT}"
    echo "  Worker IP: ${WORKER_IP}"
    echo "  CPUs: ${NUM_CPUS}"
    echo "=========================================="

    # Keep alive
    while true; do
        ray status 2>/dev/null || echo "Worker ${SLURM_PROCID} health: $(date)"
        sleep 30
    done
    exit 0
fi

# =============================================================================
# HEAD MODE: rank 0 (or non-SLURM) — start Ray head + dashboard
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
# Install Ray + dependencies (may be a no-op if login node already did it)
# =============================================================================
bash "${SCRIPT_DIR}/setup.sh"

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
# Write coordination files (shared filesystem — workers and login node read these)
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
