#!/bin/bash
# start_ray_workers.sh — Connect cloud workers to Ray head via tunnel
#
# Environment variables:
#   RAY_VERSION      - Ray version to install (default: 2.40.0)
#   RAY_HEAD_ADDRESS - Address to connect to (localhost:TUNNEL_RAY_PORT)
#   DASHBOARD_URL    - Dashboard URL for POSTing worker info
#   SITE_ID          - Site identifier (e.g., site-2)

set -e

echo "=========================================="
echo "Ray Worker Starting: $(date)"
echo "=========================================="
echo "Hostname: $(hostname)"
echo "Ray head address: ${RAY_HEAD_ADDRESS}"
echo "Dashboard URL: ${DASHBOARD_URL}"
echo "Site ID: ${SITE_ID:-site-2}"

JOB_DIR="${PW_PARENT_JOB_DIR%/}"
cd "${JOB_DIR}"

SCRIPT_DIR="${JOB_DIR}/scripts"
RAY_VERSION="${RAY_VERSION:-2.40.0}"

# =============================================================================
# Install Ray
# =============================================================================
bash "${SCRIPT_DIR}/setup.sh"

VENV_DIR="${JOB_DIR}/.venv"
if [ -f "${VENV_DIR}/bin/python" ]; then
    source "${VENV_DIR}/bin/activate"
fi

# =============================================================================
# Wait for Ray head to be reachable via tunnel
# =============================================================================
echo "Waiting for Ray head at ${RAY_HEAD_ADDRESS}..."
RAY_HOST=$(echo "${RAY_HEAD_ADDRESS}" | cut -d: -f1)
RAY_TUNNEL_PORT=$(echo "${RAY_HEAD_ADDRESS}" | cut -d: -f2)

# Diagnostic: check what's listening on this machine
echo "=== Network diagnostics ==="
echo "Checking listening ports for ${RAY_TUNNEL_PORT}..."
ss -tlnp 2>/dev/null | grep ":${RAY_TUNNEL_PORT}" || echo "  Port ${RAY_TUNNEL_PORT} NOT found in ss output"
echo "All listening ports on localhost:"
ss -tlnp 2>/dev/null | grep "127.0.0" | head -20 || echo "  (ss unavailable)"
echo "Checking with netstat..."
netstat -tlnp 2>/dev/null | grep ":${RAY_TUNNEL_PORT}" || echo "  Port ${RAY_TUNNEL_PORT} NOT found in netstat output"
echo "=== End diagnostics ==="

attempt=1
MAX_ATTEMPTS=60
while [ ${attempt} -le ${MAX_ATTEMPTS} ]; do
    if python3 -c "
import socket
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.settimeout(3)
try:
    s.connect(('127.0.0.1', ${RAY_TUNNEL_PORT}))
    s.close()
    exit(0)
except Exception as e:
    print(f'  Connection error: {e}')
    exit(1)
"; then
        echo "Ray head reachable!"
        break
    fi
    if [ ${attempt} -eq 1 ] || [ ${attempt} -eq 10 ] || [ ${attempt} -eq 30 ]; then
        echo "  Diagnostic at attempt ${attempt}:"
        ss -tlnp 2>/dev/null | grep ":${RAY_TUNNEL_PORT}" || echo "    Port ${RAY_TUNNEL_PORT} still not listening"
    fi
    echo "[${attempt}/${MAX_ATTEMPTS}] Waiting for Ray head..."
    sleep 2
    ((attempt++))
done

if [ ${attempt} -gt ${MAX_ATTEMPTS} ]; then
    echo "[ERROR] Timeout waiting for Ray head at ${RAY_HOST}:${RAY_TUNNEL_PORT}"
    echo "Final diagnostic:"
    ss -tlnp 2>/dev/null | head -30 || true
    netstat -tlnp 2>/dev/null | head -30 || true
    exit 1
fi

# =============================================================================
# Stop any existing Ray and join cluster
# =============================================================================
ray stop --force 2>/dev/null || true

# Fixed ports for bidirectional tunnel — must match setup_tunnel.sh
WORKER_RAYLET_PORT=20380
WORKER_OBJ_PORT=20381
WORKER_MIN_PORT=20400
WORKER_MAX_PORT=20420

# Use 127.0.0.2 as node IP so the head reaches this worker via the forward tunnel.
# NOTE: Must use 127.0.0.2 (not 127.0.0.1) because Ray's resolve_ip_for_localhost()
# converts 127.0.0.1 to the real network IP, defeating our tunnel setup.
# 127.0.0.2 is still a valid loopback address but bypasses Ray's conversion.
WORKER_TUNNEL_IP=127.0.0.2
echo "Connecting to Ray head at ${RAY_HEAD_ADDRESS}..."
echo "  Node IP advertised: ${WORKER_TUNNEL_IP} (via SSH forward tunnel)"
echo "  Raylet port: ${WORKER_RAYLET_PORT}"
echo "  Object manager port: ${WORKER_OBJ_PORT}"
echo "  Worker process ports: ${WORKER_MIN_PORT}-${WORKER_MAX_PORT}"
ray start --address="${RAY_HEAD_ADDRESS}" \
    --node-ip-address=${WORKER_TUNNEL_IP} \
    --node-manager-port=${WORKER_RAYLET_PORT} \
    --object-manager-port=${WORKER_OBJ_PORT} \
    --min-worker-port=${WORKER_MIN_PORT} \
    --max-worker-port=${WORKER_MAX_PORT}

echo "Ray worker started!"
ray status 2>/dev/null || echo "Note: ray status may not work on worker node"

# Notify dashboard that worker joined
WORKER_IP=$(hostname -I 2>/dev/null | awk '{print $1}')
NUM_CPUS=$(nproc 2>/dev/null || echo 1)

# Auto-detect cluster name and scheduler type
CLUSTER_NAME=""
SCHEDULER_TYPE=""

# Detect scheduler type from environment
if [ -n "${SLURM_JOB_ID}" ]; then
    SCHEDULER_TYPE="slurm"
elif [ -n "${PBS_JOBID}" ]; then
    SCHEDULER_TYPE="pbs"
else
    SCHEDULER_TYPE="ssh"
fi

# Get cluster name: use first active cluster (there's typically only one per resource)
if command -v pw &>/dev/null || [ -x "${HOME}/pw/pw" ]; then
    PW_CMD=$(command -v pw 2>/dev/null || echo "${HOME}/pw/pw")
    CLUSTER_NAME=$(${PW_CMD} cluster list 2>/dev/null | awk '/^pw:\/\// {name=$1; sub(/.*\//, "", name); print name; exit}' || true)
fi

if [ -n "${DASHBOARD_URL}" ]; then
    curl -s -X POST "${DASHBOARD_URL}/api/worker" \
        -H "Content-Type: application/json" \
        -d "{
            \"site_id\": \"${SITE_ID:-site-2}\",
            \"worker_ip\": \"${WORKER_IP}\",
            \"num_cpus\": ${NUM_CPUS},
            \"cluster_name\": \"${CLUSTER_NAME}\",
            \"scheduler_type\": \"${SCHEDULER_TYPE}\"
        }" 2>/dev/null || echo "Note: Could not notify dashboard"
fi

echo "=========================================="
echo "Ray Worker RUNNING"
echo "  Connected to: ${RAY_HEAD_ADDRESS}"
echo "  Worker IP: ${WORKER_IP}"
echo "  CPUs: ${NUM_CPUS}"
echo "=========================================="

# Keep SSH session alive
while true; do
    ray status 2>/dev/null || echo "Worker health check: $(date)"
    sleep 30
done
