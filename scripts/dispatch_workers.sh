#!/bin/bash
# dispatch_workers.sh — Dispatch Ray workers across N remote compute sites
#
# Runs on the head node (site 0). For each remote site (1..N):
#   1. Allocates tunnel ports on remote
#   2. Sets up bidirectional SSH tunnel (reverse for dashboard+GCS, forward for worker ports)
#   3. Checks out repo, installs Ray, connects worker to head
#   4. Keeps SSH session alive with health check loop
#
# Environment variables:
#   TARGETS_JSON    - JSON array of target objects from workflow inputs
#   DASHBOARD_PORT  - Dashboard port on this machine
#   RAY_HEAD_IP     - Ray head node IP
#   PYTHON_VERSION  - Python micro version for worker matching
#   RAY_VERSION     - Ray version to install

set -e

JOB_DIR="${PW_PARENT_JOB_DIR%/}"
SCRIPT_DIR="${JOB_DIR}/scripts"
WORK_DIR=$(mktemp -d)
trap "rm -rf ${WORK_DIR}" EXIT

RAY_PORT=6379
RAY_VERSION="${RAY_VERSION:-2.40.0}"

# Find Python and pw
PYTHON_CMD=""
for cmd in python3 python; do
    command -v $cmd &>/dev/null && { PYTHON_CMD=$cmd; break; }
done
if [ -z "${PYTHON_CMD}" ]; then
    echo "[ERROR] Python not found"
    exit 1
fi

PW_CMD=""
for cmd in pw ~/pw/pw; do
    command -v $cmd &>/dev/null && { PW_CMD=$cmd; break; }
    [ -x "$cmd" ] && { PW_CMD=$cmd; break; }
done
if [ -z "${PW_CMD}" ]; then
    echo "[ERROR] pw CLI not found"
    exit 1
fi

# Parse targets JSON to get site list
SITES_JSON=$(${PYTHON_CMD} -c "
import json, sys, os

targets = json.loads(os.environ['TARGETS_JSON'])
sites = []
for i, t in enumerate(targets):
    res = t.get('resource', {})
    # Handle resource as string (CLI) or object (UI)
    if isinstance(res, str):
        res = {'name': res}
    sites.append({
        'index': i,
        'name': res.get('name', f'site-{i}'),
        'ip': res.get('ip', ''),
        'user': res.get('user', ''),
        'scheduler_type': res.get('schedulerType', ''),
    })
print(json.dumps(sites))
")

NUM_SITES=$(echo "${SITES_JSON}" | ${PYTHON_CMD} -c "import sys,json;print(len(json.load(sys.stdin)))")
NUM_REMOTE_SITES=$((NUM_SITES - 1))

echo "=========================================="
echo "Dispatch Workers: $(date)"
echo "=========================================="
echo "Total sites:   ${NUM_SITES}"
echo "Remote sites:  ${NUM_REMOTE_SITES}"
echo "Dashboard:     localhost:${DASHBOARD_PORT}"
echo "Ray head:      ${RAY_HEAD_IP}:${RAY_PORT}"
echo "Python:        ${PYTHON_VERSION}"
echo "Ray version:   ${RAY_VERSION}"

if [ "${NUM_REMOTE_SITES}" -eq 0 ]; then
    echo ""
    echo "No remote sites — head-only mode. Skipping worker dispatch."
    echo "=========================================="
    exit 0
fi

REPO_URL="https://github.com/parallelworks/ray-cluster-demo.git"

# Worker dispatch function for a single remote site
dispatch_worker() {
    local site_index=$1
    local site_name=$2
    local site_ip=$3
    local scheduler_type=$4

    local site_id="site-$((site_index + 1))"

    # Port allocation per site (site_index is 1-based for remote sites)
    local WORKER_TUNNEL_IP="127.0.0.$((site_index + 1))"
    local WORKER_RAYLET_PORT=$((20000 + site_index * 100 + 80))
    local WORKER_OBJ_PORT=$((20000 + site_index * 100 + 81))
    local WORKER_MIN_PORT=$((20000 + site_index * 100))
    local WORKER_MAX_PORT=$((20000 + site_index * 100 + 20))

    echo ""
    echo "[${site_id}] Dispatching worker to ${site_name} (${site_ip})"
    echo "[${site_id}] Tunnel IP: ${WORKER_TUNNEL_IP}"
    echo "[${site_id}] Ports: raylet=${WORKER_RAYLET_PORT} obj=${WORKER_OBJ_PORT} workers=${WORKER_MIN_PORT}-${WORKER_MAX_PORT}"

    # Allocate 2 ports on the remote for reverse tunnels (dashboard + Ray GCS)
    local tunnel_dashboard_port
    tunnel_dashboard_port=$(${PW_CMD} ssh "${site_name}" \
        'python3 -c "import socket; s=socket.socket(); s.bind((\"\",0)); print(s.getsockname()[1]); s.close()"' 2>/dev/null)

    if [ -z "${tunnel_dashboard_port}" ] || ! [[ "${tunnel_dashboard_port}" =~ ^[0-9]+$ ]]; then
        echo "[${site_id}] [ERROR] Failed to allocate dashboard tunnel port (got: '${tunnel_dashboard_port}')"
        return 1
    fi

    local tunnel_ray_port
    tunnel_ray_port=$(${PW_CMD} ssh "${site_name}" \
        'python3 -c "import socket; s=socket.socket(); s.bind((\"\",0)); print(s.getsockname()[1]); s.close()"' 2>/dev/null)

    if [ -z "${tunnel_ray_port}" ] || ! [[ "${tunnel_ray_port}" =~ ^[0-9]+$ ]]; then
        echo "[${site_id}] [ERROR] Failed to allocate Ray tunnel port (got: '${tunnel_ray_port}')"
        return 1
    fi

    echo "[${site_id}] Remote tunnel ports: dashboard=${tunnel_dashboard_port} ray=${tunnel_ray_port}"

    # Write coordination file for this site
    echo "${site_name}" > "${JOB_DIR}/CLOUD_CLUSTER_NAME_${site_id}"

    # Build the remote worker script
    local script_file="${WORK_DIR}/worker_${site_id}.sh"
    cat > "${script_file}" <<WORKER_SCRIPT
#!/bin/bash
set -e
WORK=\${PW_PARENT_JOB_DIR:-\${HOME}/pw/jobs/ray_worker_remote}
mkdir -p "\${WORK}"
cd "\${WORK}"
export PW_PARENT_JOB_DIR="\${WORK}"

# Checkout if not already done
if [ ! -f scripts/setup.sh ]; then
    echo 'Checking out scripts...'
    git clone --depth 1 --sparse --filter=blob:none ${REPO_URL} _checkout_tmp 2>/dev/null
    cd _checkout_tmp && git sparse-checkout set scripts 2>/dev/null && cd ..
    cp -r _checkout_tmp/scripts . && rm -rf _checkout_tmp
fi

# Setup — install Ray with matching Python version
export RAY_VERSION='${RAY_VERSION}'
export PYTHON_MICRO_VERSION='${PYTHON_VERSION}'
bash scripts/setup.sh

# Activate venv
VENV_DIR="\${WORK}/.venv"
if [ -f "\${VENV_DIR}/bin/python" ]; then
    source "\${VENV_DIR}/bin/activate"
fi

# Wait for Ray head to be reachable via tunnel
echo "Waiting for Ray head at localhost:${tunnel_ray_port}..."
attempt=1
while [ \${attempt} -le 60 ]; do
    if python3 -c "
import socket
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.settimeout(3)
try:
    s.connect(('127.0.0.1', ${tunnel_ray_port}))
    s.close()
    exit(0)
except Exception as e:
    print(f'  Connection error: {e}')
    exit(1)
"; then
        echo "Ray head reachable!"
        break
    fi
    echo "[\${attempt}/60] Waiting for Ray head..."
    sleep 2
    ((attempt++))
done

if [ \${attempt} -gt 60 ]; then
    echo "[ERROR] Timeout waiting for Ray head"
    exit 1
fi

# Stop any existing Ray and join cluster
ray stop --force 2>/dev/null || true

echo "Connecting to Ray head at localhost:${tunnel_ray_port}..."
echo "  Node IP advertised: ${WORKER_TUNNEL_IP} (via SSH forward tunnel)"
echo "  Raylet port: ${WORKER_RAYLET_PORT}"
echo "  Object manager port: ${WORKER_OBJ_PORT}"
echo "  Worker process ports: ${WORKER_MIN_PORT}-${WORKER_MAX_PORT}"
ray start --address="localhost:${tunnel_ray_port}" \\
    --node-ip-address=${WORKER_TUNNEL_IP} \\
    --node-manager-port=${WORKER_RAYLET_PORT} \\
    --object-manager-port=${WORKER_OBJ_PORT} \\
    --min-worker-port=${WORKER_MIN_PORT} \\
    --max-worker-port=${WORKER_MAX_PORT}

echo "Ray worker started!"
ray status 2>/dev/null || echo "Note: ray status may not work on worker node"

# Notify dashboard that worker joined
WORKER_IP=\$(hostname -I 2>/dev/null | awk '{print \$1}')
NUM_CPUS=\$(nproc 2>/dev/null || echo 1)

# Auto-detect scheduler type from environment
if [ -n "\${SLURM_JOB_ID}" ]; then
    SCHED_TYPE="slurm"
elif [ -n "\${PBS_JOBID}" ]; then
    SCHED_TYPE="pbs"
else
    SCHED_TYPE="ssh"
fi

DASHBOARD_URL="http://localhost:${tunnel_dashboard_port}"
curl -s -X POST "\${DASHBOARD_URL}/api/worker" \\
    -H "Content-Type: application/json" \\
    -d "{
        \"site_id\": \"${site_id}\",
        \"worker_ip\": \"\${WORKER_IP}\",
        \"num_cpus\": \${NUM_CPUS},
        \"cluster_name\": \"${site_name}\",
        \"scheduler_type\": \"\${SCHED_TYPE}\"
    }" 2>/dev/null || echo "Note: Could not notify dashboard"

echo "=========================================="
echo "Ray Worker RUNNING"
echo "  Connected to: localhost:${tunnel_ray_port}"
echo "  Worker IP: \${WORKER_IP}"
echo "  CPUs: \${NUM_CPUS}"
echo "=========================================="

# Keep SSH session alive
while true; do
    ray status 2>/dev/null || echo "Worker health check: \$(date)"
    sleep 30
done
WORKER_SCRIPT

    # Build SSH args with bidirectional tunnels
    local SSH_ARGS=(
        -i ~/.ssh/pwcli
        -o StrictHostKeyChecking=no
        -o UserKnownHostsFile=/dev/null
        -o ExitOnForwardFailure=yes
        -o ServerAliveInterval=15
        -o "ProxyCommand=${PW_CMD} ssh --proxy-command %h"
        # Reverse tunnels: remote can reach head node
        -R "${tunnel_dashboard_port}:localhost:${DASHBOARD_PORT}"
        -R "${tunnel_ray_port}:localhost:${RAY_PORT}"
        # Forward tunnels: head can reach worker via loopback IP
        -L "${WORKER_TUNNEL_IP}:${WORKER_RAYLET_PORT}:localhost:${WORKER_RAYLET_PORT}"
        -L "${WORKER_TUNNEL_IP}:${WORKER_OBJ_PORT}:localhost:${WORKER_OBJ_PORT}"
    )
    # Add forward tunnels for worker process ports
    for port in $(seq ${WORKER_MIN_PORT} ${WORKER_MAX_PORT}); do
        SSH_ARGS+=(-L "${WORKER_TUNNEL_IP}:${port}:localhost:${port}")
    done

    local script_content
    script_content=$(cat "${script_file}")
    ssh "${SSH_ARGS[@]}" "${PW_USER}@${site_name}" "${script_content}" 2>&1 | \
        sed "s/^/[${site_id}] /"
}

# Launch all remote sites in parallel
PIDS=()
SITE_NAMES=()

for i in $(seq 1 $((NUM_SITES - 1))); do
    site_name=$(echo "${SITES_JSON}" | ${PYTHON_CMD} -c "import sys,json;print(json.load(sys.stdin)[${i}]['name'])")
    site_ip=$(echo "${SITES_JSON}" | ${PYTHON_CMD} -c "import sys,json;print(json.load(sys.stdin)[${i}]['ip'])")
    scheduler_type=$(echo "${SITES_JSON}" | ${PYTHON_CMD} -c "import sys,json;print(json.load(sys.stdin)[${i}]['scheduler_type'])")

    dispatch_worker "${i}" "${site_name}" "${site_ip}" "${scheduler_type}" &
    PIDS+=($!)
    SITE_NAMES+=("${site_name}")
done

echo ""
echo "All ${NUM_REMOTE_SITES} remote sites dispatched, waiting..."

# Wait for all and collect exit codes
FAILED=0
for i in "${!PIDS[@]}"; do
    if wait "${PIDS[$i]}"; then
        echo "[site-$((i+2))] ${SITE_NAMES[$i]}: COMPLETED"
    else
        echo "[site-$((i+2))] ${SITE_NAMES[$i]}: FAILED (exit $?)"
        FAILED=$((FAILED + 1))
    fi
done

echo ""
echo "=========================================="
echo "Worker dispatch complete!"
echo "  Remote sites: ${NUM_REMOTE_SITES}"
echo "  Failed: ${FAILED}"
echo "=========================================="

if [ "${FAILED}" -gt 0 ]; then
    exit 1
fi
