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

# Parse targets JSON to get site list with scheduler config
SITES_JSON=$(${PYTHON_CMD} -c "
import json, sys, os

targets = json.loads(os.environ['TARGETS_JSON'])
sites = []
for i, t in enumerate(targets):
    res = t.get('resource', {})
    # Handle resource as string (CLI) or object (UI)
    if isinstance(res, str):
        res = {'name': res}
    # Scheduler config
    use_scheduler = t.get('scheduler', False)
    if isinstance(use_scheduler, str):
        use_scheduler = use_scheduler.lower() == 'true'
    scheduler_type = res.get('schedulerType', '')
    if use_scheduler and not scheduler_type:
        scheduler_type = 'slurm'
    slurm = t.get('slurm', {}) or {}
    sites.append({
        'index': i,
        'name': res.get('name', 'site-%d' % i),
        'ip': res.get('ip', ''),
        'user': res.get('user', ''),
        'scheduler_type': scheduler_type,
        'use_scheduler': use_scheduler,
        'slurm_partition': slurm.get('partition', ''),
        'slurm_account': slurm.get('account', ''),
        'slurm_qos': slurm.get('qos', ''),
        'slurm_time': slurm.get('time', '00:05:00'),
        'slurm_nodes': slurm.get('nodes', '1'),
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

# TCP proxy Python code (reusable)
PROXY_PY_CODE='
import socket, threading, sys, os
def proxy(src, dst):
    try:
        while True:
            d = src.recv(65536)
            if not d: break
            dst.sendall(d)
    except: pass
    finally: src.close(); dst.close()
s = socket.socket()
s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
s.bind(("0.0.0.0", int(sys.argv[1])))
s.listen(64)
if len(sys.argv) > 3:
    open(sys.argv[3], "w").write(str(os.getpid()))
while True:
    c, _ = s.accept()
    r = socket.create_connection((sys.argv[2].split(":")[0], int(sys.argv[2].split(":")[1])))
    threading.Thread(target=proxy, args=(c,r), daemon=True).start()
    threading.Thread(target=proxy, args=(r,c), daemon=True).start()
'

# Worker dispatch function for a single remote site
dispatch_worker() {
    local site_index=$1
    local site_name=$2
    local site_ip=$3
    local use_scheduler=$4
    local scheduler_type=$5
    local slurm_partition=$6
    local slurm_account=$7
    local slurm_qos=$8
    local slurm_time=$9
    local slurm_nodes=${10}

    local site_id="site-$((site_index + 1))"
    local dispatch_mode="ssh"
    if [ "${use_scheduler}" = "true" ]; then
        dispatch_mode="${scheduler_type:-slurm}"
    fi

    local num_nodes=1
    if [ "${dispatch_mode}" = "slurm" ]; then
        num_nodes="${slurm_nodes:-1}"
    fi

    echo ""
    echo "[${site_id}] Dispatching to ${site_name} (${site_ip}) [${dispatch_mode}, ${num_nodes} node(s)]"

    # Port allocation for N nodes
    # Tunnel IP: 127.0.(site_index+1).(node+1)
    # Port base: 20000 + (site_index-1)*1000 + node*100
    local NODE_TUNNEL_IPS=()
    local NODE_RAYLET_PORTS=()
    local NODE_OBJ_PORTS=()
    local NODE_MIN_PORTS=()
    local NODE_MAX_PORTS=()

    for j in $(seq 0 $((num_nodes - 1))); do
        NODE_TUNNEL_IPS+=("127.0.$((site_index + 1)).$((j + 1))")
        local base=$((20000 + (site_index - 1) * 1000 + j * 100))
        NODE_RAYLET_PORTS+=($((base + 80)))
        NODE_OBJ_PORTS+=($((base + 81)))
        NODE_MIN_PORTS+=($((base)))
        NODE_MAX_PORTS+=($((base + 20)))
    done

    echo "[${site_id}] Tunnel IPs: ${NODE_TUNNEL_IPS[*]}"

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
    )

    # Forward tunnels: head can reach each worker node via unique loopback IPs
    for j in $(seq 0 $((num_nodes - 1))); do
        local ip="${NODE_TUNNEL_IPS[$j]}"
        SSH_ARGS+=(-L "${ip}:${NODE_RAYLET_PORTS[$j]}:localhost:${NODE_RAYLET_PORTS[$j]}")
        SSH_ARGS+=(-L "${ip}:${NODE_OBJ_PORTS[$j]}:localhost:${NODE_OBJ_PORTS[$j]}")
        for port in $(seq ${NODE_MIN_PORTS[$j]} ${NODE_MAX_PORTS[$j]}); do
            SSH_ARGS+=(-L "${ip}:${port}:localhost:${port}")
        done
    done

    # Build the remote worker script
    local script_file="${WORK_DIR}/worker_${site_id}.sh"

    if [ "${dispatch_mode}" = "slurm" ]; then
        # Build srun command
        local srun_cmd="srun"
        [ -n "${slurm_partition}" ] && srun_cmd="${srun_cmd} --partition=${slurm_partition}"
        [ -n "${slurm_account}" ] && srun_cmd="${srun_cmd} --account=${slurm_account}"
        [ -n "${slurm_qos}" ] && srun_cmd="${srun_cmd} --qos=${slurm_qos}"
        [ -n "${slurm_time}" ] && srun_cmd="${srun_cmd} --time=${slurm_time}"
        srun_cmd="${srun_cmd} --nodes=${num_nodes} --ntasks=${num_nodes}"

        # Generate per-node config files content
        local node_configs=""
        for j in $(seq 0 $((num_nodes - 1))); do
            node_configs="${node_configs}
cat > \"\${WORK}/nodeinfo/config_${j}.sh\" <<'NODECONF'
MY_TUNNEL_IP=${NODE_TUNNEL_IPS[$j]}
MY_RAYLET_PORT=${NODE_RAYLET_PORTS[$j]}
MY_OBJ_PORT=${NODE_OBJ_PORTS[$j]}
MY_MIN_PORT=${NODE_MIN_PORTS[$j]}
MY_MAX_PORT=${NODE_MAX_PORTS[$j]}
MY_SITE_ID=${site_id}
MY_NODE_INDEX=${j}
NODECONF"
        done

        cat > "${script_file}" <<WORKER_SCRIPT
#!/bin/bash
set -e
WORK=\${PW_PARENT_JOB_DIR:-\${HOME}/pw/jobs/ray_worker_remote}
mkdir -p "\${WORK}"
cd "\${WORK}"
export PW_PARENT_JOB_DIR="\${WORK}"

# Always fetch latest scripts
echo 'Checking out scripts...'
rm -rf _checkout_tmp scripts
git clone --depth 1 --sparse --filter=blob:none ${REPO_URL} _checkout_tmp 2>/dev/null
cd _checkout_tmp && git sparse-checkout set scripts 2>/dev/null && cd ..
cp -r _checkout_tmp/scripts . && rm -rf _checkout_tmp

# Setup Ray
export RAY_VERSION='${RAY_VERSION}'
export PYTHON_MICRO_VERSION='${PYTHON_VERSION}'
bash scripts/setup.sh

# Activate venv
VENV_DIR="\${WORK}/.venv"
if [ -f "\${VENV_DIR}/bin/python" ]; then
    source "\${VENV_DIR}/bin/activate"
fi

LOGIN_HOST=\$(hostname)
NUM_NODES=${num_nodes}

# Start TCP proxies for reverse tunnels (Ray GCS + dashboard accessible to compute nodes)
PROXY_RAY_PORT=\$(python3 -c "import socket; s=socket.socket(); s.bind(('',0)); print(s.getsockname()[1]); s.close()")
PROXY_DASH_PORT=\$(python3 -c "import socket; s=socket.socket(); s.bind(('',0)); print(s.getsockname()[1]); s.close()")

python3 -c "${PROXY_PY_CODE}" \${PROXY_RAY_PORT} "localhost:${tunnel_ray_port}" "\${WORK}/.proxy_ray_pid" &
sleep 0.5
python3 -c "${PROXY_PY_CODE}" \${PROXY_DASH_PORT} "localhost:${tunnel_dashboard_port}" "\${WORK}/.proxy_dash_pid" &
sleep 0.5

echo "Login node proxies:"
echo "  Ray GCS: \${LOGIN_HOST}:\${PROXY_RAY_PORT} -> localhost:${tunnel_ray_port}"
echo "  Dashboard: \${LOGIN_HOST}:\${PROXY_DASH_PORT} -> localhost:${tunnel_dashboard_port}"

# Write per-node configurations
mkdir -p "\${WORK}/nodeinfo"
rm -f "\${WORK}/nodeinfo/"*
${node_configs}

cleanup() {
    kill \$(cat "\${WORK}/.proxy_ray_pid" 2>/dev/null) 2>/dev/null || true
    kill \$(cat "\${WORK}/.proxy_dash_pid" 2>/dev/null) 2>/dev/null || true
    for pf in "\${WORK}/.proxy_fwd_"*.pid; do
        [ -f "\${pf}" ] && kill \$(cat "\${pf}" 2>/dev/null) 2>/dev/null || true
    done
}
trap cleanup EXIT

echo "Submitting to SLURM: ${srun_cmd}"

# Export variables for srun tasks
export WORK_DIR="\${WORK}"
export LOGIN_HOST
export PROXY_RAY_PORT
export PROXY_DASH_PORT

${srun_cmd} bash -c '
set -e
PROC_ID=\${SLURM_PROCID:-0}
NODE_HOST=\$(hostname)

# Read my config
source "\${WORK_DIR}/nodeinfo/config_\${PROC_ID}.sh"

echo "Node \${PROC_ID}: \${NODE_HOST}"
echo "  Tunnel IP: \${MY_TUNNEL_IP}"
echo "  Raylet: \${MY_RAYLET_PORT}, Obj: \${MY_OBJ_PORT}"
echo "  Workers: \${MY_MIN_PORT}-\${MY_MAX_PORT}"

# Write hostname for coordinator
echo "\${NODE_HOST}" > "\${WORK_DIR}/nodeinfo/host_\${PROC_ID}"

# Wait for forward tunnel proxies
attempt=0
while [ ! -f "\${WORK_DIR}/nodeinfo/proxies_ready" ]; do
    sleep 1
    attempt=\$((attempt + 1))
    if [ \${attempt} -gt 180 ]; then
        echo "[ERROR] Timeout waiting for proxies"
        exit 1
    fi
done

# Activate venv
if [ -f "\${WORK_DIR}/.venv/bin/activate" ]; then
    source "\${WORK_DIR}/.venv/bin/activate"
fi

# Wait for Ray GCS
echo "Waiting for Ray head at \${LOGIN_HOST}:\${PROXY_RAY_PORT}..."
attempt=0
while [ \${attempt} -le 60 ]; do
    if python3 -c "
import socket, sys
s = socket.socket()
s.settimeout(3)
try:
    s.connect(('\''"\${LOGIN_HOST}"'\''', \${PROXY_RAY_PORT}))
    s.close()
    sys.exit(0)
except:
    sys.exit(1)
" 2>/dev/null; then
        echo "Ray head reachable!"
        break
    fi
    sleep 2
    attempt=\$((attempt + 1))
done

ray stop --force 2>/dev/null || true

echo "Starting Ray worker: address=\${LOGIN_HOST}:\${PROXY_RAY_PORT} ip=\${MY_TUNNEL_IP}"
ray start --address="\${LOGIN_HOST}:\${PROXY_RAY_PORT}" \
    --node-ip-address=\${MY_TUNNEL_IP} \
    --node-manager-port=\${MY_RAYLET_PORT} \
    --object-manager-port=\${MY_OBJ_PORT} \
    --min-worker-port=\${MY_MIN_PORT} \
    --max-worker-port=\${MY_MAX_PORT}

echo "Ray worker started on node \${PROC_ID} (\${NODE_HOST})"

# Auto-detect cluster name
CLUSTER_NAME=""
SCHED_TYPE=""
PW_CMD_LOCAL=""
for try_cmd in pw ~/pw/pw; do
    command -v \${try_cmd} &>/dev/null && { PW_CMD_LOCAL=\${try_cmd}; break; }
    [ -x "\${try_cmd}" ] && { PW_CMD_LOCAL=\${try_cmd}; break; }
done
if [ -n "\${PW_CMD_LOCAL}" ]; then
    MY_HOST=\$(hostname -s)
    while IFS= read -r line; do
        uri=\$(echo "\${line}" | awk "{print \\\$1}")
        ctype=\$(echo "\${line}" | awk "{print \\\$3}")
        cname="\${uri##*/}"
        if echo "\${MY_HOST}" | grep -qi "\${cname}"; then
            CLUSTER_NAME="\${cname}"
            case "\${ctype}" in
                *slurm*) SCHED_TYPE="slurm" ;;
                *pbs*)   SCHED_TYPE="pbs" ;;
                existing) SCHED_TYPE="ssh" ;;
                *)       SCHED_TYPE="\${ctype}" ;;
            esac
            break
        fi
    done < <(\${PW_CMD_LOCAL} cluster list 2>/dev/null | grep "^pw://\${PW_USER}/" | grep "active")
fi
if [ -z "\${SCHED_TYPE}" ]; then
    if [ -n "\${SLURM_JOB_ID}" ]; then SCHED_TYPE="slurm"
    elif [ -n "\${PBS_JOBID}" ]; then SCHED_TYPE="pbs"
    else SCHED_TYPE="ssh"
    fi
fi
[ -z "\${CLUSTER_NAME}" ] && CLUSTER_NAME="'"${site_name}"'"

WORKER_IP=\$(hostname -I 2>/dev/null | awk "{print \\\$1}")
NUM_CPUS=\$(nproc 2>/dev/null || echo 1)

DASHBOARD_URL="http://\${LOGIN_HOST}:\${PROXY_DASH_PORT}"
curl -s -X POST "\${DASHBOARD_URL}/api/worker" \
    -H "Content-Type: application/json" \
    -d "{
        \"site_id\": \"\${MY_SITE_ID}\",
        \"worker_ip\": \"\${MY_TUNNEL_IP}\",
        \"num_cpus\": \${NUM_CPUS},
        \"cluster_name\": \"\${CLUSTER_NAME}\",
        \"scheduler_type\": \"\${SCHED_TYPE}\"
    }" 2>/dev/null || echo "Note: Could not notify dashboard"

echo "Worker \${PROC_ID} RUNNING (tunnel IP: \${MY_TUNNEL_IP})"

# Keep alive
while true; do
    ray status 2>/dev/null || echo "Worker \${PROC_ID} health: \$(date)"
    sleep 30
done
' &
SRUN_PID=\$!

# Wait for all nodes to report hostnames
echo "Waiting for \${NUM_NODES} compute node(s) to start..."
attempt=0
while true; do
    count=\$(ls -1 "\${WORK}/nodeinfo/host_"* 2>/dev/null | wc -l || echo 0)
    if [ "\${count}" -ge "\${NUM_NODES}" ]; then
        break
    fi
    sleep 2
    attempt=\$((attempt + 1))
    if [ \${attempt} -gt 120 ]; then
        echo "[ERROR] Timeout waiting for compute nodes (got \${count}/\${NUM_NODES})"
        kill \${SRUN_PID} 2>/dev/null || true
        exit 1
    fi
done

echo "All \${NUM_NODES} node(s) reported. Starting forward tunnel proxies..."

# Start forward tunnel proxies: login_node:port -> compute_node:port
# The SSH -L tunnels go: head:127.0.X.Y:port -> login:port
# We proxy: login:port -> compute_node:port
for j in \$(seq 0 \$((\${NUM_NODES} - 1))); do
    NODE_HOST=\$(cat "\${WORK}/nodeinfo/host_\${j}")
    source "\${WORK}/nodeinfo/config_\${j}.sh"
    echo "  Node \${j} (\${NODE_HOST}): proxying ports \${MY_MIN_PORT}-\${MY_MAX_PORT}, \${MY_RAYLET_PORT}, \${MY_OBJ_PORT}"

    # Proxy raylet port
    python3 -c "${PROXY_PY_CODE}" \${MY_RAYLET_PORT} "\${NODE_HOST}:\${MY_RAYLET_PORT}" "\${WORK}/.proxy_fwd_\${j}_raylet.pid" &
    # Proxy object manager port
    python3 -c "${PROXY_PY_CODE}" \${MY_OBJ_PORT} "\${NODE_HOST}:\${MY_OBJ_PORT}" "\${WORK}/.proxy_fwd_\${j}_obj.pid" &
    # Proxy worker ports
    for port in \$(seq \${MY_MIN_PORT} \${MY_MAX_PORT}); do
        python3 -c "${PROXY_PY_CODE}" \${port} "\${NODE_HOST}:\${port}" "\${WORK}/.proxy_fwd_\${j}_w\${port}.pid" &
    done
done
sleep 1

# Signal nodes that proxies are ready
touch "\${WORK}/nodeinfo/proxies_ready"
echo "Forward proxies ready!"

wait \${SRUN_PID}
WORKER_SCRIPT

    else
        # SSH mode: run directly on the remote host (single node)
        local ip="${NODE_TUNNEL_IPS[0]}"
        local raylet="${NODE_RAYLET_PORTS[0]}"
        local obj="${NODE_OBJ_PORTS[0]}"
        local min_port="${NODE_MIN_PORTS[0]}"
        local max_port="${NODE_MAX_PORTS[0]}"

        cat > "${script_file}" <<WORKER_SCRIPT
#!/bin/bash
set -e
WORK=\${PW_PARENT_JOB_DIR:-\${HOME}/pw/jobs/ray_worker_remote}
mkdir -p "\${WORK}"
cd "\${WORK}"
export PW_PARENT_JOB_DIR="\${WORK}"

# Always fetch latest scripts
echo 'Checking out scripts...'
rm -rf _checkout_tmp scripts
git clone --depth 1 --sparse --filter=blob:none ${REPO_URL} _checkout_tmp 2>/dev/null
cd _checkout_tmp && git sparse-checkout set scripts 2>/dev/null && cd ..
cp -r _checkout_tmp/scripts . && rm -rf _checkout_tmp

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
echo "  Node IP advertised: ${ip} (via SSH forward tunnel)"
echo "  Raylet port: ${raylet}"
echo "  Object manager port: ${obj}"
echo "  Worker process ports: ${min_port}-${max_port}"
ray start --address="localhost:${tunnel_ray_port}" \\
    --node-ip-address=${ip} \\
    --node-manager-port=${raylet} \\
    --object-manager-port=${obj} \\
    --min-worker-port=${min_port} \\
    --max-worker-port=${max_port}

echo "Ray worker started!"
ray status 2>/dev/null || echo "Note: ray status may not work on worker node"

# Notify dashboard that worker joined
WORKER_IP=\$(hostname -I 2>/dev/null | awk '{print \$1}')
NUM_CPUS=\$(nproc 2>/dev/null || echo 1)

# Auto-detect cluster name and scheduler type using pw cluster list + hostname matching
SCHED_TYPE=""
CLUSTER_NAME=""
PW_CMD_LOCAL=""
for try_cmd in pw ~/pw/pw; do
    command -v \$try_cmd &>/dev/null && { PW_CMD_LOCAL=\$try_cmd; break; }
    [ -x "\$try_cmd" ] && { PW_CMD_LOCAL=\$try_cmd; break; }
done

if [ -n "\${PW_CMD_LOCAL}" ]; then
    MY_HOST=\$(hostname -s)
    while IFS= read -r line; do
        uri=\$(echo "\$line" | awk '{print \$1}')
        ctype=\$(echo "\$line" | awk '{print \$3}')
        cname="\${uri##*/}"
        if echo "\${MY_HOST}" | grep -qi "\${cname}"; then
            CLUSTER_NAME="\${cname}"
            case "\${ctype}" in
                *slurm*) SCHED_TYPE="slurm" ;;
                *pbs*)   SCHED_TYPE="pbs" ;;
                existing) SCHED_TYPE="ssh" ;;
                *)       SCHED_TYPE="\${ctype}" ;;
            esac
            break
        fi
    done < <(\${PW_CMD_LOCAL} cluster list 2>/dev/null | grep "^pw://\${PW_USER}/" | grep "active")
fi

# Fallback: check scheduler env vars
if [ -z "\${SCHED_TYPE}" ]; then
    if [ -n "\${SLURM_JOB_ID}" ]; then
        SCHED_TYPE="slurm"
    elif [ -n "\${PBS_JOBID}" ]; then
        SCHED_TYPE="pbs"
    else
        SCHED_TYPE="ssh"
    fi
fi
[ -z "\${CLUSTER_NAME}" ] && CLUSTER_NAME="${site_name}"

echo "Detected cluster: \${CLUSTER_NAME} (\${SCHED_TYPE})"

DASHBOARD_URL="http://localhost:${tunnel_dashboard_port}"
curl -s -X POST "\${DASHBOARD_URL}/api/worker" \\
    -H "Content-Type: application/json" \\
    -d "{
        \"site_id\": \"${site_id}\",
        \"worker_ip\": \"${ip}\",
        \"num_cpus\": \${NUM_CPUS},
        \"cluster_name\": \"\${CLUSTER_NAME}\",
        \"scheduler_type\": \"\${SCHED_TYPE}\"
    }" 2>/dev/null || echo "Note: Could not notify dashboard"

echo "=========================================="
echo "Ray Worker RUNNING"
echo "  Connected to: localhost:${tunnel_ray_port}"
echo "  Worker IP: ${ip}"
echo "  CPUs: \${NUM_CPUS}"
echo "=========================================="

# Keep SSH session alive
while true; do
    ray status 2>/dev/null || echo "Worker health check: \$(date)"
    sleep 30
done
WORKER_SCRIPT
    fi

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
    use_scheduler=$(echo "${SITES_JSON}" | ${PYTHON_CMD} -c "import sys,json;print(str(json.load(sys.stdin)[${i}].get('use_scheduler',False)).lower())")
    scheduler_type=$(echo "${SITES_JSON}" | ${PYTHON_CMD} -c "import sys,json;print(json.load(sys.stdin)[${i}].get('scheduler_type',''))")
    slurm_partition=$(echo "${SITES_JSON}" | ${PYTHON_CMD} -c "import sys,json;print(json.load(sys.stdin)[${i}].get('slurm_partition',''))")
    slurm_account=$(echo "${SITES_JSON}" | ${PYTHON_CMD} -c "import sys,json;print(json.load(sys.stdin)[${i}].get('slurm_account',''))")
    slurm_qos=$(echo "${SITES_JSON}" | ${PYTHON_CMD} -c "import sys,json;print(json.load(sys.stdin)[${i}].get('slurm_qos',''))")
    slurm_time=$(echo "${SITES_JSON}" | ${PYTHON_CMD} -c "import sys,json;print(json.load(sys.stdin)[${i}].get('slurm_time','00:05:00'))")
    slurm_nodes=$(echo "${SITES_JSON}" | ${PYTHON_CMD} -c "import sys,json;print(json.load(sys.stdin)[${i}].get('slurm_nodes','1'))")

    dispatch_worker "${i}" "${site_name}" "${site_ip}" \
        "${use_scheduler}" "${scheduler_type}" \
        "${slurm_partition}" "${slurm_account}" "${slurm_qos}" "${slurm_time}" \
        "${slurm_nodes}" &
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
