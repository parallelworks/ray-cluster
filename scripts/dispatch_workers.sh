#!/bin/bash
# dispatch_workers.sh — Dispatch Ray workers to compute sites
#
# Runs on the head node. For each worker site:
#   - Same site as head (local): submit via SLURM, workers connect directly
#   - Different site (remote): SSH tunnel + remote dispatch
#
# Environment variables:
#   WORKERS_JSON       - JSON array of worker target objects from workflow inputs
#   HEAD_RESOURCE_NAME - Name of the head resource (to detect same-site workers)
#   DASHBOARD_PORT     - Dashboard port on this machine
#   RAY_HEAD_IP        - Ray head node IP
#   PYTHON_VERSION     - Python micro version for worker matching
#   RAY_VERSION        - Ray version to install

set -e

JOB_DIR="${PW_PARENT_JOB_DIR%/}"
SCRIPT_DIR="${JOB_DIR}/scripts"
WORK_DIR=$(mktemp -d)
trap "rm -rf ${WORK_DIR}" EXIT

RAY_PORT=6379
RAY_VERSION="${RAY_VERSION:-2.40.0}"

# Auto-detect HEAD_RESOURCE_NAME if not set (template expression may not resolve in run blocks)
if [ -z "${HEAD_RESOURCE_NAME}" ]; then
    for cmd in pw ~/pw/pw; do
        command -v $cmd &>/dev/null && { PW_CMD_TMP=$cmd; break; }
        [ -x "$cmd" ] && { PW_CMD_TMP=$cmd; break; }
    done
    if [ -n "${PW_CMD_TMP}" ]; then
        MY_SHORT_HOST=$(hostname -s)
        while IFS= read -r line; do
            uri=$(echo "$line" | awk '{print $1}')
            cname="${uri##*/}"
            if echo "${MY_SHORT_HOST}" | grep -qi "${cname}"; then
                HEAD_RESOURCE_NAME="${cname}"
                break
            fi
        done < <(${PW_CMD_TMP} cluster list 2>/dev/null | grep "^pw://${PW_USER}/" | grep "active")
    fi
fi

# Fallback: if head resource not detected, assume workspace
[ -z "${HEAD_RESOURCE_NAME}" ] && HEAD_RESOURCE_NAME="workspace"

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

# Parse workers JSON to get site list with scheduler config
SITES_JSON=$(${PYTHON_CMD} -c "
import json, sys, os

workers = json.loads(os.environ.get('WORKERS_JSON', '[]'))
head_name = os.environ.get('HEAD_RESOURCE_NAME', '')
sites = []
for i, t in enumerate(workers):
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
    pbs = t.get('pbs', {}) or {}
    # Detect if worker is on the same resource as head
    is_local = (res.get('name', '') == head_name) if head_name else False
    sites.append({
        'index': i,
        'name': res.get('name', 'worker-%d' % i),
        'ip': res.get('ip', ''),
        'user': res.get('user', ''),
        'scheduler_type': scheduler_type,
        'use_scheduler': use_scheduler,
        'is_local': is_local,
        'slurm_partition': slurm.get('partition', ''),
        'slurm_account': slurm.get('account', ''),
        'slurm_qos': slurm.get('qos', ''),
        'slurm_time': slurm.get('time', '00:05:00'),
        'slurm_nodes': slurm.get('nodes', '1'),
        'slurm_gres': slurm.get('gres', ''),
        'slurm_directives': slurm.get('scheduler_directives', ''),
        'pbs_queue': pbs.get('queue', ''),
        'pbs_account': pbs.get('account', ''),
        'pbs_nodes': pbs.get('nodes', '1'),
        'pbs_select': pbs.get('select', ''),
        'pbs_walltime': pbs.get('walltime', '01:00:00'),
        'pbs_directives': pbs.get('scheduler_directives', ''),
    })
print(json.dumps(sites))
")

NUM_WORKERS=$(echo "${SITES_JSON}" | ${PYTHON_CMD} -c "import sys,json;print(len(json.load(sys.stdin)))")

echo "=========================================="
echo "Dispatch Workers: $(date)"
echo "=========================================="
echo "Worker sites:  ${NUM_WORKERS}"
echo "Parsed config: $(echo "${SITES_JSON}" | ${PYTHON_CMD} -c "import sys,json;print(json.dumps(json.load(sys.stdin),indent=2))")"
echo "Head resource: ${HEAD_RESOURCE_NAME}"
echo "Dashboard:     localhost:${DASHBOARD_PORT}"
echo "Ray head:      ${RAY_HEAD_IP}:${RAY_PORT}"
echo "Python:        ${PYTHON_VERSION}"
echo "Ray version:   ${RAY_VERSION}"

if [ "${NUM_WORKERS}" -eq 0 ]; then
    echo ""
    echo "No worker sites configured. Head-only mode."
    echo "=========================================="
    exit 0
fi

REPO_URL="https://github.com/parallelworks/ray-cluster.git"

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
s.bind(("", int(sys.argv[1])))
s.listen(64)
if len(sys.argv) > 3:
    open(sys.argv[3], "w").write(str(os.getpid()))
dest_host = sys.argv[2].split(":")[0]
dest_port = int(sys.argv[2].split(":")[1])
while True:
    c, _ = s.accept()
    try:
        r = socket.create_connection((dest_host, dest_port), timeout=10)
    except Exception:
        c.close()
        continue
    threading.Thread(target=proxy, args=(c,r), daemon=True).start()
    threading.Thread(target=proxy, args=(r,c), daemon=True).start()
'

# =============================================================================
# Local worker dispatch (same site as head — SLURM/PBS, no tunnels needed)
# =============================================================================
dispatch_local_workers() {
    local site_index=$1
    local site_name=$2
    local scheduler_type=$3
    local slurm_partition=$4
    local slurm_account=$5
    local slurm_qos=$6
    local slurm_time=$7
    local slurm_nodes=$8
    local slurm_gres=$9
    local slurm_directives=${10}
    local pbs_queue=${11}
    local pbs_account=${12}
    local pbs_nodes=${13}
    local pbs_select=${14}
    local pbs_walltime=${15}
    local pbs_directives=${16}

    local site_id="site-1"  # Local workers are part of the head's site

    if [ "${scheduler_type}" = "pbs" ]; then
        local num_nodes="${pbs_nodes:-1}"
        echo "[${site_name}] Dispatching ${num_nodes} local PBS worker(s) on ${site_name}"

        # Write the inner worker script that each node will run
        local node_script="${WORK_DIR}/worker_local_${site_index}_node.sh"
        cat > "${node_script}" <<'NODE_SCRIPT'
#!/bin/bash
set -e

# Pin BLAS threading
export OMP_NUM_THREADS=1
export MKL_NUM_THREADS=1
export OPENBLAS_NUM_THREADS=1

# Activate venv from shared filesystem
RAY_VENV="$(cat "${WORKER_JOB_DIR}/RAY_VENV_DIR" 2>/dev/null || echo "${WORKER_JOB_DIR}/.venv")"
if [ -f "${RAY_VENV}/bin/activate" ]; then
    source "${RAY_VENV}/bin/activate"
fi

ray stop --force 2>/dev/null || true
rm -rf /tmp/ray/session_* 2>/dev/null || true
sleep 1

WORKER_IP=$(hostname -I 2>/dev/null | awk '{print $1}')

# Detect GPUs — respect SLURM/scheduler allocation
NUM_GPUS=0
if [ -n "${CUDA_VISIBLE_DEVICES:-}" ]; then
    # CUDA_VISIBLE_DEVICES is set — count the devices listed
    NUM_GPUS=$(echo "${CUDA_VISIBLE_DEVICES}" | tr ',' '\n' | grep -c .)
    echo "Detected ${NUM_GPUS} GPU(s) (from CUDA_VISIBLE_DEVICES=${CUDA_VISIBLE_DEVICES})"
elif [ -z "${SLURM_JOB_ID:-}" ] && command -v nvidia-smi &>/dev/null; then
    # Not in a SLURM job — fall back to nvidia-smi (check exit code to avoid counting error text)
    GPU_LIST=$(nvidia-smi -L 2>/dev/null) && NUM_GPUS=$(echo "${GPU_LIST}" | grep -c "^GPU ")
    [ "${NUM_GPUS}" -gt 0 ] && echo "Detected ${NUM_GPUS} GPU(s)"
fi

echo "Starting Ray worker on $(hostname) (${WORKER_IP}), connecting to ${WORKER_HEAD_IP}:6379..."
RAY_GPU_ARGS=""
if [ "${NUM_GPUS}" -gt 0 ]; then
    RAY_GPU_ARGS="--num-gpus=${NUM_GPUS}"
fi
ray start --address="${WORKER_HEAD_IP}:6379" --num-cpus=1 ${RAY_GPU_ARGS}

# Auto-detect cluster name
CLUSTER_NAME=""
PW_CMD_LOCAL=""
for try_cmd in pw ~/pw/pw; do
    command -v ${try_cmd} &>/dev/null && { PW_CMD_LOCAL=${try_cmd}; break; }
    [ -x "${try_cmd}" ] && { PW_CMD_LOCAL=${try_cmd}; break; }
done
if [ -n "${PW_CMD_LOCAL}" ]; then
    MY_HOST=$(hostname -s)
    while IFS= read -r line; do
        uri=$(echo "${line}" | awk '{print $1}')
        cname="${uri##*/}"
        if echo "${MY_HOST}" | grep -qi "${cname}"; then
            CLUSTER_NAME="${cname}"
            break
        fi
    done < <(${PW_CMD_LOCAL} cluster list 2>/dev/null | grep "^pw://${PW_USER}/" | grep "active")
fi
[ -z "${CLUSTER_NAME}" ] && CLUSTER_NAME="${WORKER_SITE_NAME}"

# Register with dashboard
curl -s -X POST "http://${WORKER_HEAD_IP}:${WORKER_DASH_PORT}/api/worker" \
    -H "Content-Type: application/json" \
    -d "{
        \"site_id\": \"site-1\",
        \"worker_ip\": \"${WORKER_IP}\",
        \"num_cpus\": 1,
        \"num_gpus\": ${NUM_GPUS},
        \"cluster_name\": \"${CLUSTER_NAME}\",
        \"scheduler_type\": \"pbs\"
    }" 2>/dev/null || echo "Note: Could not notify dashboard"

echo "=========================================="
echo "Ray Worker RUNNING on $(hostname)"
echo "  Head: ${WORKER_HEAD_IP}:6379"
echo "  Worker IP: ${WORKER_IP}"
echo "=========================================="

# Keep alive
while true; do
    ray status 2>/dev/null || echo "Worker health: $(date)"
    sleep 30
done
NODE_SCRIPT
        chmod +x "${node_script}"

        # Write PBS job script to shared filesystem (JOB_DIR, not WORK_DIR which is /tmp and node-local)
        echo "  PBS config: queue='${pbs_queue}' account='${pbs_account}' nodes='${num_nodes}' select='${pbs_select}' walltime='${pbs_walltime}'"
        local script_file="${JOB_DIR}/worker_local_${site_index}.pbs"
        local pbs_select_str="${pbs_select:-${num_nodes}:ncpus=1}"
        cat > "${script_file}" <<PBS_SCRIPT
#!/bin/bash
#PBS -l select=${pbs_select_str}
#PBS -l walltime=${pbs_walltime:-01:00:00}
#PBS -j oe
#PBS -V
$([ -n "${pbs_queue}" ] && echo "#PBS -q ${pbs_queue}")
$([ -n "${pbs_account}" ] && echo "#PBS -A ${pbs_account}")
$(echo "${pbs_directives}" | grep -v '^$' || true)

export WORKER_HEAD_IP="${RAY_HEAD_IP}"
export WORKER_DASH_PORT="${DASHBOARD_PORT}"
export WORKER_JOB_DIR="${JOB_DIR}"
export WORKER_SITE_NAME="${site_name}"

# Read allocated nodes from PBS_NODEFILE
NODES=(\$(sort -u "\${PBS_NODEFILE}"))
NUM_ALLOCATED=\${#NODES[@]}
echo "PBS allocated \${NUM_ALLOCATED} node(s): \${NODES[*]}"

# Run worker on each allocated node via pbsdsh
for NODE_INDEX in \$(seq 0 \$((\${NUM_ALLOCATED} - 1))); do
    pbsdsh -n \${NODE_INDEX} -- bash ${node_script} &
done

# Wait for all pbsdsh background processes
wait
PBS_SCRIPT

        echo "[${site_name}] Submitting PBS job: qsub ${script_file}"
        local qsub_output
        qsub_output=$(qsub "${script_file}" 2>&1)
        echo "[${site_name}] ${qsub_output}"
        if ! echo "${qsub_output}" | grep -qP '^\d+'; then
            echo "[${site_name}] ERROR: qsub failed: ${qsub_output}"
            curl -s --connect-timeout 3 -X POST "http://localhost:${DASHBOARD_PORT}/api/worker/error" \
                -H "Content-Type: application/json" \
                -d "{\"site_id\": \"site-1\", \"error\": $(echo "${qsub_output}" | python3 -c 'import sys,json; print(json.dumps(sys.stdin.read().strip()))')}" \
                2>/dev/null || true
            return 1
        fi

    else
        # SLURM dispatch via sbatch (captures job ID for cleanup)
        echo "[${site_name}] Dispatching ${slurm_nodes} local SLURM worker(s) on ${site_name}"

        # Write worker script with sbatch directives to shared filesystem
        local script_file="${JOB_DIR}/worker_local_${site_index}.sh"
        local log_file="${JOB_DIR}/logs/worker_local_${site_index}.out"
        mkdir -p "${JOB_DIR}/logs"
        cat > "${script_file}" <<WORKER_SCRIPT
#!/bin/bash
#SBATCH --nodes=${slurm_nodes}
#SBATCH --ntasks=${slurm_nodes}
#SBATCH --output=${log_file}
#SBATCH --error=${log_file}
#SBATCH --job-name=ray-worker-${site_name}
#SBATCH --export=ALL
$([ -n "${slurm_partition}" ] && echo "#SBATCH --partition=${slurm_partition}")
$([ -n "${slurm_account}" ] && echo "#SBATCH --account=${slurm_account}")
$([ -n "${slurm_qos}" ] && echo "#SBATCH --qos=${slurm_qos}")
$([ -n "${slurm_time}" ] && echo "#SBATCH --time=${slurm_time}")
$([ -n "${slurm_gres}" ] && echo "#SBATCH --gres=${slurm_gres}")
$(echo "${slurm_directives}" | grep -v '^$' | grep -v '^#*$' || true)
set -e

HEAD_IP="${RAY_HEAD_IP}"
DASH_PORT="${DASHBOARD_PORT}"
JOB_DIR="${JOB_DIR}"

# Pin BLAS threading
export OMP_NUM_THREADS=1
export MKL_NUM_THREADS=1
export OPENBLAS_NUM_THREADS=1

# Activate venv from shared filesystem
RAY_VENV="\$(cat "\${JOB_DIR}/RAY_VENV_DIR" 2>/dev/null || echo "\${JOB_DIR}/.venv")"
if [ -f "\${RAY_VENV}/bin/activate" ]; then
    source "\${RAY_VENV}/bin/activate"
fi

ray stop --force 2>/dev/null || true
rm -rf /tmp/ray/session_* 2>/dev/null || true
sleep 1

WORKER_IP=\$(hostname -I 2>/dev/null | awk '{print \$1}')

# Detect GPUs — respect SLURM/scheduler allocation
NUM_GPUS=0
if [ -n "\${CUDA_VISIBLE_DEVICES:-}" ]; then
    NUM_GPUS=\$(echo "\${CUDA_VISIBLE_DEVICES}" | tr ',' '\n' | grep -c .)
    echo "Detected \${NUM_GPUS} GPU(s) (from CUDA_VISIBLE_DEVICES=\${CUDA_VISIBLE_DEVICES})"
elif [ -z "\${SLURM_JOB_ID:-}" ] && command -v nvidia-smi &>/dev/null; then
    GPU_LIST=\$(nvidia-smi -L 2>/dev/null) && NUM_GPUS=\$(echo "\${GPU_LIST}" | grep -c "^GPU ")
    [ "\${NUM_GPUS}" -gt 0 ] && echo "Detected \${NUM_GPUS} GPU(s)"
fi

echo "Starting Ray worker on \$(hostname) (\${WORKER_IP}), connecting to \${HEAD_IP}:6379..."
RAY_GPU_ARGS=""
if [ "\${NUM_GPUS}" -gt 0 ]; then
    RAY_GPU_ARGS="--num-gpus=\${NUM_GPUS}"
fi
ray start --address="\${HEAD_IP}:6379" --num-cpus=1 \${RAY_GPU_ARGS}

# Auto-detect cluster name
CLUSTER_NAME=""
PW_CMD_LOCAL=""
for try_cmd in pw ~/pw/pw; do
    command -v \${try_cmd} &>/dev/null && { PW_CMD_LOCAL=\${try_cmd}; break; }
    [ -x "\${try_cmd}" ] && { PW_CMD_LOCAL=\${try_cmd}; break; }
done
if [ -n "\${PW_CMD_LOCAL}" ]; then
    MY_HOST=\$(hostname -s)
    while IFS= read -r line; do
        uri=\$(echo "\${line}" | awk '{print \$1}')
        cname="\${uri##*/}"
        if echo "\${MY_HOST}" | grep -qi "\${cname}"; then
            CLUSTER_NAME="\${cname}"
            break
        fi
    done < <(\${PW_CMD_LOCAL} cluster list 2>/dev/null | grep "^pw://\${PW_USER}/" | grep "active")
fi
[ -z "\${CLUSTER_NAME}" ] && CLUSTER_NAME="${site_name}"

# Register with dashboard
curl -s -X POST "http://\${HEAD_IP}:\${DASH_PORT}/api/worker" \
    -H "Content-Type: application/json" \
    -d "{
        \"site_id\": \"site-1\",
        \"worker_ip\": \"\${WORKER_IP}\",
        \"num_cpus\": 1,
        \"num_gpus\": \${NUM_GPUS},
        \"cluster_name\": \"\${CLUSTER_NAME}\",
        \"scheduler_type\": \"slurm\"
    }" 2>/dev/null || echo "Note: Could not notify dashboard"

echo "=========================================="
echo "Ray Worker RUNNING on \$(hostname)"
echo "  Head: \${HEAD_IP}:6379"
echo "  Worker IP: \${WORKER_IP}"
echo "=========================================="

# Keep alive
while true; do
    ray status 2>/dev/null || echo "Worker health: \$(date)"
    sleep 30
done
WORKER_SCRIPT

        # Submit via sbatch and capture job ID
        local sbatch_output
        sbatch_output=$(sbatch "${script_file}" 2>&1)
        echo "[${site_name}] ${sbatch_output}"
        local slurm_jobid
        slurm_jobid=$(echo "${sbatch_output}" | grep -oP 'Submitted batch job \K[0-9]+')
        if [ -n "${slurm_jobid}" ]; then
            echo "${slurm_jobid}" >> "${JOB_DIR}/slurm_jobids"
            echo "[${site_name}] SLURM job ID: ${slurm_jobid} (saved to ${JOB_DIR}/slurm_jobids)"
        else
            echo "[${site_name}] ERROR: sbatch failed: ${sbatch_output}"
            # Notify dashboard of the failure
            curl -s --connect-timeout 3 -X POST "http://localhost:${DASHBOARD_PORT}/api/worker/error" \
                -H "Content-Type: application/json" \
                -d "{\"site_id\": \"site-1\", \"error\": $(echo "${sbatch_output}" | python3 -c 'import sys,json; print(json.dumps(sys.stdin.read().strip()))')}" \
                2>/dev/null || true
            return 1
        fi

        # Stream the SLURM output log in the background
        (
            # Wait for log file to appear
            for i in $(seq 1 120); do
                [ -f "${log_file}" ] && break
                sleep 2
            done
            if [ -f "${log_file}" ]; then
                tail -f "${log_file}" 2>/dev/null | sed -u "s/^/[${site_name}] /"
            fi
        ) &
    fi
}

# =============================================================================
# Remote worker dispatch (different site — SSH tunnels required)
# =============================================================================
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
    local slurm_gres=${11}
    local slurm_directives=${12}
    local pbs_queue=${13}
    local pbs_account=${14}
    local pbs_nodes=${15}
    local pbs_select=${16}
    local pbs_walltime=${17}
    local pbs_directives=${18}

    local site_id="site-$((site_index + 1))"
    local dispatch_mode="ssh"
    if [ "${use_scheduler}" = "true" ]; then
        dispatch_mode="${scheduler_type:-slurm}"
    fi

    local num_nodes=1
    if [ "${dispatch_mode}" = "slurm" ]; then
        num_nodes="${slurm_nodes:-1}"
    elif [ "${dispatch_mode}" = "pbs" ]; then
        num_nodes="${pbs_nodes:-1}"
    fi

    echo ""
    echo "[${site_name}] Dispatching to ${site_name} (${site_ip}) [${dispatch_mode}, ${num_nodes} node(s)]"

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
        NODE_MAX_PORTS+=($((base + 9)))
    done

    echo "[${site_name}] Tunnel IPs: ${NODE_TUNNEL_IPS[*]}"

    # Kill any stale SSH tunnels using the same forward tunnel ports (from prior runs)
    for j in $(seq 0 $((num_nodes - 1))); do
        local stale_ip="${NODE_TUNNEL_IPS[$j]}"
        local stale_port="${NODE_RAYLET_PORTS[$j]}"
        # Find SSH processes forwarding to these IPs/ports
        local stale_pids
        stale_pids=$(ps aux 2>/dev/null | grep "ssh.*${stale_ip}:${stale_port}" | grep -v grep | awk '{print $2}' || true)
        if [ -n "${stale_pids}" ]; then
            echo "[${site_name}] Killing stale SSH tunnels: ${stale_pids}"
            echo "${stale_pids}" | xargs kill 2>/dev/null || true
            sleep 1
        fi
    done

    # Allocate 2 ports on the remote for pw ssh tunnels (dashboard + Ray GCS)
    local tunnel_dashboard_port
    tunnel_dashboard_port=$(${PW_CMD} ssh "${site_name}" \
        'python3 -c "import socket; s=socket.socket(); s.bind((\"\",0)); print(s.getsockname()[1]); s.close()"' 2>/dev/null)

    if [ -z "${tunnel_dashboard_port}" ] || ! [[ "${tunnel_dashboard_port}" =~ ^[0-9]+$ ]]; then
        echo "[${site_name}] [ERROR] Failed to allocate dashboard tunnel port (got: '${tunnel_dashboard_port}')"
        return 1
    fi

    local tunnel_ray_port
    tunnel_ray_port=$(${PW_CMD} ssh "${site_name}" \
        'python3 -c "import socket; s=socket.socket(); s.bind((\"\",0)); print(s.getsockname()[1]); s.close()"' 2>/dev/null)

    if [ -z "${tunnel_ray_port}" ] || ! [[ "${tunnel_ray_port}" =~ ^[0-9]+$ ]]; then
        echo "[${site_name}] [ERROR] Failed to allocate Ray tunnel port (got: '${tunnel_ray_port}')"
        return 1
    fi

    echo "[${site_name}] Remote tunnel ports: dashboard=${tunnel_dashboard_port} ray=${tunnel_ray_port}"

    # Write coordination file for this site
    echo "${site_name}" > "${JOB_DIR}/CLOUD_CLUSTER_NAME_${site_id}"

    # Build SSH args with bidirectional tunnels
    local SSH_ARGS=(
        -i ~/.ssh/pwcli
        -o StrictHostKeyChecking=no
        -o UserKnownHostsFile=/dev/null
        -o ExitOnForwardFailure=yes
        -o ConnectTimeout=30
        -o ServerAliveInterval=15
        -o ServerAliveCountMax=120
        -o TCPKeepAlive=yes
        -o "ProxyCommand=${PW_CMD} ssh --proxy-command %h"
    )

    # Reverse tunnels: remote site can reach head node's Ray GCS + dashboard
    SSH_ARGS+=(-R "${tunnel_ray_port}:localhost:${RAY_PORT}")
    SSH_ARGS+=(-R "${tunnel_dashboard_port}:localhost:${DASHBOARD_PORT}")

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
        # Build SLURM sbatch directives
        local sbatch_directives=""
        [ -n "${slurm_partition}" ] && sbatch_directives="${sbatch_directives}
#SBATCH --partition=${slurm_partition}"
        [ -n "${slurm_account}" ] && sbatch_directives="${sbatch_directives}
#SBATCH --account=${slurm_account}"
        [ -n "${slurm_qos}" ] && sbatch_directives="${sbatch_directives}
#SBATCH --qos=${slurm_qos}"
        [ -n "${slurm_time}" ] && sbatch_directives="${sbatch_directives}
#SBATCH --time=${slurm_time}"
        [ -n "${slurm_gres}" ] && sbatch_directives="${sbatch_directives}
#SBATCH --gres=${slurm_gres}"
        # Append user-provided additional directives
        if [ -n "${slurm_directives}" ]; then
            local extra
            extra=$(echo "${slurm_directives}" | grep -v '^$' | grep -v '^#*$' || true)
            [ -n "${extra}" ] && sbatch_directives="${sbatch_directives}
${extra}"
        fi

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

# Activate venv (setup.sh writes path to RAY_VENV_DIR)
VENV_DIR="\$(cat "\${WORK}/RAY_VENV_DIR" 2>/dev/null || echo "\${WORK}/.venv")"
if [ -f "\${VENV_DIR}/bin/python" ]; then
    source "\${VENV_DIR}/bin/activate"
fi

# Use short hostname for LOGIN_HOST — compute nodes often can't resolve FQDNs
LOGIN_HOST=\$(hostname -s)
NUM_NODES=${num_nodes}

# Kill stale proxy processes from prior cancelled runs
echo 'Cleaning up stale proxies from prior runs...'
for pf in "\${WORK}"/.proxy_*.pid; do
    [ -f "\${pf}" ] && kill \$(cat "\${pf}" 2>/dev/null) 2>/dev/null && rm -f "\${pf}" || true
done
pkill -f "proxy.*\${WORK}" 2>/dev/null || true
sleep 1

# Allocate proxy ports early (worker tasks need PROXY_RAY_PORT)
PROXY_RAY_PORT=\$(python3 -c "import socket; s=socket.socket(); s.bind(('',0)); print(s.getsockname()[1]); s.close()")
PROXY_DASH_PORT=\$(python3 -c "import socket; s=socket.socket(); s.bind(('',0)); print(s.getsockname()[1]); s.close()")

# Write per-node configurations
mkdir -p "\${WORK}/nodeinfo"
rm -f "\${WORK}/nodeinfo/"*
${node_configs}

_exit_code=0
cleanup() {
    _exit_code=\$?
    kill \$(cat "\${WORK}/.proxy_ray_pid" 2>/dev/null) 2>/dev/null || true
    kill \$(cat "\${WORK}/.proxy_dash_pid" 2>/dev/null) 2>/dev/null || true
    for pf in "\${WORK}/.proxy_fwd_"*.pid; do
        [ -f "\${pf}" ] && kill \$(cat "\${pf}" 2>/dev/null) 2>/dev/null || true
    done
    # Cancel the SLURM job
    if [ -f "\${WORK}/slurm_jobid" ]; then
        local jid=\$(cat "\${WORK}/slurm_jobid" 2>/dev/null)
        if [ -n "\${jid}" ]; then
            echo "Cancelling SLURM job \${jid}..."
            scancel "\${jid}" 2>/dev/null || true
        fi
    fi
    exit \${_exit_code}
}
trap cleanup EXIT

# Report errors to dashboard if tunnel proxy is available
report_error() {
    local msg="\$1"
    echo "[ERROR] \${msg}"
    if [ -n "\${PROXY_DASH_PORT:-}" ]; then
        curl -s --connect-timeout 3 -X POST "http://\${LOGIN_HOST:-localhost}:\${PROXY_DASH_PORT}/api/worker/error" \
            -H "Content-Type: application/json" \
            -d "{\"site_id\": \"${site_id}\", \"error\": \$(echo "\${msg}" | python3 -c 'import sys,json; print(json.dumps(sys.stdin.read().strip()))' 2>/dev/null || echo '\"Worker dispatch failed\"')}" \
            2>/dev/null || true
    fi
}

# Write per-node task script (runs on each compute node via srun inside sbatch)
cat > "\${WORK}/srun_task.sh" <<'SRUN_TASK_EOF'
#!/bin/bash
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
RAY_VENV="\$(cat "\${WORK_DIR}/RAY_VENV_DIR" 2>/dev/null || echo "\${WORK_DIR}/.venv")"
if [ -f "\${RAY_VENV}/bin/activate" ]; then
    source "\${RAY_VENV}/bin/activate"
fi

# Wait for Ray GCS (up to 5 minutes — head may still be installing)
echo "Waiting for Ray head at \${LOGIN_HOST}:\${PROXY_RAY_PORT}... (\$(date))"
RAY_REACHABLE=false
attempt=0
while [ \${attempt} -le 150 ]; do
    if python3 -c "
import socket, sys
s = socket.socket()
s.settimeout(3)
try:
    s.connect(('\${LOGIN_HOST}', \${PROXY_RAY_PORT}))
    s.close()
    sys.exit(0)
except Exception:
    sys.exit(1)
" 2>/dev/null; then
        echo "Ray head reachable! (\$(date))"
        RAY_REACHABLE=true
        break
    fi
    [ \$((attempt % 15)) -eq 0 ] && [ \${attempt} -gt 0 ] && echo "  Still waiting for Ray head... (\$((attempt * 2))s elapsed)"
    sleep 2
    attempt=\$((attempt + 1))
done

if [ "\${RAY_REACHABLE}" != "true" ]; then
    echo "[ERROR] Cannot reach Ray head at \${LOGIN_HOST}:\${PROXY_RAY_PORT} after 300s"
    echo "  Check that the pw ssh tunnel to the head node is working."
    exit 1
fi

ray stop --force 2>/dev/null || true
rm -rf /tmp/ray/session_* 2>/dev/null || true
sleep 1

# Detect GPUs — respect SLURM/scheduler allocation
NUM_GPUS=0
if [ -n "\${CUDA_VISIBLE_DEVICES:-}" ]; then
    NUM_GPUS=\$(echo "\${CUDA_VISIBLE_DEVICES}" | tr ',' '\n' | grep -c .)
    echo "Detected \${NUM_GPUS} GPU(s) (from CUDA_VISIBLE_DEVICES=\${CUDA_VISIBLE_DEVICES})"
elif [ -z "\${SLURM_JOB_ID:-}" ] && command -v nvidia-smi &>/dev/null; then
    GPU_LIST=\$(nvidia-smi -L 2>/dev/null) && NUM_GPUS=\$(echo "\${GPU_LIST}" | grep -c "^GPU ")
    [ "\${NUM_GPUS}" -gt 0 ] && echo "Detected \${NUM_GPUS} GPU(s)"
fi

echo "Starting Ray worker: address=\${LOGIN_HOST}:\${PROXY_RAY_PORT} ip=\${MY_TUNNEL_IP}"
RAY_ARGS="--address=\${LOGIN_HOST}:\${PROXY_RAY_PORT}"
RAY_ARGS="\${RAY_ARGS} --node-ip-address=\${MY_TUNNEL_IP}"
RAY_ARGS="\${RAY_ARGS} --node-manager-port=\${MY_RAYLET_PORT}"
RAY_ARGS="\${RAY_ARGS} --object-manager-port=\${MY_OBJ_PORT}"
RAY_ARGS="\${RAY_ARGS} --min-worker-port=\${MY_MIN_PORT}"
RAY_ARGS="\${RAY_ARGS} --max-worker-port=\${MY_MAX_PORT}"
RAY_ARGS="\${RAY_ARGS} --num-cpus=1"
if [ "\${NUM_GPUS}" -gt 0 ]; then
    RAY_ARGS="\${RAY_ARGS} --num-gpus=\${NUM_GPUS}"
fi
ray start \${RAY_ARGS}

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
        uri=\$(echo "\${line}" | awk '{print \$1}')
        ctype=\$(echo "\${line}" | awk '{print \$3}')
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
[ -z "\${CLUSTER_NAME}" ] && CLUSTER_NAME="\${EXPECTED_CLUSTER_NAME:-\${MY_SITE_ID}}"

DASHBOARD_URL="http://\${LOGIN_HOST}:\${PROXY_DASH_PORT}"
curl -s -X POST "\${DASHBOARD_URL}/api/worker" \
    -H "Content-Type: application/json" \
    -d "{
        \"site_id\": \"\${EXPECTED_SITE_ID:-\${MY_SITE_ID}}\",
        \"worker_ip\": \"\${MY_TUNNEL_IP}\",
        \"num_cpus\": 1,
        \"num_gpus\": \${NUM_GPUS},
        \"cluster_name\": \"\${CLUSTER_NAME}\",
        \"scheduler_type\": \"\${SCHED_TYPE}\"
    }" 2>/dev/null || echo "Note: Could not notify dashboard"

echo "Worker \${PROC_ID} RUNNING (tunnel IP: \${MY_TUNNEL_IP})"

# Keep alive
while true; do
    ray status 2>/dev/null || echo "Worker \${PROC_ID} health: \$(date)"
    sleep 30
done
SRUN_TASK_EOF
chmod +x "\${WORK}/srun_task.sh"

# Save state for sbatch wrapper to pick up
echo "\${LOGIN_HOST}" > "\${WORK}/login_host"
echo "\${PROXY_RAY_PORT}" > "\${WORK}/proxy_ray_port"
echo "\${PROXY_DASH_PORT}" > "\${WORK}/proxy_dash_port"

# Write sbatch wrapper script that launches srun internally
cat > "\${WORK}/sbatch_wrapper.sh" <<SBATCH_WRAP_EOF
#!/bin/bash
#SBATCH --nodes=${num_nodes}
#SBATCH --ntasks=${num_nodes}
#SBATCH --job-name=ray-worker-${site_id}
#SBATCH --export=ALL
${sbatch_directives}

WORK="\${WORK}"
export WORK_DIR="\${WORK}"
export LOGIN_HOST=\\\$(cat "\${WORK}/login_host")
export PROXY_RAY_PORT=\\\$(cat "\${WORK}/proxy_ray_port")
export PROXY_DASH_PORT=\\\$(cat "\${WORK}/proxy_dash_port")
export EXPECTED_CLUSTER_NAME="${site_name}"
export EXPECTED_SITE_ID="${site_id}"

srun --ntasks=${num_nodes} bash "\${WORK}/srun_task.sh"
SBATCH_WRAP_EOF
chmod +x "\${WORK}/sbatch_wrapper.sh"

# Submit via sbatch and capture job ID
SBATCH_OUTPUT=\$(sbatch --output="\${WORK}/slurm_worker.out" --error="\${WORK}/slurm_worker.out" "\${WORK}/sbatch_wrapper.sh" 2>&1) || true
echo "sbatch: \${SBATCH_OUTPUT}"
SLURM_JOBID=\$(echo "\${SBATCH_OUTPUT}" | grep -oP 'Submitted batch job \K[0-9]+')
if [ -n "\${SLURM_JOBID}" ]; then
    echo "\${SLURM_JOBID}" > "\${WORK}/slurm_jobid"
    echo "SLURM job ID: \${SLURM_JOBID} (saved to \${WORK}/slurm_jobid)"
else
    report_error "sbatch failed: \${SBATCH_OUTPUT}"
    exit 1
fi

# Check if job was immediately rejected by scheduler
sleep 2
JOB_STATE=\$(squeue -j \${SLURM_JOBID} --noheader --format="%T" 2>/dev/null || echo "")
if [ -z "\${JOB_STATE}" ]; then
    # Job not in queue — may have been immediately rejected
    SACCT_STATE=\$(sacct -j \${SLURM_JOBID} --noheader --format=State --parsable2 2>/dev/null | head -1 || echo "")
    if [ -n "\${SACCT_STATE}" ] && [ "\${SACCT_STATE}" != "PENDING" ] && [ "\${SACCT_STATE}" != "RUNNING" ]; then
        report_error "SLURM job \${SLURM_JOBID} immediately failed: \${SACCT_STATE}"
        exit 1
    fi
fi

# Stream sbatch output log in the background
(
    for i in \$(seq 1 120); do
        [ -f "\${WORK}/slurm_worker.out" ] && break
        sleep 2
    done
    if [ -f "\${WORK}/slurm_worker.out" ]; then
        tail -f "\${WORK}/slurm_worker.out" 2>/dev/null &
    fi
) &

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
    if [ \$((attempt % 15)) -eq 0 ]; then
        echo "  Still waiting for compute nodes... (\$((attempt * 2))s elapsed, got \${count}/\${NUM_NODES})"
        squeue -u \$(whoami) --format="  %i %j %T %M %N" 2>/dev/null | head -5 || true
    fi
    if [ \${attempt} -gt 300 ]; then
        echo "[ERROR] Timeout waiting for compute nodes after 10min (got \${count}/\${NUM_NODES})"
        exit 1
    fi
done

echo "All \${NUM_NODES} node(s) reported."

# Reverse tunnel is established via SSH -R on the main connection (workspace side).
# Verify it works by connecting to localhost:tunnel_ray_port on this host.
echo "Verifying reverse tunnel to head node (localhost:${tunnel_ray_port} -> head:${RAY_PORT})..."
echo "Port status before verification:"
ss -tlnp 2>/dev/null | grep ":${tunnel_ray_port}" || echo "  Port ${tunnel_ray_port} not in ss output"
TUNNEL_OK=false
for t_attempt in \$(seq 1 30); do
    RESULT=\$(python3 -c "
import socket, sys
s = socket.socket()
s.settimeout(5)
try:
    s.connect(('127.0.0.1', ${tunnel_ray_port}))
    # Try to read a byte (Ray GCS should respond to connection)
    s.settimeout(2)
    try:
        d = s.recv(1)
        print('connected+recv=%d' % len(d))
    except socket.timeout:
        print('connected+timeout_on_recv')
    s.close()
    sys.exit(0)
except Exception as e:
    print('error: %s' % e)
    sys.exit(1)
" 2>&1)
    EXIT=\$?
    if [ \${EXIT} -eq 0 ]; then
        echo "Reverse tunnel verified (\${RESULT}) — Ray GCS reachable via 127.0.0.1:${tunnel_ray_port}"
        TUNNEL_OK=true
        break
    fi
    [ \$((t_attempt % 5)) -eq 0 ] && echo "  Attempt \${t_attempt}/30: \${RESULT}"
    sleep 2
done

if [ "\${TUNNEL_OK}" != "true" ]; then
    report_error "Cannot reach head node through reverse tunnel on port ${tunnel_ray_port}. Last result: \${RESULT}"
    exit 1
fi

# Start login node proxies (compute nodes connect here, forwarded through pw ssh tunnel)
python3 -c '${PROXY_PY_CODE}' \${PROXY_RAY_PORT} "127.0.0.1:${tunnel_ray_port}" "\${WORK}/.proxy_ray_pid" &
sleep 0.5
python3 -c '${PROXY_PY_CODE}' \${PROXY_DASH_PORT} "127.0.0.1:${tunnel_dashboard_port}" "\${WORK}/.proxy_dash_pid" &
sleep 0.5

echo "Login node proxies:"
echo "  Ray GCS: \${LOGIN_HOST}:\${PROXY_RAY_PORT} -> 127.0.0.1:${tunnel_ray_port}"
echo "  Dashboard: \${LOGIN_HOST}:\${PROXY_DASH_PORT} -> 127.0.0.1:${tunnel_dashboard_port}"

echo "Starting forward tunnel proxies..."

# Kill stale forward proxies from prior runs
for pf in "\${WORK}"/.proxy_fwd_*.pid; do
    [ -f "\${pf}" ] && kill \$(cat "\${pf}" 2>/dev/null) 2>/dev/null && rm -f "\${pf}" || true
done
sleep 0.5

# Start forward tunnel proxies: login_node:port -> compute_node:port
for j in \$(seq 0 \$((\${NUM_NODES} - 1))); do
    NODE_HOST=\$(cat "\${WORK}/nodeinfo/host_\${j}")
    source "\${WORK}/nodeinfo/config_\${j}.sh"
    echo "  Node \${j} (\${NODE_HOST}): proxying ports \${MY_MIN_PORT}-\${MY_MAX_PORT}, \${MY_RAYLET_PORT}, \${MY_OBJ_PORT}"

    # Proxy raylet port
    python3 -c '${PROXY_PY_CODE}' \${MY_RAYLET_PORT} "\${NODE_HOST}:\${MY_RAYLET_PORT}" "\${WORK}/.proxy_fwd_\${j}_raylet.pid" &
    # Proxy object manager port
    python3 -c '${PROXY_PY_CODE}' \${MY_OBJ_PORT} "\${NODE_HOST}:\${MY_OBJ_PORT}" "\${WORK}/.proxy_fwd_\${j}_obj.pid" &
    # Proxy worker ports
    for port in \$(seq \${MY_MIN_PORT} \${MY_MAX_PORT}); do
        python3 -c '${PROXY_PY_CODE}' \${port} "\${NODE_HOST}:\${port}" "\${WORK}/.proxy_fwd_\${j}_w\${port}.pid" &
    done
done
sleep 1

# Signal nodes that proxies are ready
touch "\${WORK}/nodeinfo/proxies_ready"
echo "Forward proxies ready!"

# Wait for SLURM job to finish (poll squeue)
echo "Waiting for SLURM job \${SLURM_JOBID} to complete..."
while true; do
    JOB_STATE=\$(squeue -j "\${SLURM_JOBID}" -h -o "%T" 2>/dev/null || echo "")
    if [ -z "\${JOB_STATE}" ]; then
        echo "SLURM job \${SLURM_JOBID} completed."
        break
    fi
    sleep 10
done
WORKER_SCRIPT

    elif [ "${dispatch_mode}" = "pbs" ]; then
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

        # Build PBS directives
        echo "  PBS config: queue='${pbs_queue}' account='${pbs_account}' nodes='${num_nodes}' select='${pbs_select}' walltime='${pbs_walltime}'"
        local pbs_q_directive=""
        [ -n "${pbs_queue}" ] && pbs_q_directive="#PBS -q ${pbs_queue}"
        local pbs_a_directive=""
        [ -n "${pbs_account}" ] && pbs_a_directive="#PBS -A ${pbs_account}"
        local pbs_extra_directives=""
        [ -n "${pbs_directives}" ] && pbs_extra_directives="${pbs_directives}"

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

# Activate venv (setup.sh writes path to RAY_VENV_DIR)
VENV_DIR="\$(cat "\${WORK}/RAY_VENV_DIR" 2>/dev/null || echo "\${WORK}/.venv")"
if [ -f "\${VENV_DIR}/bin/python" ]; then
    source "\${VENV_DIR}/bin/activate"
fi

# Use short hostname for LOGIN_HOST — compute nodes often can't resolve FQDNs
LOGIN_HOST=\$(hostname -s)
NUM_NODES=${num_nodes}

# Kill stale proxy processes from prior cancelled runs
echo 'Cleaning up stale proxies from prior runs...'
for pf in "\${WORK}"/.proxy_*.pid; do
    [ -f "\${pf}" ] && kill \$(cat "\${pf}" 2>/dev/null) 2>/dev/null && rm -f "\${pf}" || true
done
pkill -f "proxy.*\${WORK}" 2>/dev/null || true
sleep 1

# Reverse tunnel is established via SSH -R on the main connection (workspace side).
# Verify it works by connecting to localhost:tunnel_ray_port on this host.
echo "Verifying reverse tunnel to head node (localhost:${tunnel_ray_port} -> head:${RAY_PORT})..."
echo "Port status before verification:"
ss -tlnp 2>/dev/null | grep ":${tunnel_ray_port}" || echo "  Port ${tunnel_ray_port} not in ss output"
TUNNEL_OK=false
for t_attempt in \$(seq 1 30); do
    RESULT=\$(python3 -c "
import socket, sys
s = socket.socket()
s.settimeout(5)
try:
    s.connect(('127.0.0.1', ${tunnel_ray_port}))
    # Try to read a byte (Ray GCS should respond to connection)
    s.settimeout(2)
    try:
        d = s.recv(1)
        print('connected+recv=%d' % len(d))
    except socket.timeout:
        print('connected+timeout_on_recv')
    s.close()
    sys.exit(0)
except Exception as e:
    print('error: %s' % e)
    sys.exit(1)
" 2>&1)
    EXIT=\$?
    if [ \${EXIT} -eq 0 ]; then
        echo "Reverse tunnel verified (\${RESULT}) — Ray GCS reachable via 127.0.0.1:${tunnel_ray_port}"
        TUNNEL_OK=true
        break
    fi
    [ \$((t_attempt % 5)) -eq 0 ] && echo "  Attempt \${t_attempt}/30: \${RESULT}"
    sleep 2
done

if [ "\${TUNNEL_OK}" != "true" ]; then
    report_error "Cannot reach head node through reverse tunnel on port ${tunnel_ray_port}. Last result: \${RESULT}"
    exit 1
fi

# Start TCP proxies (compute nodes connect here, forwarded through reverse tunnel)
PROXY_RAY_PORT=\$(python3 -c "import socket; s=socket.socket(); s.bind(('',0)); print(s.getsockname()[1]); s.close()")
PROXY_DASH_PORT=\$(python3 -c "import socket; s=socket.socket(); s.bind(('',0)); print(s.getsockname()[1]); s.close()")

python3 -c '${PROXY_PY_CODE}' \${PROXY_RAY_PORT} "127.0.0.1:${tunnel_ray_port}" "\${WORK}/.proxy_ray_pid" &
sleep 0.5
python3 -c '${PROXY_PY_CODE}' \${PROXY_DASH_PORT} "127.0.0.1:${tunnel_dashboard_port}" "\${WORK}/.proxy_dash_pid" &
sleep 0.5

echo "Login node proxies:"
echo "  Ray GCS: \${LOGIN_HOST}:\${PROXY_RAY_PORT} -> 127.0.0.1:${tunnel_ray_port}"
echo "  Dashboard: \${LOGIN_HOST}:\${PROXY_DASH_PORT} -> 127.0.0.1:${tunnel_dashboard_port}"

# Write per-node configurations
mkdir -p "\${WORK}/nodeinfo"
rm -f "\${WORK}/nodeinfo/"*
${node_configs}

_exit_code=0
cleanup() {
    _exit_code=\$?
    kill \$(cat "\${WORK}/.proxy_ray_pid" 2>/dev/null) 2>/dev/null || true
    kill \$(cat "\${WORK}/.proxy_dash_pid" 2>/dev/null) 2>/dev/null || true
    for pf in "\${WORK}/.proxy_fwd_"*.pid; do
        [ -f "\${pf}" ] && kill \$(cat "\${pf}" 2>/dev/null) 2>/dev/null || true
    done
    if [ -n "\${PBS_JOBID:-}" ]; then
        echo "Cancelling PBS job \${PBS_JOBID}..."
        qdel "\${PBS_JOBID}" 2>/dev/null || true
    fi
    exit \${_exit_code}
}
trap cleanup EXIT

# Report errors to dashboard if tunnel proxy is available
report_error() {
    local msg="\$1"
    echo "[ERROR] \${msg}"
    if [ -n "\${PROXY_DASH_PORT:-}" ]; then
        curl -s --connect-timeout 3 -X POST "http://\${LOGIN_HOST:-localhost}:\${PROXY_DASH_PORT}/api/worker/error" \
            -H "Content-Type: application/json" \
            -d "{\"site_id\": \"${site_id}\", \"error\": \$(echo "\${msg}" | python3 -c 'import sys,json; print(json.dumps(sys.stdin.read().strip()))' 2>/dev/null || echo '\"Worker dispatch failed\"')}" \
            2>/dev/null || true
    fi
}

# Write PBS task script (runs on each allocated node via pbsdsh)
cat > "\${WORK}/pbs_task.sh" <<'PBS_TASK_EOF'
#!/bin/bash
set -e

# Source environment (pbsdsh does not inherit the PBS job's exports)
SCRIPT_DIR="\$(cd "\$(dirname "\$0")" && pwd)"
if [ -f "\${SCRIPT_DIR}/pbs_env.sh" ]; then
    source "\${SCRIPT_DIR}/pbs_env.sh"
fi

# Determine node index from argument or PBS_NODENUM
if [ -n "\${1}" ]; then
    PROC_ID=\${1}
elif [ -n "\${PBS_NODENUM}" ]; then
    PROC_ID=\${PBS_NODENUM}
else
    PROC_ID=0
fi
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
RAY_VENV="\$(cat "\${WORK_DIR}/RAY_VENV_DIR" 2>/dev/null || echo "\${WORK_DIR}/.venv")"
if [ -f "\${RAY_VENV}/bin/activate" ]; then
    source "\${RAY_VENV}/bin/activate"
fi

# Wait for Ray GCS (up to 5 minutes — head may still be installing)
echo "Waiting for Ray head at \${LOGIN_HOST}:\${PROXY_RAY_PORT}... (\$(date))"
RAY_REACHABLE=false
attempt=0
while [ \${attempt} -le 150 ]; do
    if python3 -c "
import socket, sys
s = socket.socket()
s.settimeout(3)
try:
    s.connect(('\${LOGIN_HOST}', \${PROXY_RAY_PORT}))
    s.close()
    sys.exit(0)
except Exception:
    sys.exit(1)
" 2>/dev/null; then
        echo "Ray head reachable! (\$(date))"
        RAY_REACHABLE=true
        break
    fi
    [ \$((attempt % 15)) -eq 0 ] && [ \${attempt} -gt 0 ] && echo "  Still waiting for Ray head... (\$((attempt * 2))s elapsed)"
    sleep 2
    attempt=\$((attempt + 1))
done

if [ "\${RAY_REACHABLE}" != "true" ]; then
    echo "[ERROR] Cannot reach Ray head at \${LOGIN_HOST}:\${PROXY_RAY_PORT} after 300s"
    echo "  Check that the pw ssh tunnel to the head node is working."
    exit 1
fi

ray stop --force 2>/dev/null || true
rm -rf /tmp/ray/session_* 2>/dev/null || true
sleep 1

echo "Starting Ray worker: address=\${LOGIN_HOST}:\${PROXY_RAY_PORT} ip=\${MY_TUNNEL_IP}"
RAY_ARGS="--address=\${LOGIN_HOST}:\${PROXY_RAY_PORT}"
RAY_ARGS="\${RAY_ARGS} --node-ip-address=\${MY_TUNNEL_IP}"
RAY_ARGS="\${RAY_ARGS} --node-manager-port=\${MY_RAYLET_PORT}"
RAY_ARGS="\${RAY_ARGS} --object-manager-port=\${MY_OBJ_PORT}"
RAY_ARGS="\${RAY_ARGS} --min-worker-port=\${MY_MIN_PORT}"
RAY_ARGS="\${RAY_ARGS} --max-worker-port=\${MY_MAX_PORT}"
RAY_ARGS="\${RAY_ARGS} --num-cpus=1"
ray start \${RAY_ARGS}

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
        uri=\$(echo "\${line}" | awk '{print \$1}')
        ctype=\$(echo "\${line}" | awk '{print \$3}')
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
    if [ -n "\${PBS_JOBID}" ]; then SCHED_TYPE="pbs"
    elif [ -n "\${SLURM_JOB_ID}" ]; then SCHED_TYPE="slurm"
    else SCHED_TYPE="ssh"
    fi
fi
[ -z "\${CLUSTER_NAME}" ] && CLUSTER_NAME="\${EXPECTED_CLUSTER_NAME:-\${MY_SITE_ID}}"

DASHBOARD_URL="http://\${LOGIN_HOST}:\${PROXY_DASH_PORT}"
curl -s -X POST "\${DASHBOARD_URL}/api/worker" \
    -H "Content-Type: application/json" \
    -d "{
        \"site_id\": \"\${EXPECTED_SITE_ID:-\${MY_SITE_ID}}\",
        \"worker_ip\": \"\${MY_TUNNEL_IP}\",
        \"num_cpus\": 1,
        \"cluster_name\": \"\${CLUSTER_NAME}\",
        \"scheduler_type\": \"\${SCHED_TYPE}\"
    }" 2>/dev/null || echo "Note: Could not notify dashboard"

echo "Worker \${PROC_ID} RUNNING (tunnel IP: \${MY_TUNNEL_IP})"

# Keep alive
while true; do
    ray status 2>/dev/null || echo "Worker \${PROC_ID} health: \$(date)"
    sleep 30
done
PBS_TASK_EOF
chmod +x "\${WORK}/pbs_task.sh"

# Write PBS job script — split into expanded header + quoted body
# Header: variables expanded on login node (WORK, LOGIN_HOST, ports)
cat > "\${WORK}/pbs_job.pbs" <<PBS_HEADER
#!/bin/bash
#PBS -l select=${pbs_select:-${num_nodes}:ncpus=1}
#PBS -l walltime=${pbs_walltime:-01:00:00}
#PBS -j oe
#PBS -V
${pbs_q_directive}
${pbs_a_directive}
${pbs_extra_directives}

export WORK_DIR=\${WORK}
export LOGIN_HOST=\${LOGIN_HOST}
export PROXY_RAY_PORT=\${PROXY_RAY_PORT}
export PROXY_DASH_PORT=\${PROXY_DASH_PORT}
export EXPECTED_CLUSTER_NAME="${site_name}"
export EXPECTED_SITE_ID="${site_id}"
export OMP_NUM_THREADS=1
export MKL_NUM_THREADS=1
export OPENBLAS_NUM_THREADS=1
PBS_HEADER
# Body: quoted heredoc so PBS_NODEFILE, NODES, etc. are NOT expanded
# on the login node — they must remain as variables for the PBS job.
# Note: \$ escaping survives the outer WORKER_SCRIPT heredoc → becomes $
# in the generated script, then <<'PBS_BODY' prevents further expansion.
cat >> "\${WORK}/pbs_job.pbs" <<'PBS_BODY'

# Write env file for pbsdsh tasks (pbsdsh does not inherit exports)
cat > "\${WORK_DIR}/pbs_env.sh" <<ENV_EOF
export WORK_DIR="\${WORK_DIR}"
export LOGIN_HOST="\${LOGIN_HOST}"
export PROXY_RAY_PORT="\${PROXY_RAY_PORT}"
export PROXY_DASH_PORT="\${PROXY_DASH_PORT}"
export EXPECTED_CLUSTER_NAME="\${EXPECTED_CLUSTER_NAME}"
export EXPECTED_SITE_ID="\${EXPECTED_SITE_ID}"
export OMP_NUM_THREADS=1
export MKL_NUM_THREADS=1
export OPENBLAS_NUM_THREADS=1
ENV_EOF

# Read allocated nodes
NODES=(\$(sort -u "\${PBS_NODEFILE}"))
NUM_ALLOCATED=\${#NODES[@]}
echo "PBS allocated \${NUM_ALLOCATED} node(s): \${NODES[*]}"

# Write hostnames for coordinator
for idx in \$(seq 0 \$((\${NUM_ALLOCATED} - 1))); do
    echo "\${NODES[\${idx}]}" > "\${WORK_DIR}/nodeinfo/host_\${idx}"
done

# Run worker on each allocated node via pbsdsh
for NODE_INDEX in \$(seq 0 \$((\${NUM_ALLOCATED} - 1))); do
    pbsdsh -n \${NODE_INDEX} -- bash "\${WORK_DIR}/pbs_task.sh" \${NODE_INDEX} &
done

# Wait for all
wait
PBS_BODY

echo "Submitting to PBS: qsub \${WORK}/pbs_job.pbs"
echo "--- pbs_job.pbs contents ---"
head -20 "\${WORK}/pbs_job.pbs"
echo "---"

QSUB_OUTPUT=\$(qsub "\${WORK}/pbs_job.pbs" 2>&1) || true
echo "PBS job submitted: \${QSUB_OUTPUT}"
PBS_JOBID=\$(echo "\${QSUB_OUTPUT}" | grep -oP '^\d+' || echo "")
if [ -z "\${PBS_JOBID}" ]; then
    report_error "qsub failed: \${QSUB_OUTPUT}"
    exit 1
fi
echo "\${PBS_JOBID}" > "\${WORK}/pbs_jobid"
echo "PBS job ID: \${PBS_JOBID} (saved to \${WORK}/pbs_jobid)"

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
    if [ \${attempt} -gt 300 ]; then
        report_error "Timeout waiting for compute nodes after 10min (got \${count}/\${NUM_NODES})"
        exit 1
    fi
done

echo "All \${NUM_NODES} node(s) reported. Starting forward tunnel proxies..."

# Kill stale forward proxies from prior runs
for pf in "\${WORK}"/.proxy_fwd_*.pid; do
    [ -f "\${pf}" ] && kill \$(cat "\${pf}" 2>/dev/null) 2>/dev/null && rm -f "\${pf}" || true
done
sleep 0.5

# Start forward tunnel proxies: login_node:port -> compute_node:port
for j in \$(seq 0 \$((\${NUM_NODES} - 1))); do
    NODE_HOST=\$(cat "\${WORK}/nodeinfo/host_\${j}")
    source "\${WORK}/nodeinfo/config_\${j}.sh"
    echo "  Node \${j} (\${NODE_HOST}): proxying ports \${MY_MIN_PORT}-\${MY_MAX_PORT}, \${MY_RAYLET_PORT}, \${MY_OBJ_PORT}"

    # Proxy raylet port
    python3 -c '${PROXY_PY_CODE}' \${MY_RAYLET_PORT} "\${NODE_HOST}:\${MY_RAYLET_PORT}" "\${WORK}/.proxy_fwd_\${j}_raylet.pid" &
    # Proxy object manager port
    python3 -c '${PROXY_PY_CODE}' \${MY_OBJ_PORT} "\${NODE_HOST}:\${MY_OBJ_PORT}" "\${WORK}/.proxy_fwd_\${j}_obj.pid" &
    # Proxy worker ports
    for port in \$(seq \${MY_MIN_PORT} \${MY_MAX_PORT}); do
        python3 -c '${PROXY_PY_CODE}' \${port} "\${NODE_HOST}:\${port}" "\${WORK}/.proxy_fwd_\${j}_w\${port}.pid" &
    done
done
sleep 1

# Signal nodes that proxies are ready
touch "\${WORK}/nodeinfo/proxies_ready"
echo "Forward proxies ready!"

# Wait for PBS job to finish
while qstat \${PBS_JOBID} 2>/dev/null | grep -q "\${PBS_JOBID}"; do
    sleep 10
done
echo "PBS job \${PBS_JOBID} completed."
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

_exit_code=0
cleanup() {
    _exit_code=\$?
    ray stop --force 2>/dev/null || true
    kill \${PROXY_PID:-} 2>/dev/null || true
    exit \${_exit_code}
}
trap cleanup EXIT

# Report errors to dashboard if tunnel is available
report_error() {
    local msg="\$1"
    echo "[ERROR] \${msg}"
    if [ -n "\${DASHBOARD_URL:-}" ]; then
        curl -s --connect-timeout 3 -X POST "\${DASHBOARD_URL}/api/worker/error" \
            -H "Content-Type: application/json" \
            -d "{\"site_id\": \"${site_id}\", \"error\": \$(echo "\${msg}" | python3 -c 'import sys,json; print(json.dumps(sys.stdin.read().strip()))' 2>/dev/null || echo '\"Worker dispatch failed\"')}" \
            2>/dev/null || true
    fi
}

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

# Activate venv (setup.sh writes path to RAY_VENV_DIR)
VENV_DIR="\$(cat "\${WORK}/RAY_VENV_DIR" 2>/dev/null || echo "\${WORK}/.venv")"
if [ -f "\${VENV_DIR}/bin/python" ]; then
    source "\${VENV_DIR}/bin/activate"
fi

# Dashboard URL for error reporting (set early so report_error works during setup)
DASHBOARD_URL="http://127.0.0.1:${tunnel_dashboard_port}"

# Pin BLAS threading
export OMP_NUM_THREADS=1
export MKL_NUM_THREADS=1
export OPENBLAS_NUM_THREADS=1

# Stop any existing Ray and clean up stale sessions
ray stop --force 2>/dev/null || true
rm -rf /tmp/ray/session_* 2>/dev/null || true

# Kill stale proxy processes from prior cancelled runs
echo 'Cleaning up stale proxies from prior runs...'
for pf in "\${WORK}"/.proxy_*.pid; do
    [ -f "\${pf}" ] && kill \$(cat "\${pf}" 2>/dev/null) 2>/dev/null && rm -f "\${pf}" || true
done
pkill -f "proxy.*\${WORK}" 2>/dev/null || true
sleep 1

# Reverse tunnel is established via SSH -R on the main connection (workspace side).
# Verify it works by connecting to localhost:tunnel_ray_port on this host.
echo "Verifying reverse tunnel to head node (localhost:${tunnel_ray_port} -> head:${RAY_PORT})..."
echo "Port status before verification:"
ss -tlnp 2>/dev/null | grep ":${tunnel_ray_port}" || echo "  Port ${tunnel_ray_port} not in ss output"
TUNNEL_OK=false
for t_attempt in \$(seq 1 30); do
    RESULT=\$(python3 -c "
import socket, sys
s = socket.socket()
s.settimeout(5)
try:
    s.connect(('127.0.0.1', ${tunnel_ray_port}))
    # Try to read a byte (Ray GCS should respond to connection)
    s.settimeout(2)
    try:
        d = s.recv(1)
        print('connected+recv=%d' % len(d))
    except socket.timeout:
        print('connected+timeout_on_recv')
    s.close()
    sys.exit(0)
except Exception as e:
    print('error: %s' % e)
    sys.exit(1)
" 2>&1)
    EXIT=\$?
    if [ \${EXIT} -eq 0 ]; then
        echo "Reverse tunnel verified (\${RESULT}) — Ray GCS reachable via 127.0.0.1:${tunnel_ray_port}"
        TUNNEL_OK=true
        break
    fi
    [ \$((t_attempt % 5)) -eq 0 ] && echo "  Attempt \${t_attempt}/30: \${RESULT}"
    sleep 2
done

if [ "\${TUNNEL_OK}" != "true" ]; then
    report_error "Cannot reach head node through reverse tunnel on port ${tunnel_ray_port}. Last result: \${RESULT}"
    exit 1
fi

# Start a proxy that listens on all interfaces (0.0.0.0) and forwards to the
# SSH tunnel at 127.0.0.1. Ray's GCS client resolves loopback to the machine's
# real IP on HPC systems, so we need the proxy to accept connections on any interface.
PROXY_RAY_PORT=\$(python3 -c "import socket; s=socket.socket(); s.bind(('',0)); print(s.getsockname()[1]); s.close()")
echo "Starting GCS proxy: 0.0.0.0:\${PROXY_RAY_PORT} -> 127.0.0.1:${tunnel_ray_port}"
python3 -c '${PROXY_PY_CODE}' \${PROXY_RAY_PORT} "127.0.0.1:${tunnel_ray_port}" &
PROXY_PID=\$!
sleep 0.5

echo "Connecting to Ray head via proxy at 127.0.0.1:\${PROXY_RAY_PORT}..."
echo "  Node IP advertised: ${ip} (via SSH forward tunnel)"
echo "  Raylet port: ${raylet}"
echo "  Object manager port: ${obj}"
echo "  Worker process ports: ${min_port}-${max_port}"
# Detect GPUs — respect SLURM/scheduler allocation
NUM_GPUS=0
if [ -n "\${CUDA_VISIBLE_DEVICES:-}" ]; then
    NUM_GPUS=\$(echo "\${CUDA_VISIBLE_DEVICES}" | tr ',' '\n' | grep -c .)
    echo "Detected \${NUM_GPUS} GPU(s) (from CUDA_VISIBLE_DEVICES=\${CUDA_VISIBLE_DEVICES})"
elif [ -z "\${SLURM_JOB_ID:-}" ] && command -v nvidia-smi &>/dev/null; then
    GPU_LIST=\$(nvidia-smi -L 2>/dev/null) && NUM_GPUS=\$(echo "\${GPU_LIST}" | grep -c "^GPU ")
    [ "\${NUM_GPUS}" -gt 0 ] && echo "Detected \${NUM_GPUS} GPU(s)"
fi

RAY_START_ARGS="--address=127.0.0.1:\${PROXY_RAY_PORT}"
RAY_START_ARGS="\${RAY_START_ARGS} --node-ip-address=${ip}"
RAY_START_ARGS="\${RAY_START_ARGS} --node-manager-port=${raylet}"
RAY_START_ARGS="\${RAY_START_ARGS} --object-manager-port=${obj}"
RAY_START_ARGS="\${RAY_START_ARGS} --min-worker-port=${min_port}"
RAY_START_ARGS="\${RAY_START_ARGS} --max-worker-port=${max_port}"
# Register 1 task slot per node. Tasks use internal parallelism (OpenMP, MPI, etc.)
RAY_START_ARGS="\${RAY_START_ARGS} --num-cpus=1"
if [ "\${NUM_GPUS}" -gt 0 ]; then
    RAY_START_ARGS="\${RAY_START_ARGS} --num-gpus=\${NUM_GPUS}"
fi
ray start \${RAY_START_ARGS}

echo "Ray worker started!"
ray status 2>/dev/null || echo "Note: ray status may not work on worker node"

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

curl -s -X POST "\${DASHBOARD_URL}/api/worker" \\
    -H "Content-Type: application/json" \\
    -d "{
        \"site_id\": \"${site_id}\",
        \"worker_ip\": \"${ip}\",
        \"num_cpus\": 1,
        \"num_gpus\": \${NUM_GPUS},
        \"cluster_name\": \"\${CLUSTER_NAME}\",
        \"scheduler_type\": \"\${SCHED_TYPE}\"
    }" 2>/dev/null || echo "Note: Could not notify dashboard"

echo "=========================================="
echo "Ray Worker RUNNING"
echo "  Connected to: 127.0.0.1:\${PROXY_RAY_PORT} (proxy -> 127.0.0.1:${tunnel_ray_port})"
echo "  Worker IP: ${ip}"
echo "=========================================="

# Keep SSH session alive
while true; do
    ray status 2>/dev/null || echo "Worker health check: \$(date)"
    sleep 30
done
WORKER_SCRIPT
    fi

    # Pipe script via stdin to avoid quoting issues with SSH command strings
    echo "[${site_name}] Connecting via SSH (PW proxy)..."
    set -o pipefail
    ssh "${SSH_ARGS[@]}" "${PW_USER}@${site_name}" 'bash -s' < "${script_file}" 2>&1 | \
        sed -u "s/^/[${site_name}] /"
    local ssh_exit=$?
    set +o pipefail
    echo "[${site_name}] SSH session ended (exit code: ${ssh_exit})"
    if [ ${ssh_exit} -ne 0 ]; then
        # Report error to dashboard from workspace side (direct access)
        curl -s --connect-timeout 3 -X POST "http://localhost:${DASHBOARD_PORT}/api/worker/error" \
            -H "Content-Type: application/json" \
            -d "{\"site_id\": \"${site_id}\", \"error\": \"Remote worker failed (SSH exit code ${ssh_exit})\"}" \
            2>/dev/null || true
    fi
}

# Launch all worker sites in parallel
PIDS=()
SITE_LABELS=()
SITE_IDS=()
remote_site_index=2  # Remote sites start at site-2 (site-1 = head + local workers)

for i in $(seq 0 $((NUM_WORKERS - 1))); do
    site_name=$(echo "${SITES_JSON}" | ${PYTHON_CMD} -c "import sys,json;print(json.load(sys.stdin)[${i}]['name'])")
    site_ip=$(echo "${SITES_JSON}" | ${PYTHON_CMD} -c "import sys,json;print(json.load(sys.stdin)[${i}]['ip'])")
    is_local=$(echo "${SITES_JSON}" | ${PYTHON_CMD} -c "import sys,json;print(str(json.load(sys.stdin)[${i}].get('is_local',False)).lower())")
    use_scheduler=$(echo "${SITES_JSON}" | ${PYTHON_CMD} -c "import sys,json;print(str(json.load(sys.stdin)[${i}].get('use_scheduler',False)).lower())")
    scheduler_type=$(echo "${SITES_JSON}" | ${PYTHON_CMD} -c "import sys,json;print(json.load(sys.stdin)[${i}].get('scheduler_type',''))")
    slurm_partition=$(echo "${SITES_JSON}" | ${PYTHON_CMD} -c "import sys,json;print(json.load(sys.stdin)[${i}].get('slurm_partition',''))")
    slurm_account=$(echo "${SITES_JSON}" | ${PYTHON_CMD} -c "import sys,json;print(json.load(sys.stdin)[${i}].get('slurm_account',''))")
    slurm_qos=$(echo "${SITES_JSON}" | ${PYTHON_CMD} -c "import sys,json;print(json.load(sys.stdin)[${i}].get('slurm_qos',''))")
    slurm_time=$(echo "${SITES_JSON}" | ${PYTHON_CMD} -c "import sys,json;print(json.load(sys.stdin)[${i}].get('slurm_time','00:05:00'))")
    slurm_nodes=$(echo "${SITES_JSON}" | ${PYTHON_CMD} -c "import sys,json;print(json.load(sys.stdin)[${i}].get('slurm_nodes','1'))")
    slurm_gres=$(echo "${SITES_JSON}" | ${PYTHON_CMD} -c "import sys,json;print(json.load(sys.stdin)[${i}].get('slurm_gres',''))")
    slurm_directives=$(echo "${SITES_JSON}" | ${PYTHON_CMD} -c "import sys,json;print(json.load(sys.stdin)[${i}].get('slurm_directives',''))")
    pbs_queue=$(echo "${SITES_JSON}" | ${PYTHON_CMD} -c "import sys,json;print(json.load(sys.stdin)[${i}].get('pbs_queue',''))")
    pbs_account=$(echo "${SITES_JSON}" | ${PYTHON_CMD} -c "import sys,json;print(json.load(sys.stdin)[${i}].get('pbs_account',''))")
    pbs_nodes=$(echo "${SITES_JSON}" | ${PYTHON_CMD} -c "import sys,json;print(json.load(sys.stdin)[${i}].get('pbs_nodes','1'))")
    pbs_select=$(echo "${SITES_JSON}" | ${PYTHON_CMD} -c "import sys,json;print(json.load(sys.stdin)[${i}].get('pbs_select',''))")
    pbs_walltime=$(echo "${SITES_JSON}" | ${PYTHON_CMD} -c "import sys,json;print(json.load(sys.stdin)[${i}].get('pbs_walltime','01:00:00'))")
    pbs_directives=$(echo "${SITES_JSON}" | ${PYTHON_CMD} -c "import sys,json;print(json.load(sys.stdin)[${i}].get('pbs_directives',''))")

    # Determine node count based on scheduler type
    local_num_nodes="${slurm_nodes}"
    if [ "${scheduler_type}" = "pbs" ]; then
        local_num_nodes="${pbs_nodes}"
    fi

    if [ "${is_local}" = "true" ]; then
        # Same resource as head — dispatch via local scheduler (no tunnels)
        echo ""
        echo "[site-1] Local worker site: ${site_name} (${local_num_nodes} node(s))"
        # Notify dashboard this site is pending
        pending_resp=$(curl -s --connect-timeout 3 -X POST "http://localhost:${DASHBOARD_PORT}/api/worker/pending" \
            -H "Content-Type: application/json" \
            -d "{\"site_id\": \"site-1\", \"cluster_name\": \"${site_name}\", \"scheduler_type\": \"${scheduler_type}\"}" 2>&1) \
            || echo "[site-1] Warning: pending notification failed: ${pending_resp}"
        echo "[site-1] Pending notification: ${pending_resp}"
        dispatch_local_workers "${i}" "${site_name}" "${scheduler_type}" \
            "${slurm_partition}" "${slurm_account}" "${slurm_qos}" "${slurm_time}" \
            "${slurm_nodes}" "${slurm_gres}" "${slurm_directives}" \
            "${pbs_queue}" "${pbs_account}" "${pbs_nodes}" "${pbs_select}" "${pbs_walltime}" \
            "${pbs_directives}"
        PIDS+=($!)
        SITE_LABELS+=("[site-1] ${site_name}")
        SITE_IDS+=("site-1")
    else
        # Different resource — dispatch via SSH tunnel
        echo ""
        echo "[site-${remote_site_index}] Remote worker site: ${site_name} (${site_ip})"
        # Notify dashboard this site is pending
        pending_resp=$(curl -s --connect-timeout 3 -X POST "http://localhost:${DASHBOARD_PORT}/api/worker/pending" \
            -H "Content-Type: application/json" \
            -d "{\"site_id\": \"site-${remote_site_index}\", \"cluster_name\": \"${site_name}\", \"scheduler_type\": \"${scheduler_type}\"}" 2>&1) \
            || echo "[site-${remote_site_index}] Warning: pending notification failed: ${pending_resp}"
        echo "[site-${remote_site_index}] Pending notification: ${pending_resp}"
        dispatch_worker "$((remote_site_index - 1))" "${site_name}" "${site_ip}" \
            "${use_scheduler}" "${scheduler_type}" \
            "${slurm_partition}" "${slurm_account}" "${slurm_qos}" "${slurm_time}" \
            "${slurm_nodes}" "${slurm_gres}" "${slurm_directives}" \
            "${pbs_queue}" "${pbs_account}" "${pbs_nodes}" "${pbs_select}" "${pbs_walltime}" \
            "${pbs_directives}" &
        PIDS+=($!)
        SITE_LABELS+=("[site-${remote_site_index}] ${site_name}")
        SITE_IDS+=("site-${remote_site_index}")
        remote_site_index=$((remote_site_index + 1))
    fi
done

echo ""
echo "All ${NUM_WORKERS} worker site(s) dispatched, waiting..."

# Wait for all background dispatch processes (SSH sessions, log streamers)
FAILED=0
for i in "${!PIDS[@]}"; do
    if wait "${PIDS[$i]}"; then
        echo "${SITE_LABELS[$i]}: COMPLETED"
    else
        exit_code=$?
        echo "${SITE_LABELS[$i]}: FAILED (exit ${exit_code})"
        FAILED=$((FAILED + 1))
        # Notify dashboard that this site failed
        curl -s --connect-timeout 3 -X POST "http://localhost:${DASHBOARD_PORT}/api/worker/error" \
            -H "Content-Type: application/json" \
            -d "{\"site_id\": \"${SITE_IDS[$i]}\", \"error\": \"Worker dispatch failed (exit ${exit_code}). Check workflow logs for details.\"}" \
            2>/dev/null || true
    fi
done

if [ "${FAILED}" -gt 0 ]; then
    echo ""
    echo "=========================================="
    echo "Worker dispatch FAILED"
    echo "  Worker sites: ${NUM_WORKERS}"
    echo "  Failed: ${FAILED}"
    echo "=========================================="
    exit 1
fi

echo ""
echo "=========================================="
echo "All workers dispatched successfully."
echo "  Worker sites: ${NUM_WORKERS}"
echo "=========================================="

# Stay alive while workers are connected to the Ray cluster.
# Exits if all workers disconnect (walltime expiry, failure, etc.)
# so cleanup fires and the workflow can end naturally.
VENV_DIR="$(cat "${JOB_DIR}/RAY_VENV_DIR" 2>/dev/null || echo "")"
PYTHON_CMD="${VENV_DIR}/bin/python"
[ ! -x "${PYTHON_CMD}" ] && PYTHON_CMD="python3"

echo "Monitoring cluster — will exit when all workers disconnect..."
CONSECUTIVE_ZERO=0
while true; do
    # Count worker CPUs via Ray dashboard API (avoids creating DRIVER jobs)
    WORKER_CPUS=$(curl -s --max-time 5 "http://${RAY_HEAD_IP}:8265/nodes?view=summary" 2>/dev/null | \
        ${PYTHON_CMD} -c "
import json, sys
try:
    data = json.load(sys.stdin)
    nodes = data.get('data', {}).get('summary', [])
    cpus = sum(n.get('raylet', {}).get('resourcesTotal', {}).get('CPU', 0)
               for n in nodes if n.get('raylet', {}).get('state', '') == 'ALIVE')
    print(int(cpus))
except:
    print(0)
" 2>/dev/null || echo "0")

    # Also check local SLURM jobs if any
    LOCAL_SLURM_RUNNING=false
    if [ -f "${JOB_DIR}/slurm_jobids" ]; then
        while IFS= read -r jid; do
            [ -z "${jid}" ] && continue
            JOB_STATE=$(squeue -j "${jid}" -h -o "%T" 2>/dev/null || echo "")
            if [ -n "${JOB_STATE}" ]; then
                LOCAL_SLURM_RUNNING=true
                break
            fi
        done < "${JOB_DIR}/slurm_jobids"
    fi

    if [ "${WORKER_CPUS}" = "0" ] && [ "${LOCAL_SLURM_RUNNING}" = "false" ]; then
        CONSECUTIVE_ZERO=$((CONSECUTIVE_ZERO + 1))
        if [ ${CONSECUTIVE_ZERO} -ge 4 ]; then
            echo "No workers connected for 2 minutes. Exiting."
            break
        fi
    else
        CONSECUTIVE_ZERO=0
    fi
    sleep 30
done
