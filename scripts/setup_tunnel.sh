#!/bin/bash
# setup_tunnel.sh — Create reverse SSH tunnel from on-prem to cloud
#
# Tunnels two ports to the cloud:
#   1. Dashboard port (for benchmark results POST)
#   2. Ray GCS port 6379 (for Ray worker connection)
#
# Environment variables:
#   DASHBOARD_PORT      - Port of the dashboard on this machine
#   RAY_HEAD_IP         - IP of the Ray head node
#   PW_USER             - ACTIVATE username for SSH

set -e

echo "=========================================="
echo "Setting up reverse tunnel: $(date)"
echo "=========================================="
echo "Dashboard port:  ${DASHBOARD_PORT}"
echo "Ray head IP:     ${RAY_HEAD_IP}"
echo "PW user:         ${PW_USER}"

JOB_DIR="${PW_PARENT_JOB_DIR%/}"
RAY_PORT=6379

# Fixed worker ports for bidirectional tunnel
# These must match what start_ray_workers.sh uses
WORKER_TUNNEL_IP=127.0.0.2
WORKER_RAYLET_PORT=20380
WORKER_OBJ_PORT=20381
# Worker process ports: constrained range for tunneling (need ~1 per CPU on remote node)
WORKER_MIN_PORT=20400
WORKER_MAX_PORT=20420

# Find pw CLI
PW_CMD=""
for cmd in pw ~/pw/pw; do
    command -v $cmd &>/dev/null && { PW_CMD=$cmd; break; }
    [ -x "$cmd" ] && { PW_CMD=$cmd; break; }
done
echo "PW CLI: ${PW_CMD:-not found}"

# Resolve SSH target — discover from pw cluster list
SSH_TARGET=""
if [ -n "${PW_CMD}" ]; then
    echo "Discovering cloud resource from pw cluster list..."
    while IFS= read -r uri; do
        name="${uri##*/}"
        SSH_TARGET="${name}"
        echo "  Discovered cloud resource: ${name}"
        break
    done < <(${PW_CMD} cluster list 2>/dev/null | awk '/^pw:\/\/'"${PW_USER}"'/ && /active/ && !/existing/ {print $1}')
fi

if [ -z "${SSH_TARGET}" ]; then
    echo "[ERROR] Could not determine cloud resource name"
    ${PW_CMD} cluster list 2>/dev/null || echo "  (pw CLI not available)"
    exit 1
fi

echo "SSH target: ${SSH_TARGET}"

# Helper: run command on cloud via pw ssh proxy
run_on_cloud() {
    ssh -i ~/.ssh/pwcli \
        -o StrictHostKeyChecking=no \
        -o UserKnownHostsFile=/dev/null \
        -o ProxyCommand="pw ssh --proxy-command %h" \
        "${PW_USER}@${SSH_TARGET}" "$@"
}

# Allocate two ports on the cloud side
echo "Allocating ports on cloud..."
TUNNEL_PORT=$(run_on_cloud 'python3 -c "import socket; s=socket.socket(); s.bind((\"\",0)); print(s.getsockname()[1]); s.close()"' 2>/dev/null)
TUNNEL_RAY_PORT=$(run_on_cloud 'python3 -c "import socket; s=socket.socket(); s.bind((\"\",0)); print(s.getsockname()[1]); s.close()"' 2>/dev/null)

if [ -z "${TUNNEL_PORT}" ] || ! [[ "${TUNNEL_PORT}" =~ ^[0-9]+$ ]]; then
    echo "[ERROR] Failed to allocate dashboard tunnel port (got: '${TUNNEL_PORT}')"
    exit 1
fi
if [ -z "${TUNNEL_RAY_PORT}" ] || ! [[ "${TUNNEL_RAY_PORT}" =~ ^[0-9]+$ ]]; then
    echo "[ERROR] Failed to allocate Ray tunnel port (got: '${TUNNEL_RAY_PORT}')"
    exit 1
fi

echo "Tunnel dashboard port on cloud: ${TUNNEL_PORT}"
echo "Tunnel Ray port on cloud: ${TUNNEL_RAY_PORT}"
echo "${TUNNEL_PORT}" > "${JOB_DIR}/TUNNEL_PORT"
echo "${TUNNEL_RAY_PORT}" > "${JOB_DIR}/TUNNEL_RAY_PORT"

# Start SSH tunnel with both reverse (-R) and forward (-L) port forwards:
#   Reverse (cloud can reach on-prem):
#     cloud:TUNNEL_PORT     -> onprem:DASHBOARD_PORT  (dashboard API)
#     cloud:TUNNEL_RAY_PORT -> onprem:RAY_PORT         (Ray GCS)
#   Forward (on-prem can reach cloud worker):
#     onprem:WORKER_RAYLET_PORT -> cloud:WORKER_RAYLET_PORT  (raylet heartbeats)
#     onprem:WORKER_OBJ_PORT   -> cloud:WORKER_OBJ_PORT     (object transfer)
#     onprem:20400-20420        -> cloud:20400-20420          (worker process ports)
echo "Establishing bidirectional SSH tunnel..."
echo "  Reverse: cloud:${TUNNEL_PORT} -> onprem:${DASHBOARD_PORT} (dashboard)"
echo "  Reverse: cloud:${TUNNEL_RAY_PORT} -> onprem:${RAY_PORT} (Ray GCS)"
echo "  Forward: onprem:${WORKER_RAYLET_PORT} -> cloud:${WORKER_RAYLET_PORT} (raylet)"
echo "  Forward: onprem:${WORKER_OBJ_PORT} -> cloud:${WORKER_OBJ_PORT} (object mgr)"
echo "  Forward: onprem:${WORKER_MIN_PORT}-${WORKER_MAX_PORT} -> cloud (worker procs)"

# Build SSH command with all port forwards
SSH_ARGS=(
    -i ~/.ssh/pwcli
    -o StrictHostKeyChecking=no
    -o UserKnownHostsFile=/dev/null
    -o ExitOnForwardFailure=yes
    -o ServerAliveInterval=15
    -o "ProxyCommand=pw ssh --proxy-command %h"
    -R "${TUNNEL_PORT}:localhost:${DASHBOARD_PORT}"
    -R "${TUNNEL_RAY_PORT}:localhost:${RAY_PORT}"
    -L "${WORKER_TUNNEL_IP}:${WORKER_RAYLET_PORT}:localhost:${WORKER_RAYLET_PORT}"
    -L "${WORKER_TUNNEL_IP}:${WORKER_OBJ_PORT}:localhost:${WORKER_OBJ_PORT}"
)
# Add forward tunnels for worker process ports (one per port in range)
for port in $(seq ${WORKER_MIN_PORT} ${WORKER_MAX_PORT}); do
    SSH_ARGS+=(-L "${WORKER_TUNNEL_IP}:${port}:localhost:${port}")
done

ssh "${SSH_ARGS[@]}" -N "${PW_USER}@${SSH_TARGET}" &
TUNNEL_PID=$!
sleep 3

if kill -0 ${TUNNEL_PID} 2>/dev/null; then
    echo "=========================================="
    echo "Bidirectional tunnel ESTABLISHED (PID ${TUNNEL_PID})"
    echo "  Reverse: Cloud localhost:${TUNNEL_PORT} -> On-prem localhost:${DASHBOARD_PORT}"
    echo "  Reverse: Cloud localhost:${TUNNEL_RAY_PORT} -> On-prem localhost:${RAY_PORT}"
    echo "  Forward: On-prem localhost:${WORKER_RAYLET_PORT} -> Cloud localhost:${WORKER_RAYLET_PORT}"
    echo "  Forward: On-prem localhost:${WORKER_OBJ_PORT} -> Cloud localhost:${WORKER_OBJ_PORT}"
    echo "  Forward: On-prem ${WORKER_MIN_PORT}-${WORKER_MAX_PORT} -> Cloud (worker procs)"
    echo "=========================================="

    # Verify tunnel by testing connectivity from cloud side
    echo "Verifying tunnel from cloud..."
    sleep 2
    VERIFY_OK=false
    for i in 1 2 3 4 5; do
        if run_on_cloud "python3 -c \"import socket; s=socket.socket(); s.settimeout(5); s.connect(('localhost', ${TUNNEL_RAY_PORT})); s.close(); print('Tunnel Ray port OK')\"" 2>/dev/null; then
            VERIFY_OK=true
            break
        fi
        echo "  Verification attempt ${i}/5..."
        sleep 2
    done
    if [ "${VERIFY_OK}" = "false" ]; then
        echo "[WARN] Could not verify Ray tunnel port from cloud"
    fi

    echo "TUNNEL_PORT=${TUNNEL_PORT}" >> "${OUTPUTS}"
    echo "TUNNEL_RAY_PORT=${TUNNEL_RAY_PORT}" >> "${OUTPUTS}"

    # Keep tunnel alive
    while kill -0 ${TUNNEL_PID} 2>/dev/null; do
        sleep 5
    done
    echo "Tunnel process exited"
else
    echo "[ERROR] Failed to establish tunnel"
    exit 1
fi
