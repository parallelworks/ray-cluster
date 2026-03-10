#!/bin/bash
# setup.sh — Install Ray into a virtual environment
#
# Handles HPC systems with old Python (e.g., 3.6) by bootstrapping a modern
# Python via uv's standalone builds. No containers or root access needed.
#
# Environment variables:
#   RAY_VERSION - Ray version to install (default: 2.40.0)

set -e

echo "=========================================="
echo "Ray Setup: $(date)"
echo "=========================================="

JOB_DIR="${PW_PARENT_JOB_DIR%/}"
RAY_VERSION="${RAY_VERSION:-2.40.0}"
SOFTWARE_DIR="${HOME}/pw/software"
VENV_DIR="${SOFTWARE_DIR}/ray-${RAY_VERSION}"
UV_DIR="${SOFTWARE_DIR}/.uv"
UV_BIN="${UV_DIR}/uv"
MIN_PYTHON="3.9"
TARGET_PYTHON="3.12"

mkdir -p "${SOFTWARE_DIR}"

# =============================================================================
# Find system Python
# =============================================================================
PYTHON_CMD=""
for cmd in python3 python; do
    command -v $cmd &>/dev/null && { PYTHON_CMD=$cmd; break; }
done
if [ -z "${PYTHON_CMD}" ]; then
    echo "[ERROR] Python not found"
    exit 1
fi
echo "System Python: ${PYTHON_CMD} ($(${PYTHON_CMD} --version 2>&1))"

# Check if Ray is already installed at correct version in existing venv
if [ -f "${VENV_DIR}/bin/python" ]; then
    INSTALLED_VERSION=$("${VENV_DIR}/bin/python" -c "import ray; print(ray.__version__)" 2>/dev/null || echo "")
    INSTALLED_PYTHON=$("${VENV_DIR}/bin/python" -c "import platform; print(platform.python_version())" 2>/dev/null || echo "")
    PYTHON_OK=true
    if [ -n "${PYTHON_MICRO_VERSION}" ] && [ "${INSTALLED_PYTHON}" != "${PYTHON_MICRO_VERSION}" ]; then
        echo "Python version mismatch: venv has ${INSTALLED_PYTHON}, need ${PYTHON_MICRO_VERSION}"
        echo "Removing stale venv..."
        rm -rf "${VENV_DIR}"
        PYTHON_OK=false
    fi
    if [ "${PYTHON_OK}" = "true" ] && [ "${INSTALLED_VERSION}" == "${RAY_VERSION}" ]; then
        echo "Ray ${RAY_VERSION} already installed in ${VENV_DIR}, skipping."
        echo "${VENV_DIR}" > "${JOB_DIR}/RAY_VENV_DIR"
        touch "${JOB_DIR}/SETUP_COMPLETE"
        exit 0
    fi
fi

# =============================================================================
# Check if system Python meets minimum version
# =============================================================================
python_too_old() {
    ${PYTHON_CMD} -c "
import sys
major, minor = sys.version_info[:2]
min_parts = '${MIN_PYTHON}'.split('.')
if major < int(min_parts[0]) or (major == int(min_parts[0]) and minor < int(min_parts[1])):
    sys.exit(0)  # too old
sys.exit(1)      # ok
" 2>/dev/null
}

# =============================================================================
# Install uv binary from GitHub (works on old systems with old OpenSSL)
# =============================================================================
install_uv() {
    if [ -x "${UV_BIN}" ]; then
        echo "uv: ${UV_BIN} (cached)"
        return 0
    fi

    mkdir -p "${UV_DIR}"
    echo "Installing uv to ${UV_DIR}..."

    local arch
    arch=$(uname -m)
    case "${arch}" in
        x86_64)  arch="x86_64" ;;
        aarch64) arch="aarch64" ;;
        *)
            echo "  [WARN] Unsupported architecture: ${arch}"
            return 1
            ;;
    esac

    local url="https://github.com/astral-sh/uv/releases/latest/download/uv-${arch}-unknown-linux-gnu.tar.gz"

    if command -v curl &>/dev/null; then
        curl -fsSL "${url}" | tar -xz -C "${UV_DIR}" --strip-components=1 2>/dev/null
    elif command -v wget &>/dev/null; then
        wget -qO- "${url}" | tar -xz -C "${UV_DIR}" --strip-components=1 2>/dev/null
    else
        echo "  [WARN] Neither curl nor wget available"
        return 1
    fi

    if [ -x "${UV_BIN}" ]; then
        echo "  uv installed: $(${UV_BIN} --version 2>&1)"
        return 0
    else
        echo "  [WARN] uv download failed"
        return 1
    fi
}

# =============================================================================
# Bootstrap modern Python if system Python is too old
# =============================================================================
NEED_PYTHON_BOOTSTRAP=false
if python_too_old; then
    echo ""
    echo "System Python is too old for Ray (need >= ${MIN_PYTHON})"
    echo "Bootstrapping Python ${TARGET_PYTHON} via uv..."
    NEED_PYTHON_BOOTSTRAP=true
fi

echo ""
echo "Installing Ray ${RAY_VERSION}..."

if install_uv; then
    if [ "${NEED_PYTHON_BOOTSTRAP}" = "true" ]; then
        # Use uv to download a standalone Python build
        echo "Downloading Python ${TARGET_PYTHON}..."
        ${UV_BIN} python install "${TARGET_PYTHON}" 2>&1
        echo "Python ${TARGET_PYTHON} installed via uv"
    fi

    # Use exact Python micro version if specified (ensures head/worker match)
    UV_PYTHON="${PYTHON_MICRO_VERSION:-${TARGET_PYTHON}}"
    echo "Python version for venv: ${UV_PYTHON}"

    if [ ! -d "${VENV_DIR}" ]; then
        ${UV_BIN} venv "${VENV_DIR}" --python "${UV_PYTHON}"
    fi
    # Try ray[default] first (includes built-in dashboard).
    # Fall back to ray (no extras) if it fails on restricted networks.
    echo "Attempting ray[default] install (includes Ray dashboard)..."
    if UV_HTTP_TIMEOUT=120 ${UV_BIN} pip install --python "${VENV_DIR}/bin/python" \
        "ray[default]==${RAY_VERSION}" numpy fastapi uvicorn websockets httpx 2>&1; then
        echo "ray[default] installed successfully"
    else
        echo ""
        echo "[WARN] ray[default] failed — retrying with minimal ray (no built-in dashboard)"
        UV_HTTP_TIMEOUT=120 ${UV_BIN} pip install --python "${VENV_DIR}/bin/python" \
            "ray==${RAY_VERSION}" numpy fastapi uvicorn websockets httpx
    fi
else
    if [ "${NEED_PYTHON_BOOTSTRAP}" = "true" ]; then
        echo "[ERROR] Cannot install uv and system Python is too old ($(${PYTHON_CMD} --version 2>&1))"
        echo "  Ray ${RAY_VERSION} requires Python >= ${MIN_PYTHON}"
        echo "  Options: load a newer Python module, install conda, or use a container"
        exit 1
    fi
    echo "Using pip..."
    if [ ! -d "${VENV_DIR}" ]; then
        ${PYTHON_CMD} -m venv "${VENV_DIR}"
    fi
    "${VENV_DIR}/bin/python" -m pip install --quiet --upgrade pip
    "${VENV_DIR}/bin/python" -m pip install --quiet \
        "ray[default]==${RAY_VERSION}" numpy fastapi uvicorn websockets httpx 2>&1 || {
        echo "[WARN] ray[default] failed — retrying with minimal ray"
        "${VENV_DIR}/bin/python" -m pip install --quiet \
            "ray==${RAY_VERSION}" numpy fastapi uvicorn websockets httpx
    }
fi

# Verify
"${VENV_DIR}/bin/python" -c "import ray; print('Ray ' + ray.__version__ + ' ready')"
"${VENV_DIR}/bin/python" -c "import numpy; print('NumPy ' + numpy.__version__ + ' ready')"

# Install extra pip packages if specified
if [ -n "${PIP_PACKAGES:-}" ]; then
    echo "Installing extra packages: ${PIP_PACKAGES}"
    if [ -n "${UV_BIN:-}" ]; then
        UV_HTTP_TIMEOUT=300 ${UV_BIN} pip install --python "${VENV_DIR}/bin/python" ${PIP_PACKAGES}
    else
        "${VENV_DIR}/bin/python" -m pip install --quiet ${PIP_PACKAGES}
    fi
    echo "Extra packages installed."
fi

# Write venv path for other scripts to discover
echo "${VENV_DIR}" > "${JOB_DIR}/RAY_VENV_DIR"
touch "${JOB_DIR}/SETUP_COMPLETE"
echo "=========================================="
echo "Setup complete! Ray ${RAY_VERSION} in ${VENV_DIR}"
echo "=========================================="
