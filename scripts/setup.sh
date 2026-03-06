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
VENV_DIR="${JOB_DIR}/.venv"
UV_DIR="${JOB_DIR}/.uv"
UV_BIN="${UV_DIR}/uv"
MIN_PYTHON="3.9"
TARGET_PYTHON="3.12"

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
    if [ "${INSTALLED_VERSION}" == "${RAY_VERSION}" ]; then
        echo "Ray ${RAY_VERSION} already installed, skipping."
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
    # Install ray without [default] extras to minimize dependencies.
    # The [default] extra adds virtualenv (for runtime envs) and other optional
    # packages that increase download size and can fail on restricted networks.
    # Our custom dashboard and core Ray functionality work fine without them.
    UV_HTTP_TIMEOUT=120 ${UV_BIN} pip install --python "${VENV_DIR}/bin/python" \
        "ray==${RAY_VERSION}" numpy
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
        "ray==${RAY_VERSION}" numpy
fi

# Verify
"${VENV_DIR}/bin/python" -c "import ray; print('Ray ' + ray.__version__ + ' ready')"
"${VENV_DIR}/bin/python" -c "import numpy; print('NumPy ' + numpy.__version__ + ' ready')"

touch "${JOB_DIR}/SETUP_COMPLETE"
echo "=========================================="
echo "Setup complete! Ray ${RAY_VERSION} installed."
echo "=========================================="
