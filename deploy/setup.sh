#!/usr/bin/env bash
# One-time setup for the Polymarket collector on a fresh Ubuntu droplet.
# Usage: sudo bash deploy/setup.sh https://github.com/YOUR_USER/PolyMarket_COD.git
set -euo pipefail

REPO_URL="${1:?Usage: sudo bash deploy/setup.sh <github-repo-url>}"
INSTALL_DIR="/opt/polymarket-collector"
SERVICE_NAME="polymarket-collector"
SVC_USER="polymarket"

echo "=== 1/7 Installing system packages ==="
apt-get update -qq
apt-get install -y python3 python3-venv git sqlite3

echo "=== 2/7 Creating 1GB swap file ==="
if [ ! -f /swapfile ]; then
    fallocate -l 1G /swapfile
    chmod 600 /swapfile
    mkswap /swapfile
    swapon /swapfile
    echo '/swapfile none swap sw 0 0' >> /etc/fstab
    echo "Swap enabled."
else
    echo "Swap already exists, skipping."
fi

echo "=== 3/7 Creating system user '${SVC_USER}' ==="
if ! id "${SVC_USER}" &>/dev/null; then
    useradd --system --shell /usr/sbin/nologin "${SVC_USER}"
    echo "User '${SVC_USER}' created."
else
    echo "User '${SVC_USER}' already exists, skipping."
fi

echo "=== 4/7 Cloning repository ==="
if [ -d "${INSTALL_DIR}" ]; then
    echo "${INSTALL_DIR} already exists. Pull latest instead with deploy/update.sh"
else
    git clone "${REPO_URL}" "${INSTALL_DIR}"
fi

echo "=== 5/7 Setting up Python virtual environment ==="
python3 -m venv "${INSTALL_DIR}/venv"
"${INSTALL_DIR}/venv/bin/pip" install --upgrade pip -q
"${INSTALL_DIR}/venv/bin/pip" install -r "${INSTALL_DIR}/requirements.txt" -q

echo "=== 6/7 Creating data directory ==="
mkdir -p "${INSTALL_DIR}/data"
chown -R "${SVC_USER}:${SVC_USER}" "${INSTALL_DIR}"

echo "=== 7/7 Installing systemd service ==="
cp "${INSTALL_DIR}/deploy/${SERVICE_NAME}.service" "/etc/systemd/system/${SERVICE_NAME}.service"
systemctl daemon-reload
systemctl enable "${SERVICE_NAME}"

echo ""
echo "Setup complete! Start the collector with:"
echo "  sudo systemctl start ${SERVICE_NAME}"
echo "  sudo journalctl -u ${SERVICE_NAME} -f"
