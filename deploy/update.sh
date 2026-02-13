#!/usr/bin/env bash
# Pull latest code and restart the collector service.
# Usage: cd /opt/polymarket-collector && sudo bash deploy/update.sh
set -euo pipefail

INSTALL_DIR="/opt/polymarket-collector"
SERVICE_NAME="polymarket-collector"
SVC_USER="polymarket"

cd "${INSTALL_DIR}"

echo "=== Pulling latest code ==="
sudo -u "${SVC_USER}" git pull --ff-only

echo "=== Checking if requirements changed ==="
if git diff HEAD~1 --name-only | grep -q "requirements.txt"; then
    echo "requirements.txt changed — reinstalling dependencies..."
    "${INSTALL_DIR}/venv/bin/pip" install -r "${INSTALL_DIR}/requirements.txt" -q
else
    echo "requirements.txt unchanged, skipping pip install."
fi

echo "=== Checking if service file changed ==="
if ! diff -q "${INSTALL_DIR}/deploy/${SERVICE_NAME}.service" "/etc/systemd/system/${SERVICE_NAME}.service" &>/dev/null; then
    echo "Service file changed — updating..."
    cp "${INSTALL_DIR}/deploy/${SERVICE_NAME}.service" "/etc/systemd/system/${SERVICE_NAME}.service"
    systemctl daemon-reload
else
    echo "Service file unchanged, skipping."
fi

echo "=== Restarting service ==="
systemctl restart "${SERVICE_NAME}"

echo ""
echo "Update complete. Check status with:"
echo "  sudo systemctl status ${SERVICE_NAME}"
echo "  sudo journalctl -u ${SERVICE_NAME} -f"
