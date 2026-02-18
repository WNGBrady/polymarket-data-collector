#!/usr/bin/env bash
# Pull latest code and restart the collector service.
# Usage: cd /opt/polymarket-collector && sudo bash deploy/update.sh
set -euo pipefail

INSTALL_DIR="/opt/polymarket-collector"
SERVICE_NAME="polymarket-collector"
API_SERVICE="polymarket-api"
SVC_USER="polymarket"

cd "${INSTALL_DIR}"

echo "=== Pulling latest code ==="
sudo -u "${SVC_USER}" git pull --ff-only

echo "=== Checking if collector requirements changed ==="
if git diff HEAD~1 --name-only | grep -q "^requirements.txt"; then
    echo "requirements.txt changed — reinstalling dependencies..."
    "${INSTALL_DIR}/venv/bin/pip" install -r "${INSTALL_DIR}/requirements.txt" -q
else
    echo "requirements.txt unchanged, skipping pip install."
fi

echo "=== Checking if API requirements changed ==="
if git diff HEAD~1 --name-only | grep -q "^api/requirements.txt"; then
    echo "api/requirements.txt changed — reinstalling API dependencies..."
    "${INSTALL_DIR}/api/venv/bin/pip" install -r "${INSTALL_DIR}/api/requirements.txt" -q
else
    echo "api/requirements.txt unchanged, skipping pip install."
fi

echo "=== Checking if collector service file changed ==="
if ! diff -q "${INSTALL_DIR}/deploy/${SERVICE_NAME}.service" "/etc/systemd/system/${SERVICE_NAME}.service" &>/dev/null; then
    echo "Service file changed — updating..."
    cp "${INSTALL_DIR}/deploy/${SERVICE_NAME}.service" "/etc/systemd/system/${SERVICE_NAME}.service"
    systemctl daemon-reload
else
    echo "Service file unchanged, skipping."
fi

echo "=== Checking if API service file changed ==="
if [ -f "/etc/systemd/system/${API_SERVICE}.service" ]; then
    if ! diff -q "${INSTALL_DIR}/deploy/${API_SERVICE}.service" "/etc/systemd/system/${API_SERVICE}.service" &>/dev/null; then
        echo "API service file changed — updating..."
        cp "${INSTALL_DIR}/deploy/${API_SERVICE}.service" "/etc/systemd/system/${API_SERVICE}.service"
        systemctl daemon-reload
    else
        echo "API service file unchanged, skipping."
    fi
fi

echo "=== Running DB migrations ==="
if [ -d "${INSTALL_DIR}/api/venv" ]; then
    "${INSTALL_DIR}/api/venv/bin/python" -m api.migrate || echo "  Migration skipped (may already be applied)"
fi

echo "=== Restarting services ==="
systemctl restart "${SERVICE_NAME}"
if systemctl is-enabled "${API_SERVICE}" &>/dev/null; then
    systemctl restart "${API_SERVICE}"
    echo "API service restarted."
fi

echo ""
echo "Update complete. Check status with:"
echo "  sudo systemctl status ${SERVICE_NAME}"
echo "  sudo systemctl status ${API_SERVICE}"
echo "  sudo journalctl -u ${API_SERVICE} -f"
