#!/usr/bin/env bash
# Set up the FastAPI dashboard API on the DigitalOcean droplet.
# Run as root: sudo bash deploy/setup-api.sh
set -euo pipefail

INSTALL_DIR="/opt/polymarket-collector"
API_DIR="${INSTALL_DIR}/api"
SVC_USER="polymarket"

echo "=== 1. Installing nginx ==="
apt-get update -qq
apt-get install -y nginx

echo "=== 2. Creating API virtual environment ==="
python3 -m venv "${API_DIR}/venv"
"${API_DIR}/venv/bin/pip" install -q --upgrade pip
"${API_DIR}/venv/bin/pip" install -q -r "${API_DIR}/requirements.txt"

echo "=== 3. Running database migration ==="
cd "${INSTALL_DIR}"
"${API_DIR}/venv/bin/python" -m api.migrate

echo "=== 4. Installing API systemd service ==="
cp "${INSTALL_DIR}/deploy/polymarket-api.service" /etc/systemd/system/
systemctl daemon-reload
systemctl enable polymarket-api
systemctl start polymarket-api

echo "=== 5. Configuring nginx ==="
cp "${INSTALL_DIR}/deploy/nginx-api.conf" /etc/nginx/sites-available/polymarket-api
ln -sf /etc/nginx/sites-available/polymarket-api /etc/nginx/sites-enabled/polymarket-api

# Remove default site if it exists
rm -f /etc/nginx/sites-enabled/default

nginx -t
systemctl reload nginx

echo "=== 6. Reducing collector memory limit ==="
# Collector was at 800M, reduce to 600M to leave room for API
if grep -q "MemoryMax=800M" /etc/systemd/system/polymarket-collector.service; then
    sed -i 's/MemoryMax=800M/MemoryMax=600M/' /etc/systemd/system/polymarket-collector.service
    systemctl daemon-reload
    echo "  Collector memory limit reduced to 600M"
else
    echo "  Collector memory limit already adjusted"
fi

echo ""
echo "=== Setup complete! ==="
echo ""
echo "API:       http://$(hostname -I | awk '{print $1}')/api/health"
echo "Status:    sudo systemctl status polymarket-api"
echo "Logs:      sudo journalctl -u polymarket-api -f"
echo ""
echo "Optional: Set up HTTPS with certbot:"
echo "  apt install certbot python3-certbot-nginx"
echo "  certbot --nginx -d your-domain.com"
