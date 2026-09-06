#!/bin/bash

cd ~/nsetradingbot || exit 1
source botenv/bin/activate

echo "===== STARTING ZERODHA SERVICES ====="

# Start callback/status API
python -m uvicorn Code.zerodha.callback:app \
    --host 0.0.0.0 \
    --port 8000 \
    > Output/zerodha_callback.log 2>&1 &

echo "Port 8000 started."

# Start market-data API
python -m uvicorn Code.zerodha.api:app \
    --host 0.0.0.0 \
    --port 8001 \
    > Output/zerodha_api.log 2>&1 &

echo "Port 8001 started."

# Start Tailscale Funnel for Zerodha callback
sudo tailscale funnel 8000 &

echo "Tailscale Funnel started."

# Generate Zerodha login URL
echo "===== ZERODHA LOGIN URL ====="

python -c "from Code.zerodha.config import get_kite_client; kite = get_kite_client(); print(kite.login_url())"

echo "===== ZERODHA SERVICES READY ====="