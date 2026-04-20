#!/bin/bash

# ───────────────────────────────────────────────
# NIFTY Yahoo Daily Pipeline (Cron-safe)
# ───────────────────────────────────────────────

echo "===== NIFTY DAILY JOB START ====="
date

# Move to repo
cd /home/amith/nsetradingbot || exit

# Activate virtual environment
source botenv/bin/activate

# Run data download
echo "Running Nifty data download..."
python Code/nsedatadailydownload.py

# Check if Python script succeeded
if [ $? -ne 0 ]; then
    echo "❌ Python script failed. Exiting..."
    exit 1
fi

# Git operations
echo "Running Git operations..."

# Pull latest changes (rebase to avoid merge commits)
git pull --rebase

git add -A

# Get latest data folder (THIS is your actual trading date) with  Filter only YYYYMMDD folders
LATEST_DATE=$(ls -1 Output | grep -E '^[0-9]{8}$' | sort | tail -n 1)

echo "Latest data folder detected: $LATEST_DATE"

# Commit only if changes exist
git commit -m "Nifty data update ${LATEST_DATE}" || echo "No changes to commit"

# Push changes
git push

echo "===== NIFTY DAILY JOB END ====="
date