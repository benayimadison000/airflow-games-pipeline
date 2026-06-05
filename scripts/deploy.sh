#!/bin/bash
# ── deploy.sh ────────────────────────────────────────────────────────────────
# Pulls the latest DAGs from GitHub and restarts the Airflow scheduler.
# Run this after merging a PR to master to deploy new DAG changes locally.
# Usage: bash scripts/deploy.sh
# ─────────────────────────────────────────────────────────────────────────────

set -e  # exit immediately if any command fails

AIRFLOW_HOME=~/airflow-pipeline/airflow
DAGS_FOLDER=$AIRFLOW_HOME/dags
PROJECT_DIR=~/airflow-pipeline

echo "============================================"
echo " Airflow Local Deploy"
echo "============================================"

# Step 1 — Make sure we are on master
echo "[1/5] Switching to master branch..."
cd $PROJECT_DIR
git checkout master

# Step 2 — Pull latest code from GitHub
echo "[2/5] Pulling latest code from GitHub..."
git pull origin master

# Step 3 — Activate virtual environment
echo "[3/5] Activating virtual environment..."
source $PROJECT_DIR/venv/bin/activate

# Step 4 — Install any new dependencies
echo "[4/5] Installing dependencies..."
pip install -q apache-airflow==2.10.4 \
  apache-airflow-providers-postgres \
  requests pandas sqlalchemy psycopg2-binary \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.4/constraints-3.12.txt"

# Step 5 — Restart the Airflow scheduler
# The scheduler picks up DAG changes automatically but a restart
# guarantees new DAGs are loaded immediately without waiting
echo "[5/5] Restarting Airflow scheduler..."

# Find and kill the existing scheduler process if running
SCHEDULER_PID=$(pgrep -f "airflow scheduler" || true)
if [ -n "$SCHEDULER_PID" ]; then
    echo "  Stopping scheduler (PID: $SCHEDULER_PID)..."
    kill $SCHEDULER_PID
    sleep 3
fi

# Start the scheduler in the background
echo "  Starting scheduler..."
nohup airflow scheduler > $AIRFLOW_HOME/logs/scheduler.log 2>&1 &
echo "  Scheduler started (PID: $!)"

echo "============================================"
echo " Deploy complete."
echo " DAGs folder: $DAGS_FOLDER"
echo " Scheduler log: $AIRFLOW_HOME/logs/scheduler.log"
echo "============================================"