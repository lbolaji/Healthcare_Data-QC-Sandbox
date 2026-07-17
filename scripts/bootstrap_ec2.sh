#!/usr/bin/env bash
set -euo pipefail

echo "=== Installing system deps ==="
sudo dnf install -y python3.11 python3.11-pip git || \
    sudo apt-get install -y python3.11 python3.11-venv git

echo "=== Creating data directories ==="
sudo mkdir -p /data/history /data/output
sudo chown -R ec2-user:ec2-user /data

echo "=== Setting up Python venv ==="
cd /opt/qc-sandbox
python3.11 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -e "."

echo "=== Installing CloudWatch agent ==="
wget -q https://s3.amazonaws.com/amazoncloudwatch-agent/amazon_linux/amd64/latest/amazon-cloudwatch-agent.rpm
sudo rpm -U ./amazon-cloudwatch-agent.rpm
sudo /opt/aws/amazon-cloudwatch-agent/bin/amazon-cloudwatch-agent-ctl \
    -a fetch-config -m ec2 -s \
    -c file:/opt/qc-sandbox/deploy/cloudwatch-agent.json

echo "=== Enabling systemd services ==="
sudo cp deploy/qc.service deploy/qc.timer deploy/dashboard.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now qc.timer
sudo systemctl enable --now dashboard.service

echo "=== Bootstrap complete ==="
