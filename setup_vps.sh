#!/bin/bash
# One-command VPS setup for POLY//BOT (Ubuntu 22.04)
# Run as root: bash setup_vps.sh

set -e

echo "=== Installing Docker ==="
apt-get update -qq
apt-get install -y -qq docker.io docker-compose-plugin git

echo "=== Cloning repo ==="
# Change this to your GitHub repo URL if you push it there,
# or SCP the folder manually and skip the git clone step.
# git clone https://github.com/YOUR_USER/Twix0514 /opt/polybot
# cd /opt/polybot

echo "=== If you SCP'd the files, run: ==="
echo "  cd /opt/polybot"
echo "  cp .env.example .env"
echo "  nano .env          # fill in POLY_PRIVATE_KEY and POLY_FUNDER"
echo "  docker compose up -d --build"
echo ""
echo "=== Bot management commands ==="
echo "  docker compose logs -f         # live logs"
echo "  docker compose restart         # restart bot"
echo "  docker compose down            # stop bot"
echo "  docker compose up -d --build   # rebuild and start"
