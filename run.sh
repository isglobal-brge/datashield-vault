#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")"

PORT="${1:-8000}"

# Check if port is in use, find next available
check_port() { lsof -i :"$1" >/dev/null 2>&1; }

while check_port "$PORT"; do
    echo "Port $PORT in use, trying $((PORT + 1))..."
    ((PORT++))
done

# Create data directory
mkdir -p data/collections

# Run
echo "Starting DataSHIELD Vault on port $PORT..."
export PORT
cd app && docker compose up -d --build

echo
echo "API: http://localhost:${PORT}"
echo
echo "Usage:"
echo "  mkdir data/collections/my-cohort"
echo "  cp files... data/collections/my-cohort/"
echo "  cat data/collections/my-cohort/.vault_key"
echo
echo "Stop: cd app && docker compose down"
