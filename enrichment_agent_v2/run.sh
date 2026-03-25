#!/bin/bash
set -e

cd "$(dirname "$0")"

echo "=== Propyte Enrichment Agent V2 ==="
echo "Installing dependencies..."
pip install -r requirements.txt

echo ""
echo "Starting agent..."
echo "Dashboard: http://localhost:8080"
echo ""

cd ..
python -m enrichment_agent_v2.main "$@"
