#!/bin/bash
set -e

DB_PATH="${DATABASE_PATH:-/data/etf_data.db}"

# Seed the database from the image copy if the volume is empty
if [ ! -f "$DB_PATH" ]; then
    echo "No database found at $DB_PATH — seeding from image copy..."
    cp /app/etf_data.db.seed "$DB_PATH"
    echo "Seed complete."
fi

# Run any pending migrations
python3 -c "
import sys, os
sys.path.insert(0, '/app')
from setup_db import setup_database
setup_database('$DB_PATH')
print('Migrations complete.')
"

echo "Starting ETF dashboard server on port ${PORT:-8080}..."
exec python3 /app/etf_server_with_dashboard.py
