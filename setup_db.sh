#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# setup_db.sh — Create the PostgreSQL database and user for OpenF1
# Run once before the first pipeline execution.
#
# Requirements: PostgreSQL installed via Homebrew
#   brew install postgresql@16
#   brew services start postgresql@16
# ─────────────────────────────────────────────────────────────────────────────

set -euo pipefail

DB_NAME="openf1_db"
DB_USER="openf1_user"
DB_PASS="openf1_pass"   # ← change this if you updated config.py

echo "🐘 Setting up PostgreSQL for OpenF1..."

# 1. Create the role (ignore error if already exists)
psql postgres -tc "SELECT 1 FROM pg_roles WHERE rolname='${DB_USER}'" | grep -q 1 || \
  psql postgres -c "CREATE ROLE ${DB_USER} WITH LOGIN PASSWORD '${DB_PASS}';"

# 2. Create the database (ignore error if already exists)
psql postgres -tc "SELECT 1 FROM pg_database WHERE datname='${DB_NAME}'" | grep -q 1 || \
  psql postgres -c "CREATE DATABASE ${DB_NAME} OWNER ${DB_USER};"

# 3. Grant all privileges
psql postgres -c "GRANT ALL PRIVILEGES ON DATABASE ${DB_NAME} TO ${DB_USER};"

echo ""
echo "✅ Done! Database '${DB_NAME}' is ready."
echo ""
echo "You can connect with:"
echo "  psql -U ${DB_USER} -d ${DB_NAME}"
echo ""
echo "Next step → run the pipeline:"
echo "  python main.py"
