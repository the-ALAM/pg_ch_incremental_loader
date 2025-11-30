#!/bin/sh

# Usage: ./import_branches.sh [PSQL_CONN_OPTIONS]
# Example: ./import_branches.sh -U myuser -d mydb

tail -n +2 resources/dim_tbl-brch.csv | tr -d '\r' | tr -d "'" | \
psql "$@" -c "\copy branches(id, name, address, city, country, created_at, updated_at) FROM STDIN WITH (FORMAT csv)"
