#!/bin/bash
set -e

echo "Waiting for ClickHouse to be ready..."
until clickhouse-client --query "SELECT 1" > /dev/null 2>&1; do
  sleep 2
done

echo "ClickHouse is ready. Checking if initialization is needed..."
# Check if the table already exists (indicating DDL has been run before)
if clickhouse-client --database="${CLICKHOUSE_DB}" --query "EXISTS TABLE app_user_visits_fact" 2>/dev/null | grep -q "1"; then
  echo "Tables already exist. Skipping DDL execution."
else
  echo "Executing DDL..."
  clickhouse-client --database="${CLICKHOUSE_DB}" < /docker-entrypoint-initdb.d/ddl-ch.sql
  echo "DDL execution completed."
fi

