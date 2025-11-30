#!/bin/bash

CRON_JOB="*/1 * * * * cd /mnt/d/code/pg_ch_incremental_loader && /home/alam/.local/bin/uv run ./app/loader.py >> /mnt/d/code/pg_ch_incremental_loader/r/cron.log 2>&1"
CRON_FILE="/var/spool/cron/crontabs/alam"

if ! grep -Fxq "$CRON_JOB" "$CRON_FILE"; then
    echo "$CRON_JOB" >> "$CRON_FILE"
fi
