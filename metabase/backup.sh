#!/bin/bash
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

docker exec -t metabase-db \
  pg_dump -U metabase_user -F p metabase_db \
  > "$SCRIPT_DIR/data/metabase_backup.sql"
