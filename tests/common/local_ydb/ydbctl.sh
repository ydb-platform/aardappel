#!/bin/bash
set -x

INIT_YDB_SCRIPT=/init_ydb

export YDB_GRPC_ENABLE_TLS="true"
export GRPC_TLS_PORT=${GRPC_TLS_PORT:-2135}
export GRPC_PORT=${GRPC_PORT:-2136}
export YDB_GRPC_TLS_DATA_PATH="/ydb_certs"

case "$1" in
  restart)
    echo "Stopping YDB..."
    /local_ydb stop

    delay=${2:-0}
    echo "Waiting for $delay seconds..."
    sleep $delay

    echo "Starting YDB..."
    /local_ydb start --ydb-working-dir /ydb_data --ydb-binary-path /ydbd --fixed-ports --dont-use-log-files
    ;;
  *)
    echo "Running YDB deploy..."
    /local_ydb deploy --ydb-working-dir /ydb_data --ydb-binary-path /ydbd --fixed-ports --dont-use-log-files
    if [ -f "$INIT_YDB_SCRIPT" ]; then
        sh "$INIT_YDB_SCRIPT";
    fi
    tail -f /dev/null
    ;;
esac
