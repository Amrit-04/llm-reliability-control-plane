#!/bin/sh
set -eu
wal_dir="/data/wal"
previous=""
for argument in "$@"; do
  if [ "$previous" = "--wal-dir" ]; then
    wal_dir="$argument"
  fi
  previous="$argument"
done
mkdir -p "$wal_dir"
exec /usr/local/bin/lrcp-gateway "$@"
