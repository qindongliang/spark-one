#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TARGET="${1:-}"

case "$TARGET" in
  all)
    exec mvn -f "$ROOT_DIR/pom.xml" clean package -DskipTests
    ;;
  sparkone-server|sparkone-mysql-provider|sparkone-hdfs-overwrite-extension|sparkone-kyuubi-odep-plugin|sparkone-kyuubi-odep-authz-extension)
    exec mvn -f "$ROOT_DIR/pom.xml" -pl "$TARGET" -am clean package -DskipTests
    ;;
  *)
    echo "用法: $0 [all|sparkone-server|sparkone-mysql-provider|sparkone-hdfs-overwrite-extension|sparkone-kyuubi-odep-plugin|sparkone-kyuubi-odep-authz-extension]" >&2
    exit 1
    ;;
esac
