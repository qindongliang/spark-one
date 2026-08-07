#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TARGET="${1:-}"

case "$TARGET" in
  all)
    exec mvn -f "$ROOT_DIR/pom.xml" clean package -DskipTests
    ;;
  queryone-server|queryone-mysql-provider|queryone-hdfs-workspace-extension|queryone-odep-catalog|queryone-odep-authz-extension)
    exec mvn -f "$ROOT_DIR/pom.xml" -pl "$TARGET" -am clean package -DskipTests
    ;;
  *)
    echo "用法: $0 [all|queryone-server|queryone-mysql-provider|queryone-hdfs-workspace-extension|queryone-odep-catalog|queryone-odep-authz-extension]" >&2
    exit 1
    ;;
esac
