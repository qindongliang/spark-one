#!/usr/bin/env bash

set -euo pipefail

QUERYONE_URL="${QUERYONE_URL:-http://127.0.0.1:7070}"
QUERYONE_USERNAME="${QUERYONE_USERNAME:-ha-test}"
QUERYONE_ENGINE="${QUERYONE_ENGINE:-kyuubi_yarn_cluster}"
QUERYONE_REQUEST_COUNT="${QUERYONE_REQUEST_COUNT:-20}"

usage() {
  cat <<'EOF'
测试 QueryOne 通过 ZooKeeper service discovery 提交 run_isolated 请求。

用法:
  scripts/tests/kyuubi-ha-run-isolated.sh

可选环境变量:
  QUERYONE_URL            QueryOne 地址，默认 http://127.0.0.1:7070
  QUERYONE_USERNAME       登录用户名，默认 ha-test
  QUERYONE_ENGINE         engine 名称，默认 kyuubi_yarn_cluster
  QUERYONE_REQUEST_COUNT  请求次数，默认 20

示例:
  QUERYONE_REQUEST_COUNT=50 scripts/tests/kyuubi-ha-run-isolated.sh
EOF
}

if [[ "${1:-}" == "--help" || "${1:-}" == "-h" ]]; then
  usage
  exit 0
fi

if (($# > 0)); then
  usage >&2
  exit 1
fi

if [[ ! "$QUERYONE_REQUEST_COUNT" =~ ^[1-9][0-9]*$ ]]; then
  echo "QUERYONE_REQUEST_COUNT 必须是大于 0 的整数" >&2
  exit 1
fi

if [[ ! "$QUERYONE_USERNAME" =~ ^[A-Za-z0-9._-]+$ ]]; then
  echo "QUERYONE_USERNAME 只能包含字母、数字、点、下划线和连字符" >&2
  exit 1
fi

if [[ ! "$QUERYONE_ENGINE" =~ ^[A-Za-z0-9._-]+$ ]]; then
  echo "QUERYONE_ENGINE 只能包含字母、数字、点、下划线和连字符" >&2
  exit 1
fi

QUERYONE_URL="${QUERYONE_URL%/}"
COOKIE_FILE="$(mktemp "${TMPDIR:-/tmp}/queryone-cookie.XXXXXX")"
trap 'rm -f "$COOKIE_FILE"' EXIT

echo "登录 QueryOne: username=$QUERYONE_USERNAME url=$QUERYONE_URL"
curl -fsS -c "$COOKIE_FILE" \
  "$QUERYONE_URL/api/login" \
  -H 'Content-Type: application/json' \
  -d "{\"username\":\"$QUERYONE_USERNAME\"}" \
  >/dev/null

for ((i = 1; i <= QUERYONE_REQUEST_COUNT; i++)); do
  curl -fsS -b "$COOKIE_FILE" \
    "$QUERYONE_URL/api/run" \
    -H 'Content-Type: application/json' \
    -d "{\"engine\":\"$QUERYONE_ENGINE\",\"sessionMode\":\"run_isolated\",\"script\":\"select $i as request_id;\",\"limit\":10}" \
    >/dev/null
  echo "[$i/$QUERYONE_REQUEST_COUNT] 请求成功"
done

echo "测试完成。请在两个 Kyuubi Server 日志中核对 Opening session 记录。"
