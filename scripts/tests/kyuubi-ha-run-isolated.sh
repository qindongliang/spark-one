#!/usr/bin/env bash

set -euo pipefail

SPARKONE_URL="${SPARKONE_URL:-http://127.0.0.1:7070}"
SPARKONE_USERNAME="${SPARKONE_USERNAME:-ha-test}"
SPARKONE_ENGINE="${SPARKONE_ENGINE:-kyuubi_yarn_cluster}"
SPARKONE_REQUEST_COUNT="${SPARKONE_REQUEST_COUNT:-20}"

usage() {
  cat <<'EOF'
测试 SparkOne 通过 ZooKeeper service discovery 提交 run_isolated 请求。

用法:
  scripts/tests/kyuubi-ha-run-isolated.sh

可选环境变量:
  SPARKONE_URL            SparkOne 地址，默认 http://127.0.0.1:7070
  SPARKONE_USERNAME       登录用户名，默认 ha-test
  SPARKONE_ENGINE         engine 名称，默认 kyuubi_yarn_cluster
  SPARKONE_REQUEST_COUNT  请求次数，默认 20

示例:
  SPARKONE_REQUEST_COUNT=50 scripts/tests/kyuubi-ha-run-isolated.sh
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

if [[ ! "$SPARKONE_REQUEST_COUNT" =~ ^[1-9][0-9]*$ ]]; then
  echo "SPARKONE_REQUEST_COUNT 必须是大于 0 的整数" >&2
  exit 1
fi

if [[ ! "$SPARKONE_USERNAME" =~ ^[A-Za-z0-9._-]+$ ]]; then
  echo "SPARKONE_USERNAME 只能包含字母、数字、点、下划线和连字符" >&2
  exit 1
fi

if [[ ! "$SPARKONE_ENGINE" =~ ^[A-Za-z0-9._-]+$ ]]; then
  echo "SPARKONE_ENGINE 只能包含字母、数字、点、下划线和连字符" >&2
  exit 1
fi

SPARKONE_URL="${SPARKONE_URL%/}"
COOKIE_FILE="$(mktemp "${TMPDIR:-/tmp}/sparkone-cookie.XXXXXX")"
trap 'rm -f "$COOKIE_FILE"' EXIT

echo "登录 SparkOne: username=$SPARKONE_USERNAME url=$SPARKONE_URL"
curl -fsS -c "$COOKIE_FILE" \
  "$SPARKONE_URL/api/login" \
  -H 'Content-Type: application/json' \
  -d "{\"username\":\"$SPARKONE_USERNAME\"}" \
  >/dev/null

for ((i = 1; i <= SPARKONE_REQUEST_COUNT; i++)); do
  curl -fsS -b "$COOKIE_FILE" \
    "$SPARKONE_URL/api/run" \
    -H 'Content-Type: application/json' \
    -d "{\"engine\":\"$SPARKONE_ENGINE\",\"sessionMode\":\"run_isolated\",\"script\":\"select $i as request_id;\",\"limit\":10}" \
    >/dev/null
  echo "[$i/$SPARKONE_REQUEST_COUNT] 请求成功"
done

echo "测试完成。请在两个 Kyuubi Server 日志中核对 Opening session 记录。"
