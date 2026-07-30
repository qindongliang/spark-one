#!/usr/bin/env bash

set -euo pipefail

ODEP_API_URL="${ODEP_API_URL:-http://127.0.0.1:8093}"
ODEP_CONNECT_TIMEOUT_SECONDS="${ODEP_CONNECT_TIMEOUT_SECONDS:-5}"
ODEP_REQUEST_TIMEOUT_SECONDS="${ODEP_REQUEST_TIMEOUT_SECONDS:-60}"

usage() {
  cat <<'EOF'
测试使用 OpenAPI 签名拉取 ODEP 全量数据源快照。

用法:
  scripts/tests/odep-datasource-snapshot.sh

必需环境变量:
  ODEP_KYUUBI_APP_ID       RMS 分配给 Kyuubi 的 appId
  ODEP_KYUUBI_SIGN_KEY     appId 对应的签名密钥

可选环境变量:
  ODEP_API_URL                    ODEP API 地址，默认 http://127.0.0.1:8093
  ODEP_CONNECT_TIMEOUT_SECONDS    连接超时秒数，默认 5
  ODEP_REQUEST_TIMEOUT_SECONDS    请求超时秒数，默认 60
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

: "${ODEP_KYUUBI_APP_ID:?缺少环境变量 ODEP_KYUUBI_APP_ID}"
: "${ODEP_KYUUBI_SIGN_KEY:?缺少环境变量 ODEP_KYUUBI_SIGN_KEY}"

if [[ ! "$ODEP_KYUUBI_APP_ID" =~ ^[A-Za-z0-9._-]+$ ]]; then
  echo "ODEP_KYUUBI_APP_ID 只能包含字母、数字、点、下划线和连字符" >&2
  exit 1
fi

if [[ ! "$ODEP_CONNECT_TIMEOUT_SECONDS" =~ ^[1-9][0-9]*$ ]]; then
  echo "ODEP_CONNECT_TIMEOUT_SECONDS 必须是大于 0 的整数" >&2
  exit 1
fi

if [[ ! "$ODEP_REQUEST_TIMEOUT_SECONDS" =~ ^[1-9][0-9]*$ ]]; then
  echo "ODEP_REQUEST_TIMEOUT_SECONDS 必须是大于 0 的整数" >&2
  exit 1
fi

for command_name in curl python3; do
  if ! command -v "$command_name" >/dev/null 2>&1; then
    echo "缺少命令: $command_name" >&2
    exit 1
  fi
done

ODEP_API_URL="${ODEP_API_URL%/}"
snapshot_url="$ODEP_API_URL/api/datasource/snapshot"
timestamp="$(date +%s)"
nonce="$(python3 - <<'PY'
import secrets
import string

alphabet = string.ascii_letters + string.digits
print("".join(secrets.choice(alphabet) for _ in range(16)))
PY
)"
signature="$(
  python3 - "$ODEP_KYUUBI_APP_ID" "$nonce" "$timestamp" <<'PY'
import hashlib
import os
import sys

params = {
    "appId": sys.argv[1],
    "nonce": sys.argv[2],
    "timestamp": sys.argv[3],
    "appSignKey": os.environ["ODEP_KYUUBI_SIGN_KEY"],
}
payload = "&".join(f"{key}={params[key]}" for key in sorted(params))
print(hashlib.sha1(payload.encode()).hexdigest())
PY
)"

response_file="$(mktemp "${TMPDIR:-/tmp}/odep-datasource-snapshot.XXXXXX")"
trap 'rm -f "$response_file"' EXIT

form_data="appId=$ODEP_KYUUBI_APP_ID&nonce=$nonce&timestamp=$timestamp&sign=$signature"
echo "请求 ODEP 数据源快照: url=$snapshot_url appId=$ODEP_KYUUBI_APP_ID"
if ! http_status="$(
  printf '%s' "$form_data" |
    curl -sS \
      --connect-timeout "$ODEP_CONNECT_TIMEOUT_SECONDS" \
      --max-time "$ODEP_REQUEST_TIMEOUT_SECONDS" \
      -o "$response_file" \
      -w '%{http_code}' \
      -H 'Content-Type: application/x-www-form-urlencoded' \
      --data-binary @- \
      "$snapshot_url"
)"; then
  echo "请求 ODEP 数据源快照失败，请确认服务地址和监听状态" >&2
  exit 1
fi

if [[ "$http_status" != "200" ]]; then
  echo "ODEP 数据源快照请求失败: HTTP $http_status" >&2
  exit 1
fi

python3 - "$response_file" <<'PY'
import json
import sys

response_path = sys.argv[1]
try:
    with open(response_path, encoding="utf-8") as response_file:
        response = json.load(response_file)
except (OSError, json.JSONDecodeError) as error:
    raise SystemExit(f"ODEP 返回的响应不是有效 JSON: {error}")

if not isinstance(response, dict):
    raise SystemExit("ODEP 响应根节点必须是 JSON 对象")

code = response.get("code")
success = response.get("success")
if code != 200 or success is not True:
    message = response.get("message", response.get("msg", "未知错误"))
    raise SystemExit(f"ODEP 业务请求失败: code={code}, message={message}")

snapshot = response.get("results")
if not isinstance(snapshot, dict):
    raise SystemExit("ODEP 响应缺少 results 对象")

datasources = snapshot.get("datasources")
if not isinstance(datasources, list):
    raise SystemExit("ODEP 响应缺少 results.datasources 数组")

if "${" in json.dumps(datasources, ensure_ascii=False):
    raise SystemExit("ODEP 数据源快照仍包含未解析的占位符")

print(f"数据源快照校验成功，共 {len(datasources)} 个数据源")
for datasource in datasources:
    if not isinstance(datasource, dict):
        raise SystemExit("ODEP 数据源快照包含非对象条目")
    datasource_type = json.dumps(datasource.get("type"), ensure_ascii=True)
    alias = json.dumps(datasource.get("alias"), ensure_ascii=True)
    print(f"- type={datasource_type} alias={alias}")
PY
