#!/usr/bin/env bash

set -euo pipefail

ODEP_API_URL="${ODEP_API_URL:-http://127.0.0.1:8093}"
ODEP_CONNECT_TIMEOUT_SECONDS="${ODEP_CONNECT_TIMEOUT_SECONDS:-5}"
ODEP_REQUEST_TIMEOUT_SECONDS="${ODEP_REQUEST_TIMEOUT_SECONDS:-60}"

usage() {
  cat <<'EOF'
测试 ODEP 数据源按需加载接口，不输出解析后的连接配置。

用法:
  scripts/tests/odep-datasource-api.sh <jdbc|doris>
  scripts/tests/odep-datasource-api.sh <jdbc|doris> <alias>

说明:
  只传类型时，列出该类型下的 alias 和 physicalNamespace
  同时传 alias 时，继续校验该数据源的运行时解析配置

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

if (($# < 1 || $# > 2)); then
  usage >&2
  exit 1
fi

datasource_type="$1"
datasource_alias="${2:-}"

: "${ODEP_KYUUBI_APP_ID:?缺少环境变量 ODEP_KYUUBI_APP_ID}"
: "${ODEP_KYUUBI_SIGN_KEY:?缺少环境变量 ODEP_KYUUBI_SIGN_KEY}"

if [[ "$datasource_type" != "jdbc" && "$datasource_type" != "doris" ]]; then
  echo "数据源类型只支持 jdbc 或 doris" >&2
  exit 1
fi

if [[ ! "$ODEP_KYUUBI_APP_ID" =~ ^[A-Za-z0-9._-]+$ ]]; then
  echo "ODEP_KYUUBI_APP_ID 只能包含字母、数字、点、下划线和连字符" >&2
  exit 1
fi

if [[ -n "$datasource_alias" && ! "$datasource_alias" =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]]; then
  echo "alias 必须以字母或下划线开头，且只能包含字母、数字和下划线" >&2
  exit 1
fi

for timeout_value in "$ODEP_CONNECT_TIMEOUT_SECONDS" "$ODEP_REQUEST_TIMEOUT_SECONDS"; do
  if [[ ! "$timeout_value" =~ ^[1-9][0-9]*$ ]]; then
    echo "连接和请求超时必须是大于 0 的整数" >&2
    exit 1
  fi
done

for command_name in curl python3; do
  if ! command -v "$command_name" >/dev/null 2>&1; then
    echo "缺少命令: $command_name" >&2
    exit 1
  fi
done

ODEP_API_URL="${ODEP_API_URL%/}"
index_response="$(mktemp "${TMPDIR:-/tmp}/odep-datasource-index.XXXXXX")"
resolve_response="$(mktemp "${TMPDIR:-/tmp}/odep-datasource-resolve.XXXXXX")"
trap 'rm -f "$index_response" "$resolve_response"' EXIT

signed_post() {
  local endpoint="$1"
  local response_file="$2"
  local type="$3"
  local alias="${4:-}"
  local timestamp nonce signature http_status

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

  local form_args=(
    --data-urlencode "appId=$ODEP_KYUUBI_APP_ID"
    --data-urlencode "nonce=$nonce"
    --data-urlencode "timestamp=$timestamp"
    --data-urlencode "sign=$signature"
    --data-urlencode "type=$type"
  )
  if [[ -n "$alias" ]]; then
    form_args+=(--data-urlencode "alias=$alias")
  fi

  if ! http_status="$(
    curl -sS \
      --connect-timeout "$ODEP_CONNECT_TIMEOUT_SECONDS" \
      --max-time "$ODEP_REQUEST_TIMEOUT_SECONDS" \
      -o "$response_file" \
      -w '%{http_code}' \
      "${form_args[@]}" \
      "$endpoint"
  )"; then
    echo "请求 ODEP 数据源接口失败: $endpoint" >&2
    exit 1
  fi
  if [[ "$http_status" != "200" ]]; then
    echo "ODEP 数据源接口请求失败: endpoint=$endpoint HTTP=$http_status" >&2
    exit 1
  fi
}

if [[ -n "$datasource_alias" ]]; then
  echo "验证 ODEP 数据源: type=$datasource_type alias=$datasource_alias"
else
  echo "获取 ODEP 数据源索引: type=$datasource_type"
fi
signed_post \
  "$ODEP_API_URL/api/datasource/index" \
  "$index_response" \
  "$datasource_type"
if [[ -n "$datasource_alias" ]]; then
  signed_post \
    "$ODEP_API_URL/api/datasource/resolve" \
    "$resolve_response" \
    "$datasource_type" \
    "$datasource_alias"
fi

python3 - \
  "$index_response" \
  "$resolve_response" \
  "$datasource_type" \
  "$datasource_alias" <<'PY'
import json
import sys

index_path, resolve_path, expected_type, expected_alias = sys.argv[1:]


def load_success(path, operation):
    try:
        with open(path, encoding="utf-8") as response_file:
            response = json.load(response_file)
    except (OSError, json.JSONDecodeError) as error:
        raise SystemExit(f"ODEP {operation} 响应不是有效 JSON: {error}")
    if not isinstance(response, dict):
        raise SystemExit(f"ODEP {operation} 响应根节点必须是 JSON 对象")
    if response.get("code") != 200 or response.get("success") is not True:
        message = response.get("message", response.get("msg", "未知错误"))
        raise SystemExit(
            f"ODEP {operation} 业务请求失败: code={response.get('code')}, message={message}"
        )
    return response.get("results")


index = load_success(index_path, "index")
if not isinstance(index, list):
    raise SystemExit("ODEP index results 必须是数组")

allowed_index_fields = {
    "id",
    "type",
    "alias",
    "physicalNamespace",
    "description",
    "updateTime",
}
matched = False
for item in index:
    if not isinstance(item, dict):
        raise SystemExit("ODEP index 包含非对象条目")
    unexpected = set(item) - allowed_index_fields
    if unexpected:
        raise SystemExit(
            "ODEP index 包含不允许的字段: " + ", ".join(sorted(unexpected))
        )
    if item.get("type", "").lower() != expected_type:
        raise SystemExit(f"ODEP index 返回了其他类型: {item.get('type')}")
    alias = item.get("alias")
    namespace = item.get("physicalNamespace")
    if not isinstance(alias, str) or not alias:
        raise SystemExit("ODEP index alias 不能为空")
    if not isinstance(namespace, str) or not namespace.strip():
        raise SystemExit(f"ODEP index physicalNamespace 不能为空: alias={alias}")
    matched = matched or alias.lower() == expected_alias.lower()

if not expected_alias:
    print(f"ODEP 数据源索引校验成功: type={expected_type}, count={len(index)}")
    for item in index:
        print(
            f"- alias={item['alias']} "
            f"physicalNamespace={item['physicalNamespace']}"
        )
    raise SystemExit(0)

if not matched:
    raise SystemExit(
        f"ODEP index 中不存在目标数据源: type={expected_type}, alias={expected_alias}"
    )

resolved = load_success(resolve_path, "resolve")
if not isinstance(resolved, dict) or not resolved:
    raise SystemExit("ODEP resolve results 必须是非空对象")
if "${" in json.dumps(resolved, ensure_ascii=False):
    raise SystemExit("ODEP resolve 仍包含未解析的占位符")

required_options = {
    "jdbc": {"url", "driver", "user", "password"},
    "doris": {"doris.fenodes", "user", "password"},
}[expected_type]
missing = required_options - set(resolved)
if missing:
    raise SystemExit("ODEP resolve 缺少必需字段: " + ", ".join(sorted(missing)))

print(
    f"ODEP 按需接口校验成功: type={expected_type}, alias={expected_alias}, "
    f"indexCount={len(index)}"
)
PY
