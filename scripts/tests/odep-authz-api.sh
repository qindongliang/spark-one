#!/usr/bin/env bash

set -euo pipefail

ODEP_API_URL="${ODEP_API_URL:-http://127.0.0.1:8093}"
ODEP_CONNECT_TIMEOUT_SECONDS="${ODEP_CONNECT_TIMEOUT_SECONDS:-5}"
ODEP_REQUEST_TIMEOUT_SECONDS="${ODEP_REQUEST_TIMEOUT_SECONDS:-60}"

usage() {
  cat <<'EOF'
测试 ODEP QueryOne 批量资源权限接口。

用法:
  scripts/tests/odep-authz-api.sh <subject> <allow|deny> '<requests-json>'

示例:
  # RMS 仅配置 resourceType=jdbc、resourceContent=white:ask00:read，没有配置具体 table。
  # 请求仍传 SQL 实际访问的 users 表，预期由库级资源授权通过。
  scripts/tests/odep-authz-api.sh alice allow \
    '[{"resourceType":"jdbc","database":"ask00","table":"users","action":"read"}]'

  requests='[{"resourceType":"jdbc","database":"ask00","table":"users","action":"read"},'
  requests+='{"resourceType":"hive","database":"default","table":"secret_users","action":"read"}]'
  scripts/tests/odep-authz-api.sh alice deny "$requests"

  requests='[{"resourceType":"doris","database":"analytics","table":"events","action":"read"},'
  requests+='{"resourceType":"hdfs","path":"/public/odep/user/alice/data","action":"read"}]'
  scripts/tests/odep-authz-api.sh alice allow "$requests"

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

if (($# != 3)); then
  usage >&2
  exit 1
fi

subject="$1"
expected_result="$2"
requests_json="$3"

: "${ODEP_KYUUBI_APP_ID:?缺少环境变量 ODEP_KYUUBI_APP_ID}"
: "${ODEP_KYUUBI_SIGN_KEY:?缺少环境变量 ODEP_KYUUBI_SIGN_KEY}"

if [[ -z "${subject//[[:space:]]/}" ]]; then
  echo "subject 不能为空" >&2
  exit 1
fi

if [[ "$expected_result" != "allow" && "$expected_result" != "deny" ]]; then
  echo "预期结果只支持 allow 或 deny" >&2
  exit 1
fi

if [[ ! "$ODEP_KYUUBI_APP_ID" =~ ^[A-Za-z0-9._-]+$ ]]; then
  echo "ODEP_KYUUBI_APP_ID 只能包含字母、数字、点、下划线和连字符" >&2
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

python3 - "$requests_json" <<'PY'
import json
import sys

try:
    requests = json.loads(sys.argv[1])
except json.JSONDecodeError as error:
    raise SystemExit(f"requests-json 不是有效 JSON: {error}")

if not isinstance(requests, list) or not requests:
    raise SystemExit("requests-json 必须是非空数组")

for index, request in enumerate(requests):
    if not isinstance(request, dict):
        raise SystemExit(f"requests-json 第 {index + 1} 项必须是对象")
    resource_type = request.get("resourceType")
    if not isinstance(resource_type, str) or not resource_type.strip():
        raise SystemExit(
            f"requests-json 第 {index + 1} 项的 resourceType 必须是非空字符串"
        )
    resource_type = resource_type.strip().lower()
    if resource_type not in {"jdbc", "hive", "doris", "hdfs"}:
        raise SystemExit(f"requests-json 第 {index + 1} 项的 resourceType 不受支持")
    required_fields = {"resourceType", "action"}
    if resource_type == "hdfs":
        required_fields.add("path")
    else:
        required_fields.update({"database", "table"})
    missing = required_fields - set(request)
    if missing:
        raise SystemExit(
            f"requests-json 第 {index + 1} 项缺少字段: "
            + ", ".join(sorted(missing))
        )
    for field in required_fields:
        value = request[field]
        if not isinstance(value, str) or not value.strip():
            raise SystemExit(
                f"requests-json 第 {index + 1} 项的 {field} 必须是非空字符串"
            )
    if resource_type == "hdfs" and not request["path"].strip().startswith("/"):
        raise SystemExit(f"requests-json 第 {index + 1} 项的 path 必须是绝对路径")
    if resource_type == "hdfs" and any(
        segment in {".", ".."} for segment in request["path"].strip().split("/")
    ):
        raise SystemExit(
            f"requests-json 第 {index + 1} 项的 path 不能包含 . 或 .. 路径段"
        )
    if request["action"].strip().lower() not in {"read", "write"}:
        raise SystemExit(f"requests-json 第 {index + 1} 项的 action 不受支持")
PY

ODEP_API_URL="${ODEP_API_URL%/}"
response_file="$(mktemp "${TMPDIR:-/tmp}/odep-authz.XXXXXX")"
trap 'rm -f "$response_file"' EXIT

timestamp="$(date +%s)"
nonce="$(python3 - <<'PY'
import secrets
import string

alphabet = string.ascii_letters + string.digits
print("".join(secrets.choice(alphabet) for _ in range(16)))
PY
)"
signature="$(
  python3 - \
    "$ODEP_KYUUBI_APP_ID" \
    "$nonce" \
    "$timestamp" \
    "$subject" \
    "$requests_json" <<'PY'
import hashlib
import os
import sys

params = {
    "appId": sys.argv[1],
    "nonce": sys.argv[2],
    "timestamp": sys.argv[3],
    "subject": sys.argv[4],
    "requests": sys.argv[5],
    "appSignKey": os.environ["ODEP_KYUUBI_SIGN_KEY"],
}
payload = "&".join(f"{key}={params[key]}" for key in sorted(params))
print(hashlib.sha1(payload.encode()).hexdigest())
PY
)"

echo "验证 ODEP 批量资源权限: subject=$subject expected=$expected_result"
if ! http_status="$(
  curl -sS \
    --connect-timeout "$ODEP_CONNECT_TIMEOUT_SECONDS" \
    --max-time "$ODEP_REQUEST_TIMEOUT_SECONDS" \
    -o "$response_file" \
    -w '%{http_code}' \
    --data-urlencode "appId=$ODEP_KYUUBI_APP_ID" \
    --data-urlencode "nonce=$nonce" \
    --data-urlencode "timestamp=$timestamp" \
    --data-urlencode "sign=$signature" \
    --data-urlencode "subject=$subject" \
    --data-urlencode "requests=$requests_json" \
    "$ODEP_API_URL/api/queryone/authz/check"
)"; then
  echo "请求 ODEP 批量资源权限接口失败: $ODEP_API_URL" >&2
  exit 1
fi

if [[ "$http_status" != "200" ]]; then
  echo "ODEP 批量资源权限接口请求失败: HTTP=$http_status" >&2
  exit 1
fi

python3 - \
  "$response_file" \
  "$requests_json" \
  "$expected_result" <<'PY'
import json
import sys

response_path, requests_json, expected_result = sys.argv[1:]

try:
    with open(response_path, encoding="utf-8") as response_file:
        response = json.load(response_file)
except (OSError, json.JSONDecodeError) as error:
    raise SystemExit(f"ODEP authz 响应不是有效 JSON: {error}")

if not isinstance(response, dict):
    raise SystemExit("ODEP authz 响应根节点必须是 JSON 对象")
if response.get("code") != 200 or response.get("success") is not True:
    message = response.get("message", response.get("msg", "未知错误"))
    raise SystemExit(
        f"ODEP authz 业务请求失败: code={response.get('code')}, message={message}"
    )

result = response.get("results")
if not isinstance(result, dict):
    raise SystemExit("ODEP authz results 必须是对象")

expected_allowed = expected_result == "allow"
actual_allowed = result.get("allowed")
if actual_allowed is not expected_allowed:
    raise SystemExit(
        f"ODEP authz 总体结果不符合预期: expected={expected_allowed}, "
        f"actual={actual_allowed}"
    )

requests = json.loads(requests_json)
decisions = result.get("decisions")
if not isinstance(decisions, list) or len(decisions) != len(requests):
    raise SystemExit("ODEP authz decisions 数量与请求数量不一致")

denied_count = 0
for index, (request, decision) in enumerate(zip(requests, decisions), start=1):
    if not isinstance(decision, dict):
        raise SystemExit(f"ODEP authz 第 {index} 个 decision 必须是对象")
    resource_type = request["resourceType"].strip().lower()
    normalized_request = {
        "resourceType": resource_type,
        "action": request["action"].strip().lower(),
    }
    if resource_type == "hdfs":
        normalized_request["path"] = request["path"].strip().rstrip("/") or "/"
    else:
        normalized_request["database"] = request["database"].strip()
        normalized_request["table"] = request["table"].strip()
    decision_resource = decision.get("resource")
    if not isinstance(decision_resource, dict) or any(
        decision_resource.get(key) != value
        for key, value in normalized_request.items()
    ):
        raise SystemExit(f"ODEP authz 第 {index} 个 decision 与请求资源不一致")
    item_allowed = decision.get("allowed")
    if not isinstance(item_allowed, bool):
        raise SystemExit(f"ODEP authz 第 {index} 个 decision 缺少 allowed")
    reason = decision.get("reason")
    if item_allowed and reason != "MATCHED":
        raise SystemExit(f"ODEP authz 第 {index} 个允许结果 reason 非 MATCHED")
    if not item_allowed:
        denied_count += 1
        if reason not in {"BLACKLISTED", "NO_MATCHING_RESOURCE"}:
            raise SystemExit(f"ODEP authz 第 {index} 个拒绝结果 reason 不合法: {reason}")
    if resource_type == "hdfs":
        resource_name = normalized_request["path"]
    else:
        resource_name = (
            f"{normalized_request['database']}.{normalized_request['table']}"
        )
    print(
        f"- {resource_type}:{resource_name} action={normalized_request['action']} "
        f"allowed={item_allowed} reason={reason}"
    )

if actual_allowed != (denied_count == 0):
    raise SystemExit("ODEP authz 总体 allowed 与逐项 decisions 不一致")

print(
    f"ODEP 批量资源权限校验成功: allowed={actual_allowed}, "
    f"count={len(decisions)}, denied={denied_count}"
)
PY
