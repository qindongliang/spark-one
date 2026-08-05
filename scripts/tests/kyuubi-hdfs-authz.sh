#!/usr/bin/env bash

set -euo pipefail

SPARKONE_URL="${SPARKONE_URL:-http://127.0.0.1:7070}"
SPARKONE_ENGINE="${SPARKONE_ENGINE:-kyuubi_local}"
SPARKONE_USERNAME="${SPARKONE_USERNAME:-}"
SPARKONE_WORKSPACE_ROOT="${SPARKONE_WORKSPACE_ROOT:-/public/odep/user}"
SPARKONE_SELF_RELATIVE_PATH="${SPARKONE_SELF_RELATIVE_PATH:-sparkone-authz-test/self-roundtrip}"
SPARKONE_SHARED_OWNER="${SPARKONE_SHARED_OWNER:-}"
SPARKONE_SHARED_RELATIVE_PATH="${SPARKONE_SHARED_RELATIVE_PATH:-}"
SPARKONE_SHARED_ABSOLUTE_PATH="${SPARKONE_SHARED_ABSOLUTE_PATH:-}"
SPARKONE_DENIED_ABSOLUTE_PATH="${SPARKONE_DENIED_ABSOLUTE_PATH:-}"
SPARKONE_SHARED_FORMAT="${SPARKONE_SHARED_FORMAT:-parquet}"

usage() {
  cat <<'EOF'
通过 SparkOne /api/run 验证 Kyuubi Engine 的 HDFS workspace 鉴权。

用法:
  SPARKONE_USERNAME=qindongliang \
  SPARKONE_SHARED_OWNER=firefly \
  SPARKONE_SHARED_RELATIVE_PATH=t.csv \
  SPARKONE_SHARED_ABSOLUTE_PATH=/public/odep/user/firefly/t.csv \
  SPARKONE_DENIED_ABSOLUTE_PATH=/public/odep/user/firefly/t1.csv \
  SPARKONE_SHARED_FORMAT=csv \
  scripts/tests/kyuubi-hdfs-authz.sh

必需环境变量:
  SPARKONE_USERNAME              RMS 真实用户名
  SPARKONE_SHARED_OWNER          已通过 RMS 向当前用户授权 read 的 workspace owner
  SPARKONE_SHARED_RELATIVE_PATH  owner workspace 下已存在的相对路径
  SPARKONE_DENIED_ABSOLUTE_PATH  已存在且当前用户在 RMS 中没有 read 权限的绝对 HDFS 路径

可选环境变量:
  SPARKONE_URL                   SparkOne 地址，默认 http://127.0.0.1:7070
  SPARKONE_ENGINE                Kyuubi engine，默认 kyuubi_local
  SPARKONE_WORKSPACE_ROOT        workspace 根目录，默认 /public/odep/user
  SPARKONE_SELF_RELATIVE_PATH    自有 workspace 测试目标，默认 sparkone-authz-test/self-roundtrip
  SPARKONE_SHARED_ABSOLUTE_PATH  原生 relation 读取路径，默认由 root/owner/relative path 拼接
  SPARKONE_SHARED_FORMAT         共享数据格式，默认 parquet

注意:
  脚本会 overwrite 当前用户的 SPARKONE_SELF_RELATIVE_PATH，请只在测试目录执行。
  跨 owner 路径必须预先存在，并在 RMS 配置 hdfs read 权限。
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

for command_name in curl python3; do
  if ! command -v "$command_name" >/dev/null 2>&1; then
    echo "缺少命令: $command_name" >&2
    exit 1
  fi
done

for variable_name in \
  SPARKONE_USERNAME \
  SPARKONE_SHARED_OWNER \
  SPARKONE_SHARED_RELATIVE_PATH \
  SPARKONE_DENIED_ABSOLUTE_PATH; do
  if [[ -z "${!variable_name}" ]]; then
    echo "必须设置 $variable_name" >&2
    exit 1
  fi
done

if [[ ! "$SPARKONE_USERNAME" =~ ^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$ ]]; then
  echo "SPARKONE_USERNAME 格式无效" >&2
  exit 1
fi

if [[ ! "$SPARKONE_SHARED_OWNER" =~ ^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$ ]]; then
  echo "SPARKONE_SHARED_OWNER 格式无效" >&2
  exit 1
fi

if [[ "$SPARKONE_SHARED_OWNER" == "$SPARKONE_USERNAME" ]]; then
  echo "SPARKONE_SHARED_OWNER 必须与 SPARKONE_USERNAME 不同" >&2
  exit 1
fi

if [[ ! "$SPARKONE_ENGINE" =~ ^[A-Za-z0-9._-]+$ ]]; then
  echo "SPARKONE_ENGINE 格式无效" >&2
  exit 1
fi

case "$SPARKONE_SHARED_FORMAT" in
  parquet|csv|json|orc|text|libsvm|binaryfile|excel) ;;
  *)
    echo "SPARKONE_SHARED_FORMAT 不是受支持的只读文件 provider" >&2
    exit 1
    ;;
esac

python3 - \
  "$SPARKONE_WORKSPACE_ROOT" \
  "$SPARKONE_SELF_RELATIVE_PATH" \
  "$SPARKONE_SHARED_RELATIVE_PATH" \
  "$SPARKONE_SHARED_ABSOLUTE_PATH" \
  "$SPARKONE_DENIED_ABSOLUTE_PATH" <<'PY'
import sys

root, self_relative, shared_relative, shared_absolute, denied_absolute = sys.argv[1:]

def validate_relative(name, value):
    parts = value.split("/")
    if (not value or value.startswith("/") or "`" in value or "\\" in value or
            any(ord(char) < 32 for char in value) or
            any(not part or part in {".", ".."} for part in parts)):
        raise SystemExit(f"{name} 必须是可安全写入 SQL 的 workspace 相对路径")

def validate_absolute(name, value):
    if (not value.startswith("/") or "`" in value or "'" in value or "\\" in value or
            any(ord(char) < 32 for char in value)):
        raise SystemExit(f"{name} 必须是无 authority 的绝对 HDFS 路径")
    if any(part in {".", ".."} for part in value.split("/")):
        raise SystemExit(f"{name} 不能包含 . 或 ..")

validate_absolute("SPARKONE_WORKSPACE_ROOT", root)
validate_relative("SPARKONE_SELF_RELATIVE_PATH", self_relative)
validate_relative("SPARKONE_SHARED_RELATIVE_PATH", shared_relative)
if shared_absolute:
    validate_absolute("SPARKONE_SHARED_ABSOLUTE_PATH", shared_absolute)
validate_absolute("SPARKONE_DENIED_ABSOLUTE_PATH", denied_absolute)
PY

SPARKONE_URL="${SPARKONE_URL%/}"
SPARKONE_WORKSPACE_ROOT="${SPARKONE_WORKSPACE_ROOT%/}"
if [[ -z "$SPARKONE_SHARED_ABSOLUTE_PATH" ]]; then
  SPARKONE_SHARED_ABSOLUTE_PATH="$SPARKONE_WORKSPACE_ROOT/$SPARKONE_SHARED_OWNER/$SPARKONE_SHARED_RELATIVE_PATH"
fi

COOKIE_FILE="$(mktemp "${TMPDIR:-/tmp}/sparkone-hdfs-cookie.XXXXXX")"
REQUEST_FILE="$(mktemp "${TMPDIR:-/tmp}/sparkone-hdfs-request.XXXXXX")"
RESPONSE_FILE="$(mktemp "${TMPDIR:-/tmp}/sparkone-hdfs-response.XXXXXX")"
trap 'rm -f "$COOKIE_FILE" "$REQUEST_FILE" "$RESPONSE_FILE"' EXIT

python3 - "$SPARKONE_USERNAME" >"$REQUEST_FILE" <<'PY'
import json
import sys

json.dump({"username": sys.argv[1]}, sys.stdout)
PY

echo "登录 SparkOne: username=$SPARKONE_USERNAME engine=$SPARKONE_ENGINE"
curl -fsS -c "$COOKIE_FILE" \
  "$SPARKONE_URL/api/login" \
  -H 'Content-Type: application/json' \
  --data-binary "@$REQUEST_FILE" \
  >/dev/null

run_case() {
  local label="$1"
  local expectation="$2"
  local expected_fragment="$3"
  local sql_text="$4"
  local http_code

  python3 - "$SPARKONE_ENGINE" "$sql_text" >"$REQUEST_FILE" <<'PY'
import json
import sys

json.dump({
    "engine": sys.argv[1],
    "sessionMode": "run_isolated",
    "script": sys.argv[2],
    "limit": 10,
}, sys.stdout)
PY

  http_code="$(curl -sS -o "$RESPONSE_FILE" -w '%{http_code}' -b "$COOKIE_FILE" \
    "$SPARKONE_URL/api/run" \
    -H 'Content-Type: application/json' \
    --data-binary "@$REQUEST_FILE")"

  python3 - "$label" "$expectation" "$expected_fragment" "$http_code" "$RESPONSE_FILE" <<'PY'
import json
import sys

label, expectation, expected_fragment, http_code, response_file = sys.argv[1:]
try:
    with open(response_file, encoding="utf-8") as stream:
        payload = json.load(stream)
except Exception as error:
    raise SystemExit(f"[{label}] 响应不是合法 JSON: HTTP {http_code}: {error}")

messages = []
if payload.get("error"):
    messages.append(str(payload["error"]))
for statement in payload.get("statements") or []:
    if statement.get("error"):
        messages.append(str(statement["error"]))
message = "\n".join(messages)

if expectation == "allow":
    if not http_code.startswith("2") or payload.get("success") is not True:
        raise SystemExit(
            f"[{label}] 预期成功，实际 HTTP {http_code}: " +
            json.dumps(payload, ensure_ascii=False))
elif expectation == "deny":
    if payload.get("success") is not False:
        raise SystemExit(
            f"[{label}] 预期拒绝，实际 HTTP {http_code}: " +
            json.dumps(payload, ensure_ascii=False))
    if expected_fragment and expected_fragment.lower() not in message.lower():
        raise SystemExit(
            f"[{label}] 拒绝原因缺少 {expected_fragment!r}: " +
            json.dumps(payload, ensure_ascii=False))
else:
    raise SystemExit(f"[{label}] 未知预期: {expectation}")

print(f"[{label}] 通过: expectation={expectation} HTTP={http_code}")
PY
}

self_sql="$(cat <<EOF
view sparkone_hdfs_authz_seed as
select * from values
  (1L, '$SPARKONE_USERNAME'),
  (2L, '$SPARKONE_USERNAME')
as sparkone_hdfs_authz_seed(id, owner_name);

save overwrite sparkone_hdfs_authz_seed
as parquet.\`$SPARKONE_SELF_RELATIVE_PATH\`;

load parquet.\`$SPARKONE_SELF_RELATIVE_PATH\` as sparkone_hdfs_authz_self;

select count(*) as row_count
from sparkone_hdfs_authz_self;
EOF
)"
run_case "H01 自有 workspace 读写" "allow" "" "$self_sql"

shared_load_sql="$(cat <<EOF
load $SPARKONE_SHARED_FORMAT.\`$SPARKONE_SHARED_RELATIVE_PATH\`
options owner="$SPARKONE_SHARED_OWNER"
as sparkone_hdfs_authz_shared;

select * from sparkone_hdfs_authz_shared limit 10;
EOF
)"
run_case "H02 跨 owner load 走 RMS read" "allow" "" "$shared_load_sql"

native_read_sql="select * from $SPARKONE_SHARED_FORMAT.\`$SPARKONE_SHARED_ABSOLUTE_PATH\` limit 10;"
run_case "H03 原生绝对路径读取走 RMS read" "allow" "" "$native_read_sql"

denied_read_sql="select * from $SPARKONE_SHARED_FORMAT.\`$SPARKONE_DENIED_ABSOLUTE_PATH\` limit 10;"
run_case \
  "H04 RMS 未授权路径拒绝" \
  "deny" \
  "Resource access denied: hdfs:$SPARKONE_DENIED_ABSOLUTE_PATH:read" \
  "$denied_read_sql"

save_owner_sql="$(cat <<EOF
view sparkone_hdfs_authz_save_owner as select 1L as id;
save overwrite sparkone_hdfs_authz_save_owner
as parquet.\`sparkone-authz-test/save-owner-denied\`
options owner="$SPARKONE_SHARED_OWNER";
EOF
)"
run_case "H05 save owner 拒绝" "deny" "option is not allowed" "$save_owner_sql"

native_write_sql="$(cat <<EOF
insert overwrite directory '$SPARKONE_WORKSPACE_ROOT/$SPARKONE_USERNAME/sparkone-authz-test/native-write-denied'
using parquet
select 1L as id;
EOF
)"
run_case "H06 SparkOne 原生路径写入拒绝" "deny" "only allows native read-only SQL" "$native_write_sql"

echo "HDFS 鉴权测试完成。请同时核对 Engine/ODEP 日志中的 subject、path、action 和请求次数。"
