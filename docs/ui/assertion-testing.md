# Assert 测试用例

本文用于验证 SparkOne `assert` 在 Local 和 Kyuubi 引擎上的编译、执行、结果展示和
失败短路行为。

## 自动化测试

执行：

```bash
mvn -pl sparkone-server -am \
  -Dtest=SparkOneCompilerTest,StatementPolicyTest,SparkOneRuntimePreviewTest,SparkOneEngineTest \
  -Dsurefire.failIfNoSpecifiedTests=false test
```

覆盖范围：

| 编号 | 场景 | 预期 |
| --- | --- | --- |
| A01 | DSL 编译 | 生成 `NOT COALESCE(predicate, FALSE)` 违规查询和 `AssertionPlan`。 |
| A02 | Spark SQL 谓词 | 函数、时间表达式和脚本变量可用于谓词。 |
| A03 | 非法输入 | 空谓词、空消息、谓词内分号在编译期拒绝。 |
| A04 | 检查通过 | 状态为 `passed`，继续执行后续语句。 |
| A05 | 检查失败 | 状态为 `failed`，返回违规样本并停止后续语句。 |
| A06 | `NULL` 谓词 | 作为违规行处理。 |
| A07 | 空结果表 | 状态为 `passed`。 |
| A08 | 样本上限 | 最多返回请求上限行，多余行使 `truncated=true`。 |
| A09 | 查询异常 | 结果表不存在等执行错误状态为 `error`，与业务 `failed` 区分。 |
| A10 | Kyuubi 对等 | 使用同一违规 SQL，并具有相同通过、失败和短路语义。 |
| A11 | 内联 SELECT 编译 | 生成带 `sparkone_assert_input` 别名的子查询违规扫描。 |
| A12 | 嵌套 SQL | 内部函数、子查询、`WHERE`、字符串和脚本变量保持原始 Spark SQL 语义。 |
| A13 | 内联检查执行 | Local 上通过时继续，失败时返回样本并短路。 |
| A14 | 内联 Kyuubi 对等 | Kyuubi 执行与 Local 相同的违规 SQL 和失败语义。 |
| A15 | 非法内联输入 | 空查询、括号不匹配和原生文件 provider 路径在编译期拒绝。 |

## 页面测试准备

1. 按[应用启动方法](../ops/startup.md)启动服务。
2. 打开 `http://127.0.0.1:7070` 并登录开发用户。
3. `Rows` 先设置为 `1`，便于验证违规样本截断。
4. 分别选择 Local 和 Kyuubi 执行以下脚本。Kyuubi 需要远端 engine 已可执行普通
   Spark SQL。

## T01 单行指标通过

```sql
view order_metrics as
select 100 as row_count, 0 as null_count, 0 as duplicate_count;

assert order_metrics
where "row_count > 0 and null_count = 0 and duplicate_count = 0"
message "订单完整性检查失败";

select 'continued' as marker;
```

预期：

- 检查显示 `Check passed`。
- 整次 Run 成功。
- 最后一条查询执行并返回 `continued`。

## T02 失败、样本与短路

```sql
view partition_metrics as
select * from values
  ('2026-07-25', 120),
  ('2026-07-26', 0),
  ('2026-07-27', 0)
as metrics(dt, row_count);

assert partition_metrics
where "row_count > 0"
message "存在空分区";

select 'must-not-run' as marker;
```

预期：

- 检查显示 `Check failed` 和“存在空分区”。
- 只展示 `Rows` 上限内的违规分区；设置为 `1` 时 `truncated=true`。
- 整次 Run 失败，最后一条查询不执行。

## T03 NULL 谓词

```sql
view null_metrics as
select cast(null as bigint) as row_count;

assert null_metrics
where "row_count > 0"
message "row_count 不能为空";
```

预期：检查失败，违规样本中的 `row_count` 为 `NULL`。

## T04 空结果表

```sql
view empty_metrics as
select 1 as row_count where false;

assert empty_metrics
where "row_count > 0"
message "row_count 必须大于 0";
```

预期：检查通过。空表语义是“没有违规行”，不是“必须有数据”。

## T05 显式检查必须有数据

```sql
view source_data as
select 1 as id where false;

view source_metrics as
select count(*) as row_count
from source_data;

assert source_metrics
where "row_count > 0"
message "源数据不能为空";
```

预期：检查失败。这个用例说明非空要求应显式建模为一行指标，而不是改变空结果表
的通用语义。

## T06 明细合法性

```sql
view orders as
select * from values
  (1, 10.0, 'PAID'),
  (2, -1.0, 'UNKNOWN')
as orders(id, amount, status);

assert orders
where "amount >= 0 and status in ('PAID', 'REFUNDED')"
message "存在非法订单";
```

预期：检查失败，并返回 `id=2` 的明细作为违规样本。

## T07 跨表对账

```sql
view left_amounts as
select * from values ('A', 100.0), ('B', 50.0) as t(account, amount);

view right_amounts as
select * from values ('A', 100.0), ('B', 49.0) as t(account, amount);

view reconciliation as
select
  l.account,
  l.amount as left_amount,
  r.amount as right_amount
from left_amounts l
join right_amounts r on l.account = r.account;

assert reconciliation
where "left_amount = right_amount"
message "账户金额对账失败";
```

预期：检查失败，违规样本包含账户 `B`。

## T08 时效性和历史基线

```sql
view freshness_metrics as
select current_timestamp() - interval 2 hour as max_event_time;

assert freshness_metrics
where "max_event_time >= current_timestamp() - interval 1 hour"
message "数据延迟超过 1 小时";
```

预期：检查失败。历史波动场景采用同样方式：先用 SQL 将当前指标与基线表连接，再对
偏差和容忍度列执行 `assert`。

## T09 内联单行指标

```sql
assert (
  select
    count(*) as row_count,
    count_if(order_id is null) as null_count
  from values
    (1),
    (2)
  as orders(order_id)
)
where "row_count > 0 and null_count = 0"
message "订单完整性检查失败";

select 'continued' as marker;
```

预期：

- 检查显示 `Check passed · inline query`。
- 不创建临时视图。
- 后续查询正常执行。

## T10 内联多行检查失败

```sql
assert (
  select * from values
    ('2026-07-26', 10),
    ('2026-07-27', 0)
  as metrics(dt, row_count)
)
where "row_count > 0"
message "存在空分区";

select 'must-not-run' as marker;
```

预期：检查失败，违规样本包含 `2026-07-27`，最后一条查询不执行。

## T11 内联嵌套查询与变量

```sql
set minimum_rows = "2";

assert (
  select category, count(*) as row_count
  from (
    select * from values
      (1, 'A'),
      (2, 'A'),
      (3, 'B')
    as source_data(id, category)
    where id > 0
  ) filtered
  group by category
)
where "row_count >= ${minimum_rows}"
message "分类数据量不足";
```

预期：检查失败，违规样本包含分类 `B`；内层查询的括号和 `WHERE` 不会被识别为
SparkOne DSL 边界。

## API 验收

检查 `/api/run` 响应中的对应 statement：

- `assertion.status` 只能是 `passed`、`failed` 或 `error`。
- `failed` 时 `success=false`，`error` 等于 DSL 的 `message`。
- `error` 时 `error` 是实际的违规查询执行错误。
- `failed` 时后续 statement 不应出现在 `statements` 数组。
- `rows.size <= Rows`，存在更多违规行时 `truncated=true`。
- 内联查询保持现有响应结构，`assertion.table="inline query"`。
