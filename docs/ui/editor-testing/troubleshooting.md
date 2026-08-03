# Compile、Run 与常见问题

## Compile 和 Run 的使用建议

- 写普通 Spark SQL 时，通常直接点 `Run`。
- 写 `load/save` 时，如已打开 `server.showCompiledSql = true`，可先点 `Compile` 确认转译出来的 Spark SQL 符合预期，再点 `Run`。
- 多条语句调试时，先把前置建表/建视图语句和最后查询语句放在同一次脚本里。
- 查询大表时先加 `limit`，并保持 `preview.maxRows` 和结果区 `Rows` 在较小值；默认是 10 行。
- 保存数据前先用 `select count(*)` 或抽样查询确认临时视图内容。

## 常见问题

`Run` 成功但没有表格结果：

- 原生 DDL/DML 会在 Compile 阶段拒绝；SparkOne 页面只允许查询、只读检查命令以及受控 `load/view/set/save`。

`Compile` 成功但 `Run` 失败：

- 常见原因是文件路径不存在、HDFS/Hive 权限不足、provider jar 未加载，或 Spark runtime 不支持对应数据源。

`load hive...` 带 options 报错：

- 当前 `hive` 是 catalog 表语义，不支持 `options` 参数。

`show database in hive` 报 Spark SQL 解析错误：

- `SHOW DATABASE` 没有单数形式。使用 `show namespaces in hive`，或者使用 `show databases in hive`。

临时视图查不到：

- 确认创建视图和查询视图在同一个服务进程内执行。
- 如果刚重启过服务，需要重新执行创建视图语句。

SQL 里有注释：

```sql
-- 单行注释
select 1;

/* 块注释 */
select 2;
```

注释会被 SparkOne DSL parser 忽略，不会作为独立语句执行。
