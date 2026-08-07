# Local References

本项目通过 `references/` 下的软链引用本地源码，不 vendor 这些仓库。

当前软链：

```text
references/mlsql  -> /Users/qindongliang/project/idea3/mlsql
references/kyuubi -> /Users/qindongliang/project/idea4/kyuubi
references/spark  -> /Users/qindongliang/project/idea3/fix/spark
references/rms    -> /Users/qindongliang/project/idea3/rms
references/odep-web    -> /Users/qindongliang/project/idea3/odep-web
references/odep-system -> /Users/qindongliang/project/idea3/odep-system
```

这些软链已在 `.gitignore` 中忽略。

参考结论：

- MLSQL 有价值的是 SQL-like 用户体验，但旧实现 runtime 太重。
- Kyuubi 是 Spark SQL 网关，适合后续作为执行后端。
- Spark 源码中的 `SparkSqlParser` 是校验 Spark SQL 语法的优先参考。
- RMS 是旧平台登录与跳转入口，核心是把用户带到 ODEP Web 数据平台。
- ODEP Web 是旧计算平台前端，可参考页面组织、交互和 SQL 编辑体验。
- ODEP System 是旧计算平台后端服务，可参考任务提交链路，以及通过 MLSQL 提交 Spark 计算任务的对接边界。

旧平台链路：

```text
rms -> odep-web -> odep-system -> mlsql -> spark
```

也就是从 RMS 登录跳转到 ODEP Web，再由 ODEP System 访问 MLSQL 提交 Spark 计算任务。

QueryOne 的目标不是复制旧链路，而是把可取的数据平台体验收敛到更轻、更原生适配 Spark SQL 的路线：

```text
queryone -> kyuubi -> spark
```

关键源码参考：

- Spark grammar: `references/spark/sql/api/src/main/antlr4/org/apache/spark/sql/catalyst/parser/SqlBaseParser.g4`
- Spark parser: `references/spark/sql/core/src/main/scala/org/apache/spark/sql/execution/SparkSqlParser.scala`
- Kyuubi Spark SQL execution: `references/kyuubi/externals/kyuubi-spark-sql-engine/src/main/scala/org/apache/kyuubi/engine/spark/operation`
