# Local References

本项目通过 `references/` 下的软链引用本地源码，不 vendor 这些仓库。

当前软链：

```text
references/mlsql  -> /Users/qindongliang/project/idea3/mlsql
references/kyuubi -> /Users/qindongliang/project/idea4/kyuubi
references/spark  -> /Users/qindongliang/project/idea3/fix/spark
```

这些软链已在 `.gitignore` 中忽略。

参考结论：

- MLSQL 有价值的是 SQL-like 用户体验，但旧实现 runtime 太重。
- Kyuubi 是 Spark SQL 网关，适合后续作为执行后端。
- Spark 源码中的 `SparkSqlParser` 是校验 Spark SQL 语法的优先参考。

关键源码参考：

- Spark grammar: `references/spark/sql/api/src/main/antlr4/org/apache/spark/sql/catalyst/parser/SqlBaseParser.g4`
- Spark parser: `references/spark/sql/core/src/main/scala/org/apache/spark/sql/execution/SparkSqlParser.scala`
- Kyuubi Spark SQL execution: `references/kyuubi/externals/kyuubi-spark-sql-engine/src/main/scala/org/apache/kyuubi/engine/spark/operation`
