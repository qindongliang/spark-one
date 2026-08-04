# SQL 编辑器测试手册

测试内容按职责拆分为以下子手册：

- [编辑器基础测试](editor-testing/basics.md)：页面区域、冒烟测试、Session、脚本变量、原生 SQL 和 `view`。
- [Load 文件与 Hive 测试](editor-testing/load.md)：CSV、Parquet、JSON、Hive 和受控 workspace 路径。
- [Doris 测试](editor-testing/doris.md)：Local Catalog、读取、过滤和不同表模型的 append 语义。
- [MySQL 测试](editor-testing/mysql.md)：Local 数据源、Catalog、过滤和大表分区读取。
- [Kyuubi 数据源测试](editor-testing/kyuubi.md)：三段式 Catalog、ODEP、静态数据源和 `sparkone_mysql` provider。
- [ODEP 库表鉴权测试](editor-testing/odep-authz.md)：RMS 授权准备、批量接口和 Spark Engine 端到端鉴权。
- [Save 测试](editor-testing/save.md)：写入安全策略、受控 HDFS overwrite 和 Hive append。
- [HDFS、Hive 和 Excel 测试](editor-testing/storage.md)：外部依赖、运行环境和 provider 验证。
- [Compile、Run 与常见问题](editor-testing/troubleshooting.md)：操作建议和常见错误排查。
- [Assert 测试用例](assertion-testing.md)：Local/Kyuubi 数据质量断言、失败短路和常见场景。
