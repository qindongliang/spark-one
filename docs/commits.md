# 提交与 PR 规范

SparkOne 默认使用中文提交信息，优先沿用当前仓库风格：

- `功能：...`
- `修复：...`
- `重构：...`
- `更新：...`
- `清理：...`

如果所在分支已有明确英文风格约束，可以兼容：

- `feat: ...`
- `fix: ...`
- `refactor: ...`
- `update: ...`
- `chore: ...`

## 提交格式

- 第一行写简洁中文标题，格式为 `类型：一句话概述`。
- 标题聚焦“做了什么 + 主要目的”，避免 `更新代码`、`优化逻辑` 这类空泛描述。
- 标题后空一行，再写正文

## 正文规则

- 按主题分类分段，分类名使用中文并带冒号。
- 每个分类下用短横线列出具体改动，说明改了什么、为什么改。
- 若改动影响配置、Hadoop/Hive 对接、Kerberos、Spark 版本、JAR 依赖、构建链路或兼容性，必须单独写一类说明。
- commit 正文不要写 `验证：` 分类。
- commit 正文不要写 `已执行 mvn test`、`已在 IDEA 启动验证` 这类验证执行项。
- 验证信息保留在 PR 描述或最终回复中，不放入 commit message。

SparkOne 常用分类：

- `DSL 与编译器：` 用于 `load/save/view` 语法、ANTLR grammar、SQL 转译。
- `运行时：` 用于 SparkSession、Safe Save、HDFS/Hive、Kerberos、执行保护。
- `配置与依赖：` 用于 HOCON 模板、Maven 依赖、JAR/packages/files、JVM 参数。
- `前端交互：` 用于 SQL 编辑器、语法高亮、结果展示、选中执行。
- `文档与测试：` 用于 docs、示例 SQL、单元测试或测试说明。

## 推荐示例

```text
功能：统一 load/save 参数语法为 options

DSL 与编译器：
- 将 SparkOne DSL 的参数子句统一为 options，移除 where 兼容分支。
- 增加旧式 load/save where 用法的编译期拦截，避免被误透传为原生 SQL。

前端交互：
- 将 options 加入 SQL 编辑器关键词高亮。

文档与测试：
- 更新编辑器测试示例和编译器单测，明确原生 SQL where 不受影响。
```

每次提交聚焦一个逻辑变更，例如 DSL/compiler、runtime、前端、配置、文档或测试。不要把无关重构、格式化和功能变更塞进同一次提交。

## PR 描述

- 改动内容与原因
- HOCON、JVM 参数、Hadoop/Hive、Kerberos、JAR 依赖或兼容性影响说明
- 涉及前端改动时附截图或说明页面验证点
- 手工验证步骤
