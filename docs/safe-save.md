# Safe Save

SparkOne 对 `save overwrite` 默认做保护，避免路径写错或表写错时直接覆盖已有数据。

SparkOne DSL 编译出来的文件类 `save overwrite ... as provider.\`path\`` 会走 Safe Save 备份与保护流程。`save overwrite ... as hive.\`db.table\`` 和 `save overwrite ... as doris.\`db.table\`` 会走 catalog 表覆盖确认流程，但不会做文件目录备份。

用户直接写 Spark 原生 `INSERT OVERWRITE ...` 不携带 SparkOne save metadata，默认会被拦截，避免绕过 Safe Save。

## 配置来源

`save { ... }` 下的策略参数只从启动 HOCON 或启动属性读取，不允许被页面里的 `SET sparkone.save...` 或单条 SQL `options` 覆盖。

唯一保留的单条 SQL 信号是：

- `sparkoneOverwrite`

它只在 `overwritePolicy = "requireExplicit"` 时表示“本条 overwrite 已确认”，不会改变 HOCON 中的覆盖策略，也不能绕过 `overwritePolicy = "deny"`、`allowMysqlOverwrite = false`、`allowDorisOverwrite = false` 或 `overwriteProtectedPaths`。

下面这些参数必须写在 HOCON `save` 中，不允许写在 SQL 里：

- `overwritePolicy`
- `overwriteBackup`
- `overwriteBackupPath`
- `overwriteProtectedPaths`
- `allowMysqlOverwrite`
- `allowDorisOverwrite`
- `allowNativeInsertOverwrite`
- `allowNativeDropTable`

## 后端日志观察

Safe Save 的后端日志统一使用 `Safe Save:` 前缀，默认 `server.logLevel = "info"` 时可以在 IDEA 控制台看到。

典型日志包括：

- `plan overwrite`：开始检查，包含 source table、format、原始 path、解析后的 writeTarget、FileSystem、目标是否存在、exists 耗时、策略和来源。
- `backup success`：目标存在且备份完成，包含 `rename` / `trash` / `none`、备份路径或 Trash 目录、备份耗时。
- `commit success`：Spark 写入成功后的收尾日志。
- `rollback restored backup`：写入失败时恢复了 rename 备份。
- `catalog overwrite allowed`：Hive/catalog 表 overwrite 已通过显式确认；该类目标不会执行文件备份。

策略来源说明：

- `statement-confirmation`：来自单条 `save` 的 `sparkoneOverwrite="allow"`，只表示显式确认。
- `global`：来自 HOCON 或启动参数。
- `default`：没有配置时使用 SparkOne 默认值。

## 事务性边界

Safe Save 不是严格的分布式事务，它提供的是覆盖写前的保护流程。

文件类 save：

1. 先解析目标路径和文件系统。
2. 如果目标不存在，直接放行 Spark 写入。
3. 如果目标存在且启用 `rename`，必须先把旧目录成功移动到配置的 `overwriteBackupPath`，然后才会执行 Spark save。
4. 如果目标存在且启用 `trash`，必须先成功移动到 Hadoop Trash，然后才会执行 Spark save。
5. 如果备份或移动失败，本次 Spark save 不会执行。

`rename` 模式下，如果 Spark save 后续失败，SparkOne 会尝试把备份恢复回原路径。但这仍不是强事务保证：如果恢复过程中遇到文件系统异常、权限问题、进程被杀、NameNode 故障等情况，可能需要人工根据日志里的 backup 路径处理。

因此可以把 Safe Save 理解为“先备份成功，再执行覆盖写；失败时尽力回滚”，而不是数据库意义上的原子提交。

Hive/catalog 表、Doris 和 MySQL save：

1. `save append/overwrite` 写 Hive、Doris、MySQL 都要求目标表已存在；SparkOne 不自动创建表。
2. `save overwrite ... as hive.\`db.table\`` 默认同样要求显式 `sparkoneOverwrite="allow"`。
3. `save overwrite ... as mysql.\`connection.table\`` 默认被 `save.allowMysqlOverwrite = false` 拦截。确需覆盖时，必须先在 HOCON 打开 `save.allowMysqlOverwrite = true`，然后仍要在单条语句里显式 `sparkoneOverwrite="allow"`。
4. `save overwrite ... as doris.\`db.table\`` 默认被 `save.allowDorisOverwrite = false` 拦截。确需覆盖时，必须先在 HOCON 打开 `save.allowDorisOverwrite = true`，然后仍要在单条语句里显式 `sparkoneOverwrite="allow"`。
5. Hive 通过确认后编译成 Spark 原生 `INSERT OVERWRITE TABLE db.table SELECT ...`；Doris 编译成 Spark Doris Catalog SQL `INSERT OVERWRITE TABLE doris.db.table SELECT ...`；MySQL 通过 Spark JDBC writer 执行。
6. SparkOne 不对 Hive/Doris/MySQL 表做文件目录 `rename/trash` 备份，也不承诺回滚；具体提交和失败语义由 Spark/Hive catalog、Spark Doris Connector 或 MySQL 负责。
7. `allowNativeInsertOverwrite` 只控制用户直接写的原生 `INSERT OVERWRITE`，不能替代 `allowDorisOverwrite`；SparkOne DSL 的 `save overwrite ... as doris` 携带 save metadata，会进入 Doris 专属覆盖写策略。
8. 如果要建表、改表结构、指定存储格式、管理 MySQL 索引或 Doris key/distribution，使用 Spark 原生 `CREATE TABLE` / `ALTER TABLE` 或数据库 DDL，不要放进 SparkOne `save` 的 provider options。
9. Spark JDBC 的 MySQL overwrite 不是稳定等价于“先 truncate 再 insert”；即使写了 `truncate="true"`，也可能受 schema、dialect 和 Spark 行为影响，所以 SparkOne 默认拦截。

## HOCON 全局开关

推荐默认配置：

```hocon
save {
  overwritePolicy = "requireExplicit"
  overwriteBackup = "rename"
  overwriteBackupPath = "/tmp/sparkone_back"
  allowMysqlOverwrite = false
  allowDorisOverwrite = false
  allowNativeInsertOverwrite = false
  allowNativeDropTable = false
}
```

生产环境可以额外配置全局保护的 overwrite 边界目录：

```hocon
save {
  overwriteProtectedPaths = [
    "/",
    "/user",
    "/tmp",
  ]
}
```

`overwritePolicy`：

- `requireExplicit`：默认值，每条 `save overwrite` 都要显式确认。
- `allow`：全局允许覆盖。
- `deny`：全局拒绝覆盖；单条 SQL 的 `sparkoneOverwrite="allow"` 不能绕过。

`overwriteBackup`：

- `rename`：默认值，目标存在时先移动到 `overwriteBackupPath`，写失败时尝试恢复。
- `trash`：目标存在时先移动到 Hadoop Trash，不做自动恢复。
- `none`：不备份，直接交给 Spark 覆盖，生产环境不建议使用。
- 该配置只对文件类 save 生效；Hive/catalog 和 MySQL 表 save 不做目录备份。

`overwriteBackupPath`：

- 默认值是 `/tmp/sparkone_back`。
- 不带 scheme 的路径会按目标文件系统解析：写 HDFS 时是 HDFS 路径，写 `file://` 时是本地路径。
- 显式带 scheme 时必须和目标路径在同一个 FileSystem 上；跨 HDFS/local 的 rename 会被拒绝。

`allowNativeInsertOverwrite`：

- 默认值是 `false`，表示禁止原生 Spark SQL `INSERT OVERWRITE`。
- SparkOne DSL 的 `save overwrite` 不受这个开关影响，因为它携带 save metadata，会先进入 Safe Save。
- 如果历史脚本必须继续执行原生 `INSERT OVERWRITE TABLE ...` 或 `INSERT OVERWRITE DIRECTORY ...`，可以临时设为 `true`。
- 打开后，原生 `INSERT OVERWRITE` 由 Spark 直接执行，不会走 SparkOne 的备份、保护路径和回滚逻辑。

`allowNativeDropTable`：

- 默认值是 `false`，表示禁止原生 Spark SQL `DROP TABLE`。
- 这个配置只从启动 HOCON / 启动属性读取，不允许被页面里的 `SET` 或单条 SQL 参数覆盖。
- 如果历史脚本必须继续执行 `DROP TABLE`，需要在启动配置里显式改为 `true` 并重启服务。
- 打开后，`DROP TABLE` 由 Spark/Hive catalog 直接执行，SparkOne 不做表数据备份或回滚。

`allowMysqlOverwrite`：

- 默认值是 `false`，表示禁止 `save overwrite ... as mysql.\`connection.table\``。
- 这个配置只从启动 HOCON / 启动属性读取，不允许被页面里的 `SET` 或单条 SQL 参数覆盖。
- 打开为 `true` 后，MySQL overwrite 仍然需要现有 Safe Save 显式确认，例如 `options sparkoneOverwrite="allow"`。
- 打开后，具体是 truncate 还是 drop/recreate 由 Spark JDBC、MySQL dialect、目标表结构和写入 schema 决定；SparkOne 不做 MySQL 表备份或回滚。

`allowDorisOverwrite`：

- 默认值是 `false`，表示禁止 `save overwrite ... as doris.\`db.table\``。
- 这个配置只从启动 HOCON / 启动属性读取，不允许被页面里的 `SET` 或单条 SQL 参数覆盖。
- 打开为 `true` 后，Doris overwrite 仍然需要现有 Safe Save 显式确认，例如 `options sparkoneOverwrite="allow"`。
- 打开后，SparkOne 会提交 Spark Doris Catalog 的 `INSERT OVERWRITE TABLE doris.db.table SELECT ...`；覆盖、提交和失败语义由 Spark Doris Connector / Doris 负责，SparkOne 不做 Doris 表备份或回滚。

`overwriteProtectedPaths`：

- 用于全局保护危险的 `save overwrite` 边界路径。
- HOCON 中推荐用数组配置，一行一个路径。
- 不带 scheme 的路径会按 save 目标所在文件系统解析。
- 命中规则是“等于该目录，或者目标路径是该目录的上级目录”。
- 配置 `/public/odep/user` 后，会拦截 `/`、`/public`、`/public/odep`、`/public/odep/user`，但允许 `/public/odep/user/userA`、`/public/odep/user/userB`。
- 配置 `/tmp` 后，会拦截 `/tmp` 本身，但允许 `/tmp/sparkone_test` 这类更具体目录。
- 支持整段通配符 `*`：`/*` 保护所有一级目录，`/*/*` 保护所有一级和二级目录，`/public/*/user` 保护 `/public` 下任意租户的 `user` 边界目录。
- 多条规则之间是“任意命中即拒绝”。这里的命中指当前 overwrite 目标等于某条保护边界，或者是某条保护边界的上级目录。
- 只校验文件类 `save overwrite` 的目标路径，不校验 Hive/catalog、Doris 或 MySQL 表名，也不校验 `overwriteBackupPath`。如果生产环境也不希望备份写到 `/tmp`，需要同时把 `overwriteBackupPath` 改到安全目录。
- 这个配置是启动级硬拦截，不允许被 `options sparkoneOverwrite="allow"` 或 `set sparkone.save...` 覆盖。

规则语义速查：

- `/public/odep/user`：保护该边界和它的上级目录，会拦截 `/`、`/public`、`/public/odep`、`/public/odep/user`，允许 `/public/odep/user/userA`。
- `/*`：保护所有一级目录，比如 `/hive`、`/tmp`、`/user`。
- `/*/*`：保护所有一级和二级目录，比如 `/public`、`/public/odep`，允许 `/public/odep/userA`。
- 多条规则是“任意命中即拒绝”，没有 allow 规则覆盖，避免规则冲突变复杂。

注意：`trash` 只支持非本地文件系统。`file://` 本地路径在 HOCON 中配置 `overwriteBackup = "trash"` 会被 SparkOne 拦截并报错；本地路径建议用 `rename`。

下面的功能测试案例默认使用 `/tmp/...` 作为临时路径；按 `overwriteProtectedPaths` 语义，配置 `/tmp` 只会拦截覆盖 `/tmp` 本身，不影响 `/tmp/sparkone_xxx` 测试目录。

也可以用启动参数临时覆盖：

```bash
mvn exec:java \
  -Dexec.mainClass=ai.sparkone.server.SparkOneServer \
  -Dexec.args="--save-overwrite-policy requireExplicit --save-overwrite-backup rename --save-overwrite-backup-path /tmp/sparkone_back"
```

## 案例 1：默认拒绝 overwrite

默认 `overwritePolicy = "requireExplicit"` 时，即使目标路径不存在，也要求显式确认。

```sql
view safe_save_default_block as
select * from values
  (1, 'first')
as safe_save_default_block(id, name);

save overwrite safe_save_default_block
as parquet.`file:///tmp/sparkone_safe_save_default_block`;
```

预期：执行失败，提示需要添加 `sparkoneOverwrite="allow"`。

## 案例 1.1：默认拒绝原生 INSERT OVERWRITE

原生 Spark SQL 的 `INSERT OVERWRITE` 默认会被 SparkOne 拦截：

```sql
insert overwrite directory '/tmp/sparkone_native_insert_overwrite'
using parquet
select 1 as id;
```

预期：执行失败，提示 `Native Spark SQL INSERT OVERWRITE is disabled`。

如果确实要兼容已有 Spark SQL 脚本，需要在 `conf/sparkone.conf` 中显式打开：

```hocon
save {
  allowNativeInsertOverwrite = true
}
```

注意：打开后原生 `INSERT OVERWRITE` 不走 Safe Save。生产环境更推荐把写出语句改成 SparkOne DSL：

```sql
view native_insert_replacement as
select 1 as id;

save overwrite native_insert_replacement
as parquet.`/tmp/sparkone_native_insert_overwrite`
options sparkoneOverwrite="allow";
```

## 案例 1.1.1：默认拒绝原生 DROP TABLE

原生 Spark SQL 的 `DROP TABLE` 默认会被 SparkOne 拦截：

```sql
drop table default.sparkone_danger_table;
```

预期：执行失败，提示 `Native Spark SQL DROP TABLE is disabled`。

页面里执行下面的 `SET` 也不会放开：

```sql
set sparkone.save.native.dropTable.enabled=true;

drop table default.sparkone_danger_table;
```

如果确实要兼容已有删除表脚本，需要在 `conf/sparkone.conf` 中显式打开并重启服务：

```hocon
save {
  allowNativeDropTable = true
}
```

## 案例 1.2：Hive 表 overwrite 也需要显式确认

Hive/catalog 表 save 不走文件目录备份，但 overwrite 仍然需要确认：

```sql
create table if not exists default.sparkone_safe_hive_target (
  id int
) using parquet;

view safe_hive_result as
select 1 as id;

save overwrite safe_hive_result
as hive.`default.sparkone_safe_hive_target`;
```

预期：执行失败，提示需要添加 `sparkoneOverwrite="allow"`。

显式确认后可以执行：

```sql
view safe_hive_result as
select 2 as id;

save overwrite safe_hive_result
as hive.`default.sparkone_safe_hive_target`
options sparkoneOverwrite="allow";

select * from default.sparkone_safe_hive_target;
```

预期：写入成功，表中结果为 `2`。日志会出现 `catalog overwrite allowed`，并说明 catalog 目标不会做文件备份。

## 案例 1.3：Hive append 和动态分区

`append` 不属于覆盖写，不需要 `sparkoneOverwrite`：

```sql
create table if not exists default.sparkone_safe_hive_dt (
  id int
)
using parquet
partitioned by (dt string);

view safe_hive_partition_result as
select 1 as id, "2026-06-10" as dt;

save append safe_hive_partition_result
as hive.`default.sparkone_safe_hive_dt`
partitionBy dt;

select * from default.sparkone_safe_hive_dt;
```

`partitionBy dt` 会编译成 Spark SQL 的动态分区 `PARTITION (dt)`；分区列必须在源视图字段里。

## 案例 2：单条语句显式允许

给当前 `save` 增加 `sparkoneOverwrite="allow"` 后可以执行。

```sql
view safe_save_allow_once as
select * from values
  (1, 'first')
as safe_save_allow_once(id, name);

save overwrite safe_save_allow_once
as parquet.`file:///tmp/sparkone_safe_save_allow_once`
options sparkoneOverwrite="allow";

load parquet.`file:///tmp/sparkone_safe_save_allow_once`
as safe_save_allow_once_read;

select * from safe_save_allow_once_read;
```

这个路径第一次不存在时不会产生备份。

## 案例 3：目标已存在时 rename 备份

第二次写同一路径时，默认 `rename` 会把原目录移动到 `/tmp/sparkone_back`。

```sql
view safe_save_backup_v1 as
select * from values
  (1, 'v1')
as safe_save_backup_v1(id, version);

save overwrite safe_save_backup_v1
as parquet.`file:///tmp/sparkone_safe_save_backup`
options sparkoneOverwrite="allow";

view safe_save_backup_v2 as
select * from values
  (1, 'v2')
as safe_save_backup_v2(id, version);

save overwrite safe_save_backup_v2
as parquet.`file:///tmp/sparkone_safe_save_backup`
options sparkoneOverwrite="allow";

load parquet.`file:///tmp/sparkone_safe_save_backup`
as safe_save_backup_current;

select * from safe_save_backup_current;
```

预期：当前结果是 `v2`。旧目录会被移动到：

```text
file:///tmp/sparkone_back/sparkone_safe_save_backup_yyyyMMddHHmmss
```

如果后续 Spark 写入失败，SparkOne 会尝试把这个备份恢复回原路径。

如果要调整备份根目录，修改 `conf/sparkone.conf` 并重启服务：

```hocon
save {
  overwriteBackup = "rename"
  overwriteBackupPath = "file:///tmp/my_sparkone_backups"
}
```

## 案例 4：SET 不能临时放开 save 策略

页面里执行 `SET sparkone.save...` 不会改变 `save { ... }` 策略；下面这个案例仍然会失败，因为覆盖写策略只从启动 HOCON / 启动属性读取。

```sql
set sparkone.save.overwrite.policy=allow;

view safe_save_session_block as
select * from values
  (1, 'session')
as safe_save_session_block(id, scope);

save overwrite safe_save_session_block
as parquet.`file:///tmp/sparkone_safe_save_session_block`;
```

预期：执行失败，提示需要添加 `sparkoneOverwrite="allow"` 或在 HOCON 中调整 `overwritePolicy`。

## 案例 5：单条确认不能绕过全局 deny

如果 HOCON 中配置了 `overwritePolicy = "deny"`，单条 SQL 的 `sparkoneOverwrite="allow"` 也不能覆盖它：

```hocon
save {
  overwritePolicy = "deny"
}
```

```sql
view safe_save_global_deny as
select * from values
  (1, 'deny')
as safe_save_global_deny(id, flag);

save overwrite safe_save_global_deny
as parquet.`file:///tmp/sparkone_safe_save_global_deny`
options sparkoneOverwrite="allow";
```

预期：执行失败，提示 overwrite 被 SparkOne policy 拒绝。

## 案例 6：使用 Hadoop Trash

如果希望已有目录进入 Hadoop Trash，而不是保留在 `overwriteBackupPath`，可以设置 `trash`。

```hocon
save {
  overwriteBackup = "trash"
}
```

```sql
view safe_save_trash as
select * from values
  (1, 'trash')
as safe_save_trash(id, backup_mode);

save overwrite safe_save_trash
as parquet.`hdfs:///tmp/sparkone_safe_save_trash`
options sparkoneOverwrite="allow";
```

注意：能否恢复取决于 Hadoop Trash 配置和保留时间。

如果路径是 `file:///tmp/...`，不要使用 `trash`，SparkOne 会给出明确错误提示。写本地文件时使用：

```hocon
save {
  overwriteBackup = "rename"
}
```

## 案例 7：不备份直接覆盖

`none` 会回到更接近 Spark 原生 overwrite 的行为，只保留显式确认这一层保护。

```hocon
save {
  overwriteBackup = "none"
}
```

```sql
view safe_save_no_backup as
select * from values
  (1, 'none')
as safe_save_no_backup(id, backup_mode);

save overwrite safe_save_no_backup
as parquet.`file:///tmp/sparkone_safe_save_no_backup`
options sparkoneOverwrite="allow";
```

这个模式适合临时本地测试，不建议在 HDFS 生产路径使用。

## 案例 8：全局允许但默认保留备份

如果希望测试环境里少写确认参数，可以在 `conf/sparkone.conf` 配置：

```hocon
save {
  overwritePolicy = "allow"
  overwriteBackup = "rename"
  overwriteBackupPath = "/tmp/sparkone_back"
}
```

然后 SQL 可以简化为：

```sql
view safe_save_global_allow as
select * from values
  (1, 'global')
as safe_save_global_allow(id, scope);

save overwrite safe_save_global_allow
as parquet.`file:///tmp/sparkone_safe_save_global_allow`;
```

生产环境更建议保留 `requireExplicit`。

## 案例 9：保护用户根目录，但允许用户子目录

这个配置适合“一个公共目录下面有大量用户目录”的场景。例如 `/public/odep/user` 是用户根目录，下面有 `/public/odep/user/userA`、`/public/odep/user/userB`。

先在 `conf/sparkone.conf` 配置并重启服务：

```hocon
save {
  overwritePolicy = "allow"
  overwriteBackup = "rename"
  overwriteBackupPath = "/public/odep/sparkone_back"
  overwriteProtectedPaths = [
    "/public/odep/user",
  ]
}
```

覆盖用户根目录会被拦截：

```sql
view safe_save_protected_root as
select * from values
  (1, 'blocked')
as safe_save_protected_root(id, flag);

save overwrite safe_save_protected_root
as parquet.`/public/odep/user`;
```

预期：失败，提示命中 `global protected path`，因为覆盖 `/public/odep/user` 会影响其下所有用户目录。

覆盖具体用户目录可以放行：

```sql
view safe_save_user_a as
select * from values
  (1, 'userA')
as safe_save_user_a(id, owner);

save overwrite safe_save_user_a
as parquet.`/public/odep/user/userA`;
```

预期：成功；如果 `/public/odep/user/userA` 已存在，会先按 `rename` 备份到 `/public/odep/sparkone_back`。

## 案例 10：保护路径的上级目录也会被拦截

继续使用案例 9 的配置：

```hocon
save {
  overwriteProtectedPaths = [
    "/public/odep/user",
  ]
}
```

覆盖它的上级目录也会被拦截：

```sql
view safe_save_parent_path as
select * from values
  (1, 'parent')
as safe_save_parent_path(id, flag);

save overwrite safe_save_parent_path
as parquet.`/public/odep`;
```

预期：失败。原因是覆盖 `/public/odep` 会连带删除或替换 `/public/odep/user`，所以也必须拦截。

## 案例 11：保护 `/tmp` 不影响具体测试目录

如果只想避免误写 `save overwrite ... as parquet.\`/tmp\`` 这种危险操作，可以这样配置：

```hocon
save {
  overwriteProtectedPaths = [
    "/tmp",
  ]
}
```

覆盖 `/tmp` 本身会被拦截：

```sql
view safe_save_tmp_root as
select * from values
  (1, 'tmp-root')
as safe_save_tmp_root(id, flag);

save overwrite safe_save_tmp_root
as parquet.`/tmp`;
```

但覆盖 `/tmp` 下的具体测试目录可以放行：

```sql
view safe_save_tmp_case as
select * from values
  (1, 'tmp-case')
as safe_save_tmp_case(id, flag);

save overwrite safe_save_tmp_case
as parquet.`/tmp/sparkone_safe_save_tmp_case`;
```

这个策略比“禁止 `/tmp` 及其全部子目录”更适合开发测试环境：它保护高危边界目录，同时不影响明确命名的测试输出目录。

## 案例 12：不相关路径不受保护边界影响

继续以 `/public/odep/user` 作为保护边界：

```hocon
save {
  overwriteProtectedPaths = [
    "/public/odep/user",
  ]
}
```

这条配置只保护 `/public/odep/user` 以及它的上级目录。下面这些路径都不属于这个保护链路，因此会按普通 Safe Save 规则继续执行：

```sql
view safe_save_hive_path as
select * from values
  (1, 'hive')
as safe_save_hive_path(id, name);

save overwrite safe_save_hive_path
as parquet.`/hive`;
```

```sql
view safe_save_mlsql_path as
select * from values
  (1, 'mlsql')
as safe_save_mlsql_path(id, name);

save overwrite safe_save_mlsql_path
as parquet.`/public/mlsql`;
```

```sql
view safe_save_public_data_path as
select * from values
  (1, 'data')
as safe_save_public_data_path(id, name);

save overwrite safe_save_public_data_path
as parquet.`/public/odep/data`;
```

预期：这些路径不会因为 `/public/odep/user` 被保护而被拦截。它们是否最终允许写入，仍取决于 `overwritePolicy`、`overwriteBackup`、HDFS 权限和目标路径是否存在等普通规则。

## 案例 13：使用通配符保护所有一级目录

如果希望禁止直接覆盖 HDFS 根目录下的一级目录，例如 `/hive`、`/tmp`、`/user`，可以配置：

```hocon
save {
  overwriteProtectedPaths = [
    "/*",
  ]
}
```

下面会被拦截：

```sql
view safe_save_first_level as
select * from values
  (1, 'first-level')
as safe_save_first_level(id, flag);

save overwrite safe_save_first_level
as parquet.`/hive`;
```

下面不会因为 `/*` 被拦截，因为它已经是更具体的二级目录：

```sql
view safe_save_second_level as
select * from values
  (1, 'second-level')
as safe_save_second_level(id, flag);

save overwrite safe_save_second_level
as parquet.`/hive/warehouse`;
```

## 案例 14：使用通配符保护一级和二级目录

如果希望一级目录和二级目录都不能直接覆盖，可以配置：

```hocon
save {
  overwriteProtectedPaths = [
    "/*/*",
  ]
}
```

`/*/*` 代表保护所有二级目录边界；由于覆盖一级目录也会影响其下二级目录，所以一级目录也会被拦截。

下面两个都会被拦截：

```sql
view safe_save_level1 as
select * from values
  (1, 'level1')
as safe_save_level1(id, flag);

save overwrite safe_save_level1
as parquet.`/public`;
```

```sql
view safe_save_level2 as
select * from values
  (1, 'level2')
as safe_save_level2(id, flag);

save overwrite safe_save_level2
as parquet.`/public/odep`;
```

三级业务目录可以放行：

```sql
view safe_save_level3 as
select * from values
  (1, 'level3')
as safe_save_level3(id, flag);

save overwrite safe_save_level3
as parquet.`/public/odep/userA`;
```

## 案例 15：多条保护规则任意命中即拒绝

多条规则可以混用，SparkOne 不做“允许规则”覆盖，避免规则冲突：

```hocon
save {
  overwriteProtectedPaths = [
    "/*/*",
    "/public/odep/user",
  ]
}
```

效果是：

- `/public` 命中 `/*/*` 的上级目录保护，会被拒绝。
- `/public/odep` 命中 `/*/*`，会被拒绝。
- `/public/odep/user` 命中 `/public/odep/user`，会被拒绝。
- `/public/odep/user/userA` 不命中任何保护边界，可以继续按普通 Safe Save 规则执行。

这种“任意命中即拒绝”的策略比较保守，但规则结果更容易解释，也不会出现某条宽规则被另一条窄规则意外放开的情况。
