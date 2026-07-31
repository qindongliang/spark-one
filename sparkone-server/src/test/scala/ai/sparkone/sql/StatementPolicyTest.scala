package ai.sparkone.sql

import org.junit.Assert._
import org.junit.Test

final class StatementPolicyTest {
  private val compiler = new SparkOneCompiler

  @Test
  def rejectsNativeDataAndCatalogMutations(): Unit = {
    val mutations = Seq(
      "create table default.target (id int) using parquet",
      "create table default.target_like like default.source",
      "create table default.target_ctas using parquet as select 1 as id",
      "drop table if exists default.target",
      "alter table default.target add columns (name string)",
      "create database if not exists stage2_db",
      "drop database if exists stage2_db cascade",
      "create or replace temporary view target_view as select 1 as id",
      "drop view if exists default.target_view",
      "create function stage2_fn as 'com.example.Stage2Function'",
      "drop function if exists stage2_fn",
      "analyze table default.target compute statistics",
      "repair table default.target",
      "insert into table default.target select 1",
      "insert overwrite table default.target select 1",
      "insert overwrite directory '/tmp/target' using parquet select 1",
      "load data local inpath '/tmp/source' into table default.target",
      "truncate table default.target",
      "delete from default.target where id = 1",
      "update default.target set id = 2 where id = 1",
      "merge into default.target t using default.source s on t.id = s.id " +
        "when matched then update set t.id = s.id when not matched then insert (id) values (s.id)",
      "set spark.sql.shuffle.partitions=8",
      "reset spark.sql.shuffle.partitions")

    mutations.foreach { sql =>
      val error = expectCompileException(sql)
      assertTrue(sql, error.getMessage.contains("only allows native read-only SQL"))
    }
  }

  @Test
  def allowsNativeQueriesAndReadOnlyInspectionCommands(): Unit = {
    val readOnly = Seq(
      "select 'insert overwrite table x' as message",
      "with values_cte as (select 1 as id) select * from values_cte",
      "show tables in default",
      "describe table default.target",
      "explain select 1",
      "use default")

    readOnly.foreach(sql => assertEquals(1, compiler.compile(sql).size))
  }

  @Test
  def allowsControlledDslIntents(): Unit = {
    val statements = compiler.compile(
      """load parquet.`source` as source_view;
        |view projected as select * from source_view;
        |set biz_date = "2026-07-13";
        |assert projected where "id is not null" message "id must not be null";
        |save append projected as hive.`default.target`;
        |""".stripMargin)

    assertEquals(
      Seq(
        StatementIntent.Load,
        StatementIntent.View,
        StatementIntent.SetVariable,
        StatementIntent.Assert,
        StatementIntent.Save),
      statements.map(_.intent))
  }

  @Test
  def rejectsNativeProviderPathsInQueriesAndViews(): Unit = {
    Seq(
      "select * from parquet.`/public/sparkone/user/alice/result`",
      "select * from csv.`relative/result.csv`",
      "select * from jdbc.`jdbc:mysql://mysql.internal/app`",
      "select * from avro.`/public/sparkone/user/alice/result`",
      "select * from custom.`relative/result`",
      "view leaked as select * from orc.`/public/sparkone/user/bob/result`").foreach { sql =>
      val error = expectCompileException(sql)
      assertTrue(sql, error.getMessage.contains("use SparkOne LOAD"))
    }
  }

  @Test
  def allowsThreePartCatalogTablesWithProviderLikeCatalogNames(): Unit = {
    val readOnly = Seq(
      "select * from jdbc.sync_search.drugs_suggest_new_category limit 10",
      "select * from parquet.analytics.daily_events")

    readOnly.foreach(sql => assertEquals(1, compiler.compile(sql).size))
  }

  @Test
  def rejectsWriteHiddenInsideSqlVariable(): Unit = {
    val error = expectCompileException(
      "set row_count as insert into table default.target select 1")

    assertTrue(error.getMessage.contains("only allows native read-only SQL"))
  }

  private def expectCompileException(sql: String): CompileException = {
    try {
      compiler.compile(sql)
      fail(s"Expected CompileException: $sql")
      throw new AssertionError("unreachable")
    } catch {
      case e: CompileException => e
    }
  }
}
