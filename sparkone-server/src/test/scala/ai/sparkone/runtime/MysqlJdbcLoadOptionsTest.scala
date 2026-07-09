package ai.sparkone.runtime

import org.junit.Assert._
import org.junit.Test

final class MysqlJdbcLoadOptionsTest {

  @Test
  def enrichesPartitionColumnOnlyWithDynamicBoundsAndDefaults(): Unit = {
    val options = Map(
      "url" -> "jdbc:mysql://host:3306/app",
      "dbtable" -> "(select * from big_orders where biz_date = '2026-06-10') as sparkone_mysql_load",
      "partitionColumn" -> "id")

    val enriched = MysqlJdbcLoadOptions.enrich(options, { (_, dbtable, column) =>
      assertEquals("(select * from big_orders where biz_date = '2026-06-10') as sparkone_mysql_load", dbtable)
      assertEquals("id", column)
      Some("1" -> "30000000")
    })

    assertEquals(Some("id"), enriched.get("partitionColumn"))
    assertEquals(Some("1"), enriched.get("lowerBound"))
    assertEquals(Some("30000000"), enriched.get("upperBound"))
    assertEquals(Some("10"), enriched.get("numPartitions"))
    assertEquals(Some("10000"), enriched.get("fetchsize"))
  }

  @Test
  def enrichesPartitionColumnOnlyWithoutWhereAgainstBaseTable(): Unit = {
    val options = Map(
      "url" -> "jdbc:mysql://host:3306/app",
      "dbtable" -> "big_orders",
      "partitionColumn" -> "id")

    val enriched = MysqlJdbcLoadOptions.enrich(options, { (_, dbtable, column) =>
      assertEquals("big_orders", dbtable)
      assertEquals("id", column)
      Some("8" -> "88")
    })

    assertEquals(Some("8"), enriched.get("lowerBound"))
    assertEquals(Some("88"), enriched.get("upperBound"))
    assertEquals(Some("10"), enriched.get("numPartitions"))
    assertEquals(Some("10000"), enriched.get("fetchsize"))
  }

  @Test
  def dropsPartitionOptionsWhenBoundsAreEmpty(): Unit = {
    val enriched = MysqlJdbcLoadOptions.enrich(
      Map(
        "url" -> "jdbc:mysql://host:3306/app",
        "dbtable" -> "empty_orders",
        "partitionColumn" -> "id"),
      { (_, _, _) => None })

    assertFalse(enriched.contains("partitionColumn"))
    assertFalse(enriched.contains("lowerBound"))
    assertFalse(enriched.contains("upperBound"))
    assertFalse(enriched.contains("numPartitions"))
    assertEquals(Some("10000"), enriched.get("fetchsize"))
  }

  @Test
  def rejectsPartitionHintsWithoutPartitionColumn(): Unit = {
    try {
      MysqlJdbcLoadOptions.enrich(
        Map(
          "url" -> "jdbc:mysql://host:3306/app",
          "dbtable" -> "big_orders",
          "lowerBound" -> "1",
          "upperBound" -> "10"),
        { (_, _, _) => fail("bounds query should not run"); None })
      fail("Expected CompileException")
    } catch {
      case e: ai.sparkone.sql.CompileException =>
        assertTrue(e.getMessage.contains("requires partitionColumn"))
    }
  }
}
