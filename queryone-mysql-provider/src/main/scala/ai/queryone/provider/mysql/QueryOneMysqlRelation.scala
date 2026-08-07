package ai.queryone.provider.mysql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.sources.{BaseRelation, Filter, PrunedFilteredScan}
import org.apache.spark.sql.types.StructType

final class QueryOneMysqlRelation private[mysql] (
    delegate: BaseRelation,
    val queryOneAuthzMode: String,
    val queryOneAuthzCatalog: String,
    val queryOneAuthzNamespace: String,
    val queryOneAuthzTable: String)
  extends BaseRelation with PrunedFilteredScan {

  private val scan = delegate match {
    case value: PrunedFilteredScan => value
    case _ =>
      throw new IllegalArgumentException(
        s"queryone_mysql relation does not support filtered scans: $delegate")
  }

  override def sqlContext: SQLContext = delegate.sqlContext
  override def schema: StructType = delegate.schema
  override def needConversion: Boolean = delegate.needConversion
  override def unhandledFilters(filters: Array[Filter]): Array[Filter] =
    scan.unhandledFilters(filters)

  override def buildScan(
      requiredColumns: Array[String],
      filters: Array[Filter]): RDD[Row] =
    scan.buildScan(requiredColumns, filters)

  override def toString: String = delegate.toString
}
