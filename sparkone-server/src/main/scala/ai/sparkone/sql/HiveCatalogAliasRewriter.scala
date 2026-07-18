package ai.sparkone.sql

import org.apache.spark.sql.catalyst.analysis.{
  UnresolvedNamespace,
  UnresolvedRelation,
  UnresolvedTable,
  UnresolvedTableOrView
}
import org.apache.spark.sql.catalyst.parser.SqlBaseLexer
import org.apache.spark.sql.catalyst.plans.logical.ShowNamespaces
import org.apache.spark.sql.catalyst.trees.Origin
import org.apache.spark.sql.execution.SparkSqlParser
import org.antlr.v4.runtime.{CharStreams, CommonTokenStream, Token}

import java.util.Locale
import scala.collection.JavaConverters._

final class HiveCatalogAliasRewriter(parser: SparkSqlParser = new SparkSqlParser) {
  def rewrite(sql: String): String = {
    val plan = try {
      parser.parsePlan(sql)
    } catch {
      case e: Exception =>
        throw new CompileException(s"Spark SQL parser rejected statement: ${e.getMessage}", e)
    }

    val replacements = plan.collect {
      case ShowNamespaces(namespace: UnresolvedNamespace, _, _)
          if isHiveCatalog(namespace.multipartIdentifier) =>
        namespaceReplacement(sql, namespace.multipartIdentifier, namespace.origin)
      case relation: UnresolvedRelation if isHiveTable(relation.multipartIdentifier) =>
        replacement(sql, relation.origin)
      case table: UnresolvedTable if isHiveTable(table.multipartIdentifier) =>
        replacement(sql, table.origin)
      case tableOrView: UnresolvedTableOrView if isHiveTable(tableOrView.multipartIdentifier) =>
        replacement(sql, tableOrView.origin)
      case namespace: UnresolvedNamespace if isHiveNamespace(namespace.multipartIdentifier) =>
        namespaceReplacement(sql, namespace.multipartIdentifier, namespace.origin)
    }.distinct.sortBy(replacement => -replacement.start)

    replacements.foldLeft(sql) { case (rewritten, replacement) =>
      rewritten.substring(0, replacement.start) +
        HiveCatalogAliasRewriter.SessionCatalog +
        rewritten.substring(replacement.end)
    }
  }

  private def isHiveTable(parts: Seq[String]): Boolean = {
    parts.size == 3 && isHive(parts.head)
  }

  private def isHiveNamespace(parts: Seq[String]): Boolean = {
    parts.size == 2 && isHive(parts.head)
  }

  private def isHiveCatalog(parts: Seq[String]): Boolean = {
    parts.size == 1 && isHive(parts.head)
  }

  private def isHive(value: String): Boolean = {
    value.toLowerCase(Locale.ROOT) == HiveCatalogAliasRewriter.Alias
  }

  private def replacement(sql: String, origin: Origin): CatalogReplacement = {
    val start = origin.startIndex.getOrElse {
      throw new CompileException("Cannot locate Hive catalog alias in Spark SQL statement")
    }
    val end = identifierEnd(sql, start)
    val identifier = sql.substring(start, end)
    val unquoted =
      if (identifier.startsWith("`") && identifier.endsWith("`")) {
        identifier.substring(1, identifier.length - 1).replace("``", "`")
      } else {
        identifier
      }
    if (!isHive(unquoted)) {
      throw new CompileException("Cannot locate Hive catalog alias in Spark SQL statement")
    }
    CatalogReplacement(start, end)
  }

  private def namespaceReplacement(
      sql: String,
      parts: Seq[String],
      origin: Origin): CatalogReplacement = {
    // Spark's generated lexer expects uppercase lookahead; per-character mapping preserves indexes.
    val lexer = new SqlBaseLexer(CharStreams.fromString(sql.map(_.toUpper)))
    val tokenStream = new CommonTokenStream(lexer)
    tokenStream.fill()
    val start = origin.startIndex.getOrElse(0)
    val stop = origin.stopIndex.getOrElse(sql.length - 1)
    val tokens = tokenStream.getTokens.asScala.filter { token =>
      token.getChannel == Token.DEFAULT_CHANNEL &&
        token.getType != Token.EOF &&
        token.getStartIndex >= start &&
        token.getStopIndex <= stop
    }
    val width = parts.size * 2 - 1
    val matches = tokens.sliding(width).filter { candidate =>
      candidate.zipWithIndex.forall { case (token, index) =>
        if (index % 2 == 1) token.getText == "."
        else identifierValue(token.getText).equalsIgnoreCase(parts(index / 2))
      }
    }.toSeq
    if (matches.size != 1) {
      throw new CompileException("Cannot locate Hive catalog alias in Spark SQL statement")
    }
    val alias = matches.head.head
    CatalogReplacement(alias.getStartIndex, alias.getStopIndex + 1)
  }

  private def identifierValue(value: String): String = {
    if (value.startsWith("`") && value.endsWith("`")) {
      value.substring(1, value.length - 1).replace("``", "`")
    } else {
      value
    }
  }

  private def identifierEnd(sql: String, start: Int): Int = {
    if (start < 0 || start >= sql.length) {
      throw new CompileException("Cannot locate Hive catalog alias in Spark SQL statement")
    }
    if (sql.charAt(start) == '`') {
      var index = start + 1
      while (index < sql.length) {
        if (sql.charAt(index) == '`') {
          if (index + 1 < sql.length && sql.charAt(index + 1) == '`') {
            index += 2
          } else {
            return index + 1
          }
        } else {
          index += 1
        }
      }
      throw new CompileException("Unclosed quoted Hive catalog alias")
    } else {
      var index = start
      while (index < sql.length && (sql.charAt(index).isLetterOrDigit || sql.charAt(index) == '_')) {
        index += 1
      }
      index
    }
  }
}

object HiveCatalogAliasRewriter {
  val Alias = "hive"
  val SessionCatalog = "spark_catalog"
}

private final case class CatalogReplacement(start: Int, end: Int)
