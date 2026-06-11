package ai.sparkone.sql

final class DataSourceResolver(
    providerAliases: Map[String, String] = DataSourceResolver.DefaultProviderAliases,
    catalogFormats: Set[String] = DataSourceResolver.DefaultCatalogFormats) {

  private val normalizedProviderAliases = providerAliases.map { case (key, value) =>
    key.toLowerCase -> value
  }
  private val normalizedCatalogFormats = catalogFormats.map(_.toLowerCase)

  def resolveLoad(format: String, path: String, options: Seq[(String, String)]): ResolvedLoadSource = {
    val normalized = format.toLowerCase
    if (normalizedCatalogFormats.contains(normalized)) {
      if (options.nonEmpty) {
        throw new CompileException(s"LOAD source '$format' does not support Spark SQL OPTIONS in the MVP compiler")
      }
      CatalogTableSource(SparkOneSqlRender.renderMultipartIdentifier(path, "LOAD catalog table"))
    } else {
      ProviderLoadSource(resolveProvider(format), ("path" -> path) +: options)
    }
  }

  def resolveSave(format: String): ResolvedSaveSource = {
    val normalized = format.toLowerCase
    if (normalizedCatalogFormats.contains(normalized)) {
      CatalogSaveSource(format)
    } else {
      ProviderSaveSource(resolveProvider(format))
    }
  }

  private def resolveProvider(format: String): String = {
    normalizedProviderAliases.getOrElse(format.toLowerCase, format)
  }

}

object DataSourceResolver {
  val DefaultProviderAliases: Map[String, String] = Map(
    "excel" -> "excel")

  val DefaultCatalogFormats: Set[String] = Set("hive")
}

sealed trait ResolvedLoadSource
final case class ProviderLoadSource(provider: String, options: Seq[(String, String)]) extends ResolvedLoadSource
final case class CatalogTableSource(identifier: String) extends ResolvedLoadSource

sealed trait ResolvedSaveSource
final case class ProviderSaveSource(provider: String) extends ResolvedSaveSource
final case class CatalogSaveSource(format: String) extends ResolvedSaveSource
