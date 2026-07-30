package ai.sparkone.kyuubi.odep;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

final class SparkCatalogConfigMapper {

    private static final Logger LOGGER = LoggerFactory.getLogger(SparkCatalogConfigMapper.class);
    private static final String MYSQL_CATALOG_CLASS =
            "org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCTableCatalog";
    private static final String DORIS_CATALOG_CLASS =
            "org.apache.doris.spark.catalog.DorisTableCatalog";

    Map<String, String> toSparkConf(OdepDatasourceSnapshot snapshot) {
        Map<String, String> result = new LinkedHashMap<>();
        for (OdepDatasourceSnapshot.Datasource datasource : snapshot.getDatasources()) {
            String type = datasource.getType().toLowerCase(Locale.ROOT);
            if ("jdbc".equals(type) && isMysql(datasource.getOptions())) {
                addMysql(result, datasource);
            } else if ("doris".equals(type)) {
                addDoris(result, datasource);
            } else {
                LOGGER.warn(
                        "ODEP datasource is cached but has no Spark catalog mapper: type={}, alias={}",
                        datasource.getType(),
                        datasource.getAlias());
            }
        }
        return Collections.unmodifiableMap(result);
    }

    private void addMysql(
            Map<String, String> result,
            OdepDatasourceSnapshot.Datasource datasource) {
        String catalog = catalogName("mysql", datasource.getAlias());
        String prefix = reserveCatalog(result, catalog, MYSQL_CATALOG_CLASS);
        Map<String, String> options = datasource.getOptions();
        result.put(prefix + ".url", withMysqlDatabaseTerm(required(options, "url", datasource)));
        result.put(prefix + ".driver", required(options, "driver", datasource));
        result.put(prefix + ".user", required(options, "user", datasource));
        result.put(prefix + ".password", requiredAllowEmpty(options, "password", datasource));
    }

    private void addDoris(
            Map<String, String> result,
            OdepDatasourceSnapshot.Datasource datasource) {
        String catalog = catalogName("doris", datasource.getAlias());
        String prefix = reserveCatalog(result, catalog, DORIS_CATALOG_CLASS);
        Map<String, String> options = datasource.getOptions();
        result.put(prefix + ".doris.fenodes", required(options, "doris.fenodes", datasource));
        result.put(prefix + ".doris.user", required(options, "user", datasource));
        result.put(prefix + ".doris.password", requiredAllowEmpty(options, "password", datasource));
    }

    private String reserveCatalog(
            Map<String, String> result,
            String catalog,
            String catalogClass) {
        String prefix = "spark.sql.catalog." + catalog;
        if (result.containsKey(prefix)) {
            throw new IllegalStateException("Duplicate Spark catalog name: " + catalog);
        }
        result.put(prefix, catalogClass);
        return prefix;
    }

    private String catalogName(String type, String alias) {
        String normalized = alias.trim()
                .toLowerCase(Locale.ROOT)
                .replaceAll("[^a-z0-9_]", "_")
                .replaceAll("_+", "_");
        if (normalized.isEmpty()) {
            throw new IllegalStateException(
                    "ODEP datasource alias cannot form a Spark catalog name: type="
                            + type + ", alias=" + alias);
        }
        return type + "_" + normalized;
    }

    private boolean isMysql(Map<String, String> options) {
        String url = options.getOrDefault("url", "").toLowerCase(Locale.ROOT);
        String driver = options.getOrDefault("driver", "").toLowerCase(Locale.ROOT);
        return url.startsWith("jdbc:mysql:") || driver.contains("mysql");
    }

    private String withMysqlDatabaseTerm(String url) {
        if (url.toLowerCase(Locale.ROOT).contains("databaseterm=")) {
            return url;
        }
        return url + (url.contains("?") ? "&" : "?") + "databaseTerm=SCHEMA";
    }

    private String required(
            Map<String, String> options,
            String key,
            OdepDatasourceSnapshot.Datasource datasource) {
        String value = requiredAllowEmpty(options, key, datasource);
        if (value.trim().isEmpty()) {
            throw missingOption(key, datasource);
        }
        return value;
    }

    private String requiredAllowEmpty(
            Map<String, String> options,
            String key,
            OdepDatasourceSnapshot.Datasource datasource) {
        if (!options.containsKey(key) || options.get(key) == null) {
            throw missingOption(key, datasource);
        }
        return options.get(key);
    }

    private IllegalStateException missingOption(
            String key,
            OdepDatasourceSnapshot.Datasource datasource) {
        return new IllegalStateException(
                "ODEP datasource is missing required option: type="
                        + datasource.getType() + ", alias=" + datasource.getAlias() + ", key=" + key);
    }
}
