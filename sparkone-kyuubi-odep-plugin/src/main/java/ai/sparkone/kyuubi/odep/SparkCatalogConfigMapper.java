package ai.sparkone.kyuubi.odep;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

final class SparkCatalogConfigMapper {

    private static final Logger LOGGER = LoggerFactory.getLogger(SparkCatalogConfigMapper.class);
    private static final String ROUTING_CATALOG_CLASS =
            "ai.sparkone.kyuubi.odep.catalog.OdepRoutingCatalog";
    private static final String JDBC_CATALOG_CLASS =
            "org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCTableCatalog";
    private static final String DORIS_CATALOG_CLASS =
            "org.apache.doris.spark.catalog.DorisTableCatalog";

    Map<String, String> toSparkConf(OdepDatasourceSnapshot snapshot) {
        Map<String, String> result = new LinkedHashMap<>();
        Map<String, Integer> routeCounts = new LinkedHashMap<>();
        Map<String, Set<String>> aliasesByCatalog = new LinkedHashMap<>();
        for (OdepDatasourceSnapshot.Datasource datasource : snapshot.getDatasources()) {
            String type = datasource.getType().toLowerCase(Locale.ROOT);
            if ("jdbc".equals(type)) {
                addJdbc(result, routeCounts, aliasesByCatalog, datasource);
            } else if ("doris".equals(type)) {
                addDoris(result, routeCounts, aliasesByCatalog, datasource);
            } else {
                LOGGER.warn(
                        "ODEP datasource is cached but has no Spark catalog mapper: type={}, alias={}",
                        datasource.getType(),
                        datasource.getAlias());
            }
        }
        return Collections.unmodifiableMap(result);
    }

    private void addJdbc(
            Map<String, String> result,
            Map<String, Integer> routeCounts,
            Map<String, Set<String>> aliasesByCatalog,
            OdepDatasourceSnapshot.Datasource datasource) {
        String physicalNamespace = physicalNamespace(datasource);
        if (physicalNamespace == null) {
            return;
        }
        Map<String, String> options = datasource.getOptions();
        Map<String, String> delegateOptions = new LinkedHashMap<>();
        String url = required(options, "url", datasource);
        delegateOptions.put("url", isMysql(options) ? withMysqlDatabaseTerm(url) : url);
        delegateOptions.put("driver", required(options, "driver", datasource));
        delegateOptions.put("user", required(options, "user", datasource));
        delegateOptions.put("password", requiredAllowEmpty(options, "password", datasource));
        addRoute(
                result,
                routeCounts,
                aliasesByCatalog,
                "jdbc",
                JDBC_CATALOG_CLASS,
                datasource.getAlias(),
                physicalNamespace,
                delegateOptions);
    }

    private void addDoris(
            Map<String, String> result,
            Map<String, Integer> routeCounts,
            Map<String, Set<String>> aliasesByCatalog,
            OdepDatasourceSnapshot.Datasource datasource) {
        String physicalNamespace = physicalNamespace(datasource);
        if (physicalNamespace == null) {
            return;
        }
        Map<String, String> options = datasource.getOptions();
        Map<String, String> delegateOptions = new LinkedHashMap<>();
        delegateOptions.put(
                "doris.fenodes",
                required(options, "doris.fenodes", datasource));
        delegateOptions.put("doris.user", required(options, "user", datasource));
        delegateOptions.put(
                "doris.password",
                requiredAllowEmpty(options, "password", datasource));
        copyOptionalDorisOption(options, delegateOptions, "doris.query.port");
        copyOptionalDorisOption(options, delegateOptions, "doris.request.retries");
        copyOptionalDorisOption(options, delegateOptions, "doris.read.mode");
        addRoute(
                result,
                routeCounts,
                aliasesByCatalog,
                "doris",
                DORIS_CATALOG_CLASS,
                datasource.getAlias(),
                physicalNamespace,
                delegateOptions);
    }

    private void addRoute(
            Map<String, String> result,
            Map<String, Integer> routeCounts,
            Map<String, Set<String>> aliasesByCatalog,
            String catalog,
            String delegateClass,
            String alias,
            String physicalNamespace,
            Map<String, String> delegateOptions) {
        String routedAlias = alias.trim();
        if (!routedAlias.matches("[A-Za-z_][A-Za-z0-9_]*")) {
            throw new IllegalStateException(
                    "ODEP datasource alias must be a simple Spark identifier: type="
                            + catalog + ", alias=" + alias);
        }
        String normalizedAlias = routedAlias.toLowerCase(Locale.ROOT);
        Set<String> aliases = aliasesByCatalog.computeIfAbsent(
                catalog,
                ignored -> new LinkedHashSet<>());
        if (!aliases.add(normalizedAlias)) {
            throw new IllegalStateException(
                    "Duplicate ODEP datasource alias for Spark catalog: catalog="
                            + catalog + ", alias=" + alias);
        }

        String catalogPrefix = "spark.sql.catalog." + catalog;
        result.put(catalogPrefix, ROUTING_CATALOG_CLASS);
        result.put(catalogPrefix + ".odep.delegate.class", delegateClass);

        int index = routeCounts.getOrDefault(catalog, 0);
        String routePrefix = catalogPrefix + ".odep.datasource." + index;
        result.put(routePrefix + ".alias", routedAlias);
        result.put(routePrefix + ".physicalNamespace", physicalNamespace);
        for (Map.Entry<String, String> option : delegateOptions.entrySet()) {
            result.put(routePrefix + ".option." + option.getKey(), option.getValue());
        }
        routeCounts.put(catalog, index + 1);
        result.put(catalogPrefix + ".odep.datasource.count", String.valueOf(index + 1));
    }

    private String physicalNamespace(OdepDatasourceSnapshot.Datasource datasource) {
        String value = datasource.getPhysicalNamespace();
        if (value == null || value.trim().isEmpty()) {
            LOGGER.warn(
                    "Skipping ODEP datasource without physical namespace: type={}, alias={}",
                    datasource.getType(),
                    datasource.getAlias());
            return null;
        }
        return value.trim();
    }

    private void copyOptionalDorisOption(
            Map<String, String> source,
            Map<String, String> target,
            String key) {
        String value = source.get(key);
        if (value != null && !value.trim().isEmpty()) {
            target.put(key, value);
        }
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
