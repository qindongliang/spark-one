package ai.sparkone.kyuubi.odep;

import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;

public class SparkCatalogConfigMapperTest {

    private static final String ROUTING_CATALOG_CLASS =
            "ai.sparkone.kyuubi.odep.catalog.OdepRoutingCatalog";
    private final SparkCatalogConfigMapper mapper = new SparkCatalogConfigMapper();

    @Test
    public void shouldMapJdbcAndDorisRoutes() {
        Map<String, String> mysqlOptions = new LinkedHashMap<>();
        mysqlOptions.put("url", "jdbc:mysql://mysql.internal:3306/Dworks?useUnicode=true");
        mysqlOptions.put("driver", "com.mysql.cj.jdbc.Driver");
        mysqlOptions.put("user", "reader");
        mysqlOptions.put("password", "mysql-secret");

        Map<String, String> dorisOptions = new LinkedHashMap<>();
        dorisOptions.put("doris.fenodes", "doris-fe.internal:8030");
        dorisOptions.put("user", "reader");
        dorisOptions.put("password", "doris-secret");
        dorisOptions.put("doris.query.port", "9030");

        OdepDatasourceSnapshot snapshot = new OdepDatasourceSnapshot(Arrays.asList(
                datasource("jdbc", "dworks", "Dworks", mysqlOptions),
                datasource("doris", "recommend_prod", "recommend", dorisOptions),
                datasource(
                        "es",
                        "monitor",
                        "es1",
                        Collections.singletonMap("es.username", "reader"))));

        Map<String, String> configuration = mapper.toSparkConf(snapshot);

        assertEquals(ROUTING_CATALOG_CLASS, configuration.get("spark.sql.catalog.jdbc"));
        assertEquals(
                "org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCTableCatalog",
                configuration.get("spark.sql.catalog.jdbc.odep.delegate.class"));
        assertEquals(
                "dworks",
                configuration.get("spark.sql.catalog.jdbc.odep.datasource.0.alias"));
        assertEquals(
                "Dworks",
                configuration.get(
                        "spark.sql.catalog.jdbc.odep.datasource.0.physicalNamespace"));
        assertEquals(
                "jdbc:mysql://mysql.internal:3306/Dworks?useUnicode=true&databaseTerm=SCHEMA",
                configuration.get("spark.sql.catalog.jdbc.odep.datasource.0.option.url"));

        assertEquals(ROUTING_CATALOG_CLASS, configuration.get("spark.sql.catalog.doris"));
        assertEquals(
                "org.apache.doris.spark.catalog.DorisTableCatalog",
                configuration.get("spark.sql.catalog.doris.odep.delegate.class"));
        assertEquals(
                "recommend_prod",
                configuration.get("spark.sql.catalog.doris.odep.datasource.0.alias"));
        assertEquals(
                "recommend",
                configuration.get(
                        "spark.sql.catalog.doris.odep.datasource.0.physicalNamespace"));
        assertEquals(
                "doris-fe.internal:8030",
                configuration.get(
                        "spark.sql.catalog.doris.odep.datasource.0.option.doris.fenodes"));
        assertEquals(
                "reader",
                configuration.get(
                        "spark.sql.catalog.doris.odep.datasource.0.option.doris.user"));
        assertEquals(
                "9030",
                configuration.get(
                        "spark.sql.catalog.doris.odep.datasource.0.option.doris.query.port"));
        assertFalse(configuration.containsKey("spark.sql.catalog.es"));
    }

    @Test
    public void shouldRejectDuplicateAliasesWithinCatalogIgnoringCase() {
        Map<String, String> options = mysqlOptions();
        OdepDatasourceSnapshot snapshot = new OdepDatasourceSnapshot(Arrays.asList(
                datasource("jdbc", "Search", "search_a", options),
                datasource("jdbc", "search", "search_b", options)));

        IllegalStateException error = assertThrows(
                IllegalStateException.class,
                () -> mapper.toSparkConf(snapshot));

        assertEquals(
                "Duplicate ODEP datasource alias for Spark catalog: catalog=jdbc, alias=search",
                error.getMessage());
    }

    @Test
    public void shouldSkipSupportedDatasourceWithoutPhysicalNamespace() {
        Map<String, String> configuration = mapper.toSparkConf(
                new OdepDatasourceSnapshot(Collections.singletonList(
                        datasource("jdbc", "legacy", null, mysqlOptions()))));

        assertFalse(configuration.containsKey("spark.sql.catalog.jdbc"));
    }

    @Test
    public void shouldRejectAliasThatCannotBeUsedByThreePartSql() {
        IllegalStateException error = assertThrows(
                IllegalStateException.class,
                () -> mapper.toSparkConf(new OdepDatasourceSnapshot(
                        Collections.singletonList(
                                datasource(
                                        "jdbc",
                                        "search-prod",
                                        "search",
                                        mysqlOptions())))));

        assertEquals(
                "ODEP datasource alias must be a simple Spark identifier: "
                        + "type=jdbc, alias=search-prod",
                error.getMessage());
    }

    @Test
    public void shouldKeepExistingMysqlDatabaseTerm() {
        Map<String, String> options = mysqlOptions();
        options.put("url", "jdbc:mysql://mysql.internal/db?databaseTerm=SCHEMA");

        Map<String, String> configuration = mapper.toSparkConf(
                new OdepDatasourceSnapshot(
                        Collections.singletonList(
                                datasource("jdbc", "dworks", "Dworks", options))));

        assertEquals(
                options.get("url"),
                configuration.get("spark.sql.catalog.jdbc.odep.datasource.0.option.url"));
    }

    private OdepDatasourceSnapshot.Datasource datasource(
            String type,
            String alias,
            String physicalNamespace,
            Map<String, String> options) {
        return new OdepDatasourceSnapshot.Datasource(
                1L,
                type,
                alias,
                physicalNamespace,
                null,
                options,
                null);
    }

    private Map<String, String> mysqlOptions() {
        Map<String, String> options = new LinkedHashMap<>();
        options.put("url", "jdbc:mysql://mysql.internal/db");
        options.put("driver", "com.mysql.cj.jdbc.Driver");
        options.put("user", "reader");
        options.put("password", "");
        return options;
    }
}
