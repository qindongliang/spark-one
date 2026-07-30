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

    private final SparkCatalogConfigMapper mapper = new SparkCatalogConfigMapper();

    @Test
    public void shouldMapMysqlAndDorisCatalogs() {
        Map<String, String> mysqlOptions = new LinkedHashMap<>();
        mysqlOptions.put("url", "jdbc:mysql://mysql.internal:3306/Dworks?useUnicode=true");
        mysqlOptions.put("driver", "com.mysql.cj.jdbc.Driver");
        mysqlOptions.put("user", "reader");
        mysqlOptions.put("password", "mysql-secret");

        Map<String, String> dorisOptions = new LinkedHashMap<>();
        dorisOptions.put("doris.fenodes", "doris-fe.internal:8030");
        dorisOptions.put("user", "reader");
        dorisOptions.put("password", "doris-secret");

        OdepDatasourceSnapshot snapshot = new OdepDatasourceSnapshot(Arrays.asList(
                datasource("jdbc", "dworks", mysqlOptions),
                datasource("doris", "recommend", dorisOptions),
                datasource("es", "monitor", Collections.singletonMap("es.username", "reader"))));

        Map<String, String> configuration = mapper.toSparkConf(snapshot);

        assertEquals(
                "org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCTableCatalog",
                configuration.get("spark.sql.catalog.mysql_dworks"));
        assertEquals(
                "jdbc:mysql://mysql.internal:3306/Dworks?useUnicode=true&databaseTerm=SCHEMA",
                configuration.get("spark.sql.catalog.mysql_dworks.url"));
        assertEquals(
                "org.apache.doris.spark.catalog.DorisTableCatalog",
                configuration.get("spark.sql.catalog.doris_recommend"));
        assertEquals(
                "doris-fe.internal:8030",
                configuration.get("spark.sql.catalog.doris_recommend.doris.fenodes"));
        assertFalse(configuration.containsKey("spark.sql.catalog.es_monitor"));
    }

    @Test
    public void shouldRejectDuplicateNormalizedCatalogNames() {
        Map<String, String> options = mysqlOptions();
        OdepDatasourceSnapshot snapshot = new OdepDatasourceSnapshot(Arrays.asList(
                datasource("jdbc", "foo-bar", options),
                datasource("jdbc", "foo_bar", options)));

        IllegalStateException error = assertThrows(
                IllegalStateException.class,
                () -> mapper.toSparkConf(snapshot));

        assertEquals("Duplicate Spark catalog name: mysql_foo_bar", error.getMessage());
    }

    @Test
    public void shouldKeepExistingMysqlDatabaseTerm() {
        Map<String, String> options = mysqlOptions();
        options.put("url", "jdbc:mysql://mysql.internal/db?databaseTerm=SCHEMA");

        Map<String, String> configuration = mapper.toSparkConf(
                new OdepDatasourceSnapshot(
                        Collections.singletonList(datasource("jdbc", "dworks", options))));

        assertEquals(
                options.get("url"),
                configuration.get("spark.sql.catalog.mysql_dworks.url"));
    }

    private OdepDatasourceSnapshot.Datasource datasource(
            String type,
            String alias,
            Map<String, String> options) {
        return new OdepDatasourceSnapshot.Datasource(
                1L,
                type,
                alias,
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
