package ai.sparkone.kyuubi.odep.catalog;

import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.NamespaceChange;
import org.apache.spark.sql.connector.catalog.SupportsNamespaces;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.catalog.TableChange;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.Test;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class OdepRoutingCatalogTest {

    @Test
    public void shouldResolveThreePartSqlThroughSpark() throws Exception {
        String url = "jdbc:h2:mem:odep_routing;DB_CLOSE_DELAY=-1";
        Class.forName("org.h2.Driver");
        try (Connection connection = DriverManager.getConnection(url);
                Statement statement = connection.createStatement()) {
            statement.execute("CREATE SCHEMA \"physical_db\"");
            statement.execute(
                    "CREATE TABLE \"physical_db\".\"items\" "
                            + "(\"id\" INT PRIMARY KEY, \"name\" VARCHAR(32))");
            statement.execute(
                    "INSERT INTO \"physical_db\".\"items\" VALUES (1, 'alpha')");
        }

        SparkSession spark = SparkSession.builder()
                .master("local[1]")
                .appName("odep-routing-catalog-test")
                .config("spark.ui.enabled", "false")
                .config("spark.driver.host", "127.0.0.1")
                .config("spark.driver.bindAddress", "127.0.0.1")
                .config(
                        "spark.sql.catalog.jdbc",
                        OdepRoutingCatalog.class.getName())
                .config(
                        "spark.sql.catalog.jdbc.odep.delegate.class",
                        "org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCTableCatalog")
                .config("spark.sql.catalog.jdbc.odep.datasource.count", "1")
                .config(
                        "spark.sql.catalog.jdbc.odep.datasource.0.alias",
                        "search_prod")
                .config(
                        "spark.sql.catalog.jdbc.odep.datasource.0.physicalNamespace",
                        "physical_db")
                .config(
                        "spark.sql.catalog.jdbc.odep.datasource.0.option.url",
                        url)
                .config(
                        "spark.sql.catalog.jdbc.odep.datasource.0.option.driver",
                        "org.h2.Driver")
                .getOrCreate();
        try {
            spark.sparkContext().setLogLevel("ERROR");

            Row namespace = spark.sql("SHOW NAMESPACES IN jdbc").head();
            assertEquals("search_prod", namespace.getString(0));

            Row table = spark.sql("SHOW TABLES IN jdbc.search_prod").head();
            assertEquals("search_prod", table.getString(0));
            assertEquals("items", table.getString(1));

            Row result = spark.sql(
                    "SELECT id, name FROM jdbc.search_prod.items").head();
            assertEquals(1, result.getInt(0));
            assertEquals("alpha", result.getString(1));
        } finally {
            spark.stop();
            SparkSession.clearActiveSession();
            SparkSession.clearDefaultSession();
        }
    }

    @Test
    public void shouldExposeAliasesAndRouteTablesToPhysicalNamespaces()
            throws Exception {
        OdepRoutingCatalog catalog = catalog(routes(
                route(0, "search_prod", "sync_search", "first"),
                route(1, "chat_prod", "chat", "second")));

        assertArrayEquals(
                new String[][] {{"search_prod"}, {"chat_prod"}},
                catalog.listNamespaces());
        assertTrue(catalog.namespaceExists(new String[] {"SEARCH_PROD"}));

        Identifier[] tables = catalog.listTables(new String[] {"search_prod"});
        assertEquals(1, tables.length);
        assertArrayEquals(new String[] {"search_prod"}, tables[0].namespace());
        assertEquals("items", tables[0].name());

        FakeTable loaded = (FakeTable) catalog.loadTable(
                Identifier.of(new String[] {"search_prod"}, "orders"));
        assertEquals("sync_search.orders@first", loaded.name());
    }

    @Test
    public void shouldRejectStaticOptionsSharingTheRoutingCatalogPrefix() {
        Map<String, String> options = routes(
                route(0, "recommend", "recommend_db", "doris"));
        options.put("doris.fenodes", "legacy-fe:8030");

        IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> catalog(options));

        assertTrue(error.getMessage().contains("remove conflicting static options"));
        assertTrue(error.getMessage().contains("doris.fenodes"));
    }

    @Test
    public void shouldRejectUnknownAlias() {
        OdepRoutingCatalog catalog = catalog(routes(
                route(0, "search_prod", "sync_search", "first")));

        assertThrows(
                NoSuchNamespaceException.class,
                () -> catalog.listTables(new String[] {"missing"}));
    }

    private OdepRoutingCatalog catalog(Map<String, String> options) {
        OdepRoutingCatalog catalog = new OdepRoutingCatalog();
        catalog.initialize("jdbc", new CaseInsensitiveStringMap(options));
        return catalog;
    }

    @SafeVarargs
    private final Map<String, String> routes(Map<String, String>... routes) {
        Map<String, String> options = new LinkedHashMap<>();
        options.put("odep.delegate.class", FakeDelegateCatalog.class.getName());
        options.put("odep.datasource.count", String.valueOf(routes.length));
        for (Map<String, String> route : routes) {
            options.putAll(route);
        }
        return options;
    }

    private Map<String, String> route(
            int index,
            String alias,
            String physicalNamespace,
            String marker) {
        String prefix = "odep.datasource." + index + ".";
        Map<String, String> route = new LinkedHashMap<>();
        route.put(prefix + "alias", alias);
        route.put(prefix + "physicalNamespace", physicalNamespace);
        route.put(prefix + "option.marker", marker);
        return route;
    }

    public static final class FakeDelegateCatalog
            implements TableCatalog, SupportsNamespaces {

        private String name;
        private String marker;

        @Override
        public void initialize(String name, CaseInsensitiveStringMap options) {
            this.name = name;
            this.marker = options.get("marker");
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public Identifier[] listTables(String[] namespace) {
            return new Identifier[] {Identifier.of(namespace, "items")};
        }

        @Override
        public Table loadTable(Identifier ident) {
            return new FakeTable(
                    ident.namespace()[0] + "." + ident.name() + "@" + marker);
        }

        @Override
        public Table createTable(
                Identifier ident,
                StructType schema,
                Transform[] partitions,
                Map<String, String> properties) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Table alterTable(Identifier ident, TableChange... changes) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean dropTable(Identifier ident) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void renameTable(Identifier oldIdent, Identifier newIdent) {
            throw new UnsupportedOperationException();
        }

        @Override
        public String[][] listNamespaces() {
            return new String[0][];
        }

        @Override
        public String[][] listNamespaces(String[] namespace) {
            return new String[0][];
        }

        @Override
        public Map<String, String> loadNamespaceMetadata(String[] namespace) {
            return Collections.emptyMap();
        }

        @Override
        public void createNamespace(
                String[] namespace,
                Map<String, String> metadata) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void alterNamespace(
                String[] namespace,
                NamespaceChange... changes) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean dropNamespace(String[] namespace, boolean cascade) {
            throw new UnsupportedOperationException();
        }
    }

    private static final class FakeTable implements Table {

        private final String name;

        private FakeTable(String name) {
            this.name = name;
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public StructType schema() {
            return new StructType();
        }

        @Override
        public Set<org.apache.spark.sql.connector.catalog.TableCapability>
                capabilities() {
            return Collections.emptySet();
        }
    }
}
