package ai.queryone.kyuubi.odep.catalog;

import ai.queryone.kyuubi.odep.OdepDatasourceResolver;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.NamespaceChange;
import org.apache.spark.sql.connector.catalog.SupportsNamespaces;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.catalog.TableChange;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class OdepRoutingCatalogTest {

    private final AtomicInteger indexRequests = new AtomicInteger();
    private final Map<String, AtomicInteger> resolveRequests = new ConcurrentHashMap<>();
    private HttpServer server;
    private OdepDatasourceResolver resolver;

    @Before
    public void setUp() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/api/datasource/index", this::handleIndex);
        server.createContext("/api/datasource/resolve", this::handleResolve);
        server.start();
        resolver = new OdepDatasourceResolver(
                "http://127.0.0.1:" + server.getAddress().getPort(),
                "app_kyuubi",
                "test-sign-key",
                2000,
                2000);
    }

    @After
    public void tearDown() {
        server.stop(0);
    }

    @Test
    public void shouldNotCallOdepDuringCatalogInitialization() {
        catalog(Collections.emptyMap());

        assertEquals(0, indexRequests.get());
        assertEquals(0, resolveRequestCount());
    }

    @Test
    public void shouldLoadIndexBeforeResolvingOnlyTheSelectedAlias() throws Exception {
        OdepRoutingCatalog catalog = catalog(delegateOptions());

        assertArrayEquals(
                new String[][] {{"search_prod"}, {"chat_prod"}},
                catalog.listNamespaces());
        assertTrue(catalog.namespaceExists(new String[] {"SEARCH_PROD"}));
        assertEquals(1, indexRequests.get());
        assertEquals(0, resolveRequestCount());

        Identifier[] tables = catalog.listTables(new String[] {"search_prod"});
        assertEquals(1, tables.length);
        assertArrayEquals(new String[] {"search_prod"}, tables[0].namespace());
        assertEquals("items", tables[0].name());
        assertEquals(1, resolveRequests.get("search_prod").get());
        assertEquals(0, resolveRequests.getOrDefault("chat_prod", new AtomicInteger()).get());

        FakeTable loaded = (FakeTable) catalog.loadTable(
                Identifier.of(new String[] {"SEARCH_PROD"}, "orders"));
        assertEquals("physical_search.orders@jdbc:fake:search_prod", loaded.name());
        assertEquals(1, resolveRequests.get("search_prod").get());
    }

    @Test
    public void shouldRejectStaticOptionsSharingTheRoutingCatalogPrefix() {
        Map<String, String> options = delegateOptions();
        options.put("url", "jdbc:mysql://legacy");

        IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> catalog(options));

        assertTrue(error.getMessage().contains("remove conflicting static options"));
        assertTrue(error.getMessage().contains("url"));
        assertEquals(0, indexRequests.get());
    }

    @Test
    public void shouldRejectUnknownAliasWithoutResolvingOptions() {
        OdepRoutingCatalog catalog = catalog(delegateOptions());

        assertThrows(
                NoSuchNamespaceException.class,
                () -> catalog.listTables(new String[] {"missing"}));

        assertEquals(1, indexRequests.get());
        assertEquals(0, resolveRequestCount());
    }

    private OdepRoutingCatalog catalog(Map<String, String> options) {
        OdepRoutingCatalog catalog = new OdepRoutingCatalog(resolver);
        catalog.initialize("jdbc", new CaseInsensitiveStringMap(options));
        return catalog;
    }

    private Map<String, String> delegateOptions() {
        Map<String, String> options = new LinkedHashMap<>();
        options.put("odep.delegate.class", FakeDelegateCatalog.class.getName());
        return options;
    }

    private int resolveRequestCount() {
        return resolveRequests.values().stream().mapToInt(AtomicInteger::get).sum();
    }

    private void handleIndex(HttpExchange exchange) throws IOException {
        indexRequests.incrementAndGet();
        send(exchange, "{"
                + "\"code\":200,\"success\":true,\"results\":["
                + "{\"id\":1,\"type\":\"jdbc\",\"alias\":\"search_prod\","
                + "\"physicalNamespace\":\"physical_search\"},"
                + "{\"id\":2,\"type\":\"jdbc\",\"alias\":\"chat_prod\","
                + "\"physicalNamespace\":\"physical_chat\"}]}");
    }

    private void handleResolve(HttpExchange exchange) throws IOException {
        Map<String, String> form = parseForm(readAll(exchange.getRequestBody()));
        String alias = form.get("alias");
        resolveRequests.computeIfAbsent(alias, ignored -> new AtomicInteger()).incrementAndGet();
        send(exchange, "{"
                + "\"code\":200,\"success\":true,\"results\":{"
                + "\"url\":\"jdbc:fake:" + alias + "\","
                + "\"driver\":\"fake.Driver\","
                + "\"user\":\"reader\",\"password\":\"\"}}");
    }

    private void send(HttpExchange exchange, String body) throws IOException {
        byte[] response = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(200, response.length);
        exchange.getResponseBody().write(response);
        exchange.close();
    }

    private Map<String, String> parseForm(String body) throws IOException {
        Map<String, String> result = new LinkedHashMap<>();
        for (String pair : body.split("&")) {
            String[] parts = pair.split("=", 2);
            result.put(
                    URLDecoder.decode(parts[0], StandardCharsets.UTF_8.name()),
                    URLDecoder.decode(parts[1], StandardCharsets.UTF_8.name()));
        }
        return result;
    }

    private String readAll(InputStream input) throws IOException {
        try (InputStream source = input; ByteArrayOutputStream output = new ByteArrayOutputStream()) {
            byte[] buffer = new byte[1024];
            int read;
            while ((read = source.read(buffer)) != -1) {
                output.write(buffer, 0, read);
            }
            return new String(output.toByteArray(), StandardCharsets.UTF_8);
        }
    }

    public static final class FakeDelegateCatalog
            implements TableCatalog, SupportsNamespaces {

        private String name;
        private String marker;

        @Override
        public void initialize(String name, CaseInsensitiveStringMap options) {
            this.name = name;
            this.marker = options.get("url");
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
            return new FakeTable(ident.namespace()[0] + "." + ident.name() + "@" + marker);
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
        public void createNamespace(String[] namespace, Map<String, String> metadata) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void alterNamespace(String[] namespace, NamespaceChange... changes) {
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
        public Set<org.apache.spark.sql.connector.catalog.TableCapability> capabilities() {
            return Collections.emptySet();
        }
    }
}
