package queryone.kyuubi.odep;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;

public class OdepDatasourceClientTest {

    private static final String APP_ID = "app_kyuubi";
    private static final String SIGN_KEY = "test-sign-key";

    private final Map<String, String> responses = new ConcurrentHashMap<>();
    private final Map<String, Map<String, String>> requests = new ConcurrentHashMap<>();
    private final Map<String, AtomicInteger> requestCounts = new ConcurrentHashMap<>();
    private HttpServer server;
    private String apiUrl;

    @Before
    public void setUp() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/api/datasource/index", this::handle);
        server.createContext("/api/datasource/resolve", this::handle);
        server.start();
        apiUrl = "http://127.0.0.1:" + server.getAddress().getPort();
    }

    @After
    public void tearDown() {
        server.stop(0);
    }

    @Test
    public void shouldLoadSignedIndexWithoutDatasourceOptions() {
        respond("/api/datasource/index", "{"
                + "\"code\":200,"
                + "\"success\":true,"
                + "\"results\":[{"
                + "\"id\":1,"
                + "\"type\":\"jdbc\","
                + "\"alias\":\"dworks\","
                + "\"physicalNamespace\":\"Dworks\","
                + "\"description\":\"Dworks\","
                + "\"updateTime\":\"2026-07-30 10:20:00\""
                + "}]}");

        OdepDatasourceResolver.Metadata metadata = client().loadIndex("jdbc").get(0);

        assertEquals("jdbc", metadata.getType());
        assertEquals("dworks", metadata.getAlias());
        assertEquals("Dworks", metadata.getPhysicalNamespace());
        assertEquals("jdbc", requests.get("/api/datasource/index").get("type"));
        assertFalse(requests.get("/api/datasource/index").containsKey("alias"));
    }

    @Test
    public void shouldResolveOptionsByTypeAndAlias() {
        respond("/api/datasource/resolve", "{"
                + "\"code\":200,"
                + "\"success\":true,"
                + "\"results\":{"
                + "\"url\":\"jdbc:mysql://mysql.internal:3306/Dworks\","
                + "\"driver\":\"com.mysql.cj.jdbc.Driver\","
                + "\"user\":\"reader\","
                + "\"password\":\"masked\""
                + "}}");

        Map<String, String> options = client().loadOptions("jdbc", "dworks");

        assertEquals("reader", options.get("user"));
        assertEquals("jdbc", requests.get("/api/datasource/resolve").get("type"));
        assertEquals("dworks", requests.get("/api/datasource/resolve").get("alias"));
    }

    @Test
    public void shouldCacheIndexAndResolvedAliasForEngineLifetime() {
        respond("/api/datasource/index", "{"
                + "\"code\":200,\"success\":true,\"results\":[{"
                + "\"id\":1,\"type\":\"jdbc\",\"alias\":\"dworks\","
                + "\"physicalNamespace\":\"Dworks\"}]}");
        respond("/api/datasource/resolve", "{"
                + "\"code\":200,\"success\":true,\"results\":{"
                + "\"url\":\"jdbc:mysql://mysql.internal:3306/Dworks\","
                + "\"driver\":\"com.mysql.cj.jdbc.Driver\","
                + "\"user\":\"reader\",\"password\":\"\"}}");
        OdepDatasourceResolver resolver = new OdepDatasourceResolver(client());

        resolver.list("jdbc");
        resolver.list("JDBC");
        OdepDatasourceResolver.ResolvedDatasource first = resolver.resolve("jdbc", "dworks");
        OdepDatasourceResolver.ResolvedDatasource second = resolver.resolve("JDBC", "DWORKS");

        assertEquals(1, count("/api/datasource/index"));
        assertEquals(1, count("/api/datasource/resolve"));
        assertEquals(first, second);
        assertEquals(
                "jdbc:mysql://mysql.internal:3306/Dworks?databaseTerm=SCHEMA",
                first.getOptions().get("url"));
    }

    @Test
    public void shouldRejectUnresolvedPlaceholderInResolveResponse() {
        respond("/api/datasource/resolve", "{"
                + "\"code\":200,\"success\":true,"
                + "\"results\":{\"url\":\"jdbc:mysql://${host}/db\"}}");

        IllegalStateException error = assertThrows(
                IllegalStateException.class,
                () -> client().loadOptions("jdbc", "broken"));

        assertEquals(
                "ODEP datasource resolve contains an unresolved placeholder: "
                        + "type=jdbc, alias=broken, key=url",
                error.getMessage());
    }

    @Test
    public void shouldFailWhenRequiredEngineEnvironmentIsMissing() {
        IllegalStateException error = assertThrows(
                IllegalStateException.class,
                () -> OdepDatasourceClient.fromEnvironment(Collections.emptyMap()));

        assertEquals("ODEP_API_URL must be configured", error.getMessage());
    }

    @Test
    public void shouldUseRuntimePropertiesWhenEnvironmentIsMissing() {
        respond("/api/datasource/index", "{\"code\":200,\"success\":true,\"results\":[]}");
        Map<String, String> properties = new LinkedHashMap<>();
        properties.put("queryone.odep.api.url", apiUrl);
        properties.put("queryone.odep.app.id", APP_ID);
        properties.put("queryone.odep.sign.key", SIGN_KEY);
        properties.put("queryone.odep.connect.timeout.seconds", "2");
        properties.put("queryone.odep.request.timeout.seconds", "2");

        OdepDatasourceClient.fromRuntimeConfiguration(Collections.emptyMap(), properties)
                .loadIndex("jdbc");

        assertEquals(APP_ID, requests.get("/api/datasource/index").get("appId"));
    }

    @Test
    public void shouldPreferEnvironmentOverRuntimeProperties() {
        respond("/api/datasource/index", "{\"code\":200,\"success\":true,\"results\":[]}");
        Map<String, String> properties = new LinkedHashMap<>();
        properties.put("queryone.odep.api.url", "ftp://invalid.example");
        properties.put("queryone.odep.app.id", "wrong-app");
        properties.put("queryone.odep.sign.key", "wrong-key");
        properties.put("queryone.odep.connect.timeout.seconds", "invalid");
        properties.put("queryone.odep.request.timeout.seconds", "invalid");
        Map<String, String> environment = new LinkedHashMap<>();
        environment.put("ODEP_API_URL", apiUrl);
        environment.put("ODEP_KYUUBI_APP_ID", APP_ID);
        environment.put("ODEP_KYUUBI_SIGN_KEY", SIGN_KEY);
        environment.put("ODEP_CONNECT_TIMEOUT_SECONDS", "2");
        environment.put("ODEP_REQUEST_TIMEOUT_SECONDS", "2");

        OdepDatasourceClient.fromRuntimeConfiguration(environment, properties)
                .loadIndex("jdbc");

        assertEquals(APP_ID, requests.get("/api/datasource/index").get("appId"));
    }

    private OdepDatasourceClient client() {
        return new OdepDatasourceClient(apiUrl, APP_ID, SIGN_KEY, 2000, 2000);
    }

    private void respond(String path, String response) {
        responses.put(path, response);
    }

    private int count(String path) {
        return requestCounts.getOrDefault(path, new AtomicInteger()).get();
    }

    private void handle(HttpExchange exchange) throws IOException {
        String path = exchange.getRequestURI().getPath();
        Map<String, String> form = parseForm(readAll(exchange.getRequestBody()));
        requests.put(path, form);
        requestCounts.computeIfAbsent(path, ignored -> new AtomicInteger()).incrementAndGet();
        assertFalse(form.containsKey("appSignKey"));
        String expectedSign = OdepDatasourceClient.sign(
                form.get("appId"),
                SIGN_KEY,
                form.get("nonce"),
                form.get("timestamp"));
        if (!APP_ID.equals(form.get("appId")) || !expectedSign.equals(form.get("sign"))) {
            exchange.sendResponseHeaders(403, -1);
            exchange.close();
            return;
        }

        byte[] response = responses.get(path).getBytes(StandardCharsets.UTF_8);
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
}
