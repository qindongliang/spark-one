package ai.sparkone.kyuubi.odep;

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
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;

public class OdepDatasourceClientTest {

    private static final String APP_ID = "app_kyuubi";
    private static final String SIGN_KEY = "test-sign-key";

    private final AtomicReference<String> responseBody = new AtomicReference<>();
    private HttpServer server;
    private String endpoint;

    @Before
    public void setUp() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/api/datasource/snapshot", this::handle);
        server.start();
        endpoint = "http://127.0.0.1:" + server.getAddress().getPort()
                + "/api/datasource/snapshot";
    }

    @After
    public void tearDown() {
        server.stop(0);
    }

    @Test
    public void shouldSignRequestAndParseSnapshot() {
        responseBody.set("{"
                + "\"code\":200,"
                + "\"success\":true,"
                + "\"results\":{\"datasources\":[{"
                + "\"id\":1,"
                + "\"type\":\"jdbc\","
                + "\"alias\":\"dworks\","
                + "\"description\":\"Dworks\","
                + "\"options\":{"
                + "\"url\":\"jdbc:mysql://mysql.internal:3306/Dworks\","
                + "\"driver\":\"com.mysql.cj.jdbc.Driver\","
                + "\"user\":\"reader\","
                + "\"password\":\"secret\"},"
                + "\"updateTime\":\"2026-07-30 10:20:00\""
                + "}]}}");

        OdepDatasourceSnapshot snapshot = client().load();

        assertEquals(1, snapshot.getDatasources().size());
        OdepDatasourceSnapshot.Datasource datasource = snapshot.getDatasources().get(0);
        assertEquals("jdbc", datasource.getType());
        assertEquals("dworks", datasource.getAlias());
        assertEquals("reader", datasource.getOptions().get("user"));
    }

    @Test
    public void shouldRejectUnresolvedPlaceholder() {
        responseBody.set("{"
                + "\"code\":200,"
                + "\"success\":true,"
                + "\"results\":{\"datasources\":[{"
                + "\"type\":\"jdbc\","
                + "\"alias\":\"broken\","
                + "\"options\":{\"url\":\"jdbc:mysql://${host}/db\"}"
                + "}]}}");

        IllegalStateException error = assertThrows(
                IllegalStateException.class,
                () -> client().load());

        assertEquals(
                "ODEP datasource snapshot contains an unresolved placeholder: "
                        + "type=jdbc, alias=broken, key=url",
                error.getMessage());
    }

    @Test
    public void shouldFailWhenRequiredStartupEnvironmentIsMissing() {
        IllegalStateException error = assertThrows(
                IllegalStateException.class,
                () -> OdepDatasourceClient.fromEnvironment(Collections.emptyMap()));

        assertEquals("ODEP_API_URL must be configured", error.getMessage());
    }

    private OdepDatasourceClient client() {
        return new OdepDatasourceClient(endpoint, APP_ID, SIGN_KEY, 2000, 2000);
    }

    private void handle(HttpExchange exchange) throws IOException {
        Map<String, String> form = parseForm(readAll(exchange.getRequestBody()));
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

        byte[] response = responseBody.get().getBytes(StandardCharsets.UTF_8);
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
