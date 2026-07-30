package ai.sparkone.kyuubi.odep;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;

final class OdepDatasourceClient {

    private static final int DEFAULT_CONNECT_TIMEOUT_SECONDS = 5;
    private static final int DEFAULT_READ_TIMEOUT_SECONDS = 60;
    private static final int MAX_RESPONSE_BYTES = 10 * 1024 * 1024;
    private static final char[] NONCE_ALPHABET =
            "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789".toCharArray();

    private final URL endpoint;
    private final String appId;
    private final String signKey;
    private final int connectTimeoutMillis;
    private final int readTimeoutMillis;
    private final ObjectMapper objectMapper;
    private final SecureRandom secureRandom;

    static OdepDatasourceClient fromEnvironment() {
        return fromEnvironment(System.getenv());
    }

    static OdepDatasourceClient fromEnvironment(Map<String, String> environment) {
        String apiUrl = required(environment, "ODEP_API_URL");
        String endpoint = trimTrailingSlash(apiUrl) + "/api/datasource/snapshot";
        return new OdepDatasourceClient(
                endpoint,
                required(environment, "ODEP_KYUUBI_APP_ID"),
                required(environment, "ODEP_KYUUBI_SIGN_KEY"),
                positiveSecondsMillis(
                        environment,
                        "ODEP_CONNECT_TIMEOUT_SECONDS",
                        DEFAULT_CONNECT_TIMEOUT_SECONDS),
                positiveSecondsMillis(
                        environment,
                        "ODEP_REQUEST_TIMEOUT_SECONDS",
                        DEFAULT_READ_TIMEOUT_SECONDS));
    }

    OdepDatasourceClient(
            String endpoint,
            String appId,
            String signKey,
            int connectTimeoutMillis,
            int readTimeoutMillis) {
        try {
            this.endpoint = new URL(endpoint);
        } catch (Exception e) {
            throw new IllegalArgumentException("ODEP datasource snapshot URL is invalid", e);
        }
        if (!"http".equalsIgnoreCase(this.endpoint.getProtocol())
                && !"https".equalsIgnoreCase(this.endpoint.getProtocol())) {
            throw new IllegalArgumentException(
                    "ODEP datasource snapshot URL must use HTTP or HTTPS");
        }
        this.appId = requireNonBlank(appId, "ODEP appId");
        this.signKey = requireNonBlank(signKey, "ODEP sign key");
        this.connectTimeoutMillis = requirePositive(connectTimeoutMillis, "connect timeout");
        this.readTimeoutMillis = requirePositive(readTimeoutMillis, "read timeout");
        this.objectMapper = new ObjectMapper();
        this.secureRandom = new SecureRandom();
    }

    OdepDatasourceSnapshot load() {
        HttpURLConnection connection = null;
        try {
            String timestamp = String.valueOf(System.currentTimeMillis() / 1000);
            String nonce = nonce();
            String sign = sign(appId, signKey, nonce, timestamp);
            byte[] requestBody = formBody(appId, nonce, timestamp, sign);

            connection = (HttpURLConnection) endpoint.openConnection();
            connection.setInstanceFollowRedirects(false);
            connection.setRequestMethod("POST");
            connection.setConnectTimeout(connectTimeoutMillis);
            connection.setReadTimeout(readTimeoutMillis);
            connection.setDoOutput(true);
            connection.setRequestProperty(
                    "Content-Type",
                    "application/x-www-form-urlencoded; charset=UTF-8");
            connection.setFixedLengthStreamingMode(requestBody.length);
            try (OutputStream output = connection.getOutputStream()) {
                output.write(requestBody);
            }

            int status = connection.getResponseCode();
            if (status != HttpURLConnection.HTTP_OK) {
                closeQuietly(connection.getErrorStream());
                throw new IllegalStateException("ODEP datasource snapshot returned HTTP " + status);
            }
            try (InputStream input = connection.getInputStream()) {
                return parse(readLimited(input));
            }
        } catch (IOException e) {
            throw new IllegalStateException(
                    "Failed to load ODEP datasource snapshot from " + endpoint, e);
        } finally {
            if (connection != null) {
                connection.disconnect();
            }
        }
    }

    private OdepDatasourceSnapshot parse(byte[] responseBody) throws IOException {
        JsonNode root = objectMapper.readTree(responseBody);
        if (root == null || !root.isObject()) {
            throw new IllegalStateException("ODEP datasource snapshot response must be an object");
        }
        if (root.path("code").asInt(-1) != 200 || !root.path("success").asBoolean(false)) {
            throw new IllegalStateException(
                    "ODEP datasource snapshot business request failed: code="
                            + root.path("code").asInt(-1));
        }
        JsonNode datasourceNodes = root.path("results").path("datasources");
        if (!datasourceNodes.isArray()) {
            throw new IllegalStateException(
                    "ODEP datasource snapshot response is missing results.datasources");
        }

        List<OdepDatasourceSnapshot.Datasource> datasources = new ArrayList<>();
        for (JsonNode datasourceNode : datasourceNodes) {
            String type = requiredText(datasourceNode, "type");
            String alias = requiredText(datasourceNode, "alias");
            Map<String, String> options = parseOptions(datasourceNode.path("options"), type, alias);
            datasources.add(new OdepDatasourceSnapshot.Datasource(
                    nullableLong(datasourceNode.get("id")),
                    type,
                    alias,
                    nullableText(datasourceNode.get("description")),
                    options,
                    nullableText(datasourceNode.get("updateTime"))));
        }
        return new OdepDatasourceSnapshot(datasources);
    }

    private Map<String, String> parseOptions(JsonNode optionsNode, String type, String alias) {
        if (!optionsNode.isObject()) {
            throw new IllegalStateException(
                    "ODEP datasource options must be an object: type=" + type + ", alias=" + alias);
        }
        Map<String, String> options = new LinkedHashMap<>();
        Iterator<Map.Entry<String, JsonNode>> fields = optionsNode.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> field = fields.next();
            JsonNode valueNode = field.getValue();
            if (!valueNode.isValueNode()) {
                throw new IllegalStateException(
                        "ODEP datasource option must be a scalar value: type="
                                + type + ", alias=" + alias + ", key=" + field.getKey());
            }
            String value = valueNode.isNull() ? "" : valueNode.asText();
            if (value.contains("${")) {
                throw new IllegalStateException(
                        "ODEP datasource snapshot contains an unresolved placeholder: type="
                                + type + ", alias=" + alias + ", key=" + field.getKey());
            }
            options.put(field.getKey(), value);
        }
        return options;
    }

    static String sign(String appId, String signKey, String nonce, String timestamp) {
        Map<String, String> params = new TreeMap<>();
        params.put("appId", appId);
        params.put("appSignKey", signKey);
        params.put("nonce", nonce);
        params.put("timestamp", timestamp);
        StringBuilder payload = new StringBuilder();
        for (Map.Entry<String, String> entry : params.entrySet()) {
            if (payload.length() > 0) {
                payload.append('&');
            }
            payload.append(entry.getKey()).append('=').append(entry.getValue());
        }
        try {
            byte[] digest = MessageDigest.getInstance("SHA-1")
                    .digest(payload.toString().getBytes(StandardCharsets.UTF_8));
            StringBuilder hex = new StringBuilder(digest.length * 2);
            for (byte value : digest) {
                hex.append(String.format(Locale.ROOT, "%02x", value & 0xff));
            }
            return hex.toString();
        } catch (Exception e) {
            throw new IllegalStateException("SHA-1 is unavailable", e);
        }
    }

    private byte[] formBody(String appId, String nonce, String timestamp, String sign) {
        String body = "appId=" + encode(appId)
                + "&nonce=" + encode(nonce)
                + "&timestamp=" + encode(timestamp)
                + "&sign=" + encode(sign);
        return body.getBytes(StandardCharsets.UTF_8);
    }

    private byte[] readLimited(InputStream input) throws IOException {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        byte[] buffer = new byte[8192];
        int total = 0;
        int read;
        while ((read = input.read(buffer)) != -1) {
            total += read;
            if (total > MAX_RESPONSE_BYTES) {
                throw new IllegalStateException("ODEP datasource snapshot response is too large");
            }
            output.write(buffer, 0, read);
        }
        return output.toByteArray();
    }

    private String nonce() {
        char[] value = new char[16];
        for (int i = 0; i < value.length; i++) {
            value[i] = NONCE_ALPHABET[secureRandom.nextInt(NONCE_ALPHABET.length)];
        }
        return new String(value);
    }

    private static String requiredText(JsonNode node, String field) {
        String value = nullableText(node.get(field));
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalStateException("ODEP datasource field is required: " + field);
        }
        return value.trim();
    }

    private static String nullableText(JsonNode node) {
        return node == null || node.isNull() ? null : node.asText();
    }

    private static Long nullableLong(JsonNode node) {
        return node == null || node.isNull() ? null : node.asLong();
    }

    private static String encode(String value) {
        try {
            return URLEncoder.encode(value, StandardCharsets.UTF_8.name());
        } catch (Exception e) {
            throw new IllegalStateException("UTF-8 is unavailable", e);
        }
    }

    private static String required(Map<String, String> environment, String name) {
        return requireNonBlank(environment.get(name), name);
    }

    private static String requireNonBlank(String value, String name) {
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalStateException(name + " must be configured");
        }
        return value.trim();
    }

    private static int positiveSecondsMillis(
            Map<String, String> environment,
            String name,
            int defaultSeconds) {
        String value = environment.get(name);
        if (value == null || value.trim().isEmpty()) {
            return defaultSeconds * 1000;
        }
        try {
            int seconds = requirePositive(Integer.parseInt(value.trim()), name);
            return Math.multiplyExact(seconds, 1000);
        } catch (NumberFormatException e) {
            throw new IllegalStateException(name + " must be a positive integer", e);
        } catch (ArithmeticException e) {
            throw new IllegalStateException(name + " is too large", e);
        }
    }

    private static int requirePositive(int value, String name) {
        if (value <= 0) {
            throw new IllegalStateException(name + " must be positive");
        }
        return value;
    }

    private static String trimTrailingSlash(String value) {
        String result = requireNonBlank(value, "ODEP_API_URL");
        while (result.endsWith("/")) {
            result = result.substring(0, result.length() - 1);
        }
        return result;
    }

    private static void closeQuietly(InputStream input) {
        if (input == null) {
            return;
        }
        try {
            input.close();
        } catch (IOException ignored) {
            // Ignore cleanup errors after an unsuccessful response.
        }
    }
}
