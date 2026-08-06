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
    private static final String API_URL_PROPERTY = "sparkone.odep.api.url";
    private static final String APP_ID_PROPERTY = "sparkone.odep.app.id";
    private static final String SIGN_KEY_PROPERTY = "sparkone.odep.sign.key";
    private static final String CONNECT_TIMEOUT_PROPERTY =
            "sparkone.odep.connect.timeout.seconds";
    private static final String REQUEST_TIMEOUT_PROPERTY =
            "sparkone.odep.request.timeout.seconds";
    private static final char[] NONCE_ALPHABET =
            "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789".toCharArray();

    private final URL indexEndpoint;
    private final URL resolveEndpoint;
    private final String appId;
    private final String signKey;
    private final int connectTimeoutMillis;
    private final int readTimeoutMillis;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final SecureRandom secureRandom = new SecureRandom();

    static OdepDatasourceClient fromRuntimeConfiguration() {
        Map<String, String> properties = new LinkedHashMap<>();
        properties.put(API_URL_PROPERTY, System.getProperty(API_URL_PROPERTY));
        properties.put(APP_ID_PROPERTY, System.getProperty(APP_ID_PROPERTY));
        properties.put(SIGN_KEY_PROPERTY, System.getProperty(SIGN_KEY_PROPERTY));
        properties.put(CONNECT_TIMEOUT_PROPERTY, System.getProperty(CONNECT_TIMEOUT_PROPERTY));
        properties.put(REQUEST_TIMEOUT_PROPERTY, System.getProperty(REQUEST_TIMEOUT_PROPERTY));
        return fromRuntimeConfiguration(System.getenv(), properties);
    }

    static OdepDatasourceClient fromRuntimeConfiguration(
            Map<String, String> environment,
            Map<String, String> properties) {
        Map<String, String> configuration = new LinkedHashMap<>();
        copyNonBlank(configuration, "ODEP_API_URL", properties.get(API_URL_PROPERTY));
        copyNonBlank(configuration, "ODEP_KYUUBI_APP_ID", properties.get(APP_ID_PROPERTY));
        copyNonBlank(configuration, "ODEP_KYUUBI_SIGN_KEY", properties.get(SIGN_KEY_PROPERTY));
        copyNonBlank(
                configuration,
                "ODEP_CONNECT_TIMEOUT_SECONDS",
                properties.get(CONNECT_TIMEOUT_PROPERTY));
        copyNonBlank(
                configuration,
                "ODEP_REQUEST_TIMEOUT_SECONDS",
                properties.get(REQUEST_TIMEOUT_PROPERTY));
        copyNonBlank(configuration, "ODEP_API_URL", environment.get("ODEP_API_URL"));
        copyNonBlank(
                configuration,
                "ODEP_KYUUBI_APP_ID",
                environment.get("ODEP_KYUUBI_APP_ID"));
        copyNonBlank(
                configuration,
                "ODEP_KYUUBI_SIGN_KEY",
                environment.get("ODEP_KYUUBI_SIGN_KEY"));
        copyNonBlank(
                configuration,
                "ODEP_CONNECT_TIMEOUT_SECONDS",
                environment.get("ODEP_CONNECT_TIMEOUT_SECONDS"));
        copyNonBlank(
                configuration,
                "ODEP_REQUEST_TIMEOUT_SECONDS",
                environment.get("ODEP_REQUEST_TIMEOUT_SECONDS"));
        return fromEnvironment(configuration);
    }

    static OdepDatasourceClient fromEnvironment(Map<String, String> environment) {
        return new OdepDatasourceClient(
                required(environment, "ODEP_API_URL"),
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
            String apiUrl,
            String appId,
            String signKey,
            int connectTimeoutMillis,
            int readTimeoutMillis) {
        String datasourceEndpoint = trimTrailingSlash(apiUrl) + "/api/datasource";
        try {
            indexEndpoint = new URL(datasourceEndpoint + "/index");
            resolveEndpoint = new URL(datasourceEndpoint + "/resolve");
        } catch (Exception e) {
            throw new IllegalArgumentException("ODEP datasource API URL is invalid", e);
        }
        validateHttpEndpoint(indexEndpoint);
        this.appId = requireNonBlank(appId, "ODEP appId");
        this.signKey = requireNonBlank(signKey, "ODEP sign key");
        this.connectTimeoutMillis = requirePositive(connectTimeoutMillis, "connect timeout");
        this.readTimeoutMillis = requirePositive(readTimeoutMillis, "read timeout");
    }

    List<OdepDatasourceResolver.Metadata> loadIndex(String type) {
        Map<String, String> request = new LinkedHashMap<>();
        request.put("type", type);
        JsonNode results = request(indexEndpoint, request, "index");
        if (!results.isArray()) {
            throw new IllegalStateException("ODEP datasource index results must be an array");
        }

        List<OdepDatasourceResolver.Metadata> metadata = new ArrayList<>();
        for (JsonNode datasource : results) {
            metadata.add(new OdepDatasourceResolver.Metadata(
                    nullableLong(datasource.get("id")),
                    requiredText(datasource, "type"),
                    requiredText(datasource, "alias"),
                    requiredText(datasource, "physicalNamespace"),
                    nullableText(datasource.get("description")),
                    nullableText(datasource.get("updateTime"))));
        }
        return metadata;
    }

    Map<String, String> loadOptions(String type, String alias) {
        Map<String, String> request = new LinkedHashMap<>();
        request.put("type", type);
        request.put("alias", alias);
        JsonNode results = request(resolveEndpoint, request, "resolve");
        if (!results.isObject()) {
            throw new IllegalStateException(
                    "ODEP datasource resolve results must be an object: type="
                            + type + ", alias=" + alias);
        }

        Map<String, String> options = new LinkedHashMap<>();
        Iterator<Map.Entry<String, JsonNode>> fields = results.fields();
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
                        "ODEP datasource resolve contains an unresolved placeholder: type="
                                + type + ", alias=" + alias + ", key=" + field.getKey());
            }
            options.put(field.getKey(), value);
        }
        if (options.isEmpty()) {
            throw new IllegalStateException(
                    "ODEP datasource resolve returned no options: type="
                            + type + ", alias=" + alias);
        }
        return options;
    }

    private JsonNode request(
            URL endpoint,
            Map<String, String> parameters,
            String operation) {
        HttpURLConnection connection = null;
        try {
            String timestamp = String.valueOf(System.currentTimeMillis() / 1000);
            String nonce = nonce();
            String signature = sign(appId, signKey, nonce, timestamp);
            Map<String, String> form = new LinkedHashMap<>();
            form.put("appId", appId);
            form.put("nonce", nonce);
            form.put("timestamp", timestamp);
            form.put("sign", signature);
            form.putAll(parameters);
            byte[] requestBody = formBody(form);

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
                throw new IllegalStateException(
                        "ODEP datasource " + operation + " returned HTTP " + status);
            }
            try (InputStream input = connection.getInputStream()) {
                return parseResults(readLimited(input), operation);
            }
        } catch (IOException e) {
            throw new IllegalStateException(
                    "Failed to load ODEP datasource " + operation + " from " + endpoint,
                    e);
        } finally {
            if (connection != null) {
                connection.disconnect();
            }
        }
    }

    private JsonNode parseResults(byte[] responseBody, String operation) throws IOException {
        JsonNode root = objectMapper.readTree(responseBody);
        if (root == null || !root.isObject()) {
            throw new IllegalStateException(
                    "ODEP datasource " + operation + " response must be an object");
        }
        if (root.path("code").asInt(-1) != 200 || !root.path("success").asBoolean(false)) {
            throw new IllegalStateException(
                    "ODEP datasource " + operation + " business request failed: code="
                            + root.path("code").asInt(-1));
        }
        JsonNode results = root.get("results");
        if (results == null || results.isNull()) {
            throw new IllegalStateException(
                    "ODEP datasource " + operation + " response is missing results");
        }
        return results;
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

    private byte[] formBody(Map<String, String> form) {
        StringBuilder body = new StringBuilder();
        for (Map.Entry<String, String> entry : form.entrySet()) {
            if (body.length() > 0) {
                body.append('&');
            }
            body.append(encode(entry.getKey())).append('=').append(encode(entry.getValue()));
        }
        return body.toString().getBytes(StandardCharsets.UTF_8);
    }

    private byte[] readLimited(InputStream input) throws IOException {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        byte[] buffer = new byte[8192];
        int total = 0;
        int read;
        while ((read = input.read(buffer)) != -1) {
            total += read;
            if (total > MAX_RESPONSE_BYTES) {
                throw new IllegalStateException("ODEP datasource API response is too large");
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

    private static void copyNonBlank(
            Map<String, String> target,
            String name,
            String value) {
        if (value != null && !value.trim().isEmpty()) {
            target.put(name, value.trim());
        }
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

    private static void validateHttpEndpoint(URL endpoint) {
        if (!"http".equalsIgnoreCase(endpoint.getProtocol())
                && !"https".equalsIgnoreCase(endpoint.getProtocol())) {
            throw new IllegalArgumentException(
                    "ODEP datasource API URL must use HTTP or HTTPS");
        }
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
