package ai.sparkone.kyuubi.odep;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Resolves ODEP datasource definitions lazily and caches successful lookups per engine JVM.
 */
public final class OdepDatasourceResolver {

    private final OdepDatasourceClient client;
    private final ConcurrentMap<String, Map<String, Metadata>> indexes =
            new ConcurrentHashMap<>();
    private final ConcurrentMap<String, ResolvedDatasource> resolvedDatasources =
            new ConcurrentHashMap<>();

    public OdepDatasourceResolver(
            String apiUrl,
            String appId,
            String signKey,
            int connectTimeoutMillis,
            int readTimeoutMillis) {
        this(new OdepDatasourceClient(
                apiUrl,
                appId,
                signKey,
                connectTimeoutMillis,
                readTimeoutMillis));
    }

    OdepDatasourceResolver(OdepDatasourceClient client) {
        this.client = client;
    }

    public static OdepDatasourceResolver getDefault() {
        return DefaultHolder.INSTANCE;
    }

    public List<Metadata> list(String type) {
        return new ArrayList<>(index(type).values());
    }

    public Metadata findMetadata(String type, String alias) {
        String normalizedType = identifier(type, "type");
        String normalizedAlias = identifier(alias, "alias");
        return index(normalizedType).get(normalizedAlias);
    }

    public ResolvedDatasource resolve(String type, String alias) {
        String normalizedType = identifier(type, "type");
        String normalizedAlias = identifier(alias, "alias");
        Metadata metadata = index(normalizedType).get(normalizedAlias);
        if (metadata == null) {
            throw new IllegalArgumentException(
                    "ODEP datasource is not registered with a physical namespace: type="
                            + type + ", alias=" + alias);
        }
        String cacheKey = normalizedType + "\u0000" + normalizedAlias;
        return resolvedDatasources.computeIfAbsent(
                cacheKey,
                ignored -> new ResolvedDatasource(
                        metadata,
                        delegateOptions(
                                normalizedType,
                                metadata,
                                client.loadOptions(metadata.getType(), metadata.getAlias()))));
    }

    private Map<String, Metadata> index(String type) {
        String normalizedType = identifier(type, "type");
        return indexes.computeIfAbsent(normalizedType, ignored -> loadIndex(normalizedType));
    }

    private Map<String, Metadata> loadIndex(String type) {
        Map<String, Metadata> result = new LinkedHashMap<>();
        for (Metadata metadata : client.loadIndex(type)) {
            String metadataType = identifier(metadata.getType(), "type");
            if (!type.equals(metadataType)) {
                throw new IllegalStateException(
                        "ODEP datasource index returned a different type: requested="
                                + type + ", actual=" + metadata.getType());
            }
            String alias = identifier(metadata.getAlias(), "alias");
            if (metadata.getPhysicalNamespace().trim().isEmpty()) {
                throw new IllegalStateException(
                        "ODEP datasource physical namespace is required: type="
                                + type + ", alias=" + metadata.getAlias());
            }
            if (result.put(alias, metadata) != null) {
                throw new IllegalStateException(
                        "Duplicate ODEP datasource alias: type="
                                + type + ", alias=" + metadata.getAlias());
            }
        }
        return Collections.unmodifiableMap(result);
    }

    private Map<String, String> delegateOptions(
            String type,
            Metadata metadata,
            Map<String, String> sourceOptions) {
        Map<String, String> options = new LinkedHashMap<>();
        if ("jdbc".equals(type)) {
            String url = required(sourceOptions, "url", metadata);
            options.put("url", isMysql(sourceOptions) ? withMysqlDatabaseTerm(url) : url);
            options.put("driver", required(sourceOptions, "driver", metadata));
            options.put("user", required(sourceOptions, "user", metadata));
            options.put("password", requiredAllowEmpty(sourceOptions, "password", metadata));
        } else if ("doris".equals(type)) {
            options.put("doris.fenodes", required(sourceOptions, "doris.fenodes", metadata));
            options.put("doris.user", required(sourceOptions, "user", metadata));
            options.put(
                    "doris.password",
                    requiredAllowEmpty(sourceOptions, "password", metadata));
            copyOptional(sourceOptions, options, "doris.query.port");
            copyOptional(sourceOptions, options, "doris.request.retries");
            copyOptional(sourceOptions, options, "doris.read.mode");
        } else {
            throw new IllegalArgumentException(
                    "ODEP routing catalog does not support datasource type: " + type);
        }
        return Collections.unmodifiableMap(options);
    }

    private void copyOptional(
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
            Metadata metadata) {
        String value = requiredAllowEmpty(options, key, metadata);
        if (value.trim().isEmpty()) {
            throw missingOption(key, metadata);
        }
        return value;
    }

    private String requiredAllowEmpty(
            Map<String, String> options,
            String key,
            Metadata metadata) {
        if (!options.containsKey(key) || options.get(key) == null) {
            throw missingOption(key, metadata);
        }
        return options.get(key);
    }

    private IllegalStateException missingOption(String key, Metadata metadata) {
        return new IllegalStateException(
                "ODEP datasource is missing required option: type="
                        + metadata.getType() + ", alias=" + metadata.getAlias() + ", key=" + key);
    }

    private String identifier(String value, String label) {
        if (value == null || !value.matches("[A-Za-z_][A-Za-z0-9_]*")) {
            throw new IllegalArgumentException(
                    "ODEP datasource " + label + " must be a simple identifier: " + value);
        }
        return value.toLowerCase(Locale.ROOT);
    }

    private static final class DefaultHolder {
        private static final OdepDatasourceResolver INSTANCE =
                new OdepDatasourceResolver(OdepDatasourceClient.fromEnvironment());
    }

    public static class Metadata {

        private final Long id;
        private final String type;
        private final String alias;
        private final String physicalNamespace;
        private final String description;
        private final String updateTime;

        Metadata(
                Long id,
                String type,
                String alias,
                String physicalNamespace,
                String description,
                String updateTime) {
            this.id = id;
            this.type = type;
            this.alias = alias;
            this.physicalNamespace = physicalNamespace;
            this.description = description;
            this.updateTime = updateTime;
        }

        public Long getId() {
            return id;
        }

        public String getType() {
            return type;
        }

        public String getAlias() {
            return alias;
        }

        public String getPhysicalNamespace() {
            return physicalNamespace;
        }

        public String getDescription() {
            return description;
        }

        public String getUpdateTime() {
            return updateTime;
        }
    }

    public static final class ResolvedDatasource extends Metadata {

        private final Map<String, String> options;

        private ResolvedDatasource(Metadata metadata, Map<String, String> options) {
            super(
                    metadata.getId(),
                    metadata.getType(),
                    metadata.getAlias(),
                    metadata.getPhysicalNamespace(),
                    metadata.getDescription(),
                    metadata.getUpdateTime());
            this.options = options;
        }

        public Map<String, String> getOptions() {
            return options;
        }
    }
}
