package ai.sparkone.kyuubi.odep.catalog;

import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.NamespaceChange;
import org.apache.spark.sql.connector.catalog.SupportsNamespaces;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.catalog.TableChange;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public final class OdepRoutingCatalog implements TableCatalog, SupportsNamespaces {

    private static final String ODEP_PREFIX = "odep.";
    private static final String DELEGATE_CLASS = "odep.delegate.class";
    private static final String DATASOURCE_COUNT = "odep.datasource.count";

    private String catalogName;
    private Map<String, Route> routes = Collections.emptyMap();

    @Override
    public void initialize(String name, CaseInsensitiveStringMap options) {
        if (catalogName != null) {
            throw new IllegalStateException("ODEP routing catalog is already initialized");
        }
        rejectForeignOptions(options);
        String delegateClass = required(options, DELEGATE_CLASS);
        int datasourceCount = positiveInt(required(options, DATASOURCE_COUNT), DATASOURCE_COUNT);
        Map<String, Route> loadedRoutes = new LinkedHashMap<>();
        for (int index = 0; index < datasourceCount; index++) {
            String prefix = "odep.datasource." + index + ".";
            String alias = required(options, prefix + "alias").trim();
            String physicalNamespace =
                    required(options, prefix + "physicalnamespace").trim();
            String normalizedAlias = normalize(alias);
            if (loadedRoutes.containsKey(normalizedAlias)) {
                throw new IllegalArgumentException(
                        "Duplicate ODEP routing catalog alias: " + alias);
            }
            Map<String, String> delegateOptions =
                    delegateOptions(options.asCaseSensitiveMap(), prefix + "option.");
            TableCatalog delegate = newDelegate(delegateClass);
            delegate.initialize(
                    name + "__odep_" + index,
                    new CaseInsensitiveStringMap(delegateOptions));
            if (!(delegate instanceof SupportsNamespaces)) {
                throw new IllegalArgumentException(
                        "ODEP routing delegate must support namespaces: " + delegateClass);
            }
            loadedRoutes.put(
                    normalizedAlias,
                    new Route(
                            alias,
                            physicalNamespace,
                            delegate));
        }
        catalogName = name;
        routes = Collections.unmodifiableMap(loadedRoutes);
    }

    @Override
    public String name() {
        if (catalogName == null) {
            throw new IllegalStateException("ODEP routing catalog is not initialized");
        }
        return catalogName;
    }

    @Override
    public String[][] listNamespaces() {
        String[][] namespaces = new String[routes.size()][];
        int index = 0;
        for (Route route : routes.values()) {
            namespaces[index++] = new String[] {route.alias};
        }
        return namespaces;
    }

    @Override
    public String[][] listNamespaces(String[] namespace) throws NoSuchNamespaceException {
        if (namespace.length == 0) {
            return listNamespaces();
        }
        namespaceRoute(namespace);
        return new String[0][];
    }

    @Override
    public boolean namespaceExists(String[] namespace) {
        return namespace.length == 1 && routes.containsKey(normalize(namespace[0]));
    }

    @Override
    public Map<String, String> loadNamespaceMetadata(String[] namespace)
            throws NoSuchNamespaceException {
        namespaceRoute(namespace);
        return Collections.emptyMap();
    }

    @Override
    public Identifier[] listTables(String[] namespace) throws NoSuchNamespaceException {
        Route route = namespaceRoute(namespace);
        Identifier[] physicalTables =
                route.tableCatalog.listTables(new String[] {route.physicalNamespace});
        Identifier[] logicalTables = new Identifier[physicalTables.length];
        for (int index = 0; index < physicalTables.length; index++) {
            logicalTables[index] =
                    Identifier.of(new String[] {route.alias}, physicalTables[index].name());
        }
        return logicalTables;
    }

    @Override
    public Table loadTable(Identifier ident) throws NoSuchTableException {
        Route route = tableRoute(ident);
        return route.tableCatalog.loadTable(physicalIdentifier(route, ident));
    }

    @Override
    public boolean tableExists(Identifier ident) {
        Route route = routes.get(routeKey(ident));
        return route != null
                && ident.namespace().length == 1
                && route.tableCatalog.tableExists(physicalIdentifier(route, ident));
    }

    @Override
    public void invalidateTable(Identifier ident) {
        Route route = routes.get(routeKey(ident));
        if (route != null && ident.namespace().length == 1) {
            route.tableCatalog.invalidateTable(physicalIdentifier(route, ident));
        }
    }

    @Override
    public Table createTable(
            Identifier ident,
            StructType schema,
            Transform[] partitions,
            Map<String, String> properties) {
        throw managedExternally();
    }

    @Override
    public Table alterTable(Identifier ident, TableChange... changes) {
        throw managedExternally();
    }

    @Override
    public boolean dropTable(Identifier ident) {
        throw managedExternally();
    }

    @Override
    public void renameTable(Identifier oldIdent, Identifier newIdent) {
        throw managedExternally();
    }

    @Override
    public void createNamespace(String[] namespace, Map<String, String> metadata) {
        throw managedExternally();
    }

    @Override
    public void alterNamespace(String[] namespace, NamespaceChange... changes) {
        throw managedExternally();
    }

    @Override
    public boolean dropNamespace(String[] namespace, boolean cascade) {
        throw managedExternally();
    }

    private Route namespaceRoute(String[] namespace) throws NoSuchNamespaceException {
        if (namespace.length != 1) {
            throw new NoSuchNamespaceException(namespace);
        }
        Route route = routes.get(normalize(namespace[0]));
        if (route == null) {
            throw new NoSuchNamespaceException(namespace);
        }
        return route;
    }

    private Route tableRoute(Identifier ident) throws NoSuchTableException {
        if (ident.namespace().length != 1) {
            throw new NoSuchTableException(ident);
        }
        Route route = routes.get(normalize(ident.namespace()[0]));
        if (route == null) {
            throw new NoSuchTableException(ident);
        }
        return route;
    }

    private String routeKey(Identifier ident) {
        return ident.namespace().length == 0 ? "" : normalize(ident.namespace()[0]);
    }

    private Identifier physicalIdentifier(Route route, Identifier logicalIdentifier) {
        return Identifier.of(
                new String[] {route.physicalNamespace},
                logicalIdentifier.name());
    }

    private void rejectForeignOptions(CaseInsensitiveStringMap options) {
        List<String> foreignKeys = new ArrayList<>();
        for (String key : options.keySet()) {
            if (!key.startsWith(ODEP_PREFIX)) {
                foreignKeys.add(key);
            }
        }
        if (!foreignKeys.isEmpty()) {
            Collections.sort(foreignKeys);
            throw new IllegalArgumentException(
                    "ODEP routing catalog owns its complete Spark catalog prefix; "
                            + "remove conflicting static options: " + foreignKeys);
        }
    }

    private Map<String, String> delegateOptions(
            Map<String, String> options,
            String optionPrefix) {
        Map<String, String> result = new LinkedHashMap<>();
        for (Map.Entry<String, String> option : options.entrySet()) {
            if (option.getKey().toLowerCase(Locale.ROOT).startsWith(optionPrefix)) {
                result.put(
                        option.getKey().substring(optionPrefix.length()),
                        option.getValue());
            }
        }
        if (result.isEmpty()) {
            throw new IllegalArgumentException(
                    "ODEP routing datasource has no delegate options: " + optionPrefix);
        }
        return result;
    }

    private TableCatalog newDelegate(String className) {
        try {
            ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
            Class<?> delegateClass = Class.forName(className, true, classLoader);
            Object delegate = delegateClass.getDeclaredConstructor().newInstance();
            if (!(delegate instanceof TableCatalog)) {
                throw new IllegalArgumentException(
                        "ODEP routing delegate is not a Spark TableCatalog: " + className);
            }
            return (TableCatalog) delegate;
        } catch (ReflectiveOperationException e) {
            throw new IllegalArgumentException(
                    "Cannot initialize ODEP routing delegate catalog: " + className,
                    e);
        }
    }

    private String required(CaseInsensitiveStringMap options, String key) {
        String value = options.get(key);
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException(
                    "Missing ODEP routing catalog option: " + key);
        }
        return value;
    }

    private int positiveInt(String value, String key) {
        try {
            int parsed = Integer.parseInt(value);
            if (parsed <= 0) {
                throw new NumberFormatException(value);
            }
            return parsed;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    "ODEP routing catalog option must be a positive integer: "
                            + key + "=" + value,
                    e);
        }
    }

    private String normalize(String value) {
        return value.toLowerCase(Locale.ROOT);
    }

    private UnsupportedOperationException managedExternally() {
        return new UnsupportedOperationException(
                "ODEP routing catalog namespaces and tables are managed externally");
    }

    private static final class Route {

        private final String alias;
        private final String physicalNamespace;
        private final TableCatalog tableCatalog;

        private Route(
                String alias,
                String physicalNamespace,
                TableCatalog tableCatalog) {
            this.alias = alias;
            this.physicalNamespace = physicalNamespace;
            this.tableCatalog = tableCatalog;
        }
    }
}
