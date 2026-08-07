package queryone.kyuubi.odep.catalog;

import queryone.kyuubi.odep.OdepDatasourceResolver;
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
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/** Routes {@code catalog.alias.table} to an ODEP datasource loaded on first use. */
public final class OdepRoutingCatalog implements TableCatalog, SupportsNamespaces {

    private static final String DELEGATE_CLASS = "odep.delegate.class";
    private static final String JDBC_CATALOG_CLASS =
            "org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCTableCatalog";
    private static final String DORIS_CATALOG_CLASS =
            "org.apache.doris.spark.catalog.DorisTableCatalog";

    private final OdepDatasourceResolver resolver;
    private final ConcurrentMap<String, Route> routes = new ConcurrentHashMap<>();

    private String catalogName;
    private String datasourceType;
    private String delegateClassName;

    public OdepRoutingCatalog() {
        this(OdepDatasourceResolver.getDefault());
    }

    OdepRoutingCatalog(OdepDatasourceResolver resolver) {
        this.resolver = Objects.requireNonNull(resolver, "resolver");
    }

    @Override
    public void initialize(String name, CaseInsensitiveStringMap options) {
        if (catalogName != null) {
            throw new IllegalStateException("ODEP routing catalog is already initialized");
        }
        rejectConflictingOptions(options);
        String type = normalize(name);
        String defaultDelegate;
        if ("jdbc".equals(type)) {
            defaultDelegate = JDBC_CATALOG_CLASS;
        } else if ("doris".equals(type)) {
            defaultDelegate = DORIS_CATALOG_CLASS;
        } else {
            throw new IllegalArgumentException(
                    "ODEP routing catalog name must be jdbc or doris: " + name);
        }

        String configuredDelegate = options.get(DELEGATE_CLASS);
        catalogName = name;
        datasourceType = type;
        delegateClassName = configuredDelegate == null || configuredDelegate.trim().isEmpty()
                ? defaultDelegate
                : configuredDelegate.trim();
    }

    @Override
    public String name() {
        ensureInitialized();
        return catalogName;
    }

    @Override
    public String[][] listNamespaces() {
        ensureInitialized();
        List<OdepDatasourceResolver.Metadata> metadata = resolver.list(datasourceType);
        String[][] namespaces = new String[metadata.size()][];
        for (int index = 0; index < metadata.size(); index++) {
            namespaces[index] = new String[] {metadata.get(index).getAlias()};
        }
        return namespaces;
    }

    @Override
    public String[][] listNamespaces(String[] namespace) throws NoSuchNamespaceException {
        if (namespace.length == 0) {
            return listNamespaces();
        }
        metadata(namespace);
        return new String[0][];
    }

    @Override
    public boolean namespaceExists(String[] namespace) {
        ensureInitialized();
        return namespace.length == 1
                && resolver.findMetadata(datasourceType, namespace[0]) != null;
    }

    @Override
    public Map<String, String> loadNamespaceMetadata(String[] namespace)
            throws NoSuchNamespaceException {
        metadata(namespace);
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
        Route route = optionalTableRoute(ident);
        return route != null && route.tableCatalog.tableExists(physicalIdentifier(route, ident));
    }

    @Override
    public void invalidateTable(Identifier ident) {
        Route route = optionalTableRoute(ident);
        if (route != null) {
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

    private OdepDatasourceResolver.Metadata metadata(String[] namespace)
            throws NoSuchNamespaceException {
        ensureInitialized();
        if (namespace.length != 1) {
            throw new NoSuchNamespaceException(namespace);
        }
        OdepDatasourceResolver.Metadata metadata =
                resolver.findMetadata(datasourceType, namespace[0]);
        if (metadata == null) {
            throw new NoSuchNamespaceException(namespace);
        }
        return metadata;
    }

    private Route namespaceRoute(String[] namespace) throws NoSuchNamespaceException {
        OdepDatasourceResolver.Metadata metadata = metadata(namespace);
        return route(metadata);
    }

    private Route tableRoute(Identifier ident) throws NoSuchTableException {
        if (ident.namespace().length != 1) {
            throw new NoSuchTableException(ident);
        }
        OdepDatasourceResolver.Metadata metadata =
                resolver.findMetadata(datasourceType, ident.namespace()[0]);
        if (metadata == null) {
            throw new NoSuchTableException(ident);
        }
        return route(metadata);
    }

    private Route optionalTableRoute(Identifier ident) {
        ensureInitialized();
        if (ident.namespace().length != 1) {
            return null;
        }
        OdepDatasourceResolver.Metadata metadata =
                resolver.findMetadata(datasourceType, ident.namespace()[0]);
        return metadata == null ? null : route(metadata);
    }

    private Route route(OdepDatasourceResolver.Metadata metadata) {
        String key = normalize(metadata.getAlias());
        return routes.computeIfAbsent(key, ignored -> createRoute(metadata));
    }

    private Route createRoute(OdepDatasourceResolver.Metadata metadata) {
        OdepDatasourceResolver.ResolvedDatasource datasource =
                resolver.resolve(datasourceType, metadata.getAlias());
        TableCatalog delegate = newDelegate(delegateClassName);
        delegate.initialize(
                catalogName + "__" + normalize(datasource.getAlias()),
                new CaseInsensitiveStringMap(datasource.getOptions()));
        if (!(delegate instanceof SupportsNamespaces)) {
            throw new IllegalArgumentException(
                    "ODEP routing delegate must support namespaces: " + delegateClassName);
        }
        return new Route(
                datasource.getAlias(),
                datasource.getPhysicalNamespace(),
                delegate);
    }

    private Identifier physicalIdentifier(Route route, Identifier logicalIdentifier) {
        return Identifier.of(
                new String[] {route.physicalNamespace},
                logicalIdentifier.name());
    }

    private void rejectConflictingOptions(CaseInsensitiveStringMap options) {
        List<String> conflictingKeys = new ArrayList<>();
        for (String key : options.keySet()) {
            if (!DELEGATE_CLASS.equalsIgnoreCase(key)) {
                conflictingKeys.add(key);
            }
        }
        if (!conflictingKeys.isEmpty()) {
            Collections.sort(conflictingKeys);
            throw new IllegalArgumentException(
                    "ODEP routing catalog owns its complete Spark catalog prefix; "
                            + "remove conflicting static options: " + conflictingKeys);
        }
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

    private void ensureInitialized() {
        if (catalogName == null) {
            throw new IllegalStateException("ODEP routing catalog is not initialized");
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

        private Route(String alias, String physicalNamespace, TableCatalog tableCatalog) {
            this.alias = alias;
            this.physicalNamespace = physicalNamespace;
            this.tableCatalog = tableCatalog;
        }
    }
}
