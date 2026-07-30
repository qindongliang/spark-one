package ai.sparkone.kyuubi.odep;

import org.apache.kyuubi.plugin.SessionConfAdvisor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.function.Supplier;

public final class OdepDatasourceSessionConfAdvisor implements SessionConfAdvisor {

    private static final Logger LOGGER =
            LoggerFactory.getLogger(OdepDatasourceSessionConfAdvisor.class);

    private final OdepDatasourceSnapshot snapshot;
    private final Map<String, String> sparkConfOverlay;

    public OdepDatasourceSessionConfAdvisor() {
        this(OdepDatasourceClient.fromEnvironment()::load, new SparkCatalogConfigMapper());
    }

    OdepDatasourceSessionConfAdvisor(
            Supplier<OdepDatasourceSnapshot> snapshotLoader,
            SparkCatalogConfigMapper configMapper) {
        snapshot = snapshotLoader.get();
        sparkConfOverlay = configMapper.toSparkConf(snapshot);
        LOGGER.info(
                "Loaded {} ODEP datasource definitions and generated {} Spark catalogs",
                snapshot.getDatasources().size(),
                catalogCount(sparkConfOverlay));
    }

    @Override
    public Map<String, String> getConfOverlay(
            String user,
            Map<String, String> sessionConf) {
        return sparkConfOverlay;
    }

    OdepDatasourceSnapshot getSnapshot() {
        return snapshot;
    }

    private int catalogCount(Map<String, String> configuration) {
        int count = 0;
        for (String key : configuration.keySet()) {
            if (key.startsWith("spark.sql.catalog.")
                    && key.indexOf('.', "spark.sql.catalog.".length()) < 0) {
                count++;
            }
        }
        return count;
    }
}
