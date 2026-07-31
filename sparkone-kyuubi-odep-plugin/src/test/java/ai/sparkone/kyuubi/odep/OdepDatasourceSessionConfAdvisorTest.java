package ai.sparkone.kyuubi.odep;

import org.junit.Test;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public class OdepDatasourceSessionConfAdvisorTest {

    @Test
    public void shouldLoadSnapshotOnceWhenAdvisorIsConstructed() {
        AtomicInteger loadCount = new AtomicInteger();
        Map<String, String> options = new LinkedHashMap<>();
        options.put("url", "jdbc:mysql://mysql.internal/db");
        options.put("driver", "com.mysql.cj.jdbc.Driver");
        options.put("user", "reader");
        options.put("password", "secret");
        OdepDatasourceSnapshot snapshot = new OdepDatasourceSnapshot(Collections.singletonList(
                new OdepDatasourceSnapshot.Datasource(
                        1L,
                        "jdbc",
                        "dworks",
                        "Dworks",
                        null,
                        options,
                        null)));

        OdepDatasourceSessionConfAdvisor advisor = new OdepDatasourceSessionConfAdvisor(
                () -> {
                    loadCount.incrementAndGet();
                    return snapshot;
                },
                new SparkCatalogConfigMapper());

        assertEquals(1, loadCount.get());
        assertEquals(1, advisor.getSnapshot().getDatasources().size());
        assertEquals(
                "reader",
                advisor.getConfOverlay("alice", Collections.emptyMap())
                        .get("spark.sql.catalog.jdbc.odep.datasource.0.option.user"));
        advisor.getConfOverlay("bob", Collections.emptyMap());
        assertEquals(1, loadCount.get());
    }

    @Test
    public void shouldReturnImmutableConfiguration() {
        OdepDatasourceSessionConfAdvisor advisor = new OdepDatasourceSessionConfAdvisor(
                () -> new OdepDatasourceSnapshot(Collections.emptyList()),
                new SparkCatalogConfigMapper());

        Map<String, String> configuration =
                advisor.getConfOverlay("alice", Collections.emptyMap());

        assertThrows(
                UnsupportedOperationException.class,
                () -> configuration.put("spark.sql.catalog.invalid", "invalid"));
    }

    @Test
    public void shouldPropagateSnapshotLoadFailureDuringConstruction() {
        IllegalStateException error = assertThrows(
                IllegalStateException.class,
                () -> new OdepDatasourceSessionConfAdvisor(
                        () -> {
                            throw new IllegalStateException("ODEP unavailable");
                        },
                        new SparkCatalogConfigMapper()));

        assertEquals("ODEP unavailable", error.getMessage());
    }
}
