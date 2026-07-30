package ai.sparkone.kyuubi.odep;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

final class OdepDatasourceSnapshot {

    private final List<Datasource> datasources;

    OdepDatasourceSnapshot(List<Datasource> datasources) {
        this.datasources = Collections.unmodifiableList(new ArrayList<>(datasources));
    }

    List<Datasource> getDatasources() {
        return datasources;
    }

    static final class Datasource {

        private final Long id;
        private final String type;
        private final String alias;
        private final String description;
        private final Map<String, String> options;
        private final String updateTime;

        Datasource(
                Long id,
                String type,
                String alias,
                String description,
                Map<String, String> options,
                String updateTime) {
            this.id = id;
            this.type = type;
            this.alias = alias;
            this.description = description;
            this.options = Collections.unmodifiableMap(new LinkedHashMap<>(options));
            this.updateTime = updateTime;
        }

        Long getId() {
            return id;
        }

        String getType() {
            return type;
        }

        String getAlias() {
            return alias;
        }

        String getDescription() {
            return description;
        }

        Map<String, String> getOptions() {
            return options;
        }

        String getUpdateTime() {
            return updateTime;
        }
    }
}
