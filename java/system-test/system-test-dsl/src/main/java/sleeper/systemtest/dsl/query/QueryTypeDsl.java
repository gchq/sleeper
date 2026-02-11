/*
 * Copyright 2022-2025 Crown Copyright
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package sleeper.systemtest.dsl.query;

import sleeper.core.properties.model.DataEngine;
import sleeper.core.properties.table.TableProperty;
import sleeper.systemtest.dsl.SleeperDsl;

import java.util.HashMap;
import java.util.Map;
import java.util.function.UnaryOperator;

import static sleeper.core.properties.table.TableProperty.DATA_ENGINE;

public class QueryTypeDsl {

    private static final UnaryOperator<QueryDsl> DEFAULT_QUERY_TYPE = query -> query.direct();

    private final Map<TableProperty, String> tableProperties;
    private final UnaryOperator<QueryDsl> setQueryType;

    private QueryTypeDsl(Builder builder) {
        tableProperties = builder.tableProperties;
        setQueryType = builder.setQueryType;
    }

    public static Builder builder() {
        return new Builder();
    }

    public QueryDsl query(SleeperDsl sleeper) {
        sleeper.updateTableProperties(tableProperties);
        return setQueryType.apply(sleeper.query());
    }

    public static class Builder {
        private Map<TableProperty, String> tableProperties = new HashMap<>();
        private UnaryOperator<QueryDsl> setQueryType = DEFAULT_QUERY_TYPE;

        private Builder() {
        }

        public Builder java() {
            return tableProperties(Map.of(DATA_ENGINE, DataEngine.JAVA.toString()));
        }

        public Builder dataFusion() {
            return tableProperties(Map.of(DATA_ENGINE, DataEngine.DATAFUSION_EXPERIMENTAL.toString()));
        }

        public Builder direct() {
            return setQueryType(query -> query.direct());
        }

        public Builder byQueue() {
            return setQueryType(query -> query.byQueue());
        }

        public Builder webSocket() {
            return setQueryType(query -> query.webSocket());
        }

        public Builder tableProperties(Map<TableProperty, String> tableProperties) {
            this.tableProperties.putAll(tableProperties);
            return this;
        }

        public Builder setQueryType(UnaryOperator<QueryDsl> setQueryType) {
            this.setQueryType = setQueryType;
            return this;
        }

        public QueryTypeDsl build() {
            return new QueryTypeDsl(this);
        }
    }

}
