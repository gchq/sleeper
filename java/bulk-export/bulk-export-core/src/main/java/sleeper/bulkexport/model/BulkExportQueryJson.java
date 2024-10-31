/*
 * Copyright 2022-2024 Crown Copyright
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

package sleeper.bulkexport.model;

import java.util.HashMap;
import java.util.Map;

class BulkExportQueryJson {
    private final String tableId;
    private final String tableName;
    private final String exportId;
    private final Map<String, String> resultsPublisherConfig;

    private BulkExportQueryJson(Builder builder) {
        tableName = builder.tableName;
        tableId = builder.tableId;
        exportId = builder.exportId;
        resultsPublisherConfig = builder.resultsPublisherConfig;
    }

    static BulkExportQueryJson from(BulkExportQuery query) {
        return builder()
                .tableId(query.getTableId())
                .tableName(query.getTableName())
                .exportId(query.getExportId())
                .resultsPublisherConfig(new HashMap<>()) // ToDo Fix
                .build();
    }

    static BulkExportQuery to(BulkExportQueryJson json) {
        return BulkExportQuery.builder()
                .tableId(json.tableId)
                .tableName(json.tableName)
                .exportId(json.exportId)
                .build();
    }

    private static Builder builder() {
        return new Builder();
    }

    private static final class Builder {
        private String tableName;
        private String tableId;
        private String exportId;
        private Map<String, String> resultsPublisherConfig;

        private Builder() {
        }

        public Builder exportId(String exportId) {
            this.exportId = exportId;
            return this;
        }

        public Builder tableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        public Builder tableId(String tableId) {
            this.tableId = tableId;
            return this;
        }

        public Builder resultsPublisherConfig(Map<String, String> resultsPublisherConfig) {
            this.resultsPublisherConfig = resultsPublisherConfig;
            return this;
        }

        public BulkExportQueryJson build() {
            return new BulkExportQueryJson(this);
        }
    }
}
