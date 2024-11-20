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

/**
 * A JSON representation of a request for a bulk export.
 */
class BulkExportQueryJson {
    private final String tableName;
    private final String tableId;
    private final String exportId;

    private BulkExportQueryJson(Builder builder) {
        tableName = builder.tableName;
        tableId = builder.tableId;
        exportId = builder.exportQueryId;
    }

    static BulkExportQueryJson from(BulkExportQuery query) {
        return builder()
                .tableId(query.getTableId())
                .tableName(query.getTableName())
                .exportQueryId(query.getExportId())
                .build();
    }

    BulkExportQuery to() {
        return BulkExportQuery.builder()
                .tableId(tableId)
                .tableName(tableName)
                .exportId(exportId)
                .build();
    }

    private static Builder builder() {
        return new Builder();
    }

    /**
     * Builder class for BulkExportQueryJson.
     */
    private static final class Builder {
        private String tableName;
        private String tableId;
        private String exportQueryId;

        private Builder() {
        }

        public Builder tableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        public Builder tableId(String tableId) {
            this.tableId = tableId;
            return this;
        }

        public Builder exportQueryId(String exportQueryId) {
            this.exportQueryId = exportQueryId;
            return this;
        }

        public BulkExportQueryJson build() {
            return new BulkExportQueryJson(this);
        }
    }
}
