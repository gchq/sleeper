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

import java.util.List;

/**
 * A JSON representation of a request for a bulk export.
 */
class BulkExportLeafPartitionQueryJson {
    private final String tableName;
    private final String tableId;
    private final String exportId;
    private final String subExportId;
    private final String leafPartitionId;
    private final List<String> files;

    private BulkExportLeafPartitionQueryJson(Builder builder) {
        tableName = builder.tableName;
        tableId = builder.tableId;
        exportId = builder.exportQueryId;
        subExportId = builder.subExportQueryId;
        leafPartitionId = builder.leafPartitionId;
        files = builder.files;
    }

    static BulkExportLeafPartitionQueryJson from(BulkExportLeafPartitionQuery leafQuery) {
        return builder()
                .tableId(leafQuery.getTableId())
                .exportQueryId(leafQuery.getExportId())
                .subExportQueryId(leafQuery.getSubExportId())
                .leafPartitionId(leafQuery.getLeafPartitionId())
                .files(leafQuery.getFiles())
                .build();
    }

    BulkExportLeafPartitionQuery to() {
        return BulkExportLeafPartitionQuery.builder()
                .tableId(tableId)
                .exportId(exportId)
                .subExportId(subExportId)
                .leafPartitionId(leafPartitionId)
                .files(files)
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
        private String subExportQueryId;
        private String leafPartitionId;
        private List<String> files;

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

        public Builder subExportQueryId(String subExportQueryId) {
            this.subExportQueryId = subExportQueryId;
            return this;
        }

        public Builder leafPartitionId(String leafPartitionId) {
            this.leafPartitionId = leafPartitionId;
            return this;
        }

        public Builder files(List<String> files) {
            this.files = files;
            return this;
        }

        public BulkExportLeafPartitionQueryJson build() {
            return new BulkExportLeafPartitionQueryJson(this);
        }
    }
}
