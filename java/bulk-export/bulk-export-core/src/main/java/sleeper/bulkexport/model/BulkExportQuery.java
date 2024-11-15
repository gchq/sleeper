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

import java.util.Objects;

/**
 * A request for a bulk export on a given table.
 */
public class BulkExportQuery {
    private final String exportId;
    private final String tableId;
    private final String tableName;

    private BulkExportQuery(Builder builder) {
        tableId = requireNonNull(builder.tableId, builder, "tableId field must be provided");
        tableName = requireNonNull(builder.tableName, builder, "tableName field must be provided");
        exportId = requireNonNull(builder.exportId, builder, "exportId field must be provided");
    }

    public static Builder builder() {
        return new Builder();
    }

    public String getTableId() {
        return tableId;
    }

    public String getTableName() {
        return tableName;
    }

    public String getExportId() {
        return exportId;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (object == null || getClass() != object.getClass()) {
            return false;
        }
        BulkExportQuery that = (BulkExportQuery) object;
        return Objects.equals(tableId, that.tableId)
                && Objects.equals(tableName, that.tableName)
                && Objects.equals(exportId, that.exportId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableId, tableName, exportId);
    }

    @Override
    public String toString() {
        return "BulkExportQuery{" +
                "tableId='" + tableId + '\'' +
                ", tableName='" + tableName + '\'' +
                ", exportId='" + exportId + '\'' +
                '}';
    }

    private static <T> T requireNonNull(T obj, Builder builder, String message) {
        if (obj == null) {
            throw new BulkExportQueryValidationException(builder.exportId, message);
        }
        return obj;
    }

    /**
     * Builder for the BulkExportQuery model.
     */
    public static final class Builder {
        private String tableId;
        private String tableName;
        private String exportId;

        private Builder() {
        }

        /**
         * Provide the tableId.
         *
         * @param tableId the id for the table.
         *
         * @return the builder object.
         */
        public Builder tableId(String tableId) {
            this.tableId = tableId;
            return this;
        }

        /**
         * Provide the tableName.
         *
         * @param tableName the name for the table.
         *
         * @return the builder object.
         */
        public Builder tableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        /**
         * Provide the exportId.
         *
         * @param exportId the id for the export job.
         *
         * @return the builder object.
         */
        public Builder exportId(String exportId) {
            this.exportId = exportId;
            return this;
        }

        /**
         * Builds the BulkExportQuery.
         *
         * @return a BulkExportQuery object.
         */
        public BulkExportQuery build() {
            return new BulkExportQuery(this);
        }
    }
}
