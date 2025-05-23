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
package sleeper.bulkexport.core.model;

import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertiesProvider;

import java.util.Objects;
import java.util.UUID;

/**
 * A request for a bulk export on a given table.
 */
public class BulkExportQuery {
    private final String exportId;
    private final String tableId;
    private final String tableName;

    private BulkExportQuery(Builder builder) {
        exportId = builder.exportId != null ? builder.exportId : UUID.randomUUID().toString();

        if (builder.tableId == null && builder.tableName == null) {
            throw new BulkExportQueryValidationException(
                    builder.exportId, "tableId or tableName field must be provided");
        } else if (builder.tableId != null && builder.tableName != null) {
            throw new BulkExportQueryValidationException(
                    builder.exportId,
                    "tableId or tableName field must be provided, not both");
        } else {
            tableId = builder.tableId;
            tableName = builder.tableName;
        }
    }

    /**
     * Checks that the object created is valid and has all of the required fields.
     *
     * @return the validated object
     */
    public BulkExportQuery validate() {
        return BulkExportQuery.builder()
                .tableId(tableId)
                .tableName(tableName)
                .exportId(exportId)
                .build();
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

    /**
     * Gets the table properties.
     *
     * @param  tablePropertiesProvider the provider to retrieve the properties from
     * @return                         the table properties
     */
    public TableProperties getTableProperties(TablePropertiesProvider tablePropertiesProvider) {
        if (tableId != null) {
            return tablePropertiesProvider.getById(tableId);
        } else {
            return tablePropertiesProvider.getByName(tableName);
        }
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

    /**
     * Builder to create a bulk export query.
     */
    public static final class Builder {
        private String tableId;
        private String tableName;
        private String exportId;

        private Builder() {
        }

        /**
         * Sets the Sleeper table ID.
         *
         * @param  tableId the Sleeper table ID
         * @return         the builder for chaining
         */
        public Builder tableId(String tableId) {
            this.tableId = tableId;
            return this;
        }

        /**
         * Sets the Sleeper table name.
         *
         * @param  tableName the Sleeper table name
         * @return           the builder for chaining
         */
        public Builder tableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        /**
         * Sets the ID of the export job.
         *
         * @param  exportId the id of the export job
         * @return          the builder for chaining
         */
        public Builder exportId(String exportId) {
            this.exportId = exportId;
            return this;
        }

        public BulkExportQuery build() {
            return new BulkExportQuery(this);
        }
    }
}
