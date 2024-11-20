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
import java.util.Objects;

/**
 * An export query for a leaf partition. The query contains information about
 * which files should be read.
 * That query is broken down over leaf partitions into subqueries. Each
 * subquery retrieves records from a leaf partition.
 */
public class BulkExportLeafPartitionQuery {

    private final String tableId;
    private final String exportId;
    private final String subExportId;
    private final String leafPartitionId;
    private final List<String> files;

    private BulkExportLeafPartitionQuery(Builder builder) {
        tableId = requireNonNull(builder.tableId, builder, "tableId field must be provided");
        exportId = requireNonNull(builder.exportId, builder, "exportId field must be provided");
        subExportId = requireNonNull(builder.subExportId, builder, "subExportId field must be provided");
        leafPartitionId = requireNonNull(builder.leafPartitionId, builder, "leafPartitionId field must be provided");
        files = requireNonNull(builder.files, builder, "files field must be provided");
    }

    public static Builder builder() {
        return new Builder();
    }

    public String getTableId() {
        return tableId;
    }

    public String getExportId() {
        return exportId;
    }

    public String getSubExportId() {
        return subExportId;
    }

    public String getLeafPartitionId() {
        return leafPartitionId;
    }

    public List<String> getFiles() {
        return files;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (object == null || getClass() != object.getClass()) {
            return false;
        }
        BulkExportLeafPartitionQuery that = (BulkExportLeafPartitionQuery) object;
        return Objects.equals(tableId, that.tableId)
                && Objects.equals(exportId, that.exportId)
                && Objects.equals(subExportId, that.subExportId)
                && Objects.equals(leafPartitionId, that.leafPartitionId)
                && Objects.equals(files, that.files);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableId, exportId, subExportId, leafPartitionId, files);
    }

    @Override
    public String toString() {
        return "LeafPartitionQuery{" +
                "tableId='" + tableId + '\'' +
                ", exportId='" + exportId + '\'' +
                ", subExportId='" + subExportId + '\'' +
                ", leafPartitionId='" + leafPartitionId + '\'' +
                ", files=" + files +
                '}';
    }

    private static <T> T requireNonNull(T obj, Builder builder, String message) {
        if (obj == null) {
            throw new BulkExportQueryValidationException(builder.exportId, message);
        }
        return obj;
    }

    /**
     * Builder for the BulkExportLeafPartitionQuery model.
     */
    public static final class Builder {
        private String tableId;
        private String exportId;
        private String subExportId;
        private String leafPartitionId;
        private List<String> files;

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
         * Provide the exportId.
         *
         * @param exportId the id for the export.
         *
         * @return the builder object.
         */
        public Builder exportId(String exportId) {
            this.exportId = exportId;
            return this;
        }

        /**
         * Provide the subExportId.
         *
         * @param subExportId the id for the sub export.
         *
         * @return the builder object.
         */
        public Builder subExportId(String subExportId) {
            this.subExportId = subExportId;
            return this;
        }

        /**
         * Provide the leafPartitionId.
         *
         * @param leafPartitionId the id for the leaf partition.
         *
         * @return the builder object.
         */
        public Builder leafPartitionId(String leafPartitionId) {
            this.leafPartitionId = leafPartitionId;
            return this;
        }

        /**
         * Provide the leaf partition files.
         *
         * @param files the files to be exported
         *
         * @return the builder object.
         */
        public Builder files(List<String> files) {
            this.files = files;
            return this;
        }

        /**
         * Builds the BulkExportLeafPartitionQuery.
         *
         * @return a BulkExportLeafPartitionQuery object.
         */
        public BulkExportLeafPartitionQuery build() {
            return new BulkExportLeafPartitionQuery(this);
        }
    }
}
