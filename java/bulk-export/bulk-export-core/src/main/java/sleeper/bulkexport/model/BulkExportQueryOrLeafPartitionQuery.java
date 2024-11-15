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

import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertiesProvider;

import java.util.Objects;

/**
 * A class used to determine if a query is the main bulk export or a leaf
 * partition.
 */
public class BulkExportQueryOrLeafPartitionQuery {

    private final BulkExportQuery exportQuery;
    private final BulkExportLeafPartitionQuery exportLeafPartitionQuery;

    public BulkExportQueryOrLeafPartitionQuery(BulkExportQuery exportQuery) {
        this.exportQuery = Objects.requireNonNull(exportQuery, "exportQuery must not be null");
        this.exportLeafPartitionQuery = null;
    }

    public BulkExportQueryOrLeafPartitionQuery(BulkExportLeafPartitionQuery exportLeafPartitionQuery) {
        this.exportQuery = null;
        this.exportLeafPartitionQuery = Objects.requireNonNull(exportLeafPartitionQuery,
                "exportLeafPartitionQuery must not be null");
    }

    /**
     * Is the export query a leaf query.
     *
     * @return a boolean value.
     */
    public boolean isLeafQuery() {
        return exportLeafPartitionQuery != null;
    }

    /**
     * As a BulkExportQuery if its not a leaf export query.
     *
     * @return the export query as BulkExportQuery.
     */
    public BulkExportQuery asParentQuery() {
        return Objects.requireNonNull(exportQuery, "export query is a leaf export query");
    }

    /**
     * As a BulkExportLeafPartitionQuery if its not an export query.
     *
     * @return the leaf partition export query as BulkExportLeafPartitionQuery.
     */
    public BulkExportLeafPartitionQuery asLeafExportQuery() {
        return Objects.requireNonNull(exportLeafPartitionQuery, "export query is not a leaf export query");
    }

    /**
     * Gets the export id regardless of it being an parent or leaf partition export
     * query.
     *
     * @return the export id.
     */
    public String getExportId() {
        if (exportLeafPartitionQuery != null) {
            return exportLeafPartitionQuery.getExportId();
        } else {
            return exportQuery.getExportId();
        }
    }

    /**
     * Gets the table properties for the table being exported.
     *
     * @param provider the provider to get the table properties.
     *
     * @return the table properties.
     */
    public TableProperties getTableProperties(TablePropertiesProvider provider) {
        if (exportLeafPartitionQuery != null) {
            return provider.getById(exportLeafPartitionQuery.getTableId());
        } else {
            return provider.getByName(exportQuery.getTableName());
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
        BulkExportQueryOrLeafPartitionQuery that = (BulkExportQueryOrLeafPartitionQuery) object;
        return Objects.equals(exportQuery, that.exportQuery)
                && Objects.equals(exportLeafPartitionQuery, that.exportLeafPartitionQuery);
    }

    @Override
    public int hashCode() {
        return Objects.hash(exportQuery, exportLeafPartitionQuery);
    }

    @Override
    public String toString() {
        if (exportLeafPartitionQuery != null) {
            return exportLeafPartitionQuery.toString();
        } else {
            return String.valueOf(exportQuery);
        }
    }
}
