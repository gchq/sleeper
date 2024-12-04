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

import com.google.gson.JsonElement;

import sleeper.core.range.Region;
import sleeper.core.range.RegionSerDe;

import java.util.List;
import java.util.stream.Collectors;

class BulkExportLeafPartitionQueryJson {
    private final String tableId;
    private final String exportId;
    private final String subExportId;
    private final List<JsonElement> regions;
    private final String leafPartitionId;
    private final JsonElement partitionRegion;
    private final List<String> files;

    private BulkExportLeafPartitionQueryJson(Builder builder) {
        tableId = builder.tableId;
        exportId = builder.exportId;
        regions = builder.regions;
        subExportId = builder.subExportId;
        leafPartitionId = builder.leafPartitionId;
        partitionRegion = builder.partitionRegion;
        files = builder.files;
    }

    static BulkExportLeafPartitionQueryJson from(BulkExportLeafPartitionQuery leafQuery,
            BulkExportLeafPartitionQuerySerDe.SchemaLoader schemaLoader) {
        RegionSerDe regionSerDe = regionSerDe(schemaLoader, leafQuery);
        return builder()
                .tableId(leafQuery.getTableId())
                .exportId(leafQuery.getExportId())
                .subExportId(leafQuery.getSubExportId())
                .regions(writeRegions(leafQuery.getRegions(), regionSerDe))
                .leafPartitionId(leafQuery.getLeafPartitionId())
                .partitionRegion(regionSerDe.toJsonTree(leafQuery.getPartitionRegion()))
                .files(leafQuery.getFiles())
                .build();
    }

    BulkExportLeafPartitionQuery to(BulkExportLeafPartitionQuerySerDe.SchemaLoader schemaLoader) {
        RegionSerDe regionSerDe = regionSerDeById(schemaLoader);
        Region partitionRegion = regionSerDe.fromJsonTree(this.partitionRegion);
        return BulkExportLeafPartitionQuery.builder()
                .tableId(tableId)
                .exportId(exportId)
                .subExportId(subExportId)
                .regions(readRegions(regions, regionSerDe))
                .leafPartitionId(leafPartitionId)
                .partitionRegion(partitionRegion)
                .files(files)
                .build();
    }

    private static Builder builder() {
        return new Builder();
    }

    private RegionSerDe regionSerDeById(BulkExportLeafPartitionQuerySerDe.SchemaLoader schemaLoader) {
        if (tableId == null) {
            throw new BulkExportQueryValidationException(exportId,
                    "tableId field must be provided");
        }
        return regionSerDeById(schemaLoader, exportId, tableId);
    }

    private static RegionSerDe regionSerDe(
            BulkExportLeafPartitionQuerySerDe.SchemaLoader schemaLoader, BulkExportLeafPartitionQuery query) {
        return regionSerDeById(schemaLoader, query.getExportId(), query.getTableId());
    }

    private static RegionSerDe regionSerDeById(
            BulkExportLeafPartitionQuerySerDe.SchemaLoader schemaLoader, String exportId, String tableId) {
        return new RegionSerDe(schemaLoader.getSchemaByTableId(tableId)
                .orElseThrow(() -> new BulkExportQueryValidationException(exportId,
                        "Table could not be found with ID: \"" + tableId + "\"")));
    }

    private List<Region> readRegions(List<JsonElement> regions, RegionSerDe serDe) {
        if (regions == null) {
            return null;
        }
        try {
            return regions.stream()
                    .map(serDe::fromJsonTree)
                    .collect(Collectors.toUnmodifiableList());
        } catch (RegionSerDe.KeyDoesNotExistException e) {
            throw new BulkExportQueryValidationException(exportId, e);
        }
    }

    private static List<JsonElement> writeRegions(List<Region> regions, RegionSerDe serDe) {
        return regions.stream()
                .map(serDe::toJsonTree)
                .collect(Collectors.toUnmodifiableList());
    }

    private static final class Builder {
        private String tableId;
        private String exportId;
        private List<JsonElement> regions;
        private String subExportId;
        private String leafPartitionId;
        private JsonElement partitionRegion;
        private List<String> files;

        private Builder() {
        }

        public Builder tableId(String tableId) {
            this.tableId = tableId;
            return this;
        }

        public Builder exportId(String exportId) {
            this.exportId = exportId;
            return this;
        }

        public Builder regions(List<JsonElement> regions) {
            this.regions = regions;
            return this;
        }

        public Builder subExportId(String subExportId) {
            this.subExportId = subExportId;
            return this;
        }

        public Builder leafPartitionId(String leafPartitionId) {
            this.leafPartitionId = leafPartitionId;
            return this;
        }

        public Builder partitionRegion(JsonElement partitionRegion) {
            this.partitionRegion = partitionRegion;
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
