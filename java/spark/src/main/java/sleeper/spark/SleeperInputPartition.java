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
package sleeper.spark;

import org.apache.spark.sql.connector.read.InputPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.core.range.Region;
import sleeper.core.range.RegionSerDe;
import sleeper.core.schema.Schema;
import sleeper.core.schema.SchemaSerDe;

import java.util.List;
import java.util.stream.Collectors;

/**
 * This contains all the information needed to run a Sleeper queru for a particular Spark
 * partition. It needs to be Serializable and so the schema and regions are stored as
 * {@link String}s.
 */
public class SleeperInputPartition implements InputPartition {
    private static final long serialVersionUID = 9223372036854775807L;
    private static final Logger LOGGER = LoggerFactory.getLogger(SleeperInputPartition.class);

    // All fields must be serialisable, so store the schema, partition region and query region as Strings.
    private final String tableId;
    private final String schemaAsJson;
    private final String queryId;
    private final String subQueryId;
    private final String leafPartitionId;
    private final String partitionRegionAsJson;
    private final List<String> regionsAsJson;
    private final List<String> files;

    public SleeperInputPartition(String tableId, String schemaAsJson, String queryId, String subQueryId, String leafPartitionId,
            String partitionRegionAsJson, List<String> regionsAsJson, List<String> files) {

        LOGGER.info("Initialised SleeperInputPartition for table {} with schema {}, queryId {}, subQueryId {}, leafPartitionId {}, partitionRegionAsJson {}, files {}",
                tableId, schemaAsJson, queryId, subQueryId, leafPartitionId, partitionRegionAsJson, files);
        this.tableId = tableId;
        this.schemaAsJson = schemaAsJson;
        this.queryId = queryId;
        this.subQueryId = subQueryId;
        this.leafPartitionId = leafPartitionId;
        this.partitionRegionAsJson = partitionRegionAsJson;
        this.regionsAsJson = regionsAsJson;
        this.files = files;
    }

    public String getTableId() {
        return tableId;
    }

    public String getQueryId() {
        return queryId;
    }

    public String getSubQueryId() {
        return subQueryId;
    }

    public List<String> getFiles() {
        return files;
    }

    public String getLeafPartitionId() {
        return leafPartitionId;
    }

    /**
     * The region corresponding to the Sleeper leaf partition that this InputPartition relates to.
     *
     * @return the partition region
     */
    public Region getPartitionRegion() {
        Schema schema = new SchemaSerDe().fromJson(schemaAsJson);
        RegionSerDe regionSerDe = new RegionSerDe(schema);
        return regionSerDe.fromJson(partitionRegionAsJson);
    }

    /**
     * Returns the regions that need to be queried in this InputPartition, i.e. the regions that correspond to the query
     * filters.
     *
     * @return the regions that need to be queried in this InputPartition, i.e. the regions that correspond to the query
     *         filters
     */
    public List<Region> getRegions() {
        Schema schema = new SchemaSerDe().fromJson(schemaAsJson);
        RegionSerDe regionSerDe = new RegionSerDe(schema);
        List<Region> regions = regionsAsJson.stream()
                .map(regionSerDe::fromJson)
                .collect(Collectors.toList());
        return regions;
    }

    @Override
    public String toString() {
        return "SleeperInputPartition{tableId=" + tableId + ", schemaAsJson=" + schemaAsJson + ", queryId=" + queryId + ", subQueryId=" + subQueryId + ", leafPartitionId=" + leafPartitionId
                + ", partitionRegionAsJson=" + partitionRegionAsJson + ", regionsAsJson=" + regionsAsJson + ", files=" + files + "}";
    }
}
