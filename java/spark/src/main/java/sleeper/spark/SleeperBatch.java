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

import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.sources.Filter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TableProperty;
import sleeper.core.range.Region;
import sleeper.core.range.RegionSerDe;
import sleeper.core.schema.Schema;
import sleeper.core.schema.SchemaSerDe;
import sleeper.query.core.model.LeafPartitionQuery;
import sleeper.query.core.model.Query;
import sleeper.query.core.rowretrieval.QueryPlanner;

import java.util.List;
import java.util.UUID;

/**
 * Used to create the InputPartitions that Spark will use to read the data in the Sleeper table.
 */
public class SleeperBatch implements Batch {
    private static final Logger LOGGER = LoggerFactory.getLogger(SleeperBatch.class);

    private InstanceProperties instanceProperties;
    private TableProperties tableProperties;
    private String tableId;
    private Schema schema;
    private String schemaAsJson;
    private RegionSerDe regionSerDe;
    private QueryPlanner queryPlanner;
    private Filter[] pushedFilters;

    public SleeperBatch(InstanceProperties instanceProperties, TableProperties tableProperties, QueryPlanner queryPlanner,
            Filter[] pushedFilters) {
        LOGGER.info("Created SleeperBatch");
        this.instanceProperties = instanceProperties;
        this.tableProperties = tableProperties;
        this.tableId = this.tableProperties.get(TableProperty.TABLE_ID);
        this.schema = this.tableProperties.getSchema();
        this.schemaAsJson = new SchemaSerDe().toJson(schema);
        this.regionSerDe = new RegionSerDe(this.tableProperties.getSchema());
        this.queryPlanner = queryPlanner;
        this.pushedFilters = pushedFilters;
    }

    @Override
    public InputPartition[] planInputPartitions() {
        List<Region> regions = getMinimumRegionCoveringPushedFilters();
        Query query = Query.builder()
                .queryId(UUID.randomUUID().toString())
                .tableName(tableProperties.get(TableProperty.TABLE_NAME))
                .regions(regions)
                .build();
        List<LeafPartitionQuery> leafPartitionQueries = queryPlanner.splitIntoLeafPartitionQueries(query);
        LOGGER.info("Split query into {} leaf partition queries", leafPartitionQueries.size());

        return leafPartitionQueries.stream()
                .map(q -> queryToSleeperInputPartition(q))
                .toArray(SleeperInputPartition[]::new);
    }

    private SleeperInputPartition queryToSleeperInputPartition(LeafPartitionQuery query) {
        String partitionRegionAsJson = regionSerDe.toJson(query.getPartitionRegion());
        List<String> regionsAsJson = query.getRegions()
                .stream()
                .map(r -> regionSerDe.toJson(r))
                .toList();
        return new SleeperInputPartition(tableId, schemaAsJson, query.getQueryId(), query.getSubQueryId(), query.getLeafPartitionId(),
                partitionRegionAsJson, regionsAsJson, query.getFiles());
    }

    private List<Region> getMinimumRegionCoveringPushedFilters() {
        Schema schema = tableProperties.getSchema();
        CreateRegionsFromPushedFilters createRegions = new CreateRegionsFromPushedFilters(schema);
        List<Region> minimumRegions = createRegions.getMinimumRegionCoveringPushedFilters(pushedFilters);
        return minimumRegions;
    }

    @Override
    public PartitionReaderFactory createReaderFactory() {
        return new SleeperPartitionReaderFactory(instanceProperties.saveAsString(), tableProperties.saveAsString());
    }
}
