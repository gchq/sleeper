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
package sleeper.datasource;

import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.sources.EqualTo;
import org.apache.spark.sql.sources.Filter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TableProperty;
import sleeper.core.range.Range;
import sleeper.core.range.Range.RangeFactory;
import sleeper.core.range.Region;
import sleeper.core.range.RegionSerDe;
import sleeper.core.schema.Schema;
import sleeper.core.schema.SchemaSerDe;
import sleeper.core.statestore.StateStore;
import sleeper.query.core.model.LeafPartitionQuery;
import sleeper.query.core.model.Query;
import sleeper.query.core.rowretrieval.QueryPlanner;

import java.util.List;
import java.util.UUID;

/**
 * Doesn't need to be serialisable.
 */
public class SleeperBatch implements Batch {
    private static final Logger LOGGER = LoggerFactory.getLogger(SleeperBatch.class);

    private InstanceProperties instanceProperties;
    private TableProperties tableProperties;
    private StateStore stateStore;
    private Filter[] pushedFilters;

    public SleeperBatch(InstanceProperties instanceProperties, TableProperties tableProperties, StateStore stateStore,
            Filter[] pushedFilters) {
        this.instanceProperties = instanceProperties;
        this.tableProperties = tableProperties;
        this.stateStore = stateStore;
        this.pushedFilters = pushedFilters;
    }

    @Override
    public InputPartition[] planInputPartitions() {
        String tableId = tableProperties.get(TableProperty.TABLE_ID);
        Schema schema = tableProperties.getSchema();
        String schemaAsJson = new SchemaSerDe().toJson(schema);

        // Create one InputPartition per leaf partition (later we will restrict the partitions to be read based
        // on the user's filters)
        QueryPlanner planner = new QueryPlanner(tableProperties, stateStore);
        planner.init();
        Region region;
        if (null == pushedFilters || pushedFilters.length == 0) {
            region = Region.coveringAllValuesOfAllRowKeys(schema);
        } else {
            // In SleeperScanBuilder we only accepted filters of type EqualTo on the key column.
            EqualTo equalTo = (EqualTo) pushedFilters[0];
            // TODO Sanity check this is on the first row key field
            String key = (String) equalTo.value();
            RangeFactory rangeFactory = new RangeFactory(schema);
            Range range = rangeFactory.createExactRange(schema.getRowKeyFields().get(0), key);
            region = new Region(range);
        }
        Query query = Query.builder()
                .queryId(UUID.randomUUID().toString())
                .tableName(tableProperties.get(TableProperty.TABLE_NAME))
                .regions(List.of(region))
                .build();
        List<LeafPartitionQuery> leafPartitionQueries = planner.splitIntoLeafPartitionQueries(query);
        LOGGER.info("Split query into {} leaf partition queries", leafPartitionQueries.size());

        RegionSerDe regionSerDe = new RegionSerDe(schema);
        return leafPartitionQueries.stream()
                .map(q -> new SleeperInputPartition(tableId, schemaAsJson, q.getQueryId(), q.getSubQueryId(), q.getLeafPartitionId(),
                        regionSerDe.toJson(q.getPartitionRegion()), regionSerDe.toJson(region),
                        q.getFiles()))
                .toArray(SleeperInputPartition[]::new);
    }

    @Override
    public PartitionReaderFactory createReaderFactory() {
        return new SleeperPartitionReaderFactory(Utils.saveInstancePropertiesToString(instanceProperties), Utils.saveTablePropertiesToString(tableProperties));
    }
}