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

package sleeper.systemtest.dsl.query;

import sleeper.core.partition.PartitionTree;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TableProperty;
import sleeper.core.range.Range;
import sleeper.core.range.Region;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;
import sleeper.query.model.Query;
import sleeper.systemtest.dsl.instance.SystemTestInstanceContext;

import java.util.List;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

public class QueryCreator {
    private final Schema schema;
    private final String tableName;
    private final StateStore stateStore;

    public QueryCreator(SystemTestInstanceContext instance) {
        this(instance, instance.getTableProperties());
    }

    private QueryCreator(SystemTestInstanceContext instance, TableProperties tableProperties) {
        this.schema = tableProperties.getSchema();
        this.tableName = tableProperties.get(TableProperty.TABLE_NAME);
        this.stateStore = instance.getStateStore(tableProperties);
    }

    public static List<Query> forAllTables(
            SystemTestInstanceContext instance, Function<QueryCreator, Query> queryFactory) {
        return instance.streamTableProperties()
                .map(properties -> new QueryCreator(instance, properties))
                .map(queryFactory)
                .collect(Collectors.toUnmodifiableList());
    }

    public Query allRecordsQuery() {
        return byRegions(List.of(getPartitionTree().getRootPartition().getRegion()));
    }

    public Query byRowKey(String key, List<QueryRange> ranges) {
        return byRegions(ranges.stream()
                .map(range -> new Region(new Range.RangeFactory(schema)
                        .createRange(key, range.getMin(), range.getMax())))
                .collect(Collectors.toList()));
    }

    private Query byRegions(List<Region> regions) {
        return Query.builder()
                .tableName(tableName)
                .queryId(UUID.randomUUID().toString())
                .regions(regions)
                .build();
    }

    private PartitionTree getPartitionTree() {
        try {
            return new PartitionTree(stateStore.getAllPartitions());
        } catch (StateStoreException e) {
            throw new RuntimeException(e);
        }
    }
}
