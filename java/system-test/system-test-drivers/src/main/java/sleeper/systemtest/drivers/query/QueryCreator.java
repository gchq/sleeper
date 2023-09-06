/*
 * Copyright 2022-2023 Crown Copyright
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

package sleeper.systemtest.drivers.query;

import sleeper.core.partition.PartitionTree;
import sleeper.core.range.Range;
import sleeper.core.range.Region;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;
import sleeper.query.model.Query;
import sleeper.systemtest.drivers.instance.SleeperInstanceContext;

import java.util.List;
import java.util.UUID;

public class QueryCreator {
    private final Schema schema;
    private final String tableName;
    private final StateStore stateStore;
    private final Range.RangeFactory rangeFactory;

    public QueryCreator(SleeperInstanceContext instance) {
        this.schema = instance.getTableProperties().getSchema();
        this.tableName = instance.getTableName();
        this.stateStore = instance.getStateStore();
        this.rangeFactory = new Range.RangeFactory(schema);
    }

    public Query create(String queryId, String key, Object min1, Object max1, Object min2, Object max2) {
        Region region1 = new Region(rangeFactory.createRange(key, min1, max1));
        Region region2 = new Region(rangeFactory.createRange(key, min2, max2));
        return new Query.Builder(tableName, queryId, List.of(region1, region2)).build();
    }

    public Query create(String queryId, String key, Object min, Object max) {
        Region region = new Region(rangeFactory.createRange(key, min, max));
        return new Query.Builder(tableName, queryId, region).build();
    }

    public Query allRecordsQuery() {
        return allRecordsQuery(UUID.randomUUID().toString());
    }

    public Query allRecordsQuery(String queryId) {
        return new Query.Builder(tableName, queryId,
                List.of(getPartitionTree().getRootPartition().getRegion())).build();
    }

    private PartitionTree getPartitionTree() {
        try {
            return new PartitionTree(schema, stateStore.getAllPartitions());
        } catch (StateStoreException e) {
            throw new RuntimeException(e);
        }
    }
}
