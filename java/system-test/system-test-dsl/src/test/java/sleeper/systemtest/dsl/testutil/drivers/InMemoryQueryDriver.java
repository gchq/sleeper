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

package sleeper.systemtest.dsl.testutil.drivers;

import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.key.Key;
import sleeper.core.partition.Partition;
import sleeper.core.partition.PartitionTree;
import sleeper.core.range.Range;
import sleeper.core.range.Region;
import sleeper.core.record.Record;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;
import sleeper.query.model.Query;
import sleeper.systemtest.dsl.instance.SleeperInstanceContext;
import sleeper.systemtest.dsl.query.QueryAllTablesDriver;
import sleeper.systemtest.dsl.query.QueryAllTablesInParallelDriver;
import sleeper.systemtest.dsl.query.QueryDriver;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toUnmodifiableList;

public class InMemoryQueryDriver implements QueryDriver {

    private final SleeperInstanceContext instance;
    private final InMemoryDataStore dataStore;

    private InMemoryQueryDriver(SleeperInstanceContext instance, InMemoryDataStore dataStore) {
        this.instance = instance;
        this.dataStore = dataStore;
    }

    public static QueryAllTablesDriver allTablesDriver(SleeperInstanceContext instance, InMemoryDataStore dataStore) {
        return new QueryAllTablesInParallelDriver(instance, new InMemoryQueryDriver(instance, dataStore));
    }

    @Override
    public List<Record> run(Query query) {
        TableProperties tableProperties = instance.getTablePropertiesByName(query.getTableName()).orElseThrow();
        Schema schema = tableProperties.getSchema();
        StateStore stateStore = instance.getStateStore(tableProperties);
        PartitionTree partitions = getPartitions(stateStore);
        Map<String, List<String>> filesByPartition = getFilesByPartition(stateStore);

        return query.getRegions().stream()
                .flatMap(region -> streamRecords(region, schema, partitions, filesByPartition))
                .collect(toUnmodifiableList());
    }

    private Stream<Record> streamRecords(
            Region region, Schema schema, PartitionTree partitions, Map<String, List<String>> filesByPartition) {
        Key min = Key.create(rowKeyFieldRanges(region, schema).map(Range::getMin).collect(toList()));
        Key max = Key.create(rowKeyFieldRanges(region, schema).map(Range::getMax).collect(toList()));
        Partition commonAncestor = partitions.getNearestCommonAncestor(schema, min, max);
        return streamRecordsUnderPartition(region, schema, partitions, filesByPartition, commonAncestor);
    }

    private Stream<Record> streamRecordsUnderPartition(
            Region region, Schema schema, PartitionTree partitions,
            Map<String, List<String>> filesByPartition, Partition partition) {
        Stream<Record> inPartition = steamRecordsInPartition(filesByPartition, partition)
                .filter(record -> region.isKeyInRegion(schema, record.getRowKeys(schema)));
        if (partition.isLeafPartition()) {
            return inPartition;
        } else {
            return Stream.concat(inPartition,
                    partition.getChildPartitionIds().stream()
                            .map(partitions::getPartition)
                            .flatMap(childPartition -> streamRecordsUnderPartition(
                                    region, schema, partitions, filesByPartition, childPartition)));
        }
    }

    private Stream<Record> steamRecordsInPartition(Map<String, List<String>> filesByPartition, Partition partition) {
        List<String> files = filesByPartition.get(partition.getId());
        if (files != null) {
            return files.stream().flatMap(dataStore::read);
        } else {
            return Stream.empty();
        }
    }

    private static Stream<Range> rowKeyFieldRanges(Region region, Schema schema) {
        return schema.getRowKeyFields().stream()
                .map(field -> region.getRange(field.getName()));
    }

    private PartitionTree getPartitions(StateStore stateStore) {
        try {
            return new PartitionTree(stateStore.getAllPartitions());
        } catch (StateStoreException e) {
            throw new RuntimeException(e);
        }
    }

    private Map<String, List<String>> getFilesByPartition(StateStore stateStore) {
        try {
            return stateStore.getPartitionToReferencedFilesMap();
        } catch (StateStoreException e) {
            throw new RuntimeException(e);
        }
    }
}
