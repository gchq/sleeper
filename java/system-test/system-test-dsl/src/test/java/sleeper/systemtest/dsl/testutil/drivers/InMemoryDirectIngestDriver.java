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

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.partition.PartitionTree;
import sleeper.core.record.Record;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;
import sleeper.query.runner.recordretrieval.InMemoryDataStore;
import sleeper.systemtest.dsl.ingest.DirectIngestDriver;
import sleeper.systemtest.dsl.instance.SystemTestInstanceContext;

import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.util.Spliterator.IMMUTABLE;
import static java.util.Spliterator.NONNULL;
import static java.util.Spliterators.spliteratorUnknownSize;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.configuration.properties.instance.CommonProperty.FILE_SYSTEM;
import static sleeper.configuration.properties.table.TableProperty.TABLE_ID;

public class InMemoryDirectIngestDriver implements DirectIngestDriver {
    private final SystemTestInstanceContext instance;
    private final InMemoryDataStore data;

    public InMemoryDirectIngestDriver(SystemTestInstanceContext instance, InMemoryDataStore data) {
        this.instance = instance;
        this.data = data;
    }

    public void ingest(Path tempDir, Iterator<Record> records) {
        InstanceProperties instanceProperties = instance.getInstanceProperties();
        TableProperties tableProperties = instance.getTableProperties();
        Schema schema = tableProperties.getSchema();
        StateStore stateStore = instance.getStateStore();
        String filePathPrefix = instanceProperties.get(FILE_SYSTEM)
                + instanceProperties.get(DATA_BUCKET) + "/"
                + tableProperties.get(TABLE_ID);
        PartitionTree partitions = getPartitions(stateStore);
        Map<String, List<Record>> recordsByPartition = streamRecords(records)
                .collect(Collectors.groupingBy(record ->
                        partitions.getLeafPartition(schema, record.getRowKeys(schema)).getId()));
        recordsByPartition.forEach((partitionId, recordList) ->
                writePartitionFile(partitionId, filePathPrefix, recordList, stateStore));
    }

    private static Stream<Record> streamRecords(Iterator<Record> records) {
        return StreamSupport.stream(spliteratorUnknownSize(records, NONNULL & IMMUTABLE), false);
    }

    private PartitionTree getPartitions(StateStore stateStore) {
        try {
            return new PartitionTree(stateStore.getAllPartitions());
        } catch (StateStoreException e) {
            throw new RuntimeException(e);
        }
    }

    private void writePartitionFile(String partitionId, String filePathPrefix, List<Record> records, StateStore stateStore) {
        String filename = filePathPrefix + "/" + UUID.randomUUID();
        data.addFile(filename, records);
        try {
            stateStore.addFile(FileReference.builder()
                    .filename(filename)
                    .partitionId(partitionId)
                    .numberOfRecords((long) records.size())
                    .countApproximate(false)
                    .onlyContainsDataForThisPartition(true)
                    .build());
        } catch (StateStoreException e) {
            throw new RuntimeException(e);
        }
    }
}
