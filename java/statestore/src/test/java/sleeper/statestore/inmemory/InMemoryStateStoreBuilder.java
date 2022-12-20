/*
 * Copyright 2022 Crown Copyright
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
package sleeper.statestore.inmemory;

import sleeper.core.key.Key;
import sleeper.core.partition.Partition;
import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.range.Range;
import sleeper.statestore.FileInfo;
import sleeper.statestore.StateStore;
import sleeper.statestore.StateStoreException;

import java.time.Instant;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static sleeper.statestore.inmemory.StateStoreTestHelper.inMemoryStateStoreWithFixedPartitions;

public class InMemoryStateStoreBuilder {

    private final PartitionTree tree;
    private final StateStore store;

    private InMemoryStateStoreBuilder(PartitionsBuilder partitionsBuilder) {
        tree = partitionsBuilder.buildTree();
        store = inMemoryStateStoreWithFixedPartitions(partitionsBuilder.buildList());
    }

    public static InMemoryStateStoreBuilder from(PartitionsBuilder partitionsBuilder) {
        return new InMemoryStateStoreBuilder(partitionsBuilder);
    }

    public InMemoryStateStoreBuilder singleFileInEachLeafPartitionWithRecords(long records) {
        return addFiles(tree.getLeafPartitions().stream()
                .map(partition -> partitionSingleFile(partition, records)));
    }

    public InMemoryStateStoreBuilder partitionFileWithRecords(String partitionId, String filename, long records) {
        return addFile(partitionFile(tree.getPartition(partitionId), filename, records));
    }

    public StateStore buildStateStore() {
        return store;
    }

    private InMemoryStateStoreBuilder addFiles(Stream<FileInfo> addFiles) {
        addFiles.forEach(this::addFile);
        return this;
    }

    private InMemoryStateStoreBuilder addFile(FileInfo file) {
        try {
            store.addFile(file);
        } catch (StateStoreException e) {
            throw new IllegalStateException("Failed adding file to state store", e);
        }
        return this;
    }

    private static FileInfo partitionSingleFile(Partition partition, long records) {
        return partitionFile(partition, partition.getId() + ".parquet", records);
    }

    private static FileInfo partitionFile(Partition partition, String filename, long records) {
        return FileInfo.builder()
                .rowKeyTypes(partition.getRowKeyTypes())
                .minRowKey(minRowKey(partition)).maxRowKey(maxRowKey(partition))
                .filename(filename).partitionId(partition.getId())
                .numberOfRecords(records).fileStatus(FileInfo.FileStatus.ACTIVE)
                .lastStateStoreUpdateTime(Instant.parse("2022-12-08T11:03:00.001Z"))
                .build();
    }

    private static Key minRowKey(Partition partition) {
        return Key.create(partition.getRegion().getRanges().stream()
                .map(Range::getMin)
                .collect(Collectors.toList()));
    }

    private static Key maxRowKey(Partition partition) {
        return Key.create(partition.getRegion().getRanges().stream()
                .map(Range::getMax)
                .collect(Collectors.toList()));
    }
}
