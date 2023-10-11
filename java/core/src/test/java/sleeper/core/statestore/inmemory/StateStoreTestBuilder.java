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
package sleeper.core.statestore.inmemory;

import sleeper.core.partition.Partition;
import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.FileInfo;
import sleeper.core.statestore.StateStore;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

public class StateStoreTestBuilder {

    private final PartitionTree tree;
    private final List<Partition> partitions;
    private final List<FileInfo> files = new ArrayList<>();

    private StateStoreTestBuilder(PartitionTree tree) {
        this.tree = tree;
        this.partitions = tree.getAllPartitions();
    }

    private StateStoreTestBuilder(PartitionsBuilder partitionsBuilder) {
        this(partitionsBuilder.buildTree());
    }

    public static StateStoreTestBuilder from(PartitionsBuilder partitionsBuilder) {
        return new StateStoreTestBuilder(partitionsBuilder);
    }

    public static StateStoreTestBuilder withSinglePartition(Schema schema) {
        return withSinglePartition(schema, "root");
    }

    public static StateStoreTestBuilder withSinglePartition(Schema schema, String partitionId) {
        return from(new PartitionsBuilder(schema).singlePartition(partitionId));
    }

    public StateStoreTestBuilder singleFileInEachLeafPartitionWithRecords(long records) {
        return addFiles(partitions.stream().filter(Partition::isLeafPartition)
                .map(partition -> partitionSingleFile(partition, records)));
    }

    public StateStoreTestBuilder partitionFileWithRecords(String partitionId, String filename, long records) {
        return addFile(partitionFile(tree.getPartition(partitionId), filename, records));
    }

    public StateStore buildStateStore() {
        return setupStateStore(StateStoreTestHelper.inMemoryStateStoreWithFixedPartitions(partitions));
    }

    public StateStore setupStateStore(StateStore store) {
        try {
            store.initialise(partitions);
            store.addFiles(files);
        } catch (Exception e) {
            throw new IllegalStateException("Failed setting up state store", e);
        }
        return store;
    }

    private StateStoreTestBuilder addFiles(Stream<FileInfo> addFiles) {
        addFiles.forEach(files::add);
        return this;
    }

    private StateStoreTestBuilder addFile(FileInfo file) {
        files.add(file);
        return this;
    }

    private static FileInfo partitionSingleFile(Partition partition, long records) {
        return partitionFile(partition, partition.getId() + ".parquet", records);
    }

    private static FileInfo partitionFile(Partition partition, String filename, long records) {
        return FileInfo.builder()
                .filename(filename).partitionId(partition.getId())
                .numberOfRecords(records).fileStatus(FileInfo.FileStatus.ACTIVE)
                .lastStateStoreUpdateTime(Instant.parse("2022-12-08T11:03:00.001Z"))
                .build();
    }

}
