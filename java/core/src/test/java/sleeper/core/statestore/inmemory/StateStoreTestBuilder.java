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
package sleeper.core.statestore.inmemory;

import sleeper.core.partition.Partition;
import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.statestore.SplitFileReference;
import sleeper.core.statestore.StateStore;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

public class StateStoreTestBuilder {

    private static final Instant DEFAULT_UPDATE_TIME = Instant.parse("2022-12-08T11:03:00.001Z");
    private final PartitionTree tree;
    private final List<Partition> partitions;
    private final List<FileReference> files = new ArrayList<>();
    private final FileReferenceFactory fileReferenceFactory;

    private StateStoreTestBuilder(PartitionTree tree) {
        this.tree = tree;
        this.partitions = tree.getAllPartitions();
        this.fileReferenceFactory = FileReferenceFactory.fromUpdatedAt(tree, DEFAULT_UPDATE_TIME);
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

    public StateStoreTestBuilder splitFileToPartitions(String filename, String leftPartition, String rightPartition) {
        FileReference fileToSplit = files.stream()
                .filter(fileReference -> fileReference.getFilename().equals(filename))
                .findFirst().orElseThrow();
        addFile(SplitFileReference.referenceForChildPartition(fileToSplit, leftPartition));
        addFile(SplitFileReference.referenceForChildPartition(fileToSplit, rightPartition));
        files.remove(fileToSplit);
        return this;
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

    private StateStoreTestBuilder addFiles(Stream<FileReference> addFiles) {
        addFiles.forEach(files::add);
        return this;
    }

    private StateStoreTestBuilder addFile(FileReference file) {
        files.add(file);
        return this;
    }

    private FileReference partitionSingleFile(Partition partition, long records) {
        return partitionFile(partition, partition.getId() + ".parquet", records);
    }

    private FileReference partitionFile(Partition partition, String filename, long records) {
        return fileReferenceFactory.partitionFile(partition.getId(), filename, records);
    }

}
