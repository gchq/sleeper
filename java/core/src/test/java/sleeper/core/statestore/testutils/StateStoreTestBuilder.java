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
package sleeper.core.statestore.testutils;

import sleeper.core.partition.Partition;
import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.AssignJobIdRequest;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.statestore.SplitFileReference;
import sleeper.core.statestore.StateStore;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

/**
 * A test helper for setting up partitions and files in a state store. Partitions are defined separately in a
 * {@link PartitionsBuilder}, which can then be passed into this class to define files.
 */
public class StateStoreTestBuilder {

    private static final Instant DEFAULT_UPDATE_TIME = Instant.parse("2022-12-08T11:03:00.001Z");
    private final PartitionTree tree;
    private final List<Partition> partitions;
    private final List<FileReference> files = new ArrayList<>();
    private final List<AssignJobIdRequest> assignJobIds = new ArrayList<>();
    private final FileReferenceFactory fileReferenceFactory;

    private StateStoreTestBuilder(PartitionTree tree) {
        this.tree = tree;
        this.partitions = tree.getAllPartitions();
        this.fileReferenceFactory = FileReferenceFactory.fromUpdatedAt(tree, DEFAULT_UPDATE_TIME);
    }

    private StateStoreTestBuilder(PartitionsBuilder partitionsBuilder) {
        this(partitionsBuilder.buildTree());
    }

    /**
     * Creates a state builder with the partition tree declared in the given builder. Will build the partition tree
     * immediately, based on the current state in the partitions builder.
     *
     * @param  partitionsBuilder the partitions builder
     * @return                   the state builder
     */
    public static StateStoreTestBuilder from(PartitionsBuilder partitionsBuilder) {
        return new StateStoreTestBuilder(partitionsBuilder);
    }

    /**
     * Creates a state builder with a single root partition derived from the given schema.
     *
     * @param  schema the schema
     * @return        the state builder
     */
    public static StateStoreTestBuilder withSinglePartition(Schema schema) {
        return withSinglePartition(schema, "root");
    }

    /**
     * Creates a state builder with a single root partition derived from the given schema, with the given ID.
     *
     * @param  schema      the schema
     * @param  partitionId the partition ID
     * @return             the state builder
     */
    public static StateStoreTestBuilder withSinglePartition(Schema schema, String partitionId) {
        return from(new PartitionsBuilder(schema).singlePartition(partitionId));
    }

    /**
     * Declares a single file in every leaf partition with a given number of records.
     *
     * @param  records the number of records
     * @return         the builder
     */
    public StateStoreTestBuilder singleFileInEachLeafPartitionWithRecords(long records) {
        return addFiles(partitions.stream().filter(Partition::isLeafPartition)
                .map(partition -> partitionSingleFile(partition, records)));
    }

    /**
     * Declares a file in a given partition.
     *
     * @param  partitionId the ID of the partition the file will be referenced in
     * @param  filename    the filename
     * @param  records     the number of records
     * @return             the builder
     */
    public StateStoreTestBuilder partitionFileWithRecords(String partitionId, String filename, long records) {
        return addFile(partitionFile(tree.getPartition(partitionId), filename, records));
    }

    /**
     * Splits a previously declared file reference to be referenced on two partitions. The partitions should be child
     * partitions of the partition the file is currently referenced on. This assumes the file has previously only been
     * declared on a single partition.
     *
     * @param  filename       the filename
     * @param  leftPartition  the ID of the left child partition
     * @param  rightPartition the ID of the right child partition
     * @return                the builder
     */
    public StateStoreTestBuilder splitFileToPartitions(String filename, String leftPartition, String rightPartition) {
        FileReference fileToSplit = files.stream()
                .filter(fileReference -> fileReference.getFilename().equals(filename))
                .findFirst().orElseThrow();
        addFile(SplitFileReference.referenceForChildPartition(fileToSplit, leftPartition));
        addFile(SplitFileReference.referenceForChildPartition(fileToSplit, rightPartition));
        files.remove(fileToSplit);
        return this;
    }

    /**
     * Assigns previously declared file references to a job.
     *
     * @param  jobId       the ID of the job
     * @param  partitionId the ID of the job's partition
     * @param  filenames   the filenames
     * @return             the builder
     */
    public StateStoreTestBuilder assignJobOnPartitionToFiles(String jobId, String partitionId, List<String> filenames) {
        assignJobIds.add(AssignJobIdRequest.assignJobOnPartitionToFiles(jobId, partitionId, filenames));
        return this;
    }

    /**
     * Creates an in-memory state store and sets the declared state.
     *
     * @return the state store
     */
    public StateStore buildStateStore() {
        return setupStateStore(StateStoreTestHelper.inMemoryStateStoreWithFixedPartitions(partitions));
    }

    /**
     * Sets the declared state in a given state store.
     *
     * @return the state store
     */
    public StateStore setupStateStore(StateStore store) {
        try {
            store.initialise(partitions);
            store.addFiles(files);
            if (!assignJobIds.isEmpty()) {
                store.assignJobIds(assignJobIds);
            }
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
