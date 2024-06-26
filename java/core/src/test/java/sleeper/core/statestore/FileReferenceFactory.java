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
package sleeper.core.statestore;

import sleeper.core.partition.Partition;
import sleeper.core.partition.PartitionTree;

import java.time.Instant;
import java.util.List;
import java.util.Objects;

/**
 * A test helper factory to create file references for a state store.
 */
public class FileReferenceFactory {
    private final PartitionTree partitionTree;
    private final Instant lastStateStoreUpdate;

    private FileReferenceFactory(PartitionTree partitionTree) {
        this(partitionTree, null);
    }

    private FileReferenceFactory(PartitionTree partitionTree, Instant lastStateStoreUpdate) {
        this.partitionTree = Objects.requireNonNull(partitionTree, "partitionTree must not be null");
        this.lastStateStoreUpdate = lastStateStoreUpdate;
    }

    /**
     * Creates a factory to create files in the given partition tree.
     *
     * @param  tree the tree
     * @return      the factory
     */
    public static FileReferenceFactory from(PartitionTree tree) {
        return new FileReferenceFactory(tree);
    }

    /**
     * Creates a factory to create files in the given partition tree.
     *
     * @param  partitions the partitions in the tree
     * @return            the factory
     */
    public static FileReferenceFactory from(List<Partition> partitions) {
        return from(new PartitionTree(partitions));
    }

    /**
     * Creates a factory to create files in the given state store. This will load partitions from the store, so may not
     * be efficient if the store is not in-memory.
     *
     * @param  stateStore the state store to load partitions from
     * @return            the factory
     */
    public static FileReferenceFactory from(StateStore stateStore) {
        try {
            return from(stateStore.getAllPartitions());
        } catch (StateStoreException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Creates a factory to create files in the given partition tree. Sets a fixed last updated date for any references
     * created by the factory.
     *
     * @param  tree                 the tree
     * @param  lastStateStoreUpdate the time created references should be marked as having last been updated
     * @return                      the factory
     */
    public static FileReferenceFactory fromUpdatedAt(PartitionTree tree, Instant lastStateStoreUpdate) {
        return new FileReferenceFactory(tree, lastStateStoreUpdate);
    }

    /**
     * Creates a factory to create files in the given partition tree. Sets a fixed last updated date for any references
     * created by the factory.
     *
     * @param  partitions           the partitions in the tree
     * @param  lastStateStoreUpdate the time created references should be marked as having last been updated
     * @return                      the factory
     */
    public static FileReferenceFactory fromUpdatedAt(List<Partition> partitions, Instant lastStateStoreUpdate) {
        return fromUpdatedAt(new PartitionTree(partitions), lastStateStoreUpdate);
    }

    /**
     * Creates a factory to create files in the given state store. This will load partitions from the store, so may not
     * be efficient if the store is not in-memory. Sets a fixed last updated date for any references created by the
     * factory.
     *
     * @param  stateStore           the state store to load partitions from
     * @param  lastStateStoreUpdate the time created references should be marked as having last been updated
     * @return                      the factory
     */
    public static FileReferenceFactory fromUpdatedAt(StateStore stateStore, Instant lastStateStoreUpdate) {
        try {
            return fromUpdatedAt(stateStore.getAllPartitions(), lastStateStoreUpdate);
        } catch (StateStoreException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Creates a file in the root partition. This uses the partition ID as the filename, so may only be used once.
     * Repeated use of this method will result in references to the same file. This should be avoided as it will produce
     * failures due to duplicate file references.
     *
     * @param  records the number of records in the file
     * @return         the file reference
     */
    public FileReference rootFile(long records) {
        return fileForPartition(partitionTree.getRootPartition(), records);
    }

    /**
     * Creates a file in the root partition.
     *
     * @param  filename the filename
     * @param  records  the number of records in the file
     * @return          the file reference
     */
    public FileReference rootFile(String filename, long records) {
        return fileForPartition(partitionTree.getRootPartition(), filename, records);
    }

    /**
     * Creates a file in a specified partition. This uses the partition ID as the filename, so may only be used once for
     * a given partition. Repeated use of this method with the same partition will result in references to the same
     * file. This should be avoided as it will produce failures due to duplicate file references.
     *
     * @param  partitionId the partition ID
     * @param  records     the number of records in the file
     * @return             the file reference
     */
    public FileReference partitionFile(String partitionId, long records) {
        return fileForPartition(partitionTree.getPartition(partitionId), records);
    }

    /**
     * Creates a file in a specified partition.
     *
     * @param  partitionId the partition ID
     * @param  filename    the filename
     * @param  records     the number of records in the file
     * @return             the file reference
     */
    public FileReference partitionFile(String partitionId, String filename, long records) {
        return fileForPartition(partitionTree.getPartition(partitionId), filename, records);
    }

    private FileReference fileForPartition(Partition partition, long records) {
        return fileForPartitionBuilder(partition, records).build();
    }

    private FileReference fileForPartition(Partition partition, String filename, long records) {
        return fileForPartitionBuilder(partition, records).filename(filename).build();
    }

    private FileReference.Builder fileForPartitionBuilder(Partition partition, long records) {
        return FileReference.builder()
                .filename(partition.getId() + ".parquet")
                .partitionId(partition.getId())
                .numberOfRecords(records)
                .lastStateStoreUpdateTime(lastStateStoreUpdate)
                .countApproximate(false)
                .onlyContainsDataForThisPartition(true);
    }
}
