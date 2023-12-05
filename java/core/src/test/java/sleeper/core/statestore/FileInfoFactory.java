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
package sleeper.core.statestore;

import sleeper.core.partition.Partition;
import sleeper.core.partition.PartitionTree;
import sleeper.core.schema.Schema;

import java.time.Instant;
import java.util.List;
import java.util.Objects;

public class FileInfoFactory {
    private final PartitionTree partitionTree;
    private final Instant lastStateStoreUpdate;

    private FileInfoFactory(PartitionTree partitionTree) {
        this(partitionTree, null);
    }

    private FileInfoFactory(PartitionTree partitionTree, Instant lastStateStoreUpdate) {
        this.partitionTree = Objects.requireNonNull(partitionTree, "partitionTree must not be null");
        this.lastStateStoreUpdate = lastStateStoreUpdate;
    }

    public static FileInfoFactory from(PartitionTree tree) {
        return new FileInfoFactory(tree);
    }

    public static FileInfoFactory from(Schema schema, List<Partition> partitions) {
        return from(new PartitionTree(schema, partitions));
    }

    public static FileInfoFactory from(Schema schema, StateStore stateStore) {
        try {
            return from(schema, stateStore.getAllPartitions());
        } catch (StateStoreException e) {
            throw new RuntimeException(e);
        }
    }

    public static FileInfoFactory fromUpdatedAt(PartitionTree tree, Instant lastStateStoreUpdate) {
        return new FileInfoFactory(tree, lastStateStoreUpdate);
    }

    public static FileInfoFactory fromUpdatedAt(Schema schema, List<Partition> partitions, Instant lastStateStoreUpdate) {
        return fromUpdatedAt(new PartitionTree(schema, partitions), lastStateStoreUpdate);
    }

    public static FileInfoFactory fromUpdatedAt(Schema schema, StateStore stateStore, Instant lastStateStoreUpdate) {
        try {
            return fromUpdatedAt(schema, stateStore.getAllPartitions(), lastStateStoreUpdate);
        } catch (StateStoreException e) {
            throw new RuntimeException(e);
        }
    }

    public FileInfo rootFile(long records) {
        return fileForPartition(partitionTree.getRootPartition(), records);
    }

    public FileInfo rootFile(String filename, long records) {
        return fileForPartition(partitionTree.getRootPartition(), filename, records);
    }

    public FileInfo partitionFile(String partitionId, long records) {
        return fileForPartition(partitionTree.getPartition(partitionId), records);
    }

    public FileInfo partitionFile(String partitionId, String filename, long records) {
        return fileForPartition(partitionTree.getPartition(partitionId), filename, records);
    }

    private FileInfo fileForPartition(Partition partition, long records) {
        return fileForPartitionBuilder(partition, records).build();
    }

    private FileInfo fileForPartition(Partition partition, String filename, long records) {
        return fileForPartitionBuilder(partition, records).filename(filename).build();
    }

    private FileInfo.Builder fileForPartitionBuilder(Partition partition, long records) {
        return FileInfo.wholeFile()
                .filename(partition.getId() + ".parquet")
                .partitionId(partition.getId())
                .numberOfRecords(records)
                .fileStatus(FileInfo.FileStatus.ACTIVE)
                .lastStateStoreUpdateTime(lastStateStoreUpdate);
    }
}
