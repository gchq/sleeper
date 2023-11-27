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

    private FileInfoFactory(Builder builder) {
        partitionTree = Objects.requireNonNull(builder.partitionTree, "partitionTree must not be null");
        lastStateStoreUpdate = builder.lastStateStoreUpdate;
    }

    public static Builder fromTree(PartitionTree tree) {
        return new Builder().partitionTree(tree);
    }

    public static Builder fromPartitions(Schema schema, List<Partition> partitions) {
        return fromTree(new PartitionTree(schema, partitions));
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
        return fileForPartition(partition, partition.getId() + ".parquet", records);
    }

    private FileInfo fileForPartition(Partition partition, String filename, long records) {
        return FileInfo.builder()
                .filename(filename)
                .partitionId(partition.getId())
                .numberOfRecords(records)
                .fileStatus(FileInfo.FileStatus.ACTIVE)
                .lastStateStoreUpdateTime(lastStateStoreUpdate)
                .build();
    }

    public static final class Builder {
        private PartitionTree partitionTree;
        private Instant lastStateStoreUpdate;

        private Builder() {
        }

        public Builder partitionTree(PartitionTree partitionTree) {
            this.partitionTree = partitionTree;
            return this;
        }

        public Builder lastStateStoreUpdate(Instant lastStateStoreUpdate) {
            this.lastStateStoreUpdate = lastStateStoreUpdate;
            return this;
        }

        public FileInfoFactory build() {
            return new FileInfoFactory(this);
        }
    }
}
