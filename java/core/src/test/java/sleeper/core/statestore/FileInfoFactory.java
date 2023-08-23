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

import sleeper.core.key.Key;
import sleeper.core.partition.Partition;
import sleeper.core.partition.PartitionTree;
import sleeper.core.schema.Schema;

import java.time.Instant;
import java.util.List;
import java.util.Objects;

public class FileInfoFactory {
    private final Schema schema;
    private final PartitionTree partitionTree;
    private final Instant lastStateStoreUpdate;

    public FileInfoFactory(Schema schema, List<Partition> partitions) {
        this(schema, partitions, null);
    }

    public FileInfoFactory(Schema schema, List<Partition> partitions, Instant lastStateStoreUpdate) {
        this(new Builder().schema(schema).partitions(partitions).lastStateStoreUpdate(lastStateStoreUpdate));
    }

    private FileInfoFactory(Builder builder) {
        schema = Objects.requireNonNull(builder.schema, "schema must not be null");
        partitionTree = Objects.requireNonNull(builder.partitionTree, "partitionTree must not be null");
        lastStateStoreUpdate = builder.lastStateStoreUpdate;
    }

    public static Builder builder() {
        return new Builder();
    }

    public FileInfo leafFile(long records, Object min, Object max) {
        return fileForPartition(leafPartition(min, max), records, min, max);
    }

    public FileInfo middleFile(long records, Object min, Object max) {
        return fileForPartition(middlePartition(min, max), records, min, max);
    }

    public FileInfo rootFile(long records, Object min, Object max) {
        return fileForPartition(partitionTree.getRootPartition(), records, min, max);
    }

    public FileInfo leafFile(String filename, long records, Object min, Object max) {
        return fileForPartition(leafPartition(min, max), filename, records, min, max);
    }

    public FileInfo rootFile(String filename, long records, Object min, Object max) {
        return fileForPartition(partitionTree.getRootPartition(), filename, records, min, max);
    }

    public FileInfo partitionFile(String partitionId, String filename, long records, Object min, Object max) {
        return fileForPartition(partitionTree.getPartition(partitionId), filename, records, min, max);
    }

    public FileInfo partitionFile(Partition partition, long records, Object min, Object max) {
        if (!partition.isRowKeyInPartition(schema, rowKey(min))) {
            throw new IllegalArgumentException("Min value not in partition: " + min + ", region: " + partition.getRegion());
        }
        if (!partition.isRowKeyInPartition(schema, rowKey(max))) {
            throw new IllegalArgumentException("Max value not in partition: " + min + ", region: " + partition.getRegion());
        }
        return fileForPartition(partition, records, min, max);
    }

    private Partition leafPartition(Object min, Object max) {
        if (min == null && max == null) {
            Partition partition = partitionTree.getRootPartition();
            if (!partition.getChildPartitionIds().isEmpty()) {
                throw new IllegalArgumentException("Cannot choose leaf partition for " + min + ", " + max);
            }
            return partition;
        }
        Partition partition = partitionTree.getLeafPartition(rowKey(min));
        if (!partition.isRowKeyInPartition(schema, rowKey(max))) {
            throw new IllegalArgumentException("Not in same leaf partition: " + min + ", " + max);
        }
        return partition;
    }

    private Partition middlePartition(Object min, Object max) {
        Partition partition = partitionTree.getNearestCommonAncestor(rowKey(min), rowKey(max));
        if (partition.isLeafPartition()) {
            throw new IllegalArgumentException("In same leaf partition: " + min + ", " + max);
        }
        if (partition.getParentPartitionId() == null) {
            throw new IllegalArgumentException("Nearest common ancestor is root partition: " + min + ", " + max);
        }
        return partition;
    }

    private FileInfo fileForPartition(Partition partition, long records, Object min, Object max) {
        return fileForPartition(partition, partition.getId() + ".parquet", records, min, max);
    }

    private FileInfo fileForPartition(Partition partition, String filename, long records, Object min, Object max) {
        return FileInfo.builder()
                .rowKeyTypes(partition.getRowKeyTypes())
                .minRowKey(rowKey(min))
                .maxRowKey(rowKey(max))
                .filename(filename)
                .partitionId(partition.getId())
                .numberOfRecords(records)
                .fileStatus(FileInfo.FileStatus.ACTIVE)
                .lastStateStoreUpdateTime(lastStateStoreUpdate)
                .build();
    }

    private static Key rowKey(Object value) {
        if (value == null) {
            return null;
        } else {
            return Key.create(value);
        }
    }

    public Builder toBuilder() {
        return new Builder()
                .lastStateStoreUpdate(this.lastStateStoreUpdate)
                .schema(this.schema)
                .partitionTree(this.partitionTree);
    }

    public static final class Builder {
        private Schema schema;
        private PartitionTree partitionTree;
        private Instant lastStateStoreUpdate;

        private Builder() {
        }

        public Builder schema(Schema schema) {
            this.schema = schema;
            return this;
        }

        public Builder partitionTree(PartitionTree partitionTree) {
            this.partitionTree = partitionTree;
            return this;
        }

        public Builder partitions(List<Partition> partitions) {
            return partitionTree(new PartitionTree(schema, partitions));
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
