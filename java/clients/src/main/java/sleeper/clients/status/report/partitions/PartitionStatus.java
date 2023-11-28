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
package sleeper.clients.status.report.partitions;

import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.partition.Partition;
import sleeper.core.partition.PartitionTree;
import sleeper.core.range.Range;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.FileInfo;
import sleeper.splitter.FindPartitionsToSplit;
import sleeper.splitter.PartitionSplitCheck;

import java.util.List;

public class PartitionStatus {

    private final Partition partition;
    private final List<FileInfo> filesInPartition;
    private final boolean needsSplitting;
    private final Field splitField;
    private final Object splitValue;
    private final Integer indexInParent;

    private PartitionStatus(Builder builder) {
        partition = builder.partition;
        filesInPartition = builder.filesInPartition;
        needsSplitting = builder.needsSplitting;
        splitField = builder.splitField;
        splitValue = builder.splitValue;
        indexInParent = builder.indexInParent;
    }

    static PartitionStatus from(
            TableProperties tableProperties, PartitionTree tree, Partition partition, List<FileInfo> activeFiles) {
        Schema schema = tableProperties.getSchema();
        List<FileInfo> filesInPartition = FindPartitionsToSplit.getFilesInPartition(partition, activeFiles);
        boolean needsSplitting = PartitionSplitCheck.fromFilesInPartition(tableProperties, filesInPartition).isNeedsSplitting();
        return builder().partition(partition)
                .filesInPartition(filesInPartition)
                .needsSplitting(needsSplitting)
                .splitField(splitField(partition, schema))
                .splitValue(splitValue(partition, tree, schema))
                .indexInParent(indexInParent(partition, tree))
                .build();
    }

    public boolean isNeedsSplitting() {
        return needsSplitting;
    }

    public boolean isLeafPartition() {
        return partition.isLeafPartition();
    }

    public Partition getPartition() {
        return partition;
    }

    public int getNumberOfFiles() {
        return filesInPartition.size();
    }

    public long getApproxRecords() {
        return filesInPartition.stream().mapToLong(FileInfo::getNumberOfRecords).sum();
    }

    public long getKnownRecords() {
        return filesInPartition.stream()
                .filter(fileInfo -> !fileInfo.isCountApproximate() && fileInfo.onlyContainsDataForThisPartition())
                .mapToLong(FileInfo::getNumberOfRecords)
                .sum();
    }

    public Field getSplitField() {
        return splitField;
    }

    public Object getSplitValue() {
        return splitValue;
    }

    public Integer getIndexInParent() {
        return indexInParent;
    }

    private static Builder builder() {
        return new Builder();
    }

    private static Field splitField(Partition partition, Schema schema) {
        if (partition.isLeafPartition()) {
            return null;
        }
        return dimensionRange(partition, schema, partition.getDimension()).getField();
    }

    private static Object splitValue(Partition partition, PartitionTree tree, Schema schema) {
        if (partition.isLeafPartition()) {
            return null;
        }
        Partition left = tree.getPartition(partition.getChildPartitionIds().get(0));
        return dimensionRange(left, schema, partition.getDimension()).getMax();
    }

    private static Range dimensionRange(Partition partition, Schema schema, int dimension) {
        Field splitField = schema.getRowKeyFields().get(dimension);
        return partition.getRegion().getRange(splitField.getName());
    }

    private static Integer indexInParent(Partition partition, PartitionTree tree) {
        String parentId = partition.getParentPartitionId();
        if (parentId == null) {
            return null;
        }
        Partition parent = tree.getPartition(parentId);
        return parent.getChildPartitionIds().indexOf(partition.getId());
    }

    public static final class Builder {
        private Partition partition;
        private List<FileInfo> filesInPartition;
        private boolean needsSplitting;
        private Field splitField;
        private Object splitValue;
        private Integer indexInParent;

        private Builder() {
        }

        public Builder partition(Partition partition) {
            this.partition = partition;
            return this;
        }

        public Builder filesInPartition(List<FileInfo> filesInPartition) {
            this.filesInPartition = filesInPartition;
            return this;
        }

        public Builder needsSplitting(boolean needsSplitting) {
            this.needsSplitting = needsSplitting;
            return this;
        }

        public Builder splitField(Field splitField) {
            this.splitField = splitField;
            return this;
        }

        public Builder splitValue(Object splitValue) {
            this.splitValue = splitValue;
            return this;
        }

        public Builder indexInParent(Integer indexInParent) {
            this.indexInParent = indexInParent;
            return this;
        }

        public PartitionStatus build() {
            return new PartitionStatus(this);
        }
    }
}
