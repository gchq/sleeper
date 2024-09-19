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
package sleeper.splitter.status;

import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.partition.Partition;
import sleeper.core.partition.PartitionTree;
import sleeper.core.range.Range;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.FileReference;
import sleeper.splitter.find.PartitionSplitCheck;

import java.util.List;
import java.util.Map;

public class PartitionStatus {

    private final Partition partition;
    private final List<FileReference> filesInPartition;
    private final boolean willBeSplit;
    private final boolean maySplitIfCompacted;
    private final Field splitField;
    private final Object splitValue;
    private final Integer indexInParent;

    private PartitionStatus(Builder builder) {
        partition = builder.partition;
        filesInPartition = builder.filesInPartition;
        willBeSplit = builder.willBeSplit;
        maySplitIfCompacted = builder.maySplitIfCompacted;
        splitField = builder.splitField;
        splitValue = builder.splitValue;
        indexInParent = builder.indexInParent;
    }

    static PartitionStatus from(
            TableProperties tableProperties, PartitionTree tree, Partition partition, Map<String, List<FileReference>> fileReferencesByPartition) {
        Schema schema = tableProperties.getSchema();
        PartitionSplitCheck check = PartitionSplitCheck.fromFilesInPartition(tableProperties, tree, partition, fileReferencesByPartition);
        return builder().partition(partition)
                .filesInPartition(check.getPartitionFileReferences())
                .willBeSplit(check.isNeedsSplitting())
                .maySplitIfCompacted(check.maySplitIfCompacted())
                .splitField(splitField(partition, schema))
                .splitValue(splitValue(partition, tree, schema))
                .indexInParent(indexInParent(partition, tree))
                .build();
    }

    public boolean willBeSplit() {
        return willBeSplit;
    }

    public boolean maySplitIfCompacted() {
        return maySplitIfCompacted;
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
        return filesInPartition.stream().mapToLong(FileReference::getNumberOfRecords).sum();
    }

    public long getKnownRecords() {
        return filesInPartition.stream()
                .filter(fileReference -> !fileReference.isCountApproximate() && fileReference.onlyContainsDataForThisPartition())
                .mapToLong(FileReference::getNumberOfRecords)
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
        private List<FileReference> filesInPartition;
        private boolean willBeSplit;
        private boolean maySplitIfCompacted;
        private Field splitField;
        private Object splitValue;
        private Integer indexInParent;

        private Builder() {
        }

        public Builder partition(Partition partition) {
            this.partition = partition;
            return this;
        }

        public Builder filesInPartition(List<FileReference> filesInPartition) {
            this.filesInPartition = filesInPartition;
            return this;
        }

        public Builder willBeSplit(boolean willBeSplit) {
            this.willBeSplit = willBeSplit;
            return this;
        }

        public Builder maySplitIfCompacted(boolean maySplitIfCompacted) {
            this.maySplitIfCompacted = maySplitIfCompacted;
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
