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
package sleeper.compaction.job;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.partition.Partition;
import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.partition.PartitionsFromSplitPoints;
import sleeper.core.range.Range;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.StringType;
import sleeper.core.statestore.FileInfo;
import sleeper.core.statestore.FileInfoFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

import static sleeper.configuration.properties.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.configuration.properties.table.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;

public class CompactionJobTestDataHelper {

    public static final String KEY_FIELD = "key";
    public static final String DEFAULT_TASK_ID = "test-task";
    public static final Schema SCHEMA = Schema.builder()
            .rowKeyFields(new Field(KEY_FIELD, new StringType()))
            .build();

    private final CompactionJobFactory jobFactory;
    private List<Partition> partitions;
    private PartitionTree partitionTree;
    private FileInfoFactory fileFactory;

    public CompactionJobTestDataHelper() {
        InstanceProperties instanceProperties = createTestInstanceProperties();
        TableProperties tableProperties = createTestTableProperties(instanceProperties, schemaWithKey("key"));
        this.jobFactory = new CompactionJobFactory(instanceProperties, tableProperties);
    }

    private CompactionJobTestDataHelper(InstanceProperties instanceProperties, TableProperties tableProperties) {
        this.jobFactory = new CompactionJobFactory(instanceProperties, tableProperties);
    }

    public static CompactionJobTestDataHelper forTable(InstanceProperties instanceProperties, TableProperties tableProperties) {
        return new CompactionJobTestDataHelper(instanceProperties, tableProperties);
    }

    public Partition singlePartition() {
        return singlePartitionTree().getRootPartition();
    }

    public void partitionTree(Consumer<PartitionsBuilder> config) {
        if (isPartitionsSpecified()) {
            throw new IllegalStateException("Partition tree already initialised");
        }
        setPartitions(createPartitions(config));
    }

    public CompactionJob singleFileCompaction() {
        return singleFileCompaction(singlePartition());
    }

    public CompactionJob singleFileCompaction(String partitionId) {
        return singleFileCompaction(partitionTree.getPartition(partitionId));
    }

    public CompactionJob singleFileCompaction(Partition partition) {
        validatePartitionSpecified(partition);
        return jobFactory.createCompactionJob(
                Collections.singletonList(fileInPartition(partition)),
                partition.getId());
    }

    public CompactionJob singleFileSplittingCompaction(String parentPartitionId, String leftPartitionId, String rightPartitionId) {
        Object splitPoint;
        if (!isPartitionsSpecified()) {
            splitPoint = "p";
            setPartitions(createPartitions(builder -> builder
                    .leavesWithSplits(Arrays.asList(leftPartitionId, rightPartitionId), Collections.singletonList(splitPoint))
                    .parentJoining(parentPartitionId, leftPartitionId, rightPartitionId)));
        } else {
            Partition left = partitionTree.getPartition(leftPartitionId);
            Partition right = partitionTree.getPartition(rightPartitionId);
            splitPoint = getValidSplitPoint(parentPartitionId, left, right);
        }
        return jobFactory.createSplittingCompactionJob(
                Collections.singletonList(fileInPartition(partitionTree.getPartition(parentPartitionId))),
                parentPartitionId, leftPartitionId, rightPartitionId, splitPoint, 0);
    }

    private FileInfo fileInPartition(Partition partition) {
        Range range = singleFieldRange(partition);
        String min = range.getMin() + "a";
        String max = range.getMin() + "b";
        return fileFactory.partitionFile(partition, 100L, min, max);
    }

    private PartitionTree singlePartitionTree() {
        if (!isPartitionsSpecified()) {
            setPartitions(createSinglePartition());
        } else if (!partitionTree.getRootPartition().isLeafPartition()) {
            throw new IllegalStateException(
                    "Partition tree already initialised with multiple partitions when single partition expected");
        }
        return partitionTree;
    }

    private void setPartitions(List<Partition> partitions) {
        this.partitions = partitions;
        partitionTree = new PartitionTree(SCHEMA, partitions);
        fileFactory = FileInfoFactory.builder().schema(SCHEMA).partitionTree(partitionTree).build();
    }

    private boolean isPartitionsSpecified() {
        return partitionTree != null;
    }

    private List<Partition> createSinglePartition() {
        return new PartitionsFromSplitPoints(SCHEMA, Collections.emptyList()).construct();
    }

    private List<Partition> createPartitions(Consumer<PartitionsBuilder> config) {
        PartitionsBuilder builder = new PartitionsBuilder(SCHEMA);
        config.accept(builder);
        return builder.buildList();
    }

    private void validatePartitionSpecified(Partition checkPartition) {
        for (Partition partition : partitions) {
            if (partition == checkPartition) {
                return;
            }
        }
        throw new IllegalArgumentException("Partition should be specified with helper: " + checkPartition);
    }

    private Object getValidSplitPoint(String parentId, Partition left, Partition right) {
        if (!left.getParentPartitionId().equals(parentId)
                || !right.getParentPartitionId().equals(parentId)) {
            throw new IllegalStateException("Parent partition does not match");
        }
        Object splitPoint = singleFieldRange(left).getMax();
        if (!splitPoint.equals(singleFieldRange(right).getMin())) {
            throw new IllegalStateException(
                    "Left and right partition are mismatched (expected split point " + splitPoint + ")");
        }
        return splitPoint;
    }

    private Range singleFieldRange(Partition partition) {
        return partition.getRegion().getRange(KEY_FIELD);
    }

}
