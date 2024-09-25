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
package sleeper.compaction.job;

import sleeper.core.partition.Partition;
import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.partition.PartitionsFromSplitPoints;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.StringType;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.FileReferenceFactory;

import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Function;

import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
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
    private FileReferenceFactory fileFactory;

    public CompactionJobTestDataHelper() {
        InstanceProperties instanceProperties = createTestInstanceProperties();
        TableProperties tableProperties = createTestTableProperties(instanceProperties, schemaWithKey("key"));
        this.jobFactory = new CompactionJobFactory(instanceProperties, tableProperties);
    }

    private CompactionJobTestDataHelper(CompactionJobFactory compactionJobFactory) {
        this.jobFactory = compactionJobFactory;
    }

    public static CompactionJobTestDataHelper forTable(InstanceProperties instanceProperties, TableProperties tableProperties) {
        return new CompactionJobTestDataHelper(new CompactionJobFactory(instanceProperties, tableProperties));
    }

    public Partition singlePartition() {
        return singlePartitionTree().getRootPartition();
    }

    public void partitionTree(Consumer<PartitionsBuilder> config) {
        partitionTreeFromSchema(schema -> {
            PartitionsBuilder builder = new PartitionsBuilder(schema);
            config.accept(builder);
            return builder;
        });
    }

    public void partitionTreeFromSchema(Function<Schema, PartitionsBuilder> config) {
        if (isPartitionsSpecified()) {
            throw new IllegalStateException("Partition tree already initialised");
        }
        PartitionsBuilder builder = config.apply(SCHEMA);
        setPartitions(builder.buildList());
    }

    public CompactionJob singleFileCompaction() {
        return singleFileCompaction(UUID.randomUUID().toString(), singlePartition());
    }

    public CompactionJob singleFileCompaction(String jobId) {
        return singleFileCompaction(jobId, singlePartition());
    }

    public CompactionJob singleFileCompaction(String jobId, String partitionId) {
        return singleFileCompaction(jobId, partitionTree.getPartition(partitionId));
    }

    public CompactionJob singleFileCompaction(String jobId, Partition partition) {
        validatePartitionSpecified(partition);
        return jobFactory.createCompactionJob(jobId,
                List.of(fileInPartition(partition)),
                partition.getId());
    }

    private FileReference fileInPartition(Partition partition) {
        return fileFactory.partitionFile(partition.getId(), 100L);
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
        partitionTree = new PartitionTree(partitions);
        fileFactory = FileReferenceFactory.from(partitionTree);
    }

    private boolean isPartitionsSpecified() {
        return partitionTree != null;
    }

    private List<Partition> createSinglePartition() {
        return new PartitionsFromSplitPoints(SCHEMA, Collections.emptyList()).construct();
    }

    private void validatePartitionSpecified(Partition checkPartition) {
        for (Partition partition : partitions) {
            if (partition == checkPartition) {
                return;
            }
        }
        throw new IllegalArgumentException("Partition should be specified with helper: " + checkPartition);
    }

}
