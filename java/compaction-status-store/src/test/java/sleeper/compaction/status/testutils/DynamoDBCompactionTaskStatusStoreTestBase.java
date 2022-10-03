/*
 * Copyright 2022 Crown Copyright
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
package sleeper.compaction.status.testutils;

import org.assertj.core.api.recursive.comparison.RecursiveComparisonConfiguration;
import org.junit.After;
import org.junit.Before;
import sleeper.compaction.job.CompactionJob;
import sleeper.compaction.job.CompactionJobFactory;
import sleeper.compaction.job.status.CompactionJobStatus;
import sleeper.compaction.status.task.DynamoDBCompactionTaskStatusStore;
import sleeper.compaction.status.task.DynamoDBCompactionTaskStatusStoreCreator;
import sleeper.compaction.task.CompactionTaskStatus;
import sleeper.compaction.task.CompactionTaskStatusStore;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.partition.Partition;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.partition.PartitionsFromSplitPoints;
import sleeper.core.schema.Schema;
import sleeper.statestore.FileInfoFactory;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

import static sleeper.compaction.status.DynamoDBUtils.instanceTableName;
import static sleeper.compaction.status.testutils.CompactionStatusStoreTestUtils.createInstanceProperties;
import static sleeper.compaction.status.testutils.CompactionStatusStoreTestUtils.createSchema;
import static sleeper.compaction.status.testutils.CompactionStatusStoreTestUtils.createTableProperties;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.ID;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;

public class DynamoDBCompactionTaskStatusStoreTestBase extends DynamoDBTestBase {

    protected static final RecursiveComparisonConfiguration IGNORE_UPDATE_TIMES = RecursiveComparisonConfiguration.builder()
            .withIgnoredFields("startedStatus.startUpdateTime", "finishedStatus.updateTime", "expiryDate").build();

    private final InstanceProperties instanceProperties = createInstanceProperties();
    private final String jobStatusTableName = jobStatusTableName(instanceProperties.get(ID));
    private final Schema schema = createSchema();
    private final TableProperties tableProperties = createTableProperties(schema, instanceProperties);

    protected final String tableName = tableProperties.get(TABLE_NAME);
    protected final CompactionJobFactory jobFactory = new CompactionJobFactory(instanceProperties, tableProperties);
    protected final CompactionTaskStatusStore store = DynamoDBCompactionTaskStatusStore.from(dynamoDBClient, instanceProperties);

    @Before
    public void setUp() {
        DynamoDBCompactionTaskStatusStoreCreator.create(instanceProperties, dynamoDBClient);
    }

    @After
    public void tearDown() {
        dynamoDBClient.deleteTable(jobStatusTableName);
    }

    protected Partition singlePartition() {
        return new PartitionsFromSplitPoints(schema, Collections.emptyList()).construct().get(0);
    }

    protected FileInfoFactory fileFactory(Partition singlePartition) {
        return fileFactory(Collections.singletonList(singlePartition));
    }

    protected FileInfoFactory fileFactoryWithPartitions(Consumer<PartitionsBuilder> partitionConfig) {
        PartitionsBuilder builder = new PartitionsBuilder(schema);
        partitionConfig.accept(builder);
        return fileFactory(builder.buildList());
    }

    private FileInfoFactory fileFactory(List<Partition> partitions) {
        return new FileInfoFactory(schema, partitions, Instant.now());
    }

    protected CompactionJobFactory jobFactoryForTable(String tableName) {
        TableProperties tableProperties = createTableProperties(schema, instanceProperties);
        tableProperties.set(TABLE_NAME, tableName);
        return new CompactionJobFactory(instanceProperties, tableProperties);
    }

    protected static Instant defaultStartTime() {
        return Instant.parse("2022-09-22T12:30:00.000Z");
    }

    protected static Instant defaultFinishTime() {
        return Instant.parse("2022-09-22T16:30:00.000Z");
    }

    protected static CompactionTaskStatus startedTaskWithDefaults() {
        return CompactionTaskStatus.started(defaultStartTime().toEpochMilli());
    }

    protected static CompactionTaskStatus finishedTaskWithDefaults(List<CompactionJobStatus> jobStatusList) {
        CompactionTaskStatus taskStatus = startedTaskWithDefaults();
        jobStatusList.forEach(taskStatus::addJobStatus);
        taskStatus.finished(defaultFinishTime().toEpochMilli());
        return taskStatus;
    }

    public static String jobStatusTableName(String instanceId) {
        return instanceTableName(instanceId, "compaction-task-status");
    }

    public CompactionJob singleFileSplittingCompaction(String rootPartitionId, String leftPartitionId, String rightPartitionId) {
        List<Partition> partitions = new PartitionsBuilder(schema)
                .leavesWithSplits(Arrays.asList(leftPartitionId, rightPartitionId), Collections.singletonList("p"))
                .parentJoining(rootPartitionId, leftPartitionId, rightPartitionId)
                .buildList();
        FileInfoFactory fileFactory = new FileInfoFactory(schema, partitions);
        return jobFactory.createSplittingCompactionJob(
                Collections.singletonList(fileFactory.rootFile(100L, "a", "z")),
                rootPartitionId, leftPartitionId, rightPartitionId, "p", 0);
    }

}
