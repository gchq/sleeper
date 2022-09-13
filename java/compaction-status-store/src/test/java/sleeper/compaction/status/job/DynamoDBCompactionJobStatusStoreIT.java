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
package sleeper.compaction.status.job;

import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import org.assertj.core.api.AbstractListAssert;
import org.assertj.core.api.ObjectAssert;
import org.assertj.core.groups.Tuple;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import sleeper.compaction.job.CompactionFactory;
import sleeper.compaction.job.CompactionJob;
import sleeper.compaction.job.CompactionJobStatusStore;
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
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;
import static sleeper.compaction.status.DynamoDBAttributes.getNumberAttribute;
import static sleeper.compaction.status.DynamoDBAttributes.getStringAttribute;
import static sleeper.compaction.status.job.CompactionStatusStoreTestUtils.createInstanceProperties;
import static sleeper.compaction.status.job.CompactionStatusStoreTestUtils.createSchema;
import static sleeper.compaction.status.job.CompactionStatusStoreTestUtils.createTableProperties;
import static sleeper.compaction.status.job.DynamoDBCompactionJobStatusFormat.INPUT_FILES_COUNT;
import static sleeper.compaction.status.job.DynamoDBCompactionJobStatusFormat.JOB_ID;
import static sleeper.compaction.status.job.DynamoDBCompactionJobStatusFormat.PARTITION_ID;
import static sleeper.compaction.status.job.DynamoDBCompactionJobStatusFormat.UPDATE_TIME;
import static sleeper.compaction.status.job.DynamoDBCompactionJobStatusFormat.UPDATE_TYPE;
import static sleeper.compaction.status.job.DynamoDBCompactionJobStatusFormat.UPDATE_TYPE_CREATED;
import static sleeper.compaction.status.job.DynamoDBCompactionJobStatusStore.jobStatusTableName;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.ID;

public class DynamoDBCompactionJobStatusStoreIT extends DynamoDBTestBase {

    private final InstanceProperties instanceProperties = createInstanceProperties();

    private final Schema schema = createSchema();
    private final TableProperties tableProperties = createTableProperties(schema, instanceProperties);
    private final CompactionJobStatusStore store = DynamoDBCompactionJobStatusStore.from(dynamoDBClient, instanceProperties);

    private final String tableName = jobStatusTableName(instanceProperties.get(ID));

    @Before
    public void setUp() {
        DynamoDBCompactionJobStatusStoreCreator.create(instanceProperties, dynamoDBClient);
    }

    @After
    public void tearDown() {
        dynamoDBClient.deleteTable(tableName);
    }

    @Test
    public void shouldReportCompactionJobCreated() {
        // Given
        Partition partition = singlePartition();
        FileInfoFactory fileFactory = new FileInfoFactory(schema, Collections.singletonList(partition), Instant.now());
        CompactionFactory jobFactory = new CompactionFactory(instanceProperties, tableProperties);
        CompactionJob job = jobFactory.createCompactionJob(
                Collections.singletonList(fileFactory.leafFile(100L, "a", "z")),
                partition.getId());

        // When
        store.jobCreated(job);

        // Then
        assertThatItemsInTable().containsExactly(
                createJobItem(job, 1));
    }

    @Test
    public void shouldReportSeveralCompactionJobsCreated() {
        // Given
        List<Partition> partitions = new PartitionsBuilder(schema)
                .leavesWithSplits(
                        Arrays.asList("A", "B"),
                        Collections.singletonList("ggg"))
                .parentJoining("C", "A", "B")
                .buildList();
        FileInfoFactory fileFactory = new FileInfoFactory(schema, partitions, Instant.now());
        CompactionFactory jobFactory = new CompactionFactory(instanceProperties, tableProperties);
        CompactionJob job1 = jobFactory.createCompactionJob(
                Collections.singletonList(fileFactory.leafFile(100L, "a", "c")), "A");
        CompactionJob job2 = jobFactory.createCompactionJob(
                Collections.singletonList(fileFactory.leafFile(100L, "w", "z")), "B");

        // When
        store.jobCreated(job1);
        store.jobCreated(job2);

        // Then
        assertThatItemsInTable().containsExactlyInAnyOrder(
                createJobItem(job1, 1),
                createJobItem(job2, 1));
    }

    @Test
    public void shouldReportCompactionJobCreatedWithSeveralFiles() {
        // Given
        Partition partition = singlePartition();
        FileInfoFactory fileFactory = new FileInfoFactory(schema, Collections.singletonList(partition), Instant.now());
        CompactionFactory jobFactory = new CompactionFactory(instanceProperties, tableProperties);
        CompactionJob job = jobFactory.createCompactionJob(
                Arrays.asList(
                        fileFactory.leafFile("file1", 100L, "a", "c"),
                        fileFactory.leafFile("file2", 100L, "w", "z")),
                partition.getId());

        // When
        store.jobCreated(job);

        // Then
        assertThatItemsInTable().containsExactly(
                createJobItem(job, 2));
    }

    private Partition singlePartition() {
        return new PartitionsFromSplitPoints(schema, Collections.emptyList()).construct().get(0);
    }

    private AbstractListAssert<?, List<? extends Tuple>, Tuple, ObjectAssert<Tuple>> assertThatItemsInTable() {
        return assertThat(dynamoDBClient.scan(new ScanRequest().withTableName(tableName)).getItems())
                .extracting(
                        Map::keySet,
                        map -> getStringAttribute(map, JOB_ID),
                        map -> getStringAttribute(map, UPDATE_TYPE),
                        map -> getStringAttribute(map, PARTITION_ID),
                        map -> getNumberAttribute(map, INPUT_FILES_COUNT));
    }

    private Tuple createJobItem(CompactionJob job, int inputFilesCount) {
        return tuple(
                Stream.of(JOB_ID, UPDATE_TIME, UPDATE_TYPE, PARTITION_ID, INPUT_FILES_COUNT).collect(Collectors.toSet()),
                job.getId(), UPDATE_TYPE_CREATED, job.getPartitionId(), "" + inputFilesCount);
    }
}
