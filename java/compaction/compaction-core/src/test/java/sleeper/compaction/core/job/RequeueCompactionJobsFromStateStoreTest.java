/*
 * Copyright 2022-2026 Crown Copyright
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
package sleeper.compaction.core.job;

import org.junit.jupiter.api.Test;

import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.testutils.InMemoryTransactionLogStateStore;
import sleeper.core.statestore.testutils.InMemoryTransactionLogs;
import sleeper.core.tracker.compaction.job.InMemoryCompactionJobTracker;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.compaction.core.job.CompactionJobStatusFromJobTestData.compactionJobCreated;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.schema.SchemaTestHelper.createSchemaWithKey;
import static sleeper.core.statestore.SplitFileReference.referenceForChildPartition;
import static sleeper.core.statestore.testutils.StateStoreUpdatesWrapper.update;

public class RequeueCompactionJobsFromStateStoreTest {

    Schema schema = createSchemaWithKey("key");
    InstanceProperties instanceProperties = createTestInstanceProperties();
    TableProperties tableProperties = createTestTableProperties(instanceProperties, schema);
    PartitionTree partitions = new PartitionsBuilder(schema)
            .rootFirst("root")
            .splitToNewChildren("root", "L", "R", 50L)
            .buildTree();
    StateStore stateStore = InMemoryTransactionLogStateStore
            .createAndInitialiseWithPartitions(partitions.getAllPartitions(), tableProperties, new InMemoryTransactionLogs());
    InMemoryCompactionJobTracker tracker = new InMemoryCompactionJobTracker();
    FileReferenceFactory fileFactory = FileReferenceFactory.from(partitions);
    CompactionJobFactory jobFactory = new CompactionJobFactory(instanceProperties, tableProperties);
    List<List<CompactionJob>> sentBatches = new ArrayList<>();

    @Test
    void shouldRequeueJobsAssignedToFilesAndIgnoreUnassignedFiles() {
        // Given
        FileReference file1 = fileFactory.partitionFile("L", "file1.parquet", 100);
        FileReference file2 = fileFactory.partitionFile("L", "file2.parquet", 200);
        FileReference file3 = fileFactory.partitionFile("R", "file3.parquet", 300);
        FileReference unassigned = fileFactory.partitionFile("R", "file4.parquet", 400);
        update(stateStore).addFiles(List.of(file1, file2, file3, unassigned));
        update(stateStore).assignJobId("job-1", List.of(file1, file2));
        update(stateStore).assignJobId("job-2", List.of(file3));

        // When
        List<CompactionJob> jobs = requeue();

        // Then
        CompactionJob expectedJob1 = jobFactory.createCompactionJob("job-1", List.of(file1, file2), "L");
        CompactionJob expectedJob2 = jobFactory.createCompactionJob("job-2", List.of(file3), "R");
        assertThat(jobs).containsExactlyInAnyOrder(expectedJob1, expectedJob2);
        assertThat(sentBatches).flatExtracting(batch -> batch)
                .containsExactlyInAnyOrder(expectedJob1, expectedJob2);
    }

    @Test
    void shouldRequeueOnlyAssignedReferenceOfFileSplitOverPartitions() {
        // Given
        FileReference rootFile = fileFactory.rootFile("split.parquet", 100);
        FileReference leftReference = referenceForChildPartition(rootFile, "L");
        FileReference rightReference = referenceForChildPartition(rootFile, "R");
        update(stateStore).addFiles(List.of(leftReference, rightReference));
        update(stateStore).assignJobId("job-1", List.of(leftReference));

        // When
        List<CompactionJob> jobs = requeue();

        // Then
        assertThat(jobs).containsExactly(
                jobFactory.createCompactionJob("job-1", List.of(leftReference), "L"));
        assertThat(sentBatches).containsExactly(jobs);
    }

    @Test
    void shouldSkipJobAssignedInMultiplePartitionsAndRequeueOtherJobs() {
        // Given
        FileReference file1 = fileFactory.partitionFile("L", "file1.parquet", 100);
        FileReference file2 = fileFactory.partitionFile("R", "file2.parquet", 200);
        FileReference file3 = fileFactory.partitionFile("R", "file3.parquet", 300);
        update(stateStore).addFiles(List.of(file1, file2, file3));
        update(stateStore).assignJobId("bad-job", List.of(file1));
        update(stateStore).assignJobId("bad-job", List.of(file2));
        update(stateStore).assignJobId("good-job", List.of(file3));

        // When
        List<CompactionJob> jobs = requeue();

        // Then
        CompactionJob expectedJob = jobFactory.createCompactionJob("good-job", List.of(file3), "R");
        assertThat(jobs).containsExactly(expectedJob);
        assertThat(sentBatches).containsExactly(List.of(expectedJob));
    }

    @Test
    void shouldSendNothingWhenNoFilesAreAssigned() {
        // Given
        update(stateStore).addFiles(List.of(fileFactory.partitionFile("L", "file1.parquet", 100)));

        // When
        List<CompactionJob> jobs = requeue();

        // Then
        assertThat(jobs).isEmpty();
        assertThat(sentBatches).isEmpty();
    }

    @Test
    void shouldSendJobsInBatchesOfTen() {
        // Given
        List<FileReference> files = IntStream.rangeClosed(1, 12)
                .mapToObj(i -> fileFactory.partitionFile("L", "file" + i + ".parquet", i))
                .toList();
        update(stateStore).addFiles(files);
        for (int i = 0; i < files.size(); i++) {
            update(stateStore).assignJobId("job-" + (i + 1), List.of(files.get(i)));
        }

        // When
        requeue();

        // Then
        assertThat(sentBatches).extracting(List::size).containsExactly(10, 2);
    }

    @Test
    void shouldReportJobsAsCreatedInTracker() {
        // Given
        FileReference file = fileFactory.partitionFile("L", "file1.parquet", 100);
        update(stateStore).addFiles(List.of(file));
        update(stateStore).assignJobId("job-1", List.of(file));
        tracker.fixUpdateTime(Instant.parse("2026-11-20T10:00:00Z"));

        // When
        requeue();

        // Then
        CompactionJob expectedJob = jobFactory.createCompactionJob("job-1", List.of(file), "L");
        assertThat(tracker.getAllJobs(tableProperties.get(TABLE_ID))).containsExactly(
                compactionJobCreated(expectedJob, Instant.parse("2026-11-20T10:00:00Z")));
    }

    private List<CompactionJob> requeue() {
        return new RequeueCompactionJobsFromStateStore(instanceProperties, sentBatches::add, tracker)
                .requeue(tableProperties, stateStore);
    }
}
