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
package sleeper.commit;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import sleeper.compaction.job.CompactionJob;
import sleeper.compaction.job.commit.CompactionJobCommitRequest;
import sleeper.compaction.testutils.InMemoryCompactionJobStatusStore;
import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.record.process.RecordsProcessedSummary;
import sleeper.core.record.process.status.ProcessRun;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.statestore.StateStore;
import sleeper.ingest.job.IngestJob;
import sleeper.ingest.job.commit.IngestAddFilesCommitRequest;
import sleeper.ingest.job.status.InMemoryIngestJobStatusStore;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.compaction.job.CompactionJobStatusTestData.compactionCommittedStatus;
import static sleeper.compaction.job.CompactionJobStatusTestData.compactionFinishedStatusUncommitted;
import static sleeper.compaction.job.CompactionJobStatusTestData.compactionStartedStatus;
import static sleeper.compaction.job.CompactionJobStatusTestData.jobCreated;
import static sleeper.compaction.job.status.CompactionJobFinishedEvent.compactionJobFinished;
import static sleeper.compaction.job.status.CompactionJobStartedEvent.compactionJobStarted;
import static sleeper.core.record.process.RecordsProcessedSummaryTestHelper.summary;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;
import static sleeper.core.statestore.AssignJobIdRequest.assignJobOnPartitionToFiles;
import static sleeper.core.statestore.inmemory.StateStoreTestHelper.inMemoryStateStoreWithFixedPartitions;
import static sleeper.ingest.job.status.IngestJobStartedEvent.ingestJobStarted;
import static sleeper.ingest.job.status.IngestJobStatusTestHelper.ingestAddedFilesStatus;
import static sleeper.ingest.job.status.IngestJobStatusTestHelper.ingestStartedStatus;
import static sleeper.ingest.job.status.IngestJobStatusTestHelper.jobStatus;

public class StateStoreCommitterTest {
    private static final Instant DEFAULT_UPDATE_TIME = Instant.parse("2024-06-14T13:33:00Z");
    private final Schema schema = schemaWithKey("key");
    private final PartitionTree partitions = new PartitionsBuilder(schema).singlePartition("root").buildTree();
    private final FileReferenceFactory fileFactory = FileReferenceFactory.fromUpdatedAt(partitions, DEFAULT_UPDATE_TIME);
    private final InMemoryCompactionJobStatusStore compactionJobStatusStore = new InMemoryCompactionJobStatusStore();
    private final InMemoryIngestJobStatusStore ingestJobStatusStore = new InMemoryIngestJobStatusStore();
    private final Map<String, StateStore> stateStoreByTableId = new HashMap<>();

    @Nested
    @DisplayName("Commit a compaction job")
    class CommitCompaction {

        @Test
        void shouldApplyCompactionCommitRequest() throws Exception {
            // Given
            StateStore stateStore = createTable("test-table");
            FileReference inputFile = fileFactory.rootFile("input.parquet", 123L);
            FileReference outputFile = fileFactory.rootFile("output.parquet", 123L);
            CompactionJob job = CompactionJob.builder()
                    .tableId("test-table")
                    .jobId("test-job")
                    .inputFiles(List.of("input.parquet"))
                    .outputFile("output.parquet")
                    .partitionId("root")
                    .build();
            Instant createdTime = Instant.parse("2024-06-14T15:34:00Z");
            Instant startTime = Instant.parse("2024-06-14T15:35:00Z");
            Instant commitTime = Instant.parse("2024-06-14T15:40:00Z");
            RecordsProcessedSummary summary = summary(startTime, Duration.ofMinutes(2), 123, 123);
            CompactionJobCommitRequest commitRequest = new CompactionJobCommitRequest(job, "test-task", "test-job-run", summary);

            stateStore.addFile(inputFile);
            stateStore.assignJobIds(List.of(assignJobOnPartitionToFiles(
                    "test-job", "root", List.of("input.parquet"))));
            compactionJobStatusStore.jobCreated(job, createdTime);
            compactionJobStatusStore.jobStarted(compactionJobStarted(job, startTime).taskId("test-task").jobRunId("test-job-run").build());
            compactionJobStatusStore.jobFinished(compactionJobFinished(job, summary).committedBySeparateUpdate(true)
                    .taskId("test-task").jobRunId("test-job-run").build());
            compactionJobStatusStore.fixUpdateTime(commitTime);

            // When
            committer().apply(StateStoreCommitRequest.forCompactionJob(commitRequest));

            // Then
            assertThat(stateStore.getFileReferences()).containsExactly(outputFile);
            assertThat(compactionJobStatusStore.getAllJobs("test-table")).containsExactly(
                    jobCreated(job, createdTime,
                            ProcessRun.builder().taskId("test-task")
                                    .startedStatus(compactionStartedStatus(startTime))
                                    .finishedStatus(compactionFinishedStatusUncommitted(summary))
                                    .statusUpdate(compactionCommittedStatus(commitTime))
                                    .build()));
        }
    }

    @Nested
    @DisplayName("Add files during ingest")
    class AddFiles {

        @Test
        void shouldApplyIngestJobAddFilesCommitRequest() throws Exception {
            // Given we have a commit request during an ingest job, which may still be in progress
            StateStore stateStore = createTable("test-table");
            FileReference outputFile = fileFactory.rootFile("output.parquet", 123L);
            IngestJob ingestJob = IngestJob.builder()
                    .id("test-job")
                    .tableId("test-table")
                    .files(List.of("input.parquet"))
                    .build();
            Instant startTime = Instant.parse("2024-06-20T14:50:00Z");
            Instant writtenTime = Instant.parse("2024-06-20T14:55:01Z");
            IngestAddFilesCommitRequest commitRequest = IngestAddFilesCommitRequest.builder()
                    .ingestJob(ingestJob)
                    .taskId("test-task-id")
                    .jobRunId("test-job-run-id")
                    .fileReferences(List.of(outputFile))
                    .writtenTime(writtenTime)
                    .build();

            ingestJobStatusStore.jobStarted(ingestJobStarted(ingestJob, startTime)
                    .taskId("test-task-id").jobRunId("test-job-run-id").build());

            // When
            committer().apply(StateStoreCommitRequest.forIngestAddFiles(commitRequest));

            // Then
            assertThat(stateStore.getFileReferences()).containsExactly(outputFile);
            assertThat(ingestJobStatusStore.getAllJobs("test-table"))
                    .containsExactly(jobStatus(ingestJob, ProcessRun.builder()
                            .taskId("test-task-id")
                            .startedStatus(ingestStartedStatus(ingestJob, startTime))
                            .statusUpdate(ingestAddedFilesStatus(writtenTime, 1))
                            .build()));
        }

        @Test
        void shouldApplyIngestStreamAddFilesCommitRequest() throws Exception {
            // Given we have a commit request without an ingest job (e.g. from an endless stream of records)
            StateStore stateStore = createTable("test-table");
            FileReference outputFile = fileFactory.rootFile("output.parquet", 123L);
            IngestAddFilesCommitRequest commitRequest = IngestAddFilesCommitRequest.builder()
                    .tableId("test-table")
                    .fileReferences(List.of(outputFile))
                    .build();

            // When
            committer().apply(StateStoreCommitRequest.forIngestAddFiles(commitRequest));

            // Then
            assertThat(stateStore.getFileReferences()).containsExactly(outputFile);
            assertThat(ingestJobStatusStore.getAllJobs("test-table")).isEmpty();
        }
    }

    private StateStoreCommitter committer() {
        return new StateStoreCommitter(compactionJobStatusStore, ingestJobStatusStore, stateStoreByTableId::get);
    }

    private StateStore createTable(String tableId) {
        StateStore stateStore = inMemoryStateStoreWithFixedPartitions(partitions.getAllPartitions());
        stateStore.fixFileUpdateTime(DEFAULT_UPDATE_TIME);
        stateStoreByTableId.put(tableId, stateStore);
        return stateStore;
    }
}
