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
package sleeper.statestore.committer;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import sleeper.compaction.job.CompactionJob;
import sleeper.compaction.job.commit.CompactionJobCommitRequest;
import sleeper.compaction.job.commit.CompactionJobCommitter;
import sleeper.compaction.job.commit.CompactionJobIdAssignmentCommitRequest;
import sleeper.compaction.testutils.InMemoryCompactionJobStatusStore;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.FixedTablePropertiesProvider;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.statestore.StateStoreProvider;
import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.record.process.ProcessRunTime;
import sleeper.core.record.process.RecordsProcessedSummary;
import sleeper.core.record.process.status.ProcessRun;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.StringType;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.statestore.FilesReportTestHelper;
import sleeper.core.statestore.ReplaceFileReferencesRequest;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;
import sleeper.core.statestore.commit.GarbageCollectionCommitRequest;
import sleeper.core.statestore.commit.SplitPartitionCommitRequest;
import sleeper.core.statestore.exception.FileHasReferencesException;
import sleeper.core.statestore.exception.FileNotFoundException;
import sleeper.core.statestore.exception.FileReferenceAssignedToJobException;
import sleeper.core.statestore.exception.FileReferenceNotFoundException;
import sleeper.core.statestore.exception.ReplaceRequestsFailedException;
import sleeper.core.statestore.transactionlog.InMemoryTransactionLogStore;
import sleeper.core.statestore.transactionlog.InMemoryTransactionLogs;
import sleeper.core.statestore.transactionlog.TransactionLogStateStore;
import sleeper.core.util.PollWithRetries;
import sleeper.ingest.job.IngestJob;
import sleeper.ingest.job.commit.IngestAddFilesCommitRequest;
import sleeper.ingest.job.status.InMemoryIngestJobStatusStore;
import sleeper.statestore.StateStoreFactory;
import sleeper.statestore.committer.StateStoreCommitter.RequestHandle;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toUnmodifiableList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static sleeper.compaction.job.CompactionJobStatusTestData.compactionCommittedStatus;
import static sleeper.compaction.job.CompactionJobStatusTestData.compactionFailedStatus;
import static sleeper.compaction.job.CompactionJobStatusTestData.compactionFinishedStatus;
import static sleeper.compaction.job.CompactionJobStatusTestData.compactionStartedStatus;
import static sleeper.compaction.job.CompactionJobStatusTestData.jobCreated;
import static sleeper.compaction.job.status.CompactionJobFinishedEvent.compactionJobFinished;
import static sleeper.compaction.job.status.CompactionJobStartedEvent.compactionJobStarted;
import static sleeper.configuration.properties.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.configuration.properties.table.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.configuration.properties.table.TableProperty.STATESTORE_COMMITTER_UPDATE_ON_EVERY_BATCH;
import static sleeper.configuration.properties.table.TableProperty.STATESTORE_COMMITTER_UPDATE_ON_EVERY_COMMIT;
import static sleeper.configuration.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.record.process.RecordsProcessedSummaryTestHelper.summary;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;
import static sleeper.core.statestore.AssignJobIdRequest.assignJobOnPartitionToFiles;
import static sleeper.core.statestore.FileReferenceTestData.withJobId;
import static sleeper.core.statestore.FilesReportTestHelper.activeAndReadyForGCFiles;
import static sleeper.core.statestore.ReplaceFileReferencesRequest.replaceJobFileReferences;
import static sleeper.core.util.ExponentialBackoffWithJitterTestHelper.recordWaits;
import static sleeper.ingest.job.status.IngestJobStartedEvent.ingestJobStarted;
import static sleeper.ingest.job.status.IngestJobStatusTestHelper.ingestAddedFilesStatus;
import static sleeper.ingest.job.status.IngestJobStatusTestHelper.ingestStartedStatus;
import static sleeper.ingest.job.status.IngestJobStatusTestHelper.jobStatus;

public class StateStoreCommitterTest {
    private static final Instant DEFAULT_FILE_UPDATE_TIME = FilesReportTestHelper.DEFAULT_UPDATE_TIME;
    private final Schema schema = schemaWithKey("key", new StringType());
    private final PartitionTree partitions = new PartitionsBuilder(schema).singlePartition("root").buildTree();
    private final FileReferenceFactory fileFactory = FileReferenceFactory.fromUpdatedAt(partitions, DEFAULT_FILE_UPDATE_TIME);
    private final InMemoryCompactionJobStatusStore compactionJobStatusStore = new InMemoryCompactionJobStatusStore();
    private final InMemoryIngestJobStatusStore ingestJobStatusStore = new InMemoryIngestJobStatusStore();
    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final Map<String, TableProperties> propertiesByTableId = new LinkedHashMap<>();
    private final Map<String, StateStore> stateStoreByTableId = new LinkedHashMap<>();
    private final Map<String, InMemoryTransactionLogs> transactionLogsByTableId = new LinkedHashMap<>();
    private final List<StateStoreCommitRequest> failedRequests = new ArrayList<>();
    private final List<Exception> failures = new ArrayList<>();
    private final List<Duration> retryWaits = new ArrayList<>();

    @Nested
    @DisplayName("Commit a compaction job")
    class CommitCompaction {

        @Test
        void shouldApplyCompactionCommitRequest() throws Exception {
            // Given
            StateStore stateStore = createTableGetStateStore("test-table");
            Instant createdTime = Instant.parse("2024-06-14T15:34:00Z");
            Instant startTime = Instant.parse("2024-06-14T15:35:00Z");
            RecordsProcessedSummary summary = summary(startTime, Duration.ofMinutes(2), 123, 123);
            Instant commitTime = Instant.parse("2024-06-14T15:40:00Z");
            CompactionJobCommitRequest request = createFinishedCompactionForTable("test-table", createdTime, startTime, summary);

            // When
            apply(committerWithTimes(List.of(commitTime)), StateStoreCommitRequest.forCompactionJob(request));

            // Then
            assertThat(stateStore.getFileReferences()).containsExactly(
                    fileFactory.rootFile("output.parquet", 123L));
            assertThat(compactionJobStatusStore.getAllJobs("test-table")).containsExactly(
                    jobCreated(request.getJob(), createdTime,
                            ProcessRun.builder().taskId("test-task")
                                    .startedStatus(compactionStartedStatus(startTime))
                                    .finishedStatus(compactionFinishedStatus(summary))
                                    .statusUpdate(compactionCommittedStatus(commitTime))
                                    .build()));
        }

        @Test
        void shouldFailWhenInputFileDoesNotExist() throws Exception {
            // Given
            StateStore stateStore = createTableGetStateStore("test-table");
            Instant createdTime = Instant.parse("2024-06-14T15:34:00Z");
            Instant startTime = Instant.parse("2024-06-14T15:35:00Z");
            Instant finishTime = Instant.parse("2024-06-14T15:37:00Z");
            Instant failedTime = Instant.parse("2024-06-14T15:38:00Z");
            RecordsProcessedSummary summary = summary(startTime, finishTime, 123, 123);
            CompactionJobCommitRequest request = createFinishedCompactionForTable("test-table", createdTime, startTime, summary);
            stateStore.clearFileData();

            // When
            StateStoreCommitter committer = committerWithTimes(List.of(failedTime));
            apply(committer, StateStoreCommitRequest.forCompactionJob(request));

            // Then
            assertThat(failures).singleElement().satisfies(e -> assertThat(e)
                    .isInstanceOf(RuntimeException.class)
                    .cause().isInstanceOf(ReplaceRequestsFailedException.class)
                    .cause().isInstanceOf(FileNotFoundException.class));
            assertThat(stateStore.getFileReferences()).isEmpty();
            assertThat(compactionJobStatusStore.getAllJobs("test-table")).containsExactly(
                    jobCreated(request.getJob(), createdTime,
                            ProcessRun.builder().taskId("test-task")
                                    .startedStatus(compactionStartedStatus(startTime))
                                    .statusUpdate(compactionFinishedStatus(summary))
                                    .finishedStatus(compactionFailedStatus(
                                            new ProcessRunTime(finishTime, failedTime),
                                            List.of("1 replace file reference requests failed to update the state store", "File not found: input.parquet")))
                                    .build()));
        }

        @Test
        void shouldFailWhenStateStoreThrowsUnexpectedRuntimeException() throws Exception {
            // Given
            StateStore stateStore = mock(StateStore.class);
            createTable("test-table", stateStore);
            Instant createdTime = Instant.parse("2024-06-14T15:34:00Z");
            Instant startTime = Instant.parse("2024-06-14T15:35:00Z");
            Instant finishTime = Instant.parse("2024-06-14T15:37:00Z");
            Instant failedTime = Instant.parse("2024-06-14T15:38:00Z");
            RecordsProcessedSummary summary = summary(startTime, finishTime, 123, 123);
            CompactionJobCommitRequest request = createFinishedCompactionForTable("test-table", createdTime, startTime, summary);
            RuntimeException failure = new RuntimeException("Unexpected failure");
            doThrow(failure).when(stateStore).atomicallyReplaceFileReferencesWithNewOnes(List.of(
                    CompactionJobCommitter.replaceFileReferencesRequest(request.getJob(), request.getRecordsWritten())));

            // When
            apply(committerWithTimes(List.of(failedTime)),
                    StateStoreCommitRequest.forCompactionJob(request));

            // Then
            assertThat(failures).singleElement().isSameAs(failure);
            assertThat(compactionJobStatusStore.getAllJobs("test-table")).containsExactly(
                    jobCreated(request.getJob(), createdTime,
                            ProcessRun.builder().taskId("test-task")
                                    .startedStatus(compactionStartedStatus(startTime))
                                    .statusUpdate(compactionFinishedStatus(summary))
                                    .finishedStatus(compactionFailedStatus(
                                            new ProcessRunTime(finishTime, failedTime),
                                            List.of("Unexpected failure")))
                                    .build()));
        }

        @Test
        void shouldFailWhenStateStoreThrowsUnexpectedStateStoreException() throws Exception {
            // Given
            StateStore stateStore = mock(StateStore.class);
            createTable("test-table", stateStore);
            Instant createdTime = Instant.parse("2024-06-14T15:34:00Z");
            Instant startTime = Instant.parse("2024-06-14T15:35:00Z");
            Instant finishTime = Instant.parse("2024-06-14T15:37:00Z");
            Instant failedTime = Instant.parse("2024-06-14T15:38:00Z");
            RecordsProcessedSummary summary = summary(startTime, finishTime, 123, 123);
            CompactionJobCommitRequest request = createFinishedCompactionForTable("test-table", createdTime, startTime, summary);
            ReplaceFileReferencesRequest expectedRequest = CompactionJobCommitter.replaceFileReferencesRequest(request.getJob(), request.getRecordsWritten());
            RuntimeException cause = new RuntimeException("Unexpected failure");
            ReplaceRequestsFailedException failure = new ReplaceRequestsFailedException(List.of(expectedRequest), cause);
            doThrow(failure).when(stateStore).atomicallyReplaceFileReferencesWithNewOnes(List.of(expectedRequest));

            // When
            apply(committerWithTimes(List.of(failedTime)),
                    StateStoreCommitRequest.forCompactionJob(request));

            // Then
            assertThat(failures).singleElement().satisfies(e -> assertThat(e)
                    .isInstanceOf(RuntimeException.class)
                    .cause().isSameAs(failure));
            assertThat(compactionJobStatusStore.getAllJobs("test-table")).containsExactly(
                    jobCreated(request.getJob(), createdTime,
                            ProcessRun.builder().taskId("test-task")
                                    .startedStatus(compactionStartedStatus(startTime))
                                    .statusUpdate(compactionFinishedStatus(summary))
                                    .finishedStatus(compactionFailedStatus(
                                            new ProcessRunTime(finishTime, failedTime),
                                            List.of("1 replace file reference requests failed to update the state store", "Unexpected failure")))
                                    .build()));
        }
    }

    @Nested
    @DisplayName("Assign job ID to files")
    class AssignJobIdToFiles {
        @Test
        void shouldApplyCompactionJobIdAssignmentCommitRequest() throws Exception {
            // Given
            StateStore stateStore = createTableGetStateStore("test-table");
            FileReference inputFile = fileFactory.rootFile("input.parquet", 123L);
            stateStore.addFile(inputFile);

            // When
            CompactionJobIdAssignmentCommitRequest request = new CompactionJobIdAssignmentCommitRequest(List.of(
                    assignJobOnPartitionToFiles("job1", "root", List.of("input.parquet"))), "test-table");
            apply(StateStoreCommitRequest.forCompactionJobIdAssignment(request));

            // Then
            assertThat(stateStore.getFileReferences()).containsExactly(
                    withJobId("job1", fileFactory.rootFile("input.parquet", 123L)));
        }

        @Test
        void shouldFailToApplyJobIdAssignmentIfFileReferenceIsAlreadyAssignedToJob() throws Exception {
            // Given
            StateStore stateStore = createTableGetStateStore("test-table");
            FileReference inputFile = fileFactory.rootFile("input.parquet", 123L);
            stateStore.addFile(inputFile);
            stateStore.assignJobIds(List.of(assignJobOnPartitionToFiles("job1", "root", List.of("input.parquet"))));

            // When
            CompactionJobIdAssignmentCommitRequest request = new CompactionJobIdAssignmentCommitRequest(List.of(
                    assignJobOnPartitionToFiles("job2", "root", List.of("input.parquet"))), "test-table");
            apply(StateStoreCommitRequest.forCompactionJobIdAssignment(request));

            // Then
            assertThat(failures).singleElement().satisfies(e -> assertThat(e)
                    .isInstanceOf(RuntimeException.class)
                    .cause().isInstanceOf(FileReferenceAssignedToJobException.class));
            assertThat(stateStore.getFileReferences()).containsExactly(
                    withJobId("job1", fileFactory.rootFile("input.parquet", 123L)));
        }

        @Test
        void shouldFailToApplyJobIdAssignmentIfFileReferenceDoesNotExist() throws Exception {
            // Given
            StateStore stateStore = createTableGetStateStore("test-table");

            // When
            CompactionJobIdAssignmentCommitRequest request = new CompactionJobIdAssignmentCommitRequest(List.of(
                    assignJobOnPartitionToFiles("job2", "root", List.of("input.parquet"))), "test-table");
            apply(StateStoreCommitRequest.forCompactionJobIdAssignment(request));

            // Then
            assertThat(failures).singleElement().satisfies(e -> assertThat(e)
                    .isInstanceOf(RuntimeException.class)
                    .cause().isInstanceOf(FileReferenceNotFoundException.class));
            assertThat(stateStore.getFileReferences()).isEmpty();
        }
    }

    @Nested
    @DisplayName("Add files during ingest")
    class AddFiles {

        @Test
        void shouldApplyIngestJobAddFilesCommitRequest() throws Exception {
            // Given we have a commit request during an ingest job, which may still be in progress
            StateStore stateStore = createTableGetStateStore("test-table");
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
            apply(StateStoreCommitRequest.forIngestAddFiles(commitRequest));

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
            StateStore stateStore = createTableGetStateStore("test-table");
            FileReference outputFile = fileFactory.rootFile("output.parquet", 123L);
            IngestAddFilesCommitRequest commitRequest = IngestAddFilesCommitRequest.builder()
                    .tableId("test-table")
                    .fileReferences(List.of(outputFile))
                    .build();

            // When
            apply(StateStoreCommitRequest.forIngestAddFiles(commitRequest));

            // Then
            assertThat(stateStore.getFileReferences()).containsExactly(outputFile);
            assertThat(ingestJobStatusStore.getAllJobs("test-table")).isEmpty();
        }
    }

    @Nested
    @DisplayName("Split a partition to create child partitions")
    class SplitPartition {

        @Test
        void shouldApplySplitPartitionRequest() throws Exception {
            // Given
            PartitionTree afterTree = new PartitionsBuilder(schema)
                    .rootFirst("root")
                    .splitToNewChildren("root", "left", "right", "aaa")
                    .buildTree();
            StateStore stateStore = createTableGetStateStore("test-table");
            SplitPartitionCommitRequest commitRequest = new SplitPartitionCommitRequest("test-table",
                    afterTree.getPartition("root"),
                    afterTree.getPartition("left"),
                    afterTree.getPartition("right"));

            // When
            apply(StateStoreCommitRequest.forSplitPartition(commitRequest));

            // Then
            assertThat(stateStore.getAllPartitions())
                    .containsExactlyInAnyOrderElementsOf(afterTree.getAllPartitions());
        }

        @Test
        void shouldFailWhenPartitionHasAlreadyBeenSplit() throws Exception {
            // Given
            PartitionTree afterTree = new PartitionsBuilder(schema)
                    .rootFirst("root")
                    .splitToNewChildren("root", "left", "right", "aaa")
                    .buildTree();
            StateStore stateStore = createTableGetStateStore("test-table");
            stateStore.atomicallyUpdatePartitionAndCreateNewOnes(
                    afterTree.getPartition("root"),
                    afterTree.getPartition("left"),
                    afterTree.getPartition("right"));
            SplitPartitionCommitRequest commitRequest = new SplitPartitionCommitRequest("test-table",
                    afterTree.getPartition("root"),
                    afterTree.getPartition("left"),
                    afterTree.getPartition("right"));

            // When
            apply(StateStoreCommitRequest.forSplitPartition(commitRequest));

            // Then
            assertThat(failures).singleElement().satisfies(e -> assertThat(e)
                    .isInstanceOf(RuntimeException.class)
                    .cause().isInstanceOf(StateStoreException.class));
        }
    }

    @Nested
    @DisplayName("Commit when garbage collector deleted files")
    class CommitGC {

        @Test
        void shouldApplyDeletedFilesRequest() throws Exception {
            // Given two files have been replaced by a compaction job
            StateStore stateStore = createTableGetStateStore("test-table");
            stateStore.addFiles(List.of(
                    fileFactory.rootFile("file1.parquet", 100),
                    fileFactory.rootFile("file2.parquet", 200)));
            List<String> filenames = List.of("file1.parquet", "file2.parquet");
            stateStore.assignJobIds(List.of(
                    assignJobOnPartitionToFiles("test-job", "root", filenames)));
            FileReference fileAfterCompaction = fileFactory.rootFile("after.parquet", 300);
            stateStore.atomicallyReplaceFileReferencesWithNewOnes(List.of(
                    replaceJobFileReferences("test-job", "root", filenames, fileAfterCompaction)));
            // And we have a request to commit that they have been deleted
            GarbageCollectionCommitRequest request = new GarbageCollectionCommitRequest("test-table", filenames);

            // When
            apply(StateStoreCommitRequest.forGarbageCollection(request));

            // Then
            assertThat(stateStore.getAllFilesWithMaxUnreferenced(3))
                    .isEqualTo(activeAndReadyForGCFiles(List.of(fileAfterCompaction), List.of()));
        }

        @Test
        void shouldFailWhenFileStillHasAReference() throws Exception {
            // Given
            StateStore stateStore = createTableGetStateStore("test-table");
            stateStore.addFiles(List.of(fileFactory.rootFile("file.parquet", 100)));
            GarbageCollectionCommitRequest request = new GarbageCollectionCommitRequest(
                    "test-table", List.of("file.parquet"));

            // When
            apply(StateStoreCommitRequest.forGarbageCollection(request));

            // Then
            assertThat(failures).singleElement().satisfies(e -> assertThat(e)
                    .isInstanceOf(RuntimeException.class)
                    .cause().isInstanceOf(FileHasReferencesException.class));
        }
    }

    @Nested
    @DisplayName("Apply batches of requests")
    class BatchRequests {

        @Test
        void shouldTrackFailedRequest() throws Exception {
            // Given
            StateStore stateStore = createTableGetStateStore("test-table");
            FileReference file = fileFactory.rootFile("test.parquet", 123L);
            stateStore.addFile(file);
            FileReference duplicate = fileFactory.rootFile("test.parquet", 123L);
            StateStoreCommitRequest commitRequest = addFilesRequest("test-table", duplicate);

            // When
            apply(commitRequest);

            // Then
            assertThat(stateStore.getFileReferences()).containsExactly(file);
            assertThat(failedRequests).containsExactly(commitRequest);
        }

        @Test
        void shouldFailSomeCommitsInBatch() throws Exception {
            // Given
            StateStore stateStore = createTableGetStateStore("test-table");
            FileReference file1 = fileFactory.rootFile("file-1.parquet", 100);
            FileReference duplicate1 = fileFactory.rootFile("file-1.parquet", 200);
            FileReference file2 = fileFactory.rootFile("file-2.parquet", 300);
            FileReference duplicate2 = fileFactory.rootFile("file-2.parquet", 400);
            StateStoreCommitRequest commitRequest1 = addFilesRequest("test-table", file1);
            StateStoreCommitRequest commitRequest2 = addFilesRequest("test-table", duplicate1);
            StateStoreCommitRequest commitRequest3 = addFilesRequest("test-table", file2);
            StateStoreCommitRequest commitRequest4 = addFilesRequest("test-table", duplicate2);

            // When
            apply(commitRequest1, commitRequest2, commitRequest3, commitRequest4);

            // Then
            assertThat(stateStore.getFileReferences()).containsExactly(file1, file2);
            assertThat(failedRequests).containsExactly(commitRequest2, commitRequest4);
        }
    }

    @Nested
    @DisplayName("Configure when to update from the transaction log")
    class ConfigureUpdateFromLog {

        @Test
        void shouldFailFirstAddTransactionWhenItConflictsAndConfiguredToOnlyUpdateOnFailedCommit() throws Exception {
            // Given
            TableProperties tableProperties = createTable();
            tableProperties.set(STATESTORE_COMMITTER_UPDATE_ON_EVERY_COMMIT, "false");
            tableProperties.set(STATESTORE_COMMITTER_UPDATE_ON_EVERY_BATCH, "false");
            FileReference file1 = fileFactory.rootFile("file-1.parquet", 100);
            FileReference file2 = fileFactory.rootFile("file-2.parquet", 200);
            FileReference file3 = fileFactory.rootFile("file-3.parquet", 300);
            stateStore(tableProperties).addFile(file1);
            StateStoreCommitRequest commitRequest1 = addFilesRequest(tableProperties, file2);
            StateStoreCommitRequest commitRequest2 = addFilesRequest(tableProperties, file3);
            AtomicInteger addTransactionCalls = new AtomicInteger();
            AtomicInteger readTransactionCalls = new AtomicInteger();
            filesLog(tableProperties).atStartOfAddTransaction(() -> addTransactionCalls.incrementAndGet());
            filesLog(tableProperties).atStartOfReadTransactions(() -> readTransactionCalls.incrementAndGet());

            // When
            apply(commitRequest1, commitRequest2);

            // Then
            assertThat(failedRequests).isEmpty();
            assertThat(addTransactionCalls.get()).isEqualTo(3);
            assertThat(readTransactionCalls.get()).isEqualTo(1);
            assertThat(stateStore(tableProperties).getFileReferences())
                    .containsExactly(file1, file2, file3);
            assertThat(retryWaits).isEmpty();
        }

        @Test
        void shouldSucceedFirstAddTransactionWhenItConflictsAndLambdaIsSetToUpdateOnEveryCommit() throws Exception {
            // Given
            TableProperties tableProperties = createTable();
            tableProperties.set(STATESTORE_COMMITTER_UPDATE_ON_EVERY_COMMIT, "true");
            tableProperties.set(STATESTORE_COMMITTER_UPDATE_ON_EVERY_BATCH, "false");
            FileReference file1 = fileFactory.rootFile("file-1.parquet", 100);
            FileReference file2 = fileFactory.rootFile("file-2.parquet", 200);
            FileReference file3 = fileFactory.rootFile("file-3.parquet", 300);
            stateStore(tableProperties).addFile(file1);
            AtomicInteger addTransactionCalls = new AtomicInteger();
            AtomicInteger readTransactionCalls = new AtomicInteger();
            filesLog(tableProperties).atStartOfAddTransaction(() -> addTransactionCalls.incrementAndGet());
            filesLog(tableProperties).atStartOfReadTransactions(() -> readTransactionCalls.incrementAndGet());
            StateStoreCommitRequest commitRequest1 = addFilesRequest(tableProperties, file2);
            StateStoreCommitRequest commitRequest2 = addFilesRequest(tableProperties, file3);

            // When
            apply(commitRequest1, commitRequest2);

            // Then
            assertThat(failedRequests).isEmpty();
            assertThat(addTransactionCalls.get()).isEqualTo(2);
            assertThat(readTransactionCalls.get()).isEqualTo(2);
            assertThat(stateStore(tableProperties).getFileReferences())
                    .containsExactly(file1, file2, file3);
            assertThat(retryWaits).isEmpty();
        }

        @Test
        void shouldSucceedFirstAddTransactionWhenItConflictsAndLambdaIsSetToUpdateOnEveryBatch() throws Exception {
            // Given
            TableProperties tableProperties = createTable();
            tableProperties.set(STATESTORE_COMMITTER_UPDATE_ON_EVERY_COMMIT, "false");
            tableProperties.set(STATESTORE_COMMITTER_UPDATE_ON_EVERY_BATCH, "true");
            FileReference file1 = fileFactory.rootFile("file-1.parquet", 100);
            FileReference file2 = fileFactory.rootFile("file-2.parquet", 200);
            FileReference file3 = fileFactory.rootFile("file-3.parquet", 300);
            stateStore(tableProperties).addFile(file1);
            AtomicInteger addTransactionCalls = new AtomicInteger();
            AtomicInteger readTransactionCalls = new AtomicInteger();
            filesLog(tableProperties).atStartOfAddTransaction(() -> addTransactionCalls.incrementAndGet());
            filesLog(tableProperties).atStartOfReadTransactions(() -> readTransactionCalls.incrementAndGet());
            StateStoreCommitRequest commitRequest1 = addFilesRequest(tableProperties, file2);
            StateStoreCommitRequest commitRequest2 = addFilesRequest(tableProperties, file3);

            // When
            apply(commitRequest1, commitRequest2);

            // Then
            assertThat(failedRequests).isEmpty();
            assertThat(addTransactionCalls.get()).isEqualTo(2);
            assertThat(readTransactionCalls.get()).isEqualTo(1);
            assertThat(stateStore(tableProperties).getFileReferences())
                    .containsExactly(file1, file2, file3);
            assertThat(retryWaits).isEmpty();
        }
    }

    private void apply(StateStoreCommitRequest... requests) {
        apply(committer(), requests);
    }

    private void apply(StateStoreCommitter committer, StateStoreCommitRequest... requests) {
        committer.applyBatch(PollWithRetries.noRetries(),
                Stream.of(requests)
                        .map(this::message)
                        .collect(toUnmodifiableList()));
    }

    private StateStoreCommitter committer() {
        return committerWithTimes(Instant::now);
    }

    private StateStoreCommitter committerWithTimes(Collection<Instant> times) {
        return committerWithTimes(times.iterator()::next);
    }

    private StateStoreCommitter committerWithTimes(Supplier<Instant> timeSupplier) {
        return new StateStoreCommitter(compactionJobStatusStore, ingestJobStatusStore,
                new FixedTablePropertiesProvider(propertiesByTableId.values()),
                new StateStoreProvider(instanceProperties, this::stateStoreForCommitter),
                timeSupplier);
    }

    private StateStore createTableGetStateStore(String tableId) {
        return stateStore(createTable(tableId));
    }

    private void createTable(String tableId, StateStore stateStore) {
        createTable(tableId);
        stateStoreByTableId.put(tableId, stateStore);
    }

    private TableProperties createTable(String tableId) {
        TableProperties tableProperties = createTestTableProperties(instanceProperties, schema);
        tableProperties.set(TABLE_ID, tableId);
        createTable(tableProperties);
        return tableProperties;
    }

    private TableProperties createTable() {
        TableProperties tableProperties = createTestTableProperties(instanceProperties, schema);
        createTable(tableProperties);
        return tableProperties;
    }

    private void createTable(TableProperties tableProperties) {
        propertiesByTableId.put(tableProperties.get(TABLE_ID), tableProperties);
        transactionLogsByTableId.put(tableProperties.get(TABLE_ID), new InMemoryTransactionLogs());
        try {
            stateStore(tableProperties).initialise(partitions.getAllPartitions());
        } catch (StateStoreException e) {
            throw new RuntimeException(e);
        }
    }

    private StateStore stateStore(TableProperties tableProperties) {
        return stateStoreWithConfig(tableProperties, builder -> {
        });
    }

    private StateStore stateStoreForCommitter(TableProperties tableProperties) {
        return stateStoreWithConfig(tableProperties,
                builder -> StateStoreFactory.forCommitterProcess(true, tableProperties, builder));
    }

    private StateStore stateStoreWithConfig(TableProperties tableProperties, Consumer<TransactionLogStateStore.Builder> config) {
        String tableId = tableProperties.get(TABLE_ID);
        StateStore fixedStateStore = stateStoreByTableId.get(tableId);
        if (fixedStateStore != null) {
            return fixedStateStore;
        }
        TransactionLogStateStore.Builder builder = transactionLogsByTableId.get(tableId).stateStoreBuilder(
                tableProperties.getStatus(), tableProperties.getSchema(), recordWaits(retryWaits));
        config.accept(builder);
        StateStore stateStore = builder.build();
        stateStore.fixFileUpdateTime(DEFAULT_FILE_UPDATE_TIME);
        return stateStore;
    }

    private InMemoryTransactionLogStore filesLog(TableProperties tableProperties) {
        String tableId = tableProperties.get(TABLE_ID);
        return transactionLogsByTableId.get(tableId).getFilesLogStore();
    }

    private StateStore stateStore(String tableId) {
        return stateStore(propertiesByTableId.get(tableId));
    }

    private CompactionJobCommitRequest createFinishedCompactionForTable(
            String tableId, Instant createTime, Instant startTime, RecordsProcessedSummary summary) throws Exception {
        CompactionJob job = CompactionJob.builder()
                .tableId(tableId)
                .jobId(UUID.randomUUID().toString())
                .inputFiles(List.of("input.parquet"))
                .outputFile("output.parquet")
                .partitionId("root")
                .build();
        StateStore stateStore = stateStore(tableId);
        stateStore.addFile(fileFactory.rootFile("input.parquet", 123L));
        stateStore.assignJobIds(List.of(assignJobOnPartitionToFiles(
                job.getId(), "root", List.of("input.parquet"))));
        compactionJobStatusStore.jobCreated(job, createTime);
        compactionJobStatusStore.jobStarted(compactionJobStarted(job, startTime)
                .taskId("test-task").jobRunId("test-job-run").build());
        compactionJobStatusStore.jobFinished(compactionJobFinished(job, summary)
                .taskId("test-task").jobRunId("test-job-run").build());
        return new CompactionJobCommitRequest(job, "test-task", "test-job-run", summary);
    }

    private RequestHandle message(StateStoreCommitRequest request) {
        return RequestHandle.withCallbackOnFail(request, exception -> {
            failedRequests.add(request);
            failures.add(exception);
        });
    }

    private StateStoreCommitRequest addFilesRequest(TableProperties tableProperties, FileReference... files) {
        return addFilesRequest(tableProperties.get(TABLE_ID), files);
    }

    private StateStoreCommitRequest addFilesRequest(String tableId, FileReference... files) {
        IngestAddFilesCommitRequest request = IngestAddFilesCommitRequest.builder()
                .tableId(tableId)
                .fileReferences(List.of(files))
                .build();
        return StateStoreCommitRequest.forIngestAddFiles(request);
    }
}
