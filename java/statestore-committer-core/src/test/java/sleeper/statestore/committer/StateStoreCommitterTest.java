/*
 * Copyright 2022-2025 Crown Copyright
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

import sleeper.compaction.core.job.CompactionJob;
import sleeper.compaction.core.job.CompactionJobFactory;
import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.testutils.FixedTablePropertiesProvider;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.StringType;
import sleeper.core.statestore.AllReferencesToAFile;
import sleeper.core.statestore.AssignJobIdRequest;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.statestore.FilesReportTestHelper;
import sleeper.core.statestore.ReplaceFileReferencesRequest;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;
import sleeper.core.statestore.StateStoreProvider;
import sleeper.core.statestore.commit.StateStoreCommitRequest;
import sleeper.core.statestore.exception.FileHasReferencesException;
import sleeper.core.statestore.exception.FileReferenceAssignedToJobException;
import sleeper.core.statestore.exception.FileReferenceNotFoundException;
import sleeper.core.statestore.testutils.InMemoryTransactionLogStore;
import sleeper.core.statestore.testutils.InMemoryTransactionLogsPerTable;
import sleeper.core.statestore.transactionlog.TransactionLogStateStore;
import sleeper.core.statestore.transactionlog.log.TransactionBodyStore;
import sleeper.core.statestore.transactionlog.transaction.FileReferenceTransaction;
import sleeper.core.statestore.transactionlog.transaction.PartitionTransaction;
import sleeper.core.statestore.transactionlog.transaction.TransactionType;
import sleeper.core.statestore.transactionlog.transaction.impl.AddFilesTransaction;
import sleeper.core.statestore.transactionlog.transaction.impl.AssignJobIdsTransaction;
import sleeper.core.statestore.transactionlog.transaction.impl.DeleteFilesTransaction;
import sleeper.core.statestore.transactionlog.transaction.impl.ReplaceFileReferencesTransaction;
import sleeper.core.statestore.transactionlog.transaction.impl.SplitPartitionTransaction;
import sleeper.core.tracker.job.run.JobRunSummary;
import sleeper.statestore.StateStoreFactory;
import sleeper.statestore.committer.StateStoreCommitter.RequestHandle;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toUnmodifiableList;
import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.table.TableProperty.STATESTORE_COMMITTER_UPDATE_ON_EVERY_BATCH;
import static sleeper.core.properties.table.TableProperty.STATESTORE_COMMITTER_UPDATE_ON_EVERY_COMMIT;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;
import static sleeper.core.statestore.AssignJobIdRequest.assignJobOnPartitionToFiles;
import static sleeper.core.statestore.FileReferenceTestData.withJobId;
import static sleeper.core.statestore.FilesReportTestHelper.activeAndReadyForGCFiles;
import static sleeper.core.statestore.ReplaceFileReferencesRequest.replaceJobFileReferences;
import static sleeper.core.statestore.testutils.StateStoreUpdatesWrapper.update;
import static sleeper.core.tracker.job.run.JobRunSummaryTestHelper.summary;

public class StateStoreCommitterTest {
    private static final Instant DEFAULT_FILE_UPDATE_TIME = FilesReportTestHelper.DEFAULT_UPDATE_TIME;
    private final Schema schema = schemaWithKey("key", new StringType());
    private final PartitionTree partitions = new PartitionsBuilder(schema).singlePartition("root").buildTree();
    private final FileReferenceFactory fileFactory = FileReferenceFactory.fromUpdatedAt(partitions, DEFAULT_FILE_UPDATE_TIME);
    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final Map<String, TableProperties> propertiesByTableId = new LinkedHashMap<>();
    private final Map<String, StateStore> stateStoreByTableId = new LinkedHashMap<>();
    private final InMemoryTransactionLogsPerTable transactionLogs = new InMemoryTransactionLogsPerTable();
    private final List<StateStoreCommitRequest> failedRequests = new ArrayList<>();
    private final List<Exception> failures = new ArrayList<>();

    @Nested
    @DisplayName("Commit a compaction job")
    class CommitCompaction {

        @Test
        void shouldApplyCompactionCommitRequest() throws Exception {
            // Given
            StateStore stateStore = createTableGetStateStore("test-table");
            Instant createdTime = Instant.parse("2024-06-14T15:34:00Z");
            Instant startTime = Instant.parse("2024-06-14T15:35:00Z");
            Instant finishTime = Instant.parse("2024-06-14T15:37:00Z");
            JobRunSummary summary = summary(startTime, finishTime, 123, 123);
            FileReference inputFile = fileFactory.rootFile("input.parquet", 123L);
            update(stateStore).addFile(inputFile);
            CompactionJob job = compactionFactoryForTable("test-table").createCompactionJob(List.of(inputFile), "root");
            StateStoreCommitRequest request = createAndFinishCompaction(job, createdTime, startTime, summary);

            // When
            apply(request);

            // Then
            assertThat(stateStore.getFileReferences()).containsExactly(
                    fileFactory.rootFile(job.getOutputFile(), 123L));
        }

        @Test
        void shouldFailWhenInputFileDoesNotExist() throws Exception {
            // Given
            StateStore stateStore = createTableGetStateStore("test-table");
            Instant createdTime = Instant.parse("2024-06-14T15:34:00Z");
            Instant startTime = Instant.parse("2024-06-14T15:35:00Z");
            Instant finishTime = Instant.parse("2024-06-14T15:37:00Z");
            JobRunSummary summary = summary(startTime, finishTime, 123, 123);
            FileReference inputFile = fileFactory.rootFile("input.parquet", 123L);
            update(stateStore).addFile(inputFile);
            CompactionJob job = compactionFactoryForTable("test-table").createCompactionJob(List.of(inputFile), "root");
            StateStoreCommitRequest request = createAndFinishCompaction(job, createdTime, startTime, summary);
            update(stateStore).clearFileData();

            // When
            apply(request);

            // Then
            assertThat(failures).isEmpty();
            assertThat(stateStore.getFileReferences()).isEmpty();
        }

        @Test
        void shouldFailWhenStateStoreThrowsUnexpectedException() throws Exception {
            // Given
            StateStore stateStore = createTableGetStateStore("test-table");
            Instant createdTime = Instant.parse("2024-06-14T15:34:00Z");
            Instant startTime = Instant.parse("2024-06-14T15:35:00Z");
            Instant finishTime = Instant.parse("2024-06-14T15:37:00Z");
            JobRunSummary summary = summary(startTime, finishTime, 123, 123);
            FileReference inputFile = fileFactory.rootFile("input.parquet", 123L);
            update(stateStore).addFile(inputFile);
            CompactionJob job = compactionFactoryForTable("test-table").createCompactionJob(List.of(inputFile), "root");
            StateStoreCommitRequest request = createAndFinishCompaction(job, createdTime, startTime, summary);
            RuntimeException failure = new RuntimeException("Unexpected failure");
            InMemoryTransactionLogStore filesLog = transactionLogs.forTableId("test-table").getFilesLogStore();
            filesLog.atStartOfAddTransaction(() -> {
                throw failure;
            });

            // When
            apply(request);

            // Then
            assertThat(failures).singleElement()
                    .isInstanceOf(StateStoreException.class)
                    .extracting(Exception::getCause)
                    .isSameAs(failure);
            assertThat(stateStore.getFileReferences())
                    .containsExactly(withJobId(job.getId(), inputFile));
        }
    }

    @Nested
    @DisplayName("Assign job ID to files")
    class AssignJobIdToFiles {
        @Test
        void shouldApplyCompactionJobIdAssignmentCommitRequest() throws Exception {
            // Given
            StateStore stateStore = createTableGetStateStore("test-table");
            update(stateStore).addFile(fileFactory.rootFile("input.parquet", 123L));

            // When
            apply(StateStoreCommitRequest.create("test-table", new AssignJobIdsTransaction(
                    List.of(assignJobOnPartitionToFiles("test-job", "root", List.of("input.parquet"))))));

            // Then
            assertThat(stateStore.getFileReferences()).containsExactly(
                    withJobId("test-job", fileFactory.rootFile("input.parquet", 123L)));
        }

        @Test
        void shouldFailToApplyJobIdAssignmentIfFileReferenceIsAlreadyAssignedToJob() throws Exception {
            // Given
            StateStore stateStore = createTableGetStateStore("test-table");
            update(stateStore).addFile(fileFactory.rootFile("input.parquet", 123L));
            update(stateStore).assignJobIds(List.of(assignJobOnPartitionToFiles("job1", "root", List.of("input.parquet"))));

            // When
            apply(StateStoreCommitRequest.create("test-table", new AssignJobIdsTransaction(
                    List.of(assignJobOnPartitionToFiles("job2", "root", List.of("input.parquet"))))));

            // Then
            assertThat(failures).singleElement().satisfies(e -> assertThat(e)
                    .isInstanceOf(FileReferenceAssignedToJobException.class));
            assertThat(stateStore.getFileReferences()).containsExactly(
                    withJobId("job1", fileFactory.rootFile("input.parquet", 123L)));
        }

        @Test
        void shouldFailToApplyJobIdAssignmentIfFileReferenceDoesNotExist() throws Exception {
            // Given
            StateStore stateStore = createTableGetStateStore("test-table");

            // When
            apply(StateStoreCommitRequest.create("test-table", new AssignJobIdsTransaction(
                    List.of(assignJobOnPartitionToFiles("test-job", "root", List.of("input.parquet"))))));

            // Then
            assertThat(failures).singleElement().satisfies(e -> assertThat(e)
                    .isInstanceOf(FileReferenceNotFoundException.class));
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
            AddFilesTransaction transaction = AddFilesTransaction.builder()
                    .jobId("test-job")
                    .taskId("test-task-id")
                    .jobRunId("test-job-run-id")
                    .writtenTime(Instant.parse("2024-06-20T14:55:01Z"))
                    .files(AllReferencesToAFile.newFilesWithReferences(List.of(outputFile)))
                    .build();

            // When
            apply(StateStoreCommitRequest.create("test-table", transaction));

            // Then
            assertThat(stateStore.getFileReferences()).containsExactly(outputFile);
        }

        @Test
        void shouldApplyIngestStreamAddFilesCommitRequest() throws Exception {
            // Given we have a commit request without an ingest job (e.g. from an endless stream of records)
            StateStore stateStore = createTableGetStateStore("test-table");
            FileReference outputFile = fileFactory.rootFile("output.parquet", 123L);
            AddFilesTransaction transaction = AddFilesTransaction.fromReferences(List.of(outputFile));

            // When
            apply(StateStoreCommitRequest.create("test-table", transaction));

            // Then
            assertThat(stateStore.getFileReferences()).containsExactly(outputFile);
        }

        @Test
        void shouldFailToApplyAddFilesWhenFileAlreadyExists() throws Exception {
            // Given we have a commit request during an ingest job, which may still be in progress
            StateStore stateStore = createTableGetStateStore("test-table");
            FileReference file = fileFactory.rootFile("output.parquet", 123L);
            update(stateStore).addFile(file);
            Instant writtenTime = Instant.parse("2024-06-20T14:55:01Z");
            AddFilesTransaction transaction = AddFilesTransaction.builder()
                    .jobId("test-job")
                    .taskId("test-task-id")
                    .jobRunId("test-job-run-id")
                    .writtenTime(writtenTime)
                    .files(AllReferencesToAFile.newFilesWithReferences(List.of(file)))
                    .build();

            // When
            apply(StateStoreCommitRequest.create("test-table", transaction));

            // Then
            assertThat(failures).isEmpty();
            assertThat(stateStore.getFileReferences()).containsExactly(file);
        }

        @Test
        void shouldFailToApplyAddFilesWhenStateStoreThrowsUnexpectedException() throws Exception {
            // Given we have a commit request during an ingest job, which may still be in progress
            StateStore stateStore = createTableGetStateStore("test-table");
            FileReference file = fileFactory.rootFile("output.parquet", 123L);
            AddFilesTransaction transaction = AddFilesTransaction.builder()
                    .jobId("test-job")
                    .taskId("test-task-id")
                    .jobRunId("test-job-run-id")
                    .writtenTime(Instant.parse("2024-06-20T14:55:01Z"))
                    .files(AllReferencesToAFile.newFilesWithReferences(List.of(file)))
                    .build();
            RuntimeException failure = new RuntimeException("Unexpected failure");
            InMemoryTransactionLogStore filesLog = transactionLogs.forTableId("test-table").getFilesLogStore();
            filesLog.atStartOfAddTransaction(() -> {
                throw failure;
            });

            // When
            apply(StateStoreCommitRequest.create("test-table", transaction));

            // Then
            assertThat(failures).singleElement().satisfies(e -> assertThat(e)
                    .isInstanceOf(StateStoreException.class)
                    .hasCause(failure));
            assertThat(stateStore.getFileReferences()).isEmpty();
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
            PartitionTransaction transaction = new SplitPartitionTransaction(afterTree.getPartition("root"),
                    List.of(afterTree.getPartition("left"), afterTree.getPartition("right")));

            // When
            apply(StateStoreCommitRequest.create("test-table", transaction));

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
            update(stateStore).atomicallyUpdatePartitionAndCreateNewOnes(
                    afterTree.getPartition("root"),
                    afterTree.getPartition("left"),
                    afterTree.getPartition("right"));
            PartitionTransaction transaction = new SplitPartitionTransaction(afterTree.getPartition("root"),
                    List.of(afterTree.getPartition("left"), afterTree.getPartition("right")));

            // When
            apply(StateStoreCommitRequest.create("test-table", transaction));

            // Then
            assertThat(failures).singleElement().satisfies(e -> assertThat(e)
                    .isInstanceOf(StateStoreException.class));
        }
    }

    @Nested
    @DisplayName("Commit when garbage collector deleted files")
    class CommitGC {

        @Test
        void shouldApplyDeletedFilesRequest() throws Exception {
            // Given two files have been replaced by a compaction job
            StateStore stateStore = createTableGetStateStore("test-table");
            update(stateStore).addFiles(List.of(
                    fileFactory.rootFile("file1.parquet", 100),
                    fileFactory.rootFile("file2.parquet", 200)));
            List<String> filenames = List.of("file1.parquet", "file2.parquet");
            update(stateStore).assignJobIds(List.of(
                    assignJobOnPartitionToFiles("test-job", "root", filenames)));
            FileReference fileAfterCompaction = fileFactory.rootFile("after.parquet", 300);
            update(stateStore).atomicallyReplaceFileReferencesWithNewOnes(List.of(
                    replaceJobFileReferences("test-job", filenames, fileAfterCompaction)));
            // And we have a request to commit that they have been deleted
            FileReferenceTransaction transaction = new DeleteFilesTransaction(filenames);

            // When
            apply(StateStoreCommitRequest.create("test-table", transaction));

            // Then
            assertThat(stateStore.getAllFilesWithMaxUnreferenced(3))
                    .isEqualTo(activeAndReadyForGCFiles(List.of(fileAfterCompaction), List.of()));
        }

        @Test
        void shouldFailWhenFileStillHasAReference() throws Exception {
            // Given
            StateStore stateStore = createTableGetStateStore("test-table");
            update(stateStore).addFiles(List.of(fileFactory.rootFile("file.parquet", 100)));
            FileReferenceTransaction transaction = new DeleteFilesTransaction(List.of("file.parquet"));

            // When
            apply(StateStoreCommitRequest.create("test-table", transaction));

            // Then
            assertThat(failures).singleElement().satisfies(e -> assertThat(e)
                    .isInstanceOf(FileHasReferencesException.class));
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
            update(stateStore).addFile(file);
            update(stateStore).assignJobId("job-1", "root", List.of("test.parquet"));
            StateStoreCommitRequest commitRequest = StateStoreCommitRequest.create(
                    "test-table", new AssignJobIdsTransaction(List.of(
                            assignJobOnPartitionToFiles("job-2", "root", List.of("test.parquet")))));

            // When
            apply(commitRequest);

            // Then
            assertThat(stateStore.getFileReferences()).containsExactly(withJobId("job-1", file));
            assertThat(failedRequests).containsExactly(commitRequest);
        }

        @Test
        void shouldFailSomeCommitsInBatch() throws Exception {
            // Given
            StateStore stateStore = createTableGetStateStore("test-table");
            FileReference file1 = fileFactory.rootFile("file-1.parquet", 100);
            FileReference file2 = fileFactory.rootFile("file-2.parquet", 300);
            StateStoreCommitRequest commitRequest1 = addFilesRequest("test-table", file1);
            StateStoreCommitRequest commitRequest2 = assignJobIdRequest("test-table", "job-1", "root", "file-1.parquet");
            StateStoreCommitRequest commitRequest3 = assignJobIdRequest("test-table", "job-2", "root", "file-1.parquet");
            StateStoreCommitRequest commitRequest4 = addFilesRequest("test-table", file2);
            StateStoreCommitRequest commitRequest5 = assignJobIdRequest("test-table", "job-3", "root", "file-1.parquet");

            // When
            apply(commitRequest1, commitRequest2, commitRequest3, commitRequest4, commitRequest5);

            // Then
            assertThat(stateStore.getFileReferences()).containsExactly(withJobId("job-1", file1), file2);
            assertThat(failedRequests).containsExactly(commitRequest3, commitRequest5);
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
            update(stateStore(tableProperties)).addFile(file1);
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
            assertThat(transactionLogs.getRetryWaits()).isEmpty();
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
            update(stateStore(tableProperties)).addFile(file1);
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
            assertThat(transactionLogs.getRetryWaits()).isEmpty();
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
            update(stateStore(tableProperties)).addFile(file1);
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
            assertThat(transactionLogs.getRetryWaits()).isEmpty();
        }
    }

    @Nested
    @DisplayName("Commit a transaction from S3")
    class CommitFromS3 {

        @Test
        void shouldApplyCompactionCommitByTransactionInS3() throws Exception {
            // Given
            StateStore stateStore = createTableGetStateStore("test-table");
            Instant createdTime = Instant.parse("2024-06-14T15:34:00Z");
            Instant startTime = Instant.parse("2024-06-14T15:35:00Z");
            Instant finishTime = Instant.parse("2024-06-14T15:37:00Z");
            JobRunSummary summary = summary(startTime, finishTime, 123, 123);
            FileReference inputFile = fileFactory.rootFile("input.parquet", 123L);
            update(stateStore).addFile(inputFile);
            CompactionJob job = compactionFactoryForTable("test-table").createCompactionJob(List.of(inputFile), "root");
            ReplaceFileReferencesTransaction transaction = createAndFinishCompactionAsTransaction(job, createdTime, startTime, summary);
            String bodyKey = TransactionBodyStore.createObjectKey("test-table", finishTime, "test-transaction");
            transactionLogs.getTransactionBodyStore().store(bodyKey, "test-table", transaction);
            StateStoreCommitRequest request = StateStoreCommitRequest.create("test-table", bodyKey, TransactionType.REPLACE_FILE_REFERENCES);

            // When
            apply(request);

            // Then
            assertThat(stateStore.getFileReferences()).containsExactly(
                    fileFactory.rootFile(job.getOutputFile(), 123L));
        }
    }

    private void apply(StateStoreCommitRequest... requests) {
        committer().applyBatch(operation -> operation.run(),
                Stream.of(requests)
                        .map(this::message)
                        .collect(toUnmodifiableList()));
    }

    private StateStoreCommitter committer() {
        return new StateStoreCommitter(
                new FixedTablePropertiesProvider(propertiesByTableId.values()),
                new StateStoreProvider(instanceProperties, this::stateStoreForCommitter),
                transactionLogs.getTransactionBodyStore());
    }

    private StateStore createTableGetStateStore(String tableId) {
        return stateStore(createTable(tableId));
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
        update(stateStore(tableProperties)).initialise(partitions.getAllPartitions());
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
        TransactionLogStateStore.Builder builder = transactionLogs.stateStoreBuilder(tableProperties);
        config.accept(builder);
        StateStore stateStore = builder.build();
        stateStore.fixFileUpdateTime(DEFAULT_FILE_UPDATE_TIME);
        return stateStore;
    }

    private InMemoryTransactionLogStore filesLog(TableProperties tableProperties) {
        return transactionLogs.forTable(tableProperties).getFilesLogStore();
    }

    private StateStore stateStore(String tableId) {
        return stateStore(propertiesByTableId.get(tableId));
    }

    private StateStoreCommitRequest createAndFinishCompaction(
            CompactionJob job, Instant createTime, Instant startTime, JobRunSummary summary) throws Exception {
        ReplaceFileReferencesTransaction transaction = createAndFinishCompactionAsTransaction(job, createTime, startTime, summary);
        return StateStoreCommitRequest.create(job.getTableId(), transaction);
    }

    private ReplaceFileReferencesTransaction createAndFinishCompactionAsTransaction(
            CompactionJob job, Instant createTime, Instant startTime, JobRunSummary summary) throws Exception {
        return new ReplaceFileReferencesTransaction(List.of(
                createAndFinishCompactionAsReplaceFileReferences(job, createTime, startTime, summary)));
    }

    private ReplaceFileReferencesRequest createAndFinishCompactionAsReplaceFileReferences(
            CompactionJob job, Instant createTime, Instant startTime, JobRunSummary summary) throws Exception {
        List<AssignJobIdRequest> assignIdRequests = List.of(assignJobOnPartitionToFiles(
                job.getId(), job.getPartitionId(), job.getInputFiles()));
        update(stateStore(job.getTableId())).assignJobIds(assignIdRequests);
        return job.replaceFileReferencesRequestBuilder(summary.getRecordsWritten())
                .taskId("test-task").jobRunId("test-job-run").build();
    }

    private CompactionJobFactory compactionFactoryForTable(String tableId) {
        return new CompactionJobFactory(instanceProperties, propertiesByTableId.get(tableId));
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
        return StateStoreCommitRequest.create(tableId,
                new AddFilesTransaction(AllReferencesToAFile.newFilesWithReferences(List.of(files))));
    }

    private StateStoreCommitRequest assignJobIdRequest(String tableId, String jobId, String partitionId, String... filenames) {
        return StateStoreCommitRequest.create(tableId,
                new AssignJobIdsTransaction(List.of(
                        assignJobOnPartitionToFiles(jobId, partitionId, List.of(filenames)))));
    }
}
