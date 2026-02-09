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
package sleeper.compaction.core.job.dispatch;

import org.junit.jupiter.api.Test;

import sleeper.compaction.core.job.CompactionJob;
import sleeper.compaction.core.job.CompactionJobFactory;
import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.testutils.FixedTablePropertiesProvider;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.testutils.FixedStateStoreProvider;
import sleeper.core.statestore.testutils.InMemoryTransactionLogStateStore;
import sleeper.core.statestore.testutils.InMemoryTransactionLogs;
import sleeper.core.tracker.compaction.job.InMemoryCompactionJobTracker;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.compaction.core.job.CompactionJobStatusFromJobTestData.compactionJobCreated;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.core.properties.table.TableProperty.COMPACTION_JOB_SEND_RETRY_DELAY_SECS;
import static sleeper.core.properties.table.TableProperty.COMPACTION_JOB_SEND_TIMEOUT_SECS;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.schema.SchemaTestHelper.createSchemaWithKey;
import static sleeper.core.statestore.testutils.StateStoreUpdatesWrapper.update;

public class CompactionJobDispatcherTest {

    Schema schema = createSchemaWithKey("key");
    InstanceProperties instanceProperties = createTestInstanceProperties();
    TableProperties tableProperties = createTestTableProperties(instanceProperties, schema);

    PartitionTree partitions = new PartitionsBuilder(schema).singlePartition("root").buildTree();
    StateStore stateStore = InMemoryTransactionLogStateStore
            .createAndInitialiseWithPartitions(partitions.getAllPartitions(), tableProperties,
                    new InMemoryTransactionLogs());
    InMemoryCompactionJobTracker tracker = new InMemoryCompactionJobTracker();
    FileReferenceFactory fileFactory = FileReferenceFactory.from(partitions);
    CompactionJobFactory compactionFactory = new CompactionJobFactory(instanceProperties, tableProperties);

    Map<String, List<CompactionJob>> s3PathToCompactionJobBatch = new HashMap<>();
    Queue<CompactionJob> compactionQueue = new LinkedList<>();
    Queue<BatchRequestMessage> delayedPendingQueue = new LinkedList<>();
    Queue<CompactionJobDispatchRequest> pendingDeadLetterQueue = new LinkedList<>();
    Map<String, RuntimeException> sendFailureByJobId = new HashMap<>();
    CompactionJobDispatcher.DeleteBatch deleteBatch = deleteBatch();

    @Test
    void shouldSendCompactionJobsInABatchWhenAllFilesAreAssigned() {

        // Given
        FileReference file1 = fileFactory.rootFile("test1.parquet", 1234);
        FileReference file2 = fileFactory.rootFile("test2.parquet", 5678);
        CompactionJob job1 = compactionFactory.createCompactionJob("test-job-1", List.of(file1), "root");
        CompactionJob job2 = compactionFactory.createCompactionJob("test-job-2", List.of(file2), "root");
        update(stateStore).addFiles(List.of(file1, file2));
        assignJobIds(List.of(job1, job2));

        CompactionJobDispatchRequest request = generateBatchRequest("test-batch");
        putCompactionJobBatch(request, List.of(job1, job2));
        tracker.setTimeSupplier(List.of(
                Instant.parse("2024-11-15T10:30:10Z"),
                Instant.parse("2024-11-15T10:30:11Z")).iterator()::next);

        // When
        dispatchWithNoRetry(request);

        // Then
        assertThat(compactionQueue).containsExactly(job1, job2);
        assertThat(tracker.getAllJobs(tableProperties.get(TABLE_ID))).containsExactly(
                compactionJobCreated(job2, Instant.parse("2024-11-15T10:30:11Z")),
                compactionJobCreated(job1, Instant.parse("2024-11-15T10:30:10Z")));
        assertThatNoBatchesAreRequeued();
        assertThatBatchFileIsDeleted();
    }

    @Test
    void shouldReturnBatchToTheQueueIfTheFilesForTheBatchAreUnassigned() {

        // Given
        FileReference file1 = fileFactory.rootFile("test1.parquet", 1234);
        FileReference file2 = fileFactory.rootFile("test2.parquet", 5678);
        CompactionJob job1 = compactionFactory.createCompactionJob("test-job-1", List.of(file1), "root");
        CompactionJob job2 = compactionFactory.createCompactionJob("test-job-2", List.of(file2), "root");
        update(stateStore).addFiles(List.of(file1, file2));

        tableProperties.setNumber(COMPACTION_JOB_SEND_TIMEOUT_SECS, 123);
        tableProperties.setNumber(COMPACTION_JOB_SEND_RETRY_DELAY_SECS, 12);
        CompactionJobDispatchRequest request = generateBatchRequestAtTime(
                "test-batch", Instant.parse("2024-11-15T10:20:00Z"));
        Instant retryTime = Instant.parse("2024-11-15T10:22:00Z");
        putCompactionJobBatch(request, List.of(job1, job2));

        // When
        dispatchWithTimeAtRetryCheck(request, retryTime);

        // Then
        assertThat(compactionQueue).isEmpty();
        assertThat(tracker.getAllJobs(tableProperties.get(TABLE_ID))).isEmpty();
        assertThatBatchIsPendingResendWithDelay(request, Duration.ofSeconds(12));
    }

    @Test
    void shouldReturnBatchToTheQueueIfOneFileForTheBatchIsUnassigned() {

        // Given
        FileReference file1 = fileFactory.rootFile("file1.parquet", 1234);
        FileReference file2 = fileFactory.rootFile("file2.parquet", 5678);
        CompactionJob job1 = compactionFactory.createCompactionJob("test-job-1", List.of(file1), "root");
        CompactionJob job2 = compactionFactory.createCompactionJob("test-job-2", List.of(file2), "root");
        update(stateStore).addFiles(List.of(file1, file2));
        assignJobIds(List.of(job1));

        tableProperties.setNumber(COMPACTION_JOB_SEND_TIMEOUT_SECS, 123);
        tableProperties.setNumber(COMPACTION_JOB_SEND_RETRY_DELAY_SECS, 12);
        CompactionJobDispatchRequest request = generateBatchRequestAtTime(
                "test-batch", Instant.parse("2024-11-15T10:20:00Z"));
        Instant retryTime = Instant.parse("2024-11-15T10:22:00Z");
        putCompactionJobBatch(request, List.of(job1, job2));

        // When
        dispatchWithTimeAtRetryCheck(request, retryTime);

        // Then
        assertThat(compactionQueue).isEmpty();
        assertThat(tracker.getAllJobs(tableProperties.get(TABLE_ID))).isEmpty();
        assertThatBatchIsPendingResendWithDelay(request, Duration.ofSeconds(12));
    }

    @Test
    void shouldSendBatchToDeadLetterQueueIfOneFileIsAssignedToAnotherJob() {

        // Given
        FileReference unassignedFile = fileFactory.rootFile("unassigned.parquet", 1234);
        FileReference assignedFile = fileFactory.rootFile("assigned-to-other-job.parquet", 5678);
        CompactionJob job1 = compactionFactory.createCompactionJob("test-job-1", List.of(unassignedFile), "root");
        CompactionJob job2 = compactionFactory.createCompactionJob("test-job-2", List.of(assignedFile), "root");
        CompactionJob tookFileJob = compactionFactory.createCompactionJob("took-file-job", List.of(assignedFile),
                "root");
        update(stateStore).addFiles(List.of(unassignedFile, assignedFile));
        assignJobIds(List.of(tookFileJob));

        tableProperties.setNumber(COMPACTION_JOB_SEND_TIMEOUT_SECS, 123);
        tableProperties.setNumber(COMPACTION_JOB_SEND_RETRY_DELAY_SECS, 12);
        CompactionJobDispatchRequest request = generateBatchRequestAtTime(
                "test-batch", Instant.parse("2024-11-15T10:20:00Z"));
        Instant retryTime = Instant.parse("2024-11-15T10:22:00Z");
        putCompactionJobBatch(request, List.of(job1, job2));

        // When
        dispatchWithTimeAtRetryCheck(request, retryTime);

        // Then
        assertThat(compactionQueue).isEmpty();
        assertThat(tracker.getAllJobs(tableProperties.get(TABLE_ID))).isEmpty();
        assertThatBatchIsSentToDeadLetterQueue(request);
    }

    @Test
    void shouldSendBatchToDeadLetterQueueIfOneFileHasBeenDeleted() {

        // Given
        FileReference unassignedFile = fileFactory.rootFile("unassigned.parquet", 1234);
        FileReference deletedFile = fileFactory.rootFile("deleted.parquet", 5678);
        CompactionJob job1 = compactionFactory.createCompactionJob("test-job-1", List.of(unassignedFile), "root");
        CompactionJob job2 = compactionFactory.createCompactionJob("test-job-2", List.of(deletedFile), "root");
        update(stateStore).addFiles(List.of(unassignedFile));

        tableProperties.setNumber(COMPACTION_JOB_SEND_TIMEOUT_SECS, 123);
        tableProperties.setNumber(COMPACTION_JOB_SEND_RETRY_DELAY_SECS, 12);
        CompactionJobDispatchRequest request = generateBatchRequestAtTime(
                "test-batch", Instant.parse("2024-11-15T10:20:00Z"));
        Instant retryTime = Instant.parse("2024-11-15T10:22:00Z");
        putCompactionJobBatch(request, List.of(job1, job2));

        // When
        dispatchWithTimeAtRetryCheck(request, retryTime);

        // Then
        assertThat(compactionQueue).isEmpty();
        assertThat(tracker.getAllJobs(tableProperties.get(TABLE_ID))).isEmpty();
        assertThatBatchIsSentToDeadLetterQueue(request);
    }

    @Test
    void shouldSendToDeadLetterQueueIfTheFilesAreUnassignedAndTimeoutExpired() {

        // Given
        FileReference file1 = fileFactory.rootFile("test5.parquet", 1234);
        FileReference file2 = fileFactory.rootFile("test6.parquet", 5678);
        CompactionJob job1 = compactionFactory.createCompactionJob("test-job-5", List.of(file1), "root");
        CompactionJob job2 = compactionFactory.createCompactionJob("test-job-6", List.of(file2), "root");
        update(stateStore).addFiles(List.of(file1, file2));

        tableProperties.setNumber(COMPACTION_JOB_SEND_TIMEOUT_SECS, 123);
        CompactionJobDispatchRequest request = generateBatchRequestAtTime(
                "test-batch", Instant.parse("2024-11-15T10:30:00Z"));
        Instant retryTime = Instant.parse("2024-11-15T10:33:00Z");
        putCompactionJobBatch(request, List.of(job1, job2));

        // When
        dispatchWithTimeAtRetryCheck(request, retryTime);

        // Then
        assertThat(compactionQueue).isEmpty();
        assertThat(tracker.getAllJobs(tableProperties.get(TABLE_ID))).isEmpty();
        assertThatBatchIsSentToDeadLetterQueue(request);
    }

    @Test
    void shouldFailForRetryWhenOneJobFailedToSend() {

        // Given
        FileReference file1 = fileFactory.rootFile("test1.parquet", 1234);
        FileReference file2 = fileFactory.rootFile("test2.parquet", 5678);
        FileReference file3 = fileFactory.rootFile("test3.parquet", 9000);
        CompactionJob job1 = compactionFactory.createCompactionJob("test-job-1", List.of(file1), "root");
        CompactionJob job2 = compactionFactory.createCompactionJob("test-job-2", List.of(file2), "root");
        CompactionJob job3 = compactionFactory.createCompactionJob("test-job-3", List.of(file3), "root");
        update(stateStore).addFiles(List.of(file1, file2, file3));
        assignJobIds(List.of(job1, job2, job3));
        RuntimeException sendFailure = new RuntimeException("Failed sending job");
        sendFailureByJobId.put("test-job-2", sendFailure);

        CompactionJobDispatchRequest request = generateBatchRequestAtTime(
                "test-batch", Instant.parse("2024-11-15T10:30:00Z"));
        putCompactionJobBatch(request, List.of(job1, job2, job3));
        tracker.setTimeSupplier(List.of(
                Instant.parse("2024-11-15T10:30:10Z")).iterator()::next);

        // When / Then
        assertThatThrownBy(() -> dispatchWithNoRetry(request))
                .isSameAs(sendFailure);
        assertThat(compactionQueue).containsExactly(job1);
        assertThat(tracker.getAllJobs(tableProperties.get(TABLE_ID))).containsExactly(
                compactionJobCreated(job1, Instant.parse("2024-11-15T10:30:10Z")));
        assertThat(delayedPendingQueue).isEmpty();
        assertThat(pendingDeadLetterQueue).isEmpty();
        assertThat(s3PathToCompactionJobBatch).isNotEmpty();
    }

    @Test
    void shouldDeleteAllFilesAfterBatchIsSent() {

        // Given
        FileReference file1 = fileFactory.rootFile("test1.parquet", 1234);
        CompactionJob job1 = compactionFactory.createCompactionJob("test-job-1", List.of(file1), "root");
        update(stateStore).addFiles(List.of(file1));
        assignJobIds(List.of(job1));

        CompactionJobDispatchRequest request = generateBatchRequestAtTime(
                "test-batch", Instant.parse("2024-11-15T10:30:00Z"));
        putCompactionJobBatch(request, List.of(job1));

        // When
        dispatchWithNoRetry(request);

        // Then
        assertThat(compactionQueue).containsExactly(job1);
        assertThat(s3PathToCompactionJobBatch).isEmpty();
    }

    @Test
    void shouldSendBatchToDeadLetterQueueWhenDeleteThrowsException() {

        // Given
        FileReference file1 = fileFactory.rootFile("test1.parquet", 1234);
        CompactionJob job1 = compactionFactory.createCompactionJob("test-job-1", List.of(file1), "root");
        update(stateStore).addFiles(List.of(file1));
        assignJobIds(List.of(job1));

        CompactionJobDispatchRequest request = generateBatchRequestAtTime(
                "test-batch", Instant.parse("2024-11-15T10:30:00Z"));
        putCompactionJobBatch(request, List.of(job1));

        // When
        deleteBatch = (bucketName, key) -> {
            throw new RuntimeException();
        };
        dispatchWithNoRetry(request);

        // Then
        assertThat(pendingDeadLetterQueue).containsExactly(request);
        assertThat(s3PathToCompactionJobBatch).isNotEmpty();
        assertThat(compactionQueue).containsExactly(job1);
    }

    private void putCompactionJobBatch(CompactionJobDispatchRequest request, List<CompactionJob> jobs) {
        s3PathToCompactionJobBatch.put(instanceProperties.get(DATA_BUCKET) + "/" + request.getBatchKey(), jobs);
    }

    private void dispatchWithNoRetry(CompactionJobDispatchRequest request) {
        dispatcher(List.of()).dispatch(request);
    }

    private void dispatchWithTimeAtRetryCheck(CompactionJobDispatchRequest request, Instant time) {
        dispatcher(List.of(time)).dispatch(request);
    }

    private CompactionJobDispatcher dispatcher(List<Instant> times) {
        return new CompactionJobDispatcher(instanceProperties, new FixedTablePropertiesProvider(tableProperties),
                FixedStateStoreProvider.singleTable(tableProperties, stateStore), tracker,
                readBatch(), sendJobs(), 1, deleteBatch,
                returnRequest(), pendingDeadLetterQueue::add, times.iterator()::next);
    }

    private CompactionJobDispatcher.DeleteBatch deleteBatch() {
        return (bucketName, key) -> s3PathToCompactionJobBatch.remove(bucketName + "/" + key);
    }

    private CompactionJobDispatcher.ReadBatch readBatch() {
        return (bucketName, key) -> s3PathToCompactionJobBatch.get(bucketName + "/" + key);
    }

    private CompactionJobDispatcher.SendJobs sendJobs() {
        return jobs -> {
            for (CompactionJob job : jobs) {
                RuntimeException sendFailure = sendFailureByJobId.get(job.getId());
                if (sendFailure != null) {
                    throw sendFailure;
                } else {
                    compactionQueue.add(job);
                }
            }
        };
    }

    private CompactionJobDispatcher.ReturnRequestToPendingQueue returnRequest() {
        return (request, delaySeconds) -> delayedPendingQueue.add(
                BatchRequestMessage.requestAndDelay(request, Duration.ofSeconds(delaySeconds)));
    }

    private void assignJobIds(List<CompactionJob> jobs) {
        for (CompactionJob job : jobs) {
            update(stateStore).assignJobIds(List.of(job.createAssignJobIdRequest()));
        }
    }

    private CompactionJobDispatchRequest generateBatchRequestAtTime(String batchId, Instant timeNow) {
        return CompactionJobDispatchRequest.forTableWithBatchIdAtTime(
                tableProperties, batchId, timeNow);
    }

    private CompactionJobDispatchRequest generateBatchRequest(String batchId) {
        return CompactionJobDispatchRequest.forTableWithBatchIdAtTime(
                tableProperties, batchId, Instant.now());
    }

    private record BatchRequestMessage(CompactionJobDispatchRequest request, int delaySeconds) {
        static BatchRequestMessage requestAndDelay(CompactionJobDispatchRequest request, Duration duration) {
            return new BatchRequestMessage(request, (int) duration.toSeconds());
        }
    }

    private void assertThatNoBatchesAreRequeued() {
        assertThat(delayedPendingQueue).isEmpty();
        assertThat(pendingDeadLetterQueue).isEmpty();
    }

    private void assertThatBatchFileIsDeleted() {
        assertThat(s3PathToCompactionJobBatch).isEmpty();
    }

    private void assertThatBatchIsPendingResendWithDelay(CompactionJobDispatchRequest request, Duration delayDuration) {
        assertThat(delayedPendingQueue).containsExactly(
                BatchRequestMessage.requestAndDelay(request, delayDuration));
        assertThat(pendingDeadLetterQueue).isEmpty();
        assertThat(s3PathToCompactionJobBatch).isNotEmpty();
    }

    private void assertThatBatchIsSentToDeadLetterQueue(CompactionJobDispatchRequest request) {
        assertThat(delayedPendingQueue).isEmpty();
        assertThat(pendingDeadLetterQueue).containsExactly(request);
        assertThat(s3PathToCompactionJobBatch).isNotEmpty();
    }

}
