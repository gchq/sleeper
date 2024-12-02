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
package sleeper.compaction.core.job.dispatch;

import org.junit.jupiter.api.Test;

import sleeper.compaction.core.job.CompactionJob;
import sleeper.compaction.core.job.CompactionJobFactory;
import sleeper.compaction.core.testutils.InMemoryCompactionJobStatusStore;
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

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.compaction.core.job.CompactionJobStatusTestData.jobCreated;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.core.properties.table.TableProperty.COMPACTION_JOB_SEND_RETRY_DELAY_SECS;
import static sleeper.core.properties.table.TableProperty.COMPACTION_JOB_SEND_TIMEOUT_SECS;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;
import static sleeper.core.statestore.testutils.StateStoreTestHelper.inMemoryStateStoreWithFixedPartitions;

public class CompactionJobDispatcherTest {

    Schema schema = schemaWithKey("key");
    InstanceProperties instanceProperties = createTestInstanceProperties();
    TableProperties tableProperties = createTestTableProperties(instanceProperties, schema);

    PartitionTree partitions = new PartitionsBuilder(schema).singlePartition("root").buildTree();
    StateStore stateStore = inMemoryStateStoreWithFixedPartitions(partitions.getAllPartitions());
    InMemoryCompactionJobStatusStore statusStore = new InMemoryCompactionJobStatusStore();
    FileReferenceFactory fileFactory = FileReferenceFactory.from(partitions);
    CompactionJobFactory compactionFactory = new CompactionJobFactory(instanceProperties, tableProperties);

    Map<String, List<CompactionJob>> s3PathToCompactionJobBatch = new HashMap<>();
    Queue<CompactionJob> compactionQueue = new LinkedList<>();
    Queue<BatchRequestMessage> delayedPendingQueue = new LinkedList<>();

    @Test
    void shouldSendCompactionJobsInABatchWhenAllFilesAreAssigned() throws Exception {

        // Given
        FileReference file1 = fileFactory.rootFile("test1.parquet", 1234);
        FileReference file2 = fileFactory.rootFile("test2.parquet", 5678);
        CompactionJob job1 = compactionFactory.createCompactionJob("test-job-1", List.of(file1), "root");
        CompactionJob job2 = compactionFactory.createCompactionJob("test-job-2", List.of(file2), "root");
        stateStore.addFiles(List.of(file1, file2));
        assignJobIds(List.of(job1, job2));

        CompactionJobDispatchRequest request = generateBatchRequestAtTime(
                "test-batch", Instant.parse("2024-11-15T10:30:00Z"));
        putCompactionJobBatch(request, List.of(job1, job2));

        statusStore.setTimeSupplier(List.of(
                Instant.parse("2024-11-15T10:30:10Z"),
                Instant.parse("2024-11-15T10:30:11Z")).iterator()::next);

        // When
        dispatchWithNoRetry(request);

        // Then
        assertThat(compactionQueue).containsExactly(job1, job2);
        assertThat(statusStore.getAllJobs(tableProperties.get(TABLE_ID))).containsExactly(
                jobCreated(job2, Instant.parse("2024-11-15T10:30:11Z")),
                jobCreated(job1, Instant.parse("2024-11-15T10:30:10Z")));
        assertThat(delayedPendingQueue).isEmpty();
    }

    @Test
    void shouldReturnBatchToTheQueueIfTheFilesForTheBatchAreUnassigned() throws Exception {

        // Given
        FileReference file1 = fileFactory.rootFile("test3.parquet", 1234);
        FileReference file2 = fileFactory.rootFile("test4.parquet", 5678);
        CompactionJob job1 = compactionFactory.createCompactionJob("test-job-3", List.of(file1), "root");
        CompactionJob job2 = compactionFactory.createCompactionJob("test-job-4", List.of(file2), "root");
        stateStore.addFiles(List.of(file1, file2));

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
        assertThat(statusStore.getAllJobs(tableProperties.get(TABLE_ID))).isEmpty();
        assertThat(delayedPendingQueue).containsExactly(
                BatchRequestMessage.requestAndDelay(request, Duration.ofSeconds(12)));
    }

    @Test
    void shouldFailIfTheFilesAreUnassignedAndTimeoutExpired() throws Exception {

        // Given
        FileReference file1 = fileFactory.rootFile("test5.parquet", 1234);
        FileReference file2 = fileFactory.rootFile("test6.parquet", 5678);
        CompactionJob job1 = compactionFactory.createCompactionJob("test-job-5", List.of(file1), "root");
        CompactionJob job2 = compactionFactory.createCompactionJob("test-job-6", List.of(file2), "root");
        stateStore.addFiles(List.of(file1, file2));

        tableProperties.setNumber(COMPACTION_JOB_SEND_TIMEOUT_SECS, 123);
        CompactionJobDispatchRequest request = generateBatchRequestAtTime(
                "test-batch", Instant.parse("2024-11-15T10:30:00Z"));
        Instant retryTime = Instant.parse("2024-11-15T10:33:00Z");
        putCompactionJobBatch(request, List.of(job1, job2));

        // When / Then
        assertThatThrownBy(() -> dispatchWithTimeAtRetryCheck(request, retryTime))
                .isInstanceOf(CompactionJobBatchExpiredException.class);
        assertThat(compactionQueue).isEmpty();
        assertThat(statusStore.getAllJobs(tableProperties.get(TABLE_ID))).isEmpty();
        assertThat(delayedPendingQueue).isEmpty();
    }

    private void putCompactionJobBatch(CompactionJobDispatchRequest request, List<CompactionJob> jobs) {
        s3PathToCompactionJobBatch.put(instanceProperties.get(DATA_BUCKET) + "/" + request.getBatchKey(), jobs);
    }

    private void dispatchWithNoRetry(CompactionJobDispatchRequest request) throws Exception {
        dispatcher(List.of()).dispatch(request);
    }

    private void dispatchWithTimeAtRetryCheck(CompactionJobDispatchRequest request, Instant time) throws Exception {
        dispatcher(List.of(time)).dispatch(request);
    }

    private CompactionJobDispatcher dispatcher(List<Instant> times) {
        return new CompactionJobDispatcher(instanceProperties, new FixedTablePropertiesProvider(tableProperties),
                new FixedStateStoreProvider(tableProperties, stateStore), statusStore, readBatch(),
                compactionQueue::add, returnRequest(), times.iterator()::next);
    }

    private CompactionJobDispatcher.ReadBatch readBatch() {
        return (bucketName, key) -> s3PathToCompactionJobBatch.get(bucketName + "/" + key);
    }

    private CompactionJobDispatcher.ReturnRequestToPendingQueue returnRequest() {
        return (request, delaySeconds) -> delayedPendingQueue.add(
                BatchRequestMessage.requestAndDelay(request, Duration.ofSeconds(delaySeconds)));
    }

    private void assignJobIds(List<CompactionJob> jobs) throws Exception {
        for (CompactionJob job : jobs) {
            stateStore.assignJobIds(List.of(job.createAssignJobIdRequest()));
        }
    }

    private CompactionJobDispatchRequest generateBatchRequestAtTime(String batchId, Instant timeNow) {
        return CompactionJobDispatchRequest.forTableWithBatchIdAtTime(
                tableProperties, batchId, timeNow);
    }

    private record BatchRequestMessage(CompactionJobDispatchRequest request, int delaySeconds) {
        static BatchRequestMessage requestAndDelay(CompactionJobDispatchRequest request, Duration duration) {
            return new BatchRequestMessage(request, (int) duration.toSeconds());
        }
    }
}
