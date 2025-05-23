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
package sleeper.ingest.runner.impl.commit;

import org.junit.jupiter.api.Test;

import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;
import sleeper.core.statestore.commit.StateStoreCommitRequest;
import sleeper.core.statestore.testutils.InMemoryTransactionLogStateStore;
import sleeper.core.statestore.testutils.InMemoryTransactionLogs;
import sleeper.core.statestore.transactionlog.transaction.impl.AddFilesTransaction;
import sleeper.core.tracker.ingest.job.InMemoryIngestJobTracker;
import sleeper.core.tracker.ingest.job.update.IngestJobRunIds;
import sleeper.core.tracker.ingest.job.update.IngestJobStartedEvent;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.schema.SchemaTestHelper.createSchemaWithKey;
import static sleeper.core.statestore.FileReferenceTestData.withLastUpdate;
import static sleeper.core.testutils.SupplierTestHelper.supplyTimes;
import static sleeper.core.tracker.ingest.job.IngestJobStatusTestData.ingestAddedFilesStatus;
import static sleeper.core.tracker.ingest.job.IngestJobStatusTestData.ingestJobStatus;
import static sleeper.core.tracker.ingest.job.IngestJobStatusTestData.ingestStartedStatus;
import static sleeper.core.tracker.job.run.JobRunTestData.jobRunOnTask;
import static sleeper.core.tracker.job.status.JobStatusUpdateTestHelper.failedStatus;

public class AddFilesToStateStoreTest {
    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final Schema schema = createSchemaWithKey("key");
    private final PartitionTree partitionTree = new PartitionsBuilder(schema).singlePartition("root").buildTree();
    private final FileReferenceFactory factory = FileReferenceFactory.from(partitionTree);
    private final TableProperties tableProperties = createTestTableProperties(instanceProperties, createSchemaWithKey("key"));
    private final String tableId = tableProperties.get(TABLE_ID);
    private final InMemoryTransactionLogs transactionLogs = new InMemoryTransactionLogs();
    private final InMemoryIngestJobTracker jobTracker = new InMemoryIngestJobTracker();
    private final StateStore stateStore = InMemoryTransactionLogStateStore.createAndInitialise(tableProperties, transactionLogs);
    private final List<StateStoreCommitRequest> stateStoreCommitQueue = new ArrayList<>();

    @Test
    void shouldCommitAsynchronouslyWithJob() throws Exception {
        // Given
        FileReference file = factory.rootFile("test.parquet", 123L);
        Instant startTime = Instant.parse("2025-02-26T11:30:00Z");
        Instant writtenTime = Instant.parse("2025-02-26T11:31:00Z");
        IngestJobRunIds runIds = IngestJobRunIds.builder().tableId(tableId)
                .jobId("test-job").jobRunId("test-run").taskId("test-task").build();
        IngestJobStartedEvent startedEvent = IngestJobStartedEvent.builder()
                .jobRunIds(runIds)
                .fileCount(2)
                .startTime(startTime)
                .build();
        jobTracker.jobStarted(startedEvent);

        // When
        AddFilesToStateStore.asynchronousWithJob(tableProperties, stateStoreCommitQueue::add, supplyTimes(writtenTime), runIds)
                .addFiles(List.of(file));

        // Then
        assertThat(stateStoreCommitQueue)
                .containsExactly(StateStoreCommitRequest.create(tableProperties.get(TABLE_ID),
                        AddFilesTransaction.builder()
                                .jobRunIds(runIds)
                                .writtenTime(writtenTime)
                                .fileReferences(List.of(file))
                                .build()));
        assertThat(jobTracker.getAllJobs(tableId)).containsExactly(
                ingestJobStatus("test-job",
                        jobRunOnTask("test-task",
                                ingestStartedStatus(startTime, 2))));
    }

    @Test
    void shouldCommitSynchronouslyWithJob() {
        // Given
        FileReference file = factory.rootFile("test.parquet", 123L);
        Instant startTime = Instant.parse("2025-02-26T11:30:00Z");
        Instant writtenTime = Instant.parse("2025-02-26T11:31:00Z");
        Instant updateTime = Instant.parse("2025-02-26T11:31:00Z");
        IngestJobRunIds runIds = IngestJobRunIds.builder().tableId(tableId)
                .jobId("test-job").jobRunId("test-run").taskId("test-task").build();
        IngestJobStartedEvent startedEvent = IngestJobStartedEvent.builder()
                .jobRunIds(runIds)
                .fileCount(2)
                .startTime(startTime)
                .build();
        jobTracker.jobStarted(startedEvent);
        stateStore.fixFileUpdateTime(updateTime);

        // When
        AddFilesToStateStore.synchronousWithJob(tableProperties, stateStore, jobTracker, supplyTimes(writtenTime), runIds)
                .addFiles(List.of(file));

        // Then
        assertThat(stateStore.getFileReferences()).containsExactly(withLastUpdate(updateTime, file));
        assertThat(transactionLogs.getLastFilesTransaction(tableProperties))
                .isEqualTo(AddFilesTransaction.fromReferences(List.of(file)));
        assertThat(jobTracker.getAllJobs(tableId)).containsExactly(
                ingestJobStatus("test-job",
                        jobRunOnTask("test-task",
                                ingestStartedStatus(startTime, 2),
                                ingestAddedFilesStatus(writtenTime, 1))));
    }

    @Test
    void shouldTrackFailureCommittingSynchronouslyWithJob() {
        // Given
        FileReference file = factory.rootFile("test.parquet", 123L);
        Instant startTime = Instant.parse("2025-02-26T11:30:00Z");
        Instant writtenTime = Instant.parse("2025-02-26T11:31:00Z");
        Instant updateTime = Instant.parse("2025-02-26T11:31:00Z");
        IngestJobRunIds runIds = IngestJobRunIds.builder().tableId(tableId)
                .jobId("test-job").jobRunId("test-run").taskId("test-task").build();
        IngestJobStartedEvent startedEvent = IngestJobStartedEvent.builder()
                .jobRunIds(runIds)
                .fileCount(2)
                .startTime(startTime)
                .build();
        jobTracker.jobStarted(startedEvent);
        stateStore.fixFileUpdateTime(updateTime);
        RuntimeException failure = new IllegalStateException("Test add transaction failure");
        transactionLogs.getFilesLogStore().atStartOfAddTransaction(() -> {
            throw failure;
        });
        AddFilesToStateStore addFiles = AddFilesToStateStore.synchronousWithJob(tableProperties, stateStore, jobTracker, supplyTimes(writtenTime), runIds);

        // When / Then
        assertThatThrownBy(() -> addFiles.addFiles(List.of(file)))
                .isInstanceOf(StateStoreException.class)
                .cause().isSameAs(failure);
        assertThat(stateStore.getFileReferences()).isEmpty();
        assertThat(jobTracker.getAllJobs(tableId)).containsExactly(
                ingestJobStatus("test-job",
                        jobRunOnTask("test-task",
                                ingestStartedStatus(startTime, 2),
                                failedStatus(writtenTime, List.of("Failed adding transaction", "Test add transaction failure")))));
    }

    @Test
    void shouldCommitAsynchronouslyWithNoJob() throws Exception {
        // Given
        FileReference file = factory.rootFile("test.parquet", 123L);

        // When
        AddFilesToStateStore.asynchronousNoJob(tableProperties, stateStoreCommitQueue::add)
                .addFiles(List.of(file));

        // Then
        assertThat(stateStoreCommitQueue)
                .containsExactly(StateStoreCommitRequest.create(tableProperties.get(TABLE_ID),
                        AddFilesTransaction.fromReferences(List.of(file))));
    }

    @Test
    void shouldCommitSynchronouslyWithNoJob() {
        // Given
        FileReference file = factory.rootFile("test.parquet", 123L);
        Instant updateTime = Instant.parse("2025-02-26T11:31:00Z");
        stateStore.fixFileUpdateTime(updateTime);

        // When
        AddFilesToStateStore.synchronousNoJob(stateStore).addFiles(List.of(file));

        // Then
        assertThat(stateStore.getFileReferences()).containsExactly(withLastUpdate(updateTime, file));
        assertThat(transactionLogs.getLastFilesTransaction(tableProperties))
                .isEqualTo(AddFilesTransaction.fromReferences(List.of(file)));
    }
}
