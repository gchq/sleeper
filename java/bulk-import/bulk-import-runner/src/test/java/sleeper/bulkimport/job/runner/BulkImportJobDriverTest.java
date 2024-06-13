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

package sleeper.bulkimport.job.runner;

import org.junit.jupiter.api.Test;

import sleeper.bulkimport.job.BulkImportJob;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.FixedTablePropertiesProvider;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;
import sleeper.core.statestore.inmemory.StateStoreTestHelper;
import sleeper.ingest.job.status.InMemoryIngestJobStatusStore;
import sleeper.ingest.job.status.IngestJobStatus;
import sleeper.ingest.job.status.IngestJobStatusStore;
import sleeper.statestore.FixedStateStoreProvider;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static sleeper.configuration.properties.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.configuration.properties.table.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.configuration.properties.table.TableProperty.TABLE_ID;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;
import static sleeper.core.record.process.RecordsProcessedSummaryTestHelper.summary;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;
import static sleeper.core.statestore.FileReferenceTestData.defaultFileOnRootPartitionWithRecords;
import static sleeper.ingest.job.status.IngestJobStatusTestHelper.finishedIngestJobWithValidation;
import static sleeper.ingest.job.status.IngestJobValidatedEvent.ingestJobAccepted;

class BulkImportJobDriverTest {
    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final Schema schema = schemaWithKey("key");
    private final TableProperties tableProperties = createTestTableProperties(instanceProperties, schema);
    private final StateStore stateStore = StateStoreTestHelper.inMemoryStateStoreWithFixedSinglePartition(schema);
    private final IngestJobStatusStore statusStore = new InMemoryIngestJobStatusStore();

    @Test
    void shouldReportJobFinished() throws Exception {
        // Given
        BulkImportJob job = singleFileImportJob();
        Instant validationTime = Instant.parse("2023-04-06T12:30:01Z");
        Instant startTime = Instant.parse("2023-04-06T12:40:01Z");
        Instant finishTime = Instant.parse("2023-04-06T12:41:01Z");
        List<FileReference> outputFiles = List.of(
                defaultFileOnRootPartitionWithRecords("test-output.parquet", 100));

        // When
        runJob(job, "test-run", "test-task", validationTime, startTime, finishTime, outputFiles);

        // Then
        assertThat(allJobsReported())
                .containsExactly(finishedIngestJobWithValidation(job.toIngestJob(), "test-task",
                        validationTime, summary(startTime, finishTime, 100, 100)));
        assertThat(stateStore.getFileReferences())
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("lastStateStoreUpdateTime")
                .isEqualTo(outputFiles);
    }

    @Test
    void shouldReportJobFinishedWithNoRecordsWhenJobFailed() throws Exception {
        // Given
        BulkImportJob job = singleFileImportJob();
        Instant validationTime = Instant.parse("2023-04-06T12:30:01Z");
        Instant startTime = Instant.parse("2023-04-06T12:40:01Z");
        Instant finishTime = Instant.parse("2023-04-06T12:41:01Z");
        RuntimeException jobFailure = new RuntimeException("Failed running job");

        // When
        assertThatThrownBy(() -> runJob(job, "test-run", "test-task",
                validationTime, startTime, finishTime,
                foundJob -> {
                    throw jobFailure;
                })).isSameAs(jobFailure);

        // Then
        assertThat(allJobsReported())
                .containsExactly(finishedIngestJobWithValidation(job.toIngestJob(), "test-task",
                        validationTime, summary(startTime, finishTime, 0, 0)));
        assertThat(stateStore.getFileReferences()).isEmpty();
    }

    @Test
    void shouldReportJobFinishedWithNoRecordsWhenStateStoreUpdateFailed() throws Exception {
        // Given
        BulkImportJob job = singleFileImportJob();
        Instant validationTime = Instant.parse("2023-04-06T12:30:01Z");
        Instant startTime = Instant.parse("2023-04-06T12:40:01Z");
        Instant finishTime = Instant.parse("2023-04-06T12:41:01Z");
        StateStoreException jobFailure = new StateStoreException("Failed updating files");
        List<FileReference> outputFiles = List.of(
                defaultFileOnRootPartitionWithRecords("test-output.parquet", 100));
        StateStore stateStore = mock(StateStore.class);
        doThrow(jobFailure).when(stateStore).addFiles(outputFiles);

        // When
        assertThatThrownBy(() -> runJob(job, "test-run", "test-task",
                validationTime, startTime, finishTime, outputFiles, stateStore))
                .isInstanceOf(RuntimeException.class).hasCauseReference(jobFailure);

        // Then
        assertThat(allJobsReported())
                .containsExactly(finishedIngestJobWithValidation(job.toIngestJob(), "test-task",
                        validationTime, summary(startTime, finishTime, 0, 0)));
        verify(stateStore).addFiles(outputFiles);
        verifyNoMoreInteractions(stateStore);
    }

    @Test
    void shouldReportJobFinishedWithNoRecordsWhenStateStoreUpdateHadUnexpectedFailure() throws Exception {
        // Given
        BulkImportJob job = singleFileImportJob();
        Instant validationTime = Instant.parse("2023-04-06T12:30:01Z");
        Instant startTime = Instant.parse("2023-04-06T12:40:01Z");
        Instant finishTime = Instant.parse("2023-04-06T12:41:01Z");
        RuntimeException jobFailure = new RuntimeException("Failed updating files");
        List<FileReference> outputFiles = List.of(
                defaultFileOnRootPartitionWithRecords("test-output.parquet", 100));
        StateStore stateStore = mock(StateStore.class);
        doThrow(jobFailure).when(stateStore).addFiles(outputFiles);

        // When
        assertThatThrownBy(() -> runJob(job, "test-run", "test-task",
                validationTime, startTime, finishTime, outputFiles, stateStore))
                .isInstanceOf(RuntimeException.class).hasCauseReference(jobFailure);

        // Then
        assertThat(allJobsReported())
                .containsExactly(finishedIngestJobWithValidation(job.toIngestJob(), "test-task",
                        validationTime, summary(startTime, finishTime, 0, 0)));
        verify(stateStore).addFiles(outputFiles);
        verifyNoMoreInteractions(stateStore);
    }

    private void runJob(
            BulkImportJob job, String jobRunId, String taskId, Instant validationTime,
            Instant startTime, Instant finishTime, List<FileReference> outputFiles) throws Exception {
        runJob(job, jobRunId, taskId, validationTime, startTime, finishTime, outputFiles, stateStore);
    }

    private void runJob(
            BulkImportJob job, String jobRunId, String taskId, Instant validationTime,
            Instant startTime, Instant finishTime, List<FileReference> outputFiles,
            StateStore stateStore) throws Exception {
        BulkImportJobOutput output = new BulkImportJobOutput(outputFiles, () -> {
        });
        runJob(job, jobRunId, taskId, validationTime, startTime, finishTime, bulkImportJob -> output, stateStore);
    }

    private void runJob(
            BulkImportJob job, String jobRunId, String taskId,
            Instant validationTime, Instant startTime, Instant finishTime,
            BulkImportJobDriver.SessionRunner sessionRunner) throws Exception {
        runJob(job, jobRunId, taskId, validationTime, startTime, finishTime, sessionRunner, stateStore);
    }

    private void runJob(
            BulkImportJob job, String jobRunId, String taskId, Instant validationTime,
            Instant startTime, Instant finishTime,
            BulkImportJobDriver.SessionRunner sessionRunner,
            StateStore stateStore) throws Exception {
        statusStore.jobValidated(ingestJobAccepted(job.toIngestJob(), validationTime).jobRunId(jobRunId).build());
        BulkImportJobDriver driver = new BulkImportJobDriver(sessionRunner,
                new FixedTablePropertiesProvider(tableProperties),
                new FixedStateStoreProvider(tableProperties, stateStore),
                statusStore, List.of(startTime, finishTime).iterator()::next);
        driver.run(job, jobRunId, taskId);
    }

    private BulkImportJob singleFileImportJob() {
        return BulkImportJob.builder()
                .id("test-job")
                .tableId(tableProperties.get(TABLE_ID))
                .tableName(tableProperties.get(TABLE_NAME))
                .files(List.of("test.parquet")).build();
    }

    private List<IngestJobStatus> allJobsReported() {
        return statusStore.getAllJobs(tableProperties.get(TABLE_ID));
    }
}
