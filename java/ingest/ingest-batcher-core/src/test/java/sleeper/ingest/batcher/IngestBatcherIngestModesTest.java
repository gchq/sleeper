/*
 * Copyright 2022-2023 Crown Copyright
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
package sleeper.ingest.batcher;

import org.junit.jupiter.api.Test;

import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.validation.BatchIngestMode;
import sleeper.ingest.job.IngestJob;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.BULK_IMPORT_EKS_JOB_QUEUE_URL;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.BULK_IMPORT_EMR_JOB_QUEUE_URL;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.BULK_IMPORT_PERSISTENT_EMR_JOB_QUEUE_URL;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.INGEST_JOB_QUEUE_URL;
import static sleeper.configuration.properties.table.TableProperty.INGEST_BATCHER_INGEST_MODE;

class IngestBatcherIngestModesTest extends IngestBatcherTestBase {

    @Test
    void shouldCreateJobOnStandardIngestQueue() {
        // Given
        instanceProperties.set(INGEST_JOB_QUEUE_URL, "ingest-url");
        tableProperties.set(INGEST_BATCHER_INGEST_MODE, BatchIngestMode.STANDARD_INGEST.toString());
        addFileToStore("test-bucket/ingest.parquet");

        // When
        batchFilesWithJobIds("test-job");

        // Then
        assertThat(queues.getMessagesByQueueUrl()).isEqualTo(Map.of(
                "ingest-url", List.of(jobWithFiles("test-job", "test-bucket/ingest.parquet"))));
    }

    @Test
    void shouldCreateJobOnEmrQueue() {
        // Given
        instanceProperties.set(BULK_IMPORT_EMR_JOB_QUEUE_URL, "emr-url");
        tableProperties.set(INGEST_BATCHER_INGEST_MODE, BatchIngestMode.BULK_IMPORT_EMR.toString());
        addFileToStore("test-bucket/bulk-import.parquet");

        // When
        batchFilesWithJobIds("test-job");

        // Then
        assertThat(queues.getMessagesByQueueUrl()).isEqualTo(Map.of(
                "emr-url", List.of(jobWithFiles("test-job", "test-bucket/bulk-import.parquet"))));
    }

    @Test
    void shouldCreateJobOnPersistentEmrQueue() {
        // Given
        instanceProperties.set(BULK_IMPORT_PERSISTENT_EMR_JOB_QUEUE_URL, "persistent-emr-url");
        tableProperties.set(INGEST_BATCHER_INGEST_MODE, BatchIngestMode.BULK_IMPORT_PERSISTENT_EMR.toString());
        addFileToStore("test-bucket/bulk-import.parquet");

        // When
        batchFilesWithJobIds("test-job");

        // Then
        assertThat(queues.getMessagesByQueueUrl()).isEqualTo(Map.of(
                "persistent-emr-url", List.of(jobWithFiles("test-job", "test-bucket/bulk-import.parquet"))));
    }

    @Test
    void shouldCreateJobOnEksQueue() {
        // Given
        instanceProperties.set(BULK_IMPORT_EKS_JOB_QUEUE_URL, "eks-url");
        tableProperties.set(INGEST_BATCHER_INGEST_MODE, BatchIngestMode.BULK_IMPORT_EKS.toString());
        addFileToStore("test-bucket/bulk-import.parquet");

        // When
        batchFilesWithJobIds("test-job");

        // Then
        assertThat(queues.getMessagesByQueueUrl()).isEqualTo(Map.of(
                "eks-url", List.of(jobWithFiles("test-job", "test-bucket/bulk-import.parquet"))));
    }

    @Test
    void shouldCreateJobsOnDifferentQueueForEachOfTwoTables() {
        // Given
        instanceProperties.set(INGEST_JOB_QUEUE_URL, "ingest-url");
        instanceProperties.set(BULK_IMPORT_EMR_JOB_QUEUE_URL, "bulk-import-url");
        TableProperties ingestTable = createTableProperties("ingest-table");
        TableProperties bulkImportTable = createTableProperties("bulk-import-table");
        ingestTable.set(INGEST_BATCHER_INGEST_MODE, BatchIngestMode.STANDARD_INGEST.toString());
        bulkImportTable.set(INGEST_BATCHER_INGEST_MODE, BatchIngestMode.BULK_IMPORT_EMR.toString());
        addFileToStore(builder -> builder.file("test-bucket/ingest.parquet").tableName("ingest-table"));
        addFileToStore(builder -> builder.file("test-bucket/bulk-import.parquet").tableName("bulk-import-table"));

        // When
        batchFilesWithTablesAndJobIds(List.of(ingestTable, bulkImportTable), List.of("test-job-1", "test-job-2"));

        // Then
        assertThat(queues.getMessagesByQueueUrl()).isEqualTo(Map.of(
                "ingest-url", List.of(IngestJob.builder()
                        .files("test-bucket/ingest.parquet")
                        .tableName("ingest-table")
                        .id("test-job-1")
                        .build()),
                "bulk-import-url", List.of(IngestJob.builder()
                        .files("test-bucket/bulk-import.parquet")
                        .tableName("bulk-import-table")
                        .id("test-job-2")
                        .build())));
    }

    @Test
    void shouldConsumeFilesAndRequireResubmissionIfQueueIsNotConfigured() {
        instanceProperties.unset(INGEST_JOB_QUEUE_URL);
        tableProperties.set(INGEST_BATCHER_INGEST_MODE, BatchIngestMode.STANDARD_INGEST.toString());
        addFileToStore("test-bucket/ingest.parquet");

        // When
        batchFilesWithJobIds("test-job");

        // Then
        assertThat(queues.getMessagesByQueueUrl()).isEmpty();
        assertThat(store.getPendingFilesOldestFirst()).isEmpty();
    }

    @Test
    void shouldConsumeFilesAndRequireResubmissionIfQueueModeIsNotValid() {
        tableProperties.set(INGEST_BATCHER_INGEST_MODE, "invalid");
        addFileToStore("test-bucket/ingest.parquet");

        // When
        batchFilesWithJobIds("test-job");

        // Then
        assertThat(queues.getMessagesByQueueUrl()).isEmpty();
        assertThat(store.getPendingFilesOldestFirst()).isEmpty();
    }
}
