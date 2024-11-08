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
package sleeper.ingest.batcher.core;

import org.junit.jupiter.api.Test;

import sleeper.core.properties.SleeperPropertiesInvalidException;
import sleeper.core.properties.table.TableProperties;
import sleeper.ingest.core.job.IngestJob;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.BULK_IMPORT_EKS_JOB_QUEUE_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.BULK_IMPORT_EMR_JOB_QUEUE_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.BULK_IMPORT_PERSISTENT_EMR_JOB_QUEUE_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.INGEST_JOB_QUEUE_URL;
import static sleeper.core.properties.table.TableProperty.INGEST_BATCHER_INGEST_QUEUE;
import static sleeper.core.properties.validation.IngestQueue.BULK_IMPORT_EKS;
import static sleeper.core.properties.validation.IngestQueue.BULK_IMPORT_EMR;
import static sleeper.core.properties.validation.IngestQueue.BULK_IMPORT_PERSISTENT_EMR;
import static sleeper.core.properties.validation.IngestQueue.STANDARD_INGEST;

class IngestBatcherIngestModesTest extends IngestBatcherTestBase {

    @Test
    void shouldCreateJobOnStandardIngestQueue() {
        // Given
        instanceProperties.set(INGEST_JOB_QUEUE_URL, "ingest-url");
        tableProperties.setEnum(INGEST_BATCHER_INGEST_QUEUE, STANDARD_INGEST);
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
        tableProperties.setEnum(INGEST_BATCHER_INGEST_QUEUE, BULK_IMPORT_EMR);
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
        tableProperties.setEnum(INGEST_BATCHER_INGEST_QUEUE, BULK_IMPORT_PERSISTENT_EMR);
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
        tableProperties.setEnum(INGEST_BATCHER_INGEST_QUEUE, BULK_IMPORT_EKS);
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
        ingestTable.setEnum(INGEST_BATCHER_INGEST_QUEUE, STANDARD_INGEST);
        bulkImportTable.setEnum(INGEST_BATCHER_INGEST_QUEUE, BULK_IMPORT_EMR);
        addFileToStore(builder -> builder.file("test-bucket/ingest.parquet").tableId("ingest-table"));
        addFileToStore(builder -> builder.file("test-bucket/bulk-import.parquet").tableId("bulk-import-table"));

        // When
        batchFilesWithTablesAndJobIds(List.of(ingestTable, bulkImportTable), List.of("test-job-1", "test-job-2"));

        // Then
        assertThat(queues.getMessagesByQueueUrl()).isEqualTo(Map.of(
                "ingest-url", List.of(IngestJob.builder()
                        .files(List.of("test-bucket/ingest.parquet"))
                        .tableId("ingest-table")
                        .id("test-job-1")
                        .build()),
                "bulk-import-url", List.of(IngestJob.builder()
                        .files(List.of("test-bucket/bulk-import.parquet"))
                        .tableId("bulk-import-table")
                        .id("test-job-2")
                        .build())));
    }

    @Test
    void shouldConsumeFilesAndRequireResubmissionIfQueueIsNotConfigured() {
        instanceProperties.unset(INGEST_JOB_QUEUE_URL);
        tableProperties.setEnum(INGEST_BATCHER_INGEST_QUEUE, STANDARD_INGEST);
        addFileToStore("test-bucket/ingest.parquet");

        // When
        batchFilesWithJobIds("test-job");

        // Then
        assertThat(queues.getMessagesByQueueUrl()).isEmpty();
        assertThat(store.getPendingFilesOldestFirst()).isEmpty();
    }

    @Test
    void shouldFailBatchingIfIngestModeIsNotValid() {
        tableProperties.set(INGEST_BATCHER_INGEST_QUEUE, "invalid");
        addFileToStore("test-bucket/ingest.parquet");

        // When / Then
        assertThatThrownBy(() -> batchFilesWithJobIds("test-job"))
                .isInstanceOf(SleeperPropertiesInvalidException.class);
    }
}
