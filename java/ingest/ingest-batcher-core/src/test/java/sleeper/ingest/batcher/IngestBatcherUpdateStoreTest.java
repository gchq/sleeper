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

import sleeper.ingest.job.IngestJob;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.INGEST_JOB_QUEUE_URL;
import static sleeper.configuration.properties.table.TableProperty.INGEST_BATCHER_MIN_JOB_FILES;
import static sleeper.configuration.properties.table.TableProperty.INGEST_BATCHER_MIN_JOB_SIZE;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;
import static sleeper.ingest.batcher.testutil.FileIngestRequestTestHelper.onJob;

class IngestBatcherUpdateStoreTest extends IngestBatcherTestBase {
    @Test
    void shouldReportFileAssignedToJobWhenJobIsSent() {
        // Given
        tableProperties.set(INGEST_BATCHER_MIN_JOB_FILES, "1");
        FileIngestRequest request = addFileToStore("test-bucket/test.parquet");

        // When
        batchFilesWithJobIds("test-job-id");

        // Then
        assertThat(store.getAllFilesNewestFirst()).containsExactly(
                onJob("test-job-id", request));
        assertThat(store.getPendingFilesOldestFirst()).isEmpty();
    }

    @Test
    void shouldReportFileNotAssignedToJobWhenNotSent() {
        // Given
        tableProperties.set(INGEST_BATCHER_MIN_JOB_SIZE, "1K");
        FileIngestRequest request = addFileToStore(builder -> builder.fileSizeBytes(512));

        // When
        batchFilesWithJobIds("test-job-id");

        // Then
        assertThat(store.getAllFilesNewestFirst()).containsExactly(request);
        assertThat(store.getPendingFilesOldestFirst()).containsExactly(request);
    }

    @Test
    void shouldNotSendFileIfFailedToAssignJob() {
        // Given
        FileIngestRequest request = ingestRequest().build();
        IngestBatcherStore store = mock(IngestBatcherStore.class);
        when(store.getPendingFilesOldestFirst()).thenReturn(List.of(request));
        doThrow(new RuntimeException("Failed assigning job"))
                .when(store).assignJobGetAssigned("fail-job-id", List.of(request));

        // When
        batchFilesWithJobIds(List.of("fail-job-id"), builder -> builder.store(store));

        // Then
        assertThat(queues.getMessagesByQueueUrl()).isEmpty();
        verify(store).assignJobGetAssigned("fail-job-id", List.of(request));
    }

    @Test
    void shouldLeaveFileAssignedIfFailedToSendJob() {
        // Given
        tableProperties.set(TABLE_NAME, "fail-table");
        FileIngestRequest request = addFileToStore(builder -> builder
                .file("test-bucket/fail.parquet")
                .tableName("fail-table"));
        IngestJob expectedJob = IngestJob.builder()
                .id("fail-job-id")
                .tableName("fail-table")
                .files("test-bucket/fail.parquet")
                .build();

        instanceProperties.set(INGEST_JOB_QUEUE_URL, "fail-ingest-queue-url");
        IngestBatcherQueueClient queueClient = mock(IngestBatcherQueueClient.class);
        doThrow(new RuntimeException("Failed sending job"))
                .when(queueClient).send("fail-ingest-queue-url", expectedJob);

        // When
        batchFilesWithJobIds(List.of("fail-job-id"), builder -> builder.queueClient(queueClient));

        // Then
        assertThat(store.getPendingFilesOldestFirst()).isEmpty();
        assertThat(store.getAllFilesNewestFirst())
                .containsExactly(onJob("fail-job-id", request));
        verify(queueClient).send("fail-ingest-queue-url", expectedJob);
    }
}
