/*
 * Copyright 2022-2026 Crown Copyright
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

import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.testutils.FixedTablePropertiesProvider;
import sleeper.ingest.batcher.core.testutil.InMemoryIngestBatcherStore;
import sleeper.ingest.core.job.IngestJob;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

public class IngestBatcherTest extends IngestBatcherTestBase {

    @Test
    void shouldDeleteFileWhenTableWasDeleted() {
        // Given
        addFileToStore(ingestRequest()
                .tableId("deleted-table")
                .build());

        // When
        batchFilesWithTablesAndJobIds(List.of(), List.of());

        // Then
        assertThat(queues.getMessagesByQueueUrl()).isEmpty();
        assertThat(store.getAllFilesNewestFirst()).isEmpty();
    }

    @Test
    void shouldCreateJobsWhenOneTableWasDeleted() {
        // Given
        IngestBatcherTrackedFile file1 = ingestRequest()
                .tableId("table-1")
                .file("file-1")
                .build();
        IngestBatcherTrackedFile file2 = ingestRequest()
                .tableId("table-2-deleted")
                .file("file-2-ignore")
                .build();
        IngestBatcherTrackedFile file3 = ingestRequest()
                .tableId("table-3")
                .file("file-3")
                .build();
        addFilesToStore(file1, file2, file3);
        List<TableProperties> tables = List.of(
                createTableProperties("table-1"),
                createTableProperties("table-3"));

        // When
        batchFilesWithTablesAndJobIds(tables, List.of("job-1", "job-2"));

        // Then
        assertThat(queues.getMessagesByQueueUrl()).isEqualTo(queueMessages(
                IngestJob.builder()
                        .tableId("table-1")
                        .id("job-1")
                        .files(List.of("file-1"))
                        .build(),
                IngestJob.builder()
                        .tableId("table-3")
                        .id("job-2")
                        .files(List.of("file-3"))
                        .build()));
        assertThat(store.getAllFilesNewestFirst()).containsExactly(
                file3.toBuilder().jobId("job-2").build(),
                file1.toBuilder().jobId("job-1").build());
    }

    @Test
    void shouldCreateJobsForOtherTablesWhenFilesForDeletedTableCannotBeDeleted() {
        // Given
        IngestBatcherStore store = storeFailingDeletes();
        IngestBatcherTrackedFile file1 = ingestRequest()
                .tableId("table-1")
                .file("file-1")
                .build();
        IngestBatcherTrackedFile file2 = ingestRequest()
                .tableId("table-2-deleted")
                .file("file-2-ignore")
                .build();
        IngestBatcherTrackedFile file3 = ingestRequest()
                .tableId("table-3")
                .file("file-3")
                .build();
        store.addFile(file1);
        store.addFile(file2);
        store.addFile(file3);
        List<TableProperties> tables = List.of(
                createTableProperties("table-1"),
                createTableProperties("table-3"));

        // When
        Throwable failure = catchThrowable(() -> batchFilesWithJobIds(List.of("job-1", "job-2"), builder -> builder
                .tablePropertiesProvider(new FixedTablePropertiesProvider(tables))
                .store(store)));

        // Then
        assertThat(failure).hasMessage("Failed deleting files for table table-2-deleted");
        assertThat(queues.getMessagesByQueueUrl()).isEqualTo(queueMessages(
                IngestJob.builder()
                        .tableId("table-1")
                        .id("job-1")
                        .files(List.of("file-1"))
                        .build(),
                IngestJob.builder()
                        .tableId("table-3")
                        .id("job-2")
                        .files(List.of("file-3"))
                        .build()));
        assertThat(store.getAllFilesNewestFirst()).containsExactly(
                file3.toBuilder().jobId("job-2").build(),
                file2,
                file1.toBuilder().jobId("job-1").build());
    }

    @Test
    void shouldReportFailuresForAllTablesWhenFilesForMultipleDeletedTablesCannotBeDeleted() {
        // Given
        IngestBatcherStore store = storeFailingDeletes();
        store.addFile(ingestRequest()
                .tableId("table-1-deleted")
                .file("file-1")
                .build());
        store.addFile(ingestRequest()
                .tableId("table-2-deleted")
                .file("file-2")
                .build());

        // When
        Throwable failure = catchThrowable(() -> batchFilesWithJobIds(List.of(), builder -> builder
                .tablePropertiesProvider(new FixedTablePropertiesProvider(List.of()))
                .store(store)));

        // Then
        assertThat(failure).hasMessage("Failed deleting files for table table-1-deleted");
        assertThat(failure.getSuppressed())
                .extracting(Throwable::getMessage)
                .containsExactly("Failed deleting files for table table-2-deleted");
    }

    private IngestBatcherStore storeFailingDeletes() {
        return new InMemoryIngestBatcherStore() {
            @Override
            public void deleteFiles(List<IngestBatcherTrackedFile> files) {
                throw new RuntimeException("Failed deleting files for table " + files.get(0).getTableId());
            }
        };
    }
}
