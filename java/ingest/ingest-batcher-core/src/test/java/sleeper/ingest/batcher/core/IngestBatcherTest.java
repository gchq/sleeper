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
package sleeper.ingest.batcher.core;

import org.junit.jupiter.api.Test;

import sleeper.core.properties.table.TableProperties;
import sleeper.ingest.core.job.IngestJob;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

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

}
