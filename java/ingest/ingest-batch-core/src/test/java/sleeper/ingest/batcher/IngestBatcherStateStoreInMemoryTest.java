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

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class IngestBatcherStateStoreInMemoryTest {
    private final IngestBatcherStateStore store = new IngestBatcherStateStoreInMemory();

    @Test
    void shouldTrackOneFile() {
        // Given
        FileIngestRequest fileIngestRequest = fileRequest()
                .pathToFile("test-bucket/test.parquet")
                .tableName("test-table").build();

        // When
        store.addFile(fileIngestRequest);

        // Then
        assertThat(store.getAllFiles())
                .containsExactly(fileIngestRequest);
        assertThat(store.getPendingFiles())
                .containsExactly(fileIngestRequest);
    }

    @Test
    void shouldOverwriteTrackingInformationWhenAddingTheSameFileTwice() {
        // Given
        FileIngestRequest fileIngestRequest1 = fileRequest()
                .pathToFile("test-bucket/test.parquet")
                .tableName("test-table")
                .fileSizeBytes(1024).build();
        FileIngestRequest fileIngestRequest2 = fileRequest()
                .pathToFile("test-bucket/test.parquet")
                .tableName("test-table")
                .fileSizeBytes(2048).build();

        // When
        store.addFile(fileIngestRequest1);
        store.addFile(fileIngestRequest2);

        // Then
        assertThat(store.getAllFiles())
                .containsExactly(fileIngestRequest2);
        assertThat(store.getPendingFiles())
                .containsExactly(fileIngestRequest2);
    }

    @Test
    void shouldTrackTheSameFileForMultipleTables() {
        // Given
        FileIngestRequest fileIngestRequest1 = fileRequest()
                .pathToFile("test-bucket/test.parquet")
                .tableName("test-table-1").build();
        FileIngestRequest fileIngestRequest2 = fileRequest()
                .pathToFile("test-bucket/test.parquet")
                .tableName("test-table-2").build();

        // When
        store.addFile(fileIngestRequest1);
        store.addFile(fileIngestRequest2);

        // Then
        assertThat(store.getAllFiles())
                .containsExactly(fileIngestRequest1, fileIngestRequest2);
        assertThat(store.getPendingFiles())
                .containsExactly(fileIngestRequest1, fileIngestRequest2);
    }

    @Test
    void shouldTrackJobWasCreatedWithTwoFiles() {
        // Given
        FileIngestRequest fileIngestRequest1 = fileRequest()
                .pathToFile("test-bucket/test-1.parquet")
                .tableName("test-table-1").build();
        FileIngestRequest fileIngestRequest2 = fileRequest()
                .pathToFile("test-bucket/test-2.parquet")
                .tableName("test-table-1").build();

        // When
        store.addFile(fileIngestRequest1);
        store.addFile(fileIngestRequest2);
        store.assignJob("test-job", List.of(fileIngestRequest1, fileIngestRequest2));

        // Then
        assertThat(store.getAllFiles()).containsExactly(
                onJob("test-job", fileIngestRequest1),
                onJob("test-job", fileIngestRequest2));
        assertThat(store.getPendingFiles()).isEmpty();
    }

    @Test
    void shouldSendSameFileTwiceIfFirstRequestAssignedToJob() {
        // Given
        FileIngestRequest fileIngestRequest1 = fileRequest()
                .pathToFile("test-bucket/test.parquet")
                .tableName("test-table-1").build();
        FileIngestRequest fileIngestRequest2 = fileRequest()
                .pathToFile("test-bucket/test.parquet")
                .tableName("test-table-1").build();

        // When
        store.addFile(fileIngestRequest1);
        store.assignJob("test-job", List.of(fileIngestRequest1));
        store.addFile(fileIngestRequest2);

        // Then
        assertThat(store.getAllFiles()).containsExactly(
                onJob("test-job", fileIngestRequest1),
                fileIngestRequest2);
        assertThat(store.getPendingFiles()).containsExactly(fileIngestRequest2);
    }

    @Test
    void shouldRetainAllFileRequestParametersAfterAssigningToJob() {
        // Given
        FileIngestRequest fileIngestRequest = fileRequest()
                .pathToFile("test-bucket/test.parquet")
                .tableName("test-table-1")
                .fileSizeBytes(1234L).build();

        // When
        store.addFile(fileIngestRequest);
        store.assignJob("test-job", List.of(fileIngestRequest));

        // Then
        assertThat(store.getAllFiles()).containsExactly(
                fileRequest()
                        .pathToFile("test-bucket/test.parquet")
                        .tableName("test-table-1")
                        .fileSizeBytes(1234L)
                        .jobId("test-job")
                        .build());
    }

    private static FileIngestRequest onJob(String jobId, FileIngestRequest request) {
        return request.toBuilder().jobId(jobId).build();
    }

    private static FileIngestRequest.Builder fileRequest() {
        return FileIngestRequest.builder().fileSizeBytes(1024);
    }

}
