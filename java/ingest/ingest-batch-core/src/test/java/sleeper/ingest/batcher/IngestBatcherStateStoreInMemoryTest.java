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

import static org.assertj.core.api.Assertions.assertThat;

public class IngestBatcherStateStoreInMemoryTest {
    private final IngestBatcherStateStore store = new IngestBatcherStateStoreInMemory();

    @Test
    void shouldTrackOneFile() {
        // Given
        FileIngestRequest fileIngestRequest = FileIngestRequest.builder()
                .pathToFile("test-bucket/test.parquet")
                .tableName("test-table").build();

        // When
        store.addFile(fileIngestRequest);

        // Then
        assertThat(store.getAllFiles())
                .containsExactly(fileIngestRequest);
    }

    @Test
    void shouldNotTrackFileWhenAddingTheSameFileTwice() {
        // Given
        FileIngestRequest fileIngestRequest = FileIngestRequest.builder()
                .pathToFile("test-bucket/test.parquet")
                .tableName("test-table").build();

        // When
        store.addFile(fileIngestRequest);
        store.addFile(fileIngestRequest);

        // Then
        assertThat(store.getAllFiles())
                .containsExactly(fileIngestRequest);
    }
}
