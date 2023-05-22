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
import static sleeper.configuration.properties.table.TableProperty.INGEST_BATCHER_MIN_JOB_FILES;
import static sleeper.configuration.properties.table.TableProperty.INGEST_BATCHER_MIN_JOB_SIZE;
import static sleeper.ingest.batcher.FileIngestRequestTestHelper.onJob;

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
}
