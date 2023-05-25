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

package sleeper.ingest.batcher.submitter;

import org.junit.jupiter.api.Test;

import sleeper.ingest.batcher.FileIngestRequest;
import sleeper.ingest.batcher.IngestBatcherStore;
import sleeper.ingest.batcher.testutil.IngestBatcherStoreInMemory;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

class IngestBatcherSubmitterLambdaTest {

    private final IngestBatcherStore store = new IngestBatcherStoreInMemory();
    private final IngestBatcherSubmitterLambda lambda = new IngestBatcherSubmitterLambda(store);

    @Test
    void shouldStoreFileIngestRequestFromJson() {
        // Given
        String json = "{" +
                "\"pathToFile\":\"test-bucket/test-file-1.parquet\"," +
                "\"fileSizeBytes\":1024," +
                "\"tableName\":\"test-table\"" +
                "}";
        Instant receivedTime = Instant.parse("2023-05-19T15:33:42Z");

        // When
        lambda.handleMessage(json, receivedTime);

        // Then
        assertThat(store.getAllFilesNewestFirst())
                .containsExactly(FileIngestRequest.builder()
                        .pathToFile("test-bucket/test-file-1.parquet")
                        .fileSizeBytes(1024)
                        .tableName("test-table")
                        .receivedTime(receivedTime)
                        .build());
    }
}
