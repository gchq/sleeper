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
package sleeper.ingest.batcher.core.testutil;

import sleeper.ingest.batcher.core.IngestBatcherTrackedFile;

import java.time.Duration;
import java.time.Instant;

public class FileIngestRequestTestHelper {

    public static final String DEFAULT_TABLE_ID = "test-table-id";
    public static final Instant FIRST_REQUEST_TIME = Instant.parse("2023-05-19T15:33:42Z");
    private int requestCount = 0;

    public IngestBatcherTrackedFile.Builder fileRequest() {
        int requestIndex = requestCount++;
        int requestNum = requestIndex + 1;
        return IngestBatcherTrackedFile.builder()
                .fileSizeBytes(1024)
                .tableId(DEFAULT_TABLE_ID)
                .file("test-bucket/auto-named-file-" + requestNum + ".parquet")
                .receivedTime(FIRST_REQUEST_TIME.plus(Duration.ofSeconds(requestIndex)));
    }

    public static IngestBatcherTrackedFile onJob(String jobId, IngestBatcherTrackedFile request) {
        return request.toBuilder().jobId(jobId).build();
    }
}
