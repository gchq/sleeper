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

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class FileIngestRequestTestHelperTest {

    private final FileIngestRequestTestHelper helper = new FileIngestRequestTestHelper();
    private final FileIngestRequestTestHelper assertHelper = new FileIngestRequestTestHelper();

    @Test
    void shouldGenerateDefaultValues() {
        assertThat(helper.fileRequest().build()).isEqualTo(
                FileIngestRequest.builder()
                        .pathToFile("test-bucket/auto-named-file-1.parquet")
                        .fileSizeBytes(1024)
                        .tableName("test-table")
                        .receivedTime(Instant.parse("2023-05-19T15:33:42Z"))
                        .build());
    }

    @Test
    void shouldGenerateFilePaths() {
        assertThat(List.of(
                helper.fileRequest().build(),
                helper.fileRequest().build(),
                helper.fileRequest().build()
        )).containsExactly(
                assertHelper.fileRequest().pathToFile("test-bucket/auto-named-file-1.parquet").build(),
                assertHelper.fileRequest().pathToFile("test-bucket/auto-named-file-2.parquet").build(),
                assertHelper.fileRequest().pathToFile("test-bucket/auto-named-file-3.parquet").build()
        );
    }

    @Test
    void shouldGenerateReceivedTimes() {
        assertThat(List.of(
                helper.fileRequest().build(),
                helper.fileRequest().build(),
                helper.fileRequest().build()
        )).containsExactly(
                assertHelper.fileRequest().receivedTime(Instant.parse("2023-05-19T15:33:42Z")).build(),
                assertHelper.fileRequest().receivedTime(Instant.parse("2023-05-19T15:33:43Z")).build(),
                assertHelper.fileRequest().receivedTime(Instant.parse("2023-05-19T15:33:44Z")).build()
        );
    }
}
