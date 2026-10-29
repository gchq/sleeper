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
package sleeper.clients.table;

import org.junit.jupiter.api.Test;

import sleeper.clients.testutil.ToStringConsoleOutput;
import sleeper.configuration.utils.S3ExpandDirectoriesResult;
import sleeper.configuration.utils.S3FileDetails;
import sleeper.configuration.utils.S3Path;
import sleeper.configuration.utils.S3PathContents;
import sleeper.ingest.batcher.core.IngestBatcherSubmitRequest;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class SendToIngestBatcherTest {

    private final List<IngestBatcherSubmitRequest> sentRequests = new ArrayList<>();
    private final ToStringConsoleOutput out = new ToStringConsoleOutput();

    @Test
    void shouldSendFilesFoundInDirectory() {
        // Given
        S3ExpandDirectoriesResult result = new S3ExpandDirectoriesResult(List.of(
                new S3PathContents(S3Path.parse("test-bucket/dir"), List.of(
                        new S3FileDetails("test-bucket", "dir/file1.parquet", 1_000_000),
                        new S3FileDetails("test-bucket", "dir/file2.parquet", 2_000_000)))));

        // When
        boolean sent = SendToIngestBatcher.send("test-table", result, sentRequests::add, out.consoleOut());

        // Then
        assertThat(sent).isTrue();
        assertThat(sentRequests).containsExactly(new IngestBatcherSubmitRequest("test-table",
                List.of("test-bucket/dir/file1.parquet", "test-bucket/dir/file2.parquet")));
        assertThat(out.toString()).isEqualTo("""
                Files found:
                  test-bucket/dir/file1.parquet (1.0MB)
                  test-bucket/dir/file2.parquet (2.0MB)
                Number of files found: 2
                Total size: 3.0MB

                Sent to ingest batcher for table test-table
                """);
    }

    @Test
    void shouldSendNothingWhenAnyPathIsMissing() {
        // Given
        S3ExpandDirectoriesResult result = new S3ExpandDirectoriesResult(List.of(
                new S3PathContents(S3Path.parse("test-bucket/file.parquet"), List.of(
                        new S3FileDetails("test-bucket", "file.parquet", 1_000_000))),
                new S3PathContents(S3Path.parse("test-bucket/typo.parquet"), List.of())));

        // When
        boolean sent = SendToIngestBatcher.send("test-table", result, sentRequests::add, out.consoleOut());

        // Then
        assertThat(sent).isFalse();
        assertThat(sentRequests).isEmpty();
        assertThat(out.toString()).isEqualTo("""
                Files found:
                  test-bucket/file.parquet (1.0MB)
                Number of files found: 1
                Total size: 1.0MB

                Paths not found:
                  test-bucket/typo.parquet

                Nothing was sent to the ingest batcher, including the files found.
                Fix or remove the missing paths and try again.
                """);
    }
}
