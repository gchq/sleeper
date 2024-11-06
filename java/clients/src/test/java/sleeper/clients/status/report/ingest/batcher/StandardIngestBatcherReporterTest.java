/*
 * Copyright 2022-2024 Crown Copyright
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

package sleeper.clients.status.report.ingest.batcher;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import sleeper.core.table.InMemoryTableIndex;
import sleeper.core.table.TableIndex;
import sleeper.ingest.batcher.core.FileIngestRequest;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.clients.status.report.ingest.batcher.IngestBatcherReporterTestHelper.TEST_TABLE;
import static sleeper.clients.status.report.ingest.batcher.IngestBatcherReporterTestHelper.filesWithLargeAndDecimalSizes;
import static sleeper.clients.status.report.ingest.batcher.IngestBatcherReporterTestHelper.multiplePendingFiles;
import static sleeper.clients.status.report.ingest.batcher.IngestBatcherReporterTestHelper.onePendingAndTwoBatchedFiles;
import static sleeper.clients.testutil.ClientTestUtils.example;

public class StandardIngestBatcherReporterTest {

    private final TableIndex tableIndex = new InMemoryTableIndex();

    @BeforeEach
    void setUp() {
        tableIndex.create(TEST_TABLE);
    }

    @Nested
    @DisplayName("Query all files")
    class QueryAllFiles {
        @Test
        void shouldReportNoFiles() throws IOException {
            // Given
            List<FileIngestRequest> noFiles = Collections.emptyList();

            // When / Then
            assertThat(getStandardReport(BatcherQuery.Type.ALL, noFiles)).hasToString(
                    example("reports/ingest/batcher/standard/all/noFiles.txt"));
        }

        @Test
        void shouldReportOnePendingFileAndTwoBatchedFiles() throws IOException {
            // Given
            List<FileIngestRequest> fileIngestRequestList = onePendingAndTwoBatchedFiles();

            // When / Then
            assertThat(getStandardReport(BatcherQuery.Type.ALL, fileIngestRequestList)).hasToString(
                    example("reports/ingest/batcher/standard/all/onePendingAndTwoBatchedFiles.txt"));
        }

        @Test
        void shouldReportLargeAndDecimalFileSizes() throws IOException {
            // Given
            List<FileIngestRequest> fileIngestRequestList = filesWithLargeAndDecimalSizes();

            // When / Then
            assertThat(getStandardReport(BatcherQuery.Type.ALL, fileIngestRequestList)).hasToString(
                    example("reports/ingest/batcher/standard/all/largeAndDecimalFileSizes.txt"));
        }
    }

    @Nested
    @DisplayName("Query pending files")
    class QueryPendingFiles {
        @Test
        void shouldReportNoFiles() throws IOException {
            // Given
            List<FileIngestRequest> noFiles = Collections.emptyList();

            // When / Then
            assertThat(getStandardReport(BatcherQuery.Type.PENDING, noFiles)).hasToString(
                    example("reports/ingest/batcher/standard/pending/noFiles.txt"));
        }

        @Test
        void shouldReportMultiplePendingFiles() throws IOException {
            // Given
            List<FileIngestRequest> fileIngestRequestList = multiplePendingFiles();

            // When / Then
            assertThat(getStandardReport(BatcherQuery.Type.PENDING, fileIngestRequestList)).hasToString(
                    example("reports/ingest/batcher/standard/pending/multiplePendingFiles.txt"));
        }
    }

    private String getStandardReport(BatcherQuery.Type queryType, List<FileIngestRequest> fileRequestList) {
        return IngestBatcherReporterTestHelper.getStandardReport(tableIndex, queryType, fileRequestList);
    }
}
