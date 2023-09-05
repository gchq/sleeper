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

package sleeper.systemtest.suite;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.io.TempDir;

import sleeper.configuration.properties.table.TableProperty;
import sleeper.systemtest.suite.dsl.SleeperSystemTest;

import java.io.IOException;
import java.nio.file.Path;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.systemtest.suite.fixtures.SystemTestInstance.MAIN;
import static sleeper.systemtest.suite.testutil.TestContextFactory.testContext;

@Tag("SystemTest")
public class PythonApiIT {
    @TempDir
    private Path tempDir;
    private final SleeperSystemTest sleeper = SleeperSystemTest.getInstance();

    @BeforeEach
    void setup() {
        sleeper.connectToInstance(MAIN);
        sleeper.reporting().startRecording();
    }

    @AfterEach
    void tearDown(TestInfo testInfo) {
        sleeper.reporting().printIngestTasksAndJobs(testContext(testInfo));
    }

    @Nested
    @DisplayName("Ingest files")
    @Disabled("temporarily disabling to test other nested class")
    class IngestFiles {
        @Test
        void shouldBatchWriteOneFile() throws IOException, InterruptedException {
            // Given
            sleeper.localFiles(tempDir)
                    .createWithNumberedRecords("file.parquet", LongStream.range(0, 100));

            // When
            sleeper.pythonApi(tempDir)
                    .ingest().batchWrite("file.parquet")
                    .invokeTask().waitForJobs();

            // Then
            assertThat(sleeper.directQuery().allRecordsInTable())
                    .containsExactlyElementsOf(sleeper.generateNumberedRecords(LongStream.range(0, 100)));
            assertThat(sleeper.tableFiles().active()).hasSize(1);
        }

        @Test
        void shouldIngestTwoFilesFromS3() throws IOException, InterruptedException {
            // Given
            sleeper.sourceFiles()
                    .createWithNumberedRecords("file1.parquet", LongStream.range(0, 100))
                    .createWithNumberedRecords("file2.parquet", LongStream.range(100, 200));

            // When
            sleeper.pythonApi(tempDir)
                    .ingest().fromS3("file1.parquet", "file2.parquet")
                    .invokeTask().waitForJobs();

            // Then
            assertThat(sleeper.directQuery().allRecordsInTable())
                    .containsExactlyElementsOf(sleeper.generateNumberedRecords(LongStream.range(0, 200)));
            assertThat(sleeper.tableFiles().active()).hasSize(1);
        }

        @Test
        void shouldIngestDirectoryFromS3() throws IOException, InterruptedException {
            // Given
            sleeper.sourceFiles()
                    .createWithNumberedRecords("test-dir/file1.parquet", LongStream.range(0, 100))
                    .createWithNumberedRecords("test-dir/file2.parquet", LongStream.range(100, 200));

            // When
            sleeper.pythonApi(tempDir)
                    .ingest().fromS3("test-dir")
                    .invokeTask().waitForJobs();

            // Then
            assertThat(sleeper.directQuery().allRecordsInTable())
                    .containsExactlyElementsOf(sleeper.generateNumberedRecords(LongStream.range(0, 200)));
            assertThat(sleeper.tableFiles().active()).hasSize(1);
        }

        @Test
        void shouldBulkImportFilesFromS3() throws IOException, InterruptedException {
            // Given
            sleeper.updateTableProperties(tableProperties ->
                    tableProperties.set(TableProperty.BULK_IMPORT_MIN_LEAF_PARTITION_COUNT, "1"));
            sleeper.sourceFiles()
                    .createWithNumberedRecords("file1.parquet", LongStream.range(0, 100))
                    .createWithNumberedRecords("file2.parquet", LongStream.range(100, 200));

            // When
            sleeper.pythonApi(tempDir)
                    .bulkImport().emrServerless("file1.parquet", "file2.parquet")
                    .waitForJobs();

            // Then
            assertThat(sleeper.directQuery().allRecordsInTable())
                    .containsExactlyElementsOf(sleeper.generateNumberedRecords(LongStream.range(0, 200)));
            assertThat(sleeper.tableFiles().active()).hasSize(1);
        }
    }

    @Nested
    @DisplayName("Run SQS query")
    class RunSQSQuery {
        @Test
        void shouldRunExactKeyQueryWithMapOfKeyToListOfKeys() throws IOException, InterruptedException {
            // Given
            sleeper.ingest().direct(tempDir).numberedRecords(LongStream.range(0, 100));

            // When/Then
            assertThat(sleeper.pythonApi(tempDir)
                    .query().exactKeys("key",
                            "row-0000000000000000001",
                            "row-0000000000000000002")
                    .results())
                    .containsExactlyElementsOf(sleeper.generateNumberedRecords(LongStream.rangeClosed(1, 2)));
        }

        @Test
        void shouldRunRangeKeyQuery() throws IOException, InterruptedException {
            // Given
            sleeper.ingest().direct(tempDir).numberedRecords(LongStream.range(0, 100));

            // When/Then
            assertThat(sleeper.pythonApi(tempDir)
                    .query().range("key",
                            "row-0000000000000000010",
                            "row-0000000000000000020")
                    .results())
                    .containsExactlyElementsOf(sleeper.generateNumberedRecords(LongStream.range(10, 20)));
        }
    }
}
