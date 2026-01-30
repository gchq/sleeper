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

package sleeper.systemtest.suite;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import sleeper.systemtest.dsl.SleeperDsl;
import sleeper.systemtest.dsl.extension.AfterTestReports;
import sleeper.systemtest.dsl.reporting.SystemTestReports;
import sleeper.systemtest.suite.testutil.SystemTest;

import java.nio.file.Path;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.systemtest.suite.fixtures.SystemTestInstance.MAIN;

@SystemTest
public class PythonIngestST {
    @TempDir
    private Path tempDir;

    @BeforeEach
    void setup(SleeperDsl sleeper, AfterTestReports reporting) {
        reporting.reportIfTestFailed(SystemTestReports.SystemTestBuilder::ingestTasksAndJobs);
        sleeper.connectToInstanceAddOnlineTable(MAIN);
    }

    @Test
    void shouldBatchWriteOneFile(SleeperDsl sleeper) {
        // Given
        sleeper.localFiles(tempDir)
                .createWithNumberedRows("file.parquet", LongStream.range(0, 100));

        // When
        sleeper.pythonApi()
                .ingestByQueue().uploadingLocalFile(tempDir, "file.parquet")
                .waitForTask().waitForJobs();

        // Then
        assertThat(sleeper.directQuery().allRowsInTable())
                .containsExactlyElementsOf(sleeper.generateNumberedRows().iterableOverRange(0, 100));
        assertThat(sleeper.tableFiles().references()).hasSize(1);
    }

    @Test
    void shouldIngestTwoFilesFromS3(SleeperDsl sleeper) {
        // Given
        sleeper.sourceFiles()
                .createWithNumberedRows("file1.parquet", LongStream.range(0, 100))
                .createWithNumberedRows("file2.parquet", LongStream.range(100, 200));

        // When
        sleeper.pythonApi()
                .ingestByQueue().fromS3("file1.parquet", "file2.parquet")
                .waitForTask().waitForJobs();

        // Then
        assertThat(sleeper.directQuery().allRowsInTable())
                .containsExactlyElementsOf(sleeper.generateNumberedRows().iterableOverRange(0, 200));
        assertThat(sleeper.tableFiles().references()).hasSize(1);
    }

    @Test
    void shouldIngestDirectoryFromS3(SleeperDsl sleeper) {
        // Given
        sleeper.sourceFiles()
                .createWithNumberedRows("test-dir/file1.parquet", LongStream.range(0, 100))
                .createWithNumberedRows("test-dir/file2.parquet", LongStream.range(100, 200));

        // When
        sleeper.pythonApi()
                .ingestByQueue().fromS3("test-dir")
                .waitForTask().waitForJobs();

        // Then
        assertThat(sleeper.directQuery().allRowsInTable())
                .containsExactlyElementsOf(sleeper.generateNumberedRows().iterableOverRange(0, 200));
        assertThat(sleeper.tableFiles().references()).hasSize(1);
    }
}
