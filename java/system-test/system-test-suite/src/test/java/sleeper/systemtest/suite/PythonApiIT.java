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
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.io.TempDir;

import sleeper.systemtest.suite.dsl.SleeperSystemTest;

import java.io.IOException;
import java.nio.file.Path;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.systemtest.suite.fixtures.SystemTestInstance.MAIN;
import static sleeper.systemtest.suite.testutil.PythonApiTestHelper.PYTHON_DIR;
import static sleeper.systemtest.suite.testutil.PythonApiTestHelper.buildPythonApi;
import static sleeper.systemtest.suite.testutil.TestContextFactory.testContext;

@Tag("SystemTest")
public class PythonApiIT {
    @TempDir
    private Path tempDir;
    private final SleeperSystemTest sleeper = SleeperSystemTest.getInstance();

    @BeforeAll
    static void beforeAll() throws IOException, InterruptedException {
        buildPythonApi();
    }

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
    @DisplayName("Standard ingest")
    class StandardIngest {
        @Test
        void shouldBatchWriteOneFile() throws IOException, InterruptedException {
            // Given
            sleeper.localFiles(tempDir)
                    .createWithNumberedRecords("file.parquet", LongStream.range(0, 100));

            // When
            sleeper.pythonApi(PYTHON_DIR, tempDir)
                    .ingest().batchWrite("file.parquet")
                    .invokeTasks().waitForJobs();

            // Then
            assertThat(sleeper.directQuery().allRecordsInTable())
                    .containsExactlyElementsOf(sleeper.generateNumberedRecords(LongStream.range(0, 100)));
            assertThat(sleeper.stateStore().numActiveFiles()).isEqualTo(1);
        }

        @Test
        void shouldIngestTwoFilesFromS3() throws IOException, InterruptedException {
            // Given
            sleeper.sourceFiles()
                    .createWithNumberedRecords("file1.parquet", LongStream.range(0, 100))
                    .createWithNumberedRecords("file2.parquet", LongStream.range(100, 200));

            // When
            sleeper.pythonApi(PYTHON_DIR, tempDir)
                    .ingest().fromS3("file1.parquet", "file2.parquet")
                    .invokeTasks().waitForJobs();

            // Then
            assertThat(sleeper.directQuery().allRecordsInTable())
                    .containsExactlyElementsOf(sleeper.generateNumberedRecords(LongStream.range(0, 200)));
            assertThat(sleeper.stateStore().numActiveFiles()).isEqualTo(1);
        }
    }
}
