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

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import sleeper.systemtest.suite.dsl.SleeperSystemTest;

import java.io.IOException;
import java.nio.file.Path;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.systemtest.suite.fixtures.SystemTestInstance.MAIN;
import static sleeper.systemtest.suite.testutil.PythonApiTestHelper.PYTHON_DIR;
import static sleeper.systemtest.suite.testutil.PythonApiTestHelper.buildPythonApi;

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
    }

    @Test
    void shouldIngestOneFileWithTenRecords() throws IOException, InterruptedException {
        // Given
        Path file = tempDir.resolve("file.parquet");
        sleeper.localFiles().createWithNumberedRecords(file, LongStream.range(0, 10));

        // When
        sleeper.pythonApi(PYTHON_DIR).ingest().byQueue(file);
        sleeper.ingest().byQueue().invokeTasks().waitForJobs();

        // Then
        assertThat(sleeper.directQuery().allRecordsInTable())
                .containsExactlyElementsOf(sleeper.generateNumberedRecords(LongStream.range(0, 10)));
    }
}
