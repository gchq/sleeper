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
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import sleeper.systemtest.suite.dsl.SleeperSystemTest;

import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.systemtest.suite.fixtures.SystemTestInstance.MAIN;
import static sleeper.systemtest.suite.testutil.TestContextFactory.testContext;

@Tag("SystemTest")
public class IngestIT {
    private final SleeperSystemTest sleeper = SleeperSystemTest.getInstance();

    @BeforeEach
    void setUp() {
        sleeper.connectToInstance(MAIN);
        sleeper.reporting().startRecording();
    }

    @AfterEach
    void tearDown(TestInfo testInfo) {
        sleeper.reporting().printIngestTasksAndJobs(testContext(testInfo));
    }

    @Test
    void shouldIngest1File() throws InterruptedException {
        // Given
        sleeper.sourceFiles()
                .createWithNumberedRecords("file.parquet", LongStream.range(0, 100));

        // When
        sleeper.ingest().byQueue().sendSourceFiles("file.parquet")
                .invokeTasks().waitForJobs();

        // Then
        assertThat(sleeper.directQuery().allRecordsInTable())
                .containsExactlyElementsOf(sleeper.generateNumberedRecords(LongStream.range(0, 100)));
        assertThat(sleeper.stateStore().numActiveFiles()).isEqualTo(1);
    }

    @Test
    void shouldIngest4FilesInOneJob() throws InterruptedException {
        // Given
        sleeper.sourceFiles()
                .createWithNumberedRecords("file1.parquet", LongStream.range(0, 100))
                .createWithNumberedRecords("file2.parquet", LongStream.range(100, 200))
                .createWithNumberedRecords("file3.parquet", LongStream.range(200, 300))
                .createWithNumberedRecords("file4.parquet", LongStream.range(300, 400));

        // When
        sleeper.ingest().byQueue().sendSourceFiles("file1.parquet", "file2.parquet", "file3.parquet", "file4.parquet")
                .invokeTasks().waitForJobs();

        // Then
        assertThat(sleeper.directQuery().allRecordsInTable())
                .containsExactlyElementsOf(sleeper.generateNumberedRecords(LongStream.range(0, 400)));
        assertThat(sleeper.stateStore().numActiveFiles()).isEqualTo(1);
    }

    @Test
    void shouldIngest4FilesInTwoJobs() throws InterruptedException {
        // Given
        sleeper.sourceFiles()
                .createWithNumberedRecords("file1.parquet", LongStream.range(0, 100))
                .createWithNumberedRecords("file2.parquet", LongStream.range(100, 200))
                .createWithNumberedRecords("file3.parquet", LongStream.range(200, 300))
                .createWithNumberedRecords("file4.parquet", LongStream.range(300, 400));

        // When
        sleeper.ingest().byQueue()
                .sendSourceFiles("file1.parquet", "file2.parquet")
                .sendSourceFiles("file3.parquet", "file4.parquet")
                .invokeTasks().waitForJobs();

        // Then
        assertThat(sleeper.directQuery().allRecordsInTable())
                .containsExactlyElementsOf(sleeper.generateNumberedRecords(LongStream.range(0, 400)));
        assertThat(sleeper.stateStore().numActiveFiles()).isEqualTo(2);
    }
}
