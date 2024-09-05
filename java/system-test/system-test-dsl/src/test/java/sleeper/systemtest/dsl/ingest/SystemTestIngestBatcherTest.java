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

package sleeper.systemtest.dsl.ingest;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import sleeper.systemtest.dsl.SleeperSystemTest;
import sleeper.systemtest.dsl.sourcedata.RecordNumbers;
import sleeper.systemtest.dsl.testutil.InMemoryDslTest;
import sleeper.systemtest.dsl.testutil.InMemorySystemTestDrivers;

import java.util.Map;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.configuration.properties.table.TableProperty.INGEST_BATCHER_INGEST_QUEUE;
import static sleeper.configuration.properties.table.TableProperty.INGEST_BATCHER_MAX_JOB_FILES;
import static sleeper.configuration.properties.table.TableProperty.INGEST_BATCHER_MIN_JOB_FILES;
import static sleeper.configuration.properties.table.TableProperty.INGEST_BATCHER_MIN_JOB_SIZE;
import static sleeper.configuration.properties.validation.IngestQueue.STANDARD_INGEST;
import static sleeper.systemtest.dsl.testutil.InMemoryTestInstance.MAIN;

@InMemoryDslTest
public class SystemTestIngestBatcherTest {

    @BeforeEach
    void setUp(SleeperSystemTest sleeper) {
        sleeper.connectToInstance(MAIN);
    }

    @Test
    void shouldCreateTwoStandardIngestJobsWithMaxJobFilesOfThree(SleeperSystemTest sleeper, InMemorySystemTestDrivers drivers) {
        // Given
        sleeper.updateTableProperties(Map.of(
                INGEST_BATCHER_INGEST_QUEUE, STANDARD_INGEST.toString(),
                INGEST_BATCHER_MIN_JOB_FILES, "1",
                INGEST_BATCHER_MIN_JOB_SIZE, "1K",
                INGEST_BATCHER_MAX_JOB_FILES, "3"));
        drivers.fixSizeOfFilesSeenByBatcherInBytes(1024);
        RecordNumbers numbers = sleeper.scrambleNumberedRecords(LongStream.range(0, 400));
        sleeper.sourceFiles()
                .createWithNumberedRecords("file1.parquet", numbers.range(0, 100))
                .createWithNumberedRecords("file2.parquet", numbers.range(100, 200))
                .createWithNumberedRecords("file3.parquet", numbers.range(200, 300))
                .createWithNumberedRecords("file4.parquet", numbers.range(300, 400));

        // When
        SystemTestIngestBatcher.Result result = sleeper.ingest().batcher()
                .sendSourceFiles("file1.parquet", "file2.parquet", "file3.parquet", "file4.parquet")
                .invoke().invokeStandardIngestTask().waitForIngestJobs().getInvokeResult();

        // Then
        assertThat(result.numJobsCreated()).isEqualTo(2);
        assertThat(sleeper.directQuery().allRecordsInTable())
                .containsExactlyInAnyOrderElementsOf(sleeper.generateNumberedRecords(LongStream.range(0, 400)));
        assertThat(sleeper.tableFiles().references()).hasSize(2);
    }

}
