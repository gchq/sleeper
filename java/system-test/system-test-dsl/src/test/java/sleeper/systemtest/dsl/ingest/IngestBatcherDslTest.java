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

package sleeper.systemtest.dsl.ingest;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import sleeper.systemtest.dsl.SleeperDsl;
import sleeper.systemtest.dsl.sourcedata.RowNumbers;
import sleeper.systemtest.dsl.testutil.InMemoryDslTest;
import sleeper.systemtest.dsl.testutil.InMemorySystemTestDrivers;

import java.util.Map;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.model.IngestQueue.STANDARD_INGEST;
import static sleeper.core.properties.table.TableProperty.INGEST_BATCHER_INGEST_QUEUE;
import static sleeper.core.properties.table.TableProperty.INGEST_BATCHER_MAX_JOB_FILES;
import static sleeper.core.properties.table.TableProperty.INGEST_BATCHER_MIN_JOB_FILES;
import static sleeper.core.properties.table.TableProperty.INGEST_BATCHER_MIN_JOB_SIZE;
import static sleeper.systemtest.dsl.testutil.InMemoryTestInstance.IN_MEMORY_MAIN;

@InMemoryDslTest
public class IngestBatcherDslTest {

    @BeforeEach
    void setUp(SleeperDsl sleeper) {
        sleeper.connectToInstanceAddOnlineTable(IN_MEMORY_MAIN);
    }

    @Test
    void shouldCreateTwoStandardIngestJobsWithMaxJobFilesOfThree(SleeperDsl sleeper, InMemorySystemTestDrivers drivers) {
        // Given
        sleeper.updateTableProperties(Map.of(
                INGEST_BATCHER_INGEST_QUEUE, STANDARD_INGEST.toString(),
                INGEST_BATCHER_MIN_JOB_FILES, "1",
                INGEST_BATCHER_MIN_JOB_SIZE, "1K",
                INGEST_BATCHER_MAX_JOB_FILES, "3"));
        drivers.fixSizeOfFilesSeenByBatcherInBytes(1024);
        RowNumbers numbers = sleeper.scrambleNumberedRows(LongStream.range(0, 400));
        sleeper.sourceFiles()
                .createWithNumberedRows("file1.parquet", numbers.range(0, 100))
                .createWithNumberedRows("file2.parquet", numbers.range(100, 200))
                .createWithNumberedRows("file3.parquet", numbers.range(200, 300))
                .createWithNumberedRows("file4.parquet", numbers.range(300, 400));

        // When
        sleeper.ingest().batcher()
                .sendSourceFilesExpectingJobs(2, "file1.parquet", "file2.parquet", "file3.parquet", "file4.parquet")
                .waitForStandardIngestTask().waitForIngestJobs();

        // Then
        assertThat(sleeper.directQuery().allRowsInTable())
                .containsExactlyInAnyOrderElementsOf(sleeper.generateNumberedRows()
                        .iterableOverRange(0, 400));
        assertThat(sleeper.tableFiles().references()).hasSize(2);
    }

}
