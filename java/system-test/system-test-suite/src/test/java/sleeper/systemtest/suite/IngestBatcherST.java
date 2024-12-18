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

package sleeper.systemtest.suite;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import sleeper.core.util.PollWithRetries;
import sleeper.systemtest.dsl.SleeperSystemTest;
import sleeper.systemtest.dsl.extension.AfterTestReports;
import sleeper.systemtest.dsl.reporting.SystemTestReports;
import sleeper.systemtest.dsl.sourcedata.RecordNumbers;
import sleeper.systemtest.suite.testutil.SystemTest;

import java.time.Duration;
import java.util.Map;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.table.TableProperty.BULK_IMPORT_MIN_LEAF_PARTITION_COUNT;
import static sleeper.core.properties.table.TableProperty.INGEST_BATCHER_INGEST_QUEUE;
import static sleeper.core.properties.table.TableProperty.INGEST_BATCHER_MAX_JOB_FILES;
import static sleeper.core.properties.table.TableProperty.INGEST_BATCHER_MIN_JOB_FILES;
import static sleeper.core.properties.table.TableProperty.INGEST_BATCHER_MIN_JOB_SIZE;
import static sleeper.core.properties.validation.IngestQueue.BULK_IMPORT_EMR_SERVERLESS;
import static sleeper.core.properties.validation.IngestQueue.STANDARD_INGEST;
import static sleeper.systemtest.suite.fixtures.SystemTestInstance.MAIN;

@SystemTest
public class IngestBatcherST {

    @BeforeEach
    void setUp(SleeperSystemTest sleeper, AfterTestReports reporting) {
        sleeper.connectToInstance(MAIN);
        sleeper.ingest().batcher().clearStore();
        reporting.reportIfTestFailed(SystemTestReports.SystemTestBuilder::ingestTasksAndJobs);
    }

    @Test
    void shouldCreateTwoStandardIngestJobsWithMaxJobFilesOfThree(SleeperSystemTest sleeper) {
        // Given
        sleeper.updateTableProperties(Map.of(
                INGEST_BATCHER_INGEST_QUEUE, STANDARD_INGEST.toString(),
                INGEST_BATCHER_MIN_JOB_FILES, "1",
                INGEST_BATCHER_MIN_JOB_SIZE, "1K",
                INGEST_BATCHER_MAX_JOB_FILES, "3"));
        RecordNumbers numbers = sleeper.scrambleNumberedRecords(LongStream.range(0, 400));
        sleeper.sourceFiles()
                .createWithNumberedRecords("file1.parquet", numbers.range(0, 100))
                .createWithNumberedRecords("file2.parquet", numbers.range(100, 200))
                .createWithNumberedRecords("file3.parquet", numbers.range(200, 300))
                .createWithNumberedRecords("file4.parquet", numbers.range(300, 400));

        // When
        sleeper.ingest().batcher()
                .sendSourceFilesExpectingJobs(2, "file1.parquet", "file2.parquet", "file3.parquet", "file4.parquet")
                .waitForStandardIngestTask().waitForIngestJobs();

        // Then
        assertThat(sleeper.directQuery().allRecordsInTable())
                .containsExactlyElementsOf(sleeper.generateNumberedRecords(LongStream.range(0, 400)));
        assertThat(sleeper.tableFiles().references()).hasSize(2);
    }

    @Test
    void shouldCreateOneBulkImportJobWithMaxJobFilesOfTen(SleeperSystemTest sleeper) {
        // Given
        sleeper.updateTableProperties(Map.of(
                INGEST_BATCHER_INGEST_QUEUE, BULK_IMPORT_EMR_SERVERLESS.toString(),
                INGEST_BATCHER_MIN_JOB_FILES, "1",
                INGEST_BATCHER_MIN_JOB_SIZE, "1K",
                INGEST_BATCHER_MAX_JOB_FILES, "10",
                BULK_IMPORT_MIN_LEAF_PARTITION_COUNT, "1"));
        RecordNumbers numbers = sleeper.scrambleNumberedRecords(LongStream.range(0, 400));
        sleeper.sourceFiles()
                .createWithNumberedRecords("file1.parquet", numbers.range(0, 100))
                .createWithNumberedRecords("file2.parquet", numbers.range(100, 200))
                .createWithNumberedRecords("file3.parquet", numbers.range(200, 300))
                .createWithNumberedRecords("file4.parquet", numbers.range(300, 400));

        // When
        sleeper.ingest().batcher()
                .sendSourceFilesExpectingJobs(1, "file1.parquet", "file2.parquet", "file3.parquet", "file4.parquet")
                .waitForBulkImportJobs(
                        PollWithRetries.intervalAndPollingTimeout(Duration.ofSeconds(30), Duration.ofMinutes(30)));

        // Then
        assertThat(sleeper.directQuery().allRecordsInTable())
                .containsExactlyElementsOf(sleeper.generateNumberedRecords(LongStream.range(0, 400)));
        assertThat(sleeper.tableFiles().references()).hasSize(1);
    }
}
