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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import sleeper.core.util.PollWithRetries;
import sleeper.systemtest.datageneration.RecordNumbers;
import sleeper.systemtest.suite.dsl.SleeperSystemTest;
import sleeper.systemtest.suite.dsl.ingest.SystemTestIngestBatcher;
import sleeper.systemtest.suite.testutil.ReportingExtension;

import java.time.Duration;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.configuration.properties.table.TableProperty.BULK_IMPORT_MIN_LEAF_PARTITION_COUNT;
import static sleeper.configuration.properties.table.TableProperty.INGEST_BATCHER_INGEST_MODE;
import static sleeper.configuration.properties.table.TableProperty.INGEST_BATCHER_MAX_JOB_FILES;
import static sleeper.configuration.properties.table.TableProperty.INGEST_BATCHER_MIN_JOB_FILES;
import static sleeper.configuration.properties.table.TableProperty.INGEST_BATCHER_MIN_JOB_SIZE;
import static sleeper.configuration.properties.validation.BatchIngestMode.BULK_IMPORT_EMR_SERVERLESS;
import static sleeper.configuration.properties.validation.BatchIngestMode.STANDARD_INGEST;
import static sleeper.systemtest.suite.fixtures.SystemTestInstance.MAIN;

@Tag("SystemTest")
public class IngestBatcherIT {

    private final SleeperSystemTest sleeper = SleeperSystemTest.getInstance();

    @RegisterExtension
    public final ReportingExtension reporting = ReportingExtension.reportIfFailed(
            sleeper.reportsForExtension().ingestTasksAndJobs());

    @BeforeEach
    void setUp() {
        sleeper.connectToInstance(MAIN);
        sleeper.ingest().batcher().clearStore();
    }

    @Test
    void shouldCreateTwoStandardIngestJobsWithMaxJobFilesOfThree() throws InterruptedException {
        // Given
        sleeper.updateTableProperties(tableProperties -> {
            tableProperties.set(INGEST_BATCHER_INGEST_MODE, STANDARD_INGEST.toString());
            tableProperties.set(INGEST_BATCHER_MIN_JOB_FILES, "1");
            tableProperties.set(INGEST_BATCHER_MIN_JOB_SIZE, "1K");
            tableProperties.set(INGEST_BATCHER_MAX_JOB_FILES, "3");
        });
        RecordNumbers numbers = sleeper.scrambleNumberedRecords(LongStream.range(0, 400));
        sleeper.sourceFiles()
                .createWithNumberedRecords("file1.parquet", numbers.range(0, 100))
                .createWithNumberedRecords("file2.parquet", numbers.range(100, 200))
                .createWithNumberedRecords("file3.parquet", numbers.range(200, 300))
                .createWithNumberedRecords("file4.parquet", numbers.range(300, 400));

        // When
        SystemTestIngestBatcher.Result result = sleeper.ingest().batcher()
                .sendSourceFiles("file1.parquet", "file2.parquet", "file3.parquet", "file4.parquet")
                .invoke().invokeStandardIngestTask().waitForJobs().getInvokeResult();

        // Then
        assertThat(result.numJobsCreated()).isEqualTo(2);
        assertThat(sleeper.directQuery().allRecordsInTable())
                .containsExactlyElementsOf(sleeper.generateNumberedRecords(LongStream.range(0, 400)));
        assertThat(sleeper.tableFiles().active()).hasSize(2);
    }

    @Test
    void shouldCreateOneBulkImportJobWithMaxJobFilesOfTen() throws InterruptedException {
        // Given
        sleeper.updateTableProperties(tableProperties -> {
            tableProperties.set(INGEST_BATCHER_INGEST_MODE, BULK_IMPORT_EMR_SERVERLESS.toString());
            tableProperties.set(INGEST_BATCHER_MIN_JOB_FILES, "1");
            tableProperties.set(INGEST_BATCHER_MIN_JOB_SIZE, "1K");
            tableProperties.set(INGEST_BATCHER_MAX_JOB_FILES, "10");
            tableProperties.set(BULK_IMPORT_MIN_LEAF_PARTITION_COUNT, "1");
        });
        RecordNumbers numbers = sleeper.scrambleNumberedRecords(LongStream.range(0, 400));
        sleeper.sourceFiles()
                .createWithNumberedRecords("file1.parquet", numbers.range(0, 100))
                .createWithNumberedRecords("file2.parquet", numbers.range(100, 200))
                .createWithNumberedRecords("file3.parquet", numbers.range(200, 300))
                .createWithNumberedRecords("file4.parquet", numbers.range(300, 400));

        // When
        SystemTestIngestBatcher.Result result = sleeper.ingest().batcher()
                .sendSourceFiles("file1.parquet", "file2.parquet", "file3.parquet", "file4.parquet")
                .invoke().waitForJobs(
                        PollWithRetries.intervalAndPollingTimeout(Duration.ofSeconds(30), Duration.ofMinutes(30)))
                .getInvokeResult();

        // Then
        assertThat(result.numJobsCreated()).isOne();
        assertThat(sleeper.directQuery().allRecordsInTable())
                .containsExactlyElementsOf(sleeper.generateNumberedRecords(LongStream.range(0, 400)));
        assertThat(sleeper.tableFiles().active()).hasSize(1);
    }
}
