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

package sleeper.clients.report.ingest.batcher;

import sleeper.clients.testutil.ToStringConsoleOutput;
import sleeper.core.table.TableIdGenerator;
import sleeper.core.table.TableIndex;
import sleeper.core.table.TableStatus;
import sleeper.core.table.TableStatusProvider;
import sleeper.core.table.TableStatusTestHelper;
import sleeper.ingest.batcher.core.IngestBatcherTrackedFile;

import java.time.Instant;
import java.util.List;

/**
 * Helpers for testing reports on the ingest batcher. Creates example ingest batcher file tracking data.
 */
public class IngestBatcherReporterTestHelper {
    private IngestBatcherReporterTestHelper() {
    }

    public static final TableStatus TEST_TABLE = TableStatusTestHelper.uniqueIdAndName("test-table-id", "test-table");

    /**
     * Creates a state where three files were submitted to the batcher, and two of those have been batched into a job.
     *
     * @return the ingest batcher store file tracking entries
     */
    public static List<IngestBatcherTrackedFile> onePendingAndTwoBatchedFiles() {
        return List.of(
                IngestBatcherTrackedFile.builder().file("file1.parquet")
                        .fileSizeBytes(123L)
                        .tableId("test-table-id")
                        .receivedTime(Instant.parse("2023-09-12T13:28:00Z")).build(),
                IngestBatcherTrackedFile.builder().file("file2.parquet")
                        .fileSizeBytes(456L)
                        .tableId("test-table-id")
                        .receivedTime(Instant.parse("2023-09-12T13:25:00Z"))
                        .jobId("test-job-1").build(),
                IngestBatcherTrackedFile.builder().file("file3.parquet")
                        .fileSizeBytes(789L)
                        .tableId(TableIdGenerator.fromRandomSeed(0).generateString())
                        .receivedTime(Instant.parse("2023-09-12T13:25:00Z"))
                        .jobId("test-job-1").build());
    }

    /**
     * Creates a state where three files were submitted to the batcher, and no jobs have been created from them yet.
     *
     * @return the ingest batcher store file tracking entries
     */
    public static List<IngestBatcherTrackedFile> multiplePendingFiles() {
        return List.of(
                IngestBatcherTrackedFile.builder().file("file1.parquet")
                        .fileSizeBytes(123L)
                        .tableId("test-table-id")
                        .receivedTime(Instant.parse("2023-09-12T13:23:00Z")).build(),
                IngestBatcherTrackedFile.builder().file("file2.parquet")
                        .fileSizeBytes(456L)
                        .tableId("test-table-id")
                        .receivedTime(Instant.parse("2023-09-12T13:25:00Z"))
                        .build(),
                IngestBatcherTrackedFile.builder().file("file3.parquet")
                        .fileSizeBytes(789L)
                        .tableId(TableIdGenerator.fromRandomSeed(0).generateString())
                        .receivedTime(Instant.parse("2023-09-12T13:28:00Z"))
                        .build());
    }

    /**
     * Creates a state where three files were submitted to the batcher, with a variety of file sizes.
     *
     * @return the ingest batcher store file tracking entries
     */
    public static List<IngestBatcherTrackedFile> filesWithLargeAndDecimalSizes() {
        return List.of(
                IngestBatcherTrackedFile.builder().file("file1.parquet")
                        .fileSizeBytes(1_200L)
                        .tableId("test-table-id")
                        .receivedTime(Instant.parse("2023-09-12T13:28:00Z")).build(),
                IngestBatcherTrackedFile.builder().file("file2.parquet")
                        .fileSizeBytes(12_300_000L)
                        .tableId("test-table-id")
                        .receivedTime(Instant.parse("2023-09-12T13:25:00Z"))
                        .jobId("test-job-1").build(),
                IngestBatcherTrackedFile.builder().file("file3.parquet")
                        .fileSizeBytes(123_400_000_000L)
                        .tableId("test-table-id")
                        .receivedTime(Instant.parse("2023-09-12T13:23:00Z"))
                        .jobId("test-job-1").build());
    }

    /**
     * Creates a report on the state of the ingest batcher in the standard, human readable format.
     *
     * @param  tableIndex      the index of Sleeper tables
     * @param  queryType       the type of query used to generate the report
     * @param  fileRequestList the data from the ingest batcher store
     * @return                 the report as a human readable string
     */
    public static String getStandardReport(TableIndex tableIndex, BatcherQuery.Type queryType, List<IngestBatcherTrackedFile> fileRequestList) {
        ToStringConsoleOutput output = new ToStringConsoleOutput();
        new StandardIngestBatcherReporter(output.getPrintStream())
                .report(fileRequestList, queryType, new TableStatusProvider(tableIndex));
        return output.toString();
    }

    /**
     * Creates a report on the state of the ingest batcher in JSON format.
     *
     * @param  tableIndex      the index of Sleeper tables
     * @param  queryType       the type of query used to generate the report
     * @param  fileRequestList the data from the ingest batcher store
     * @return                 the report as a JSON string
     */
    public static String getJsonReport(TableIndex tableIndex, BatcherQuery.Type queryType, List<IngestBatcherTrackedFile> fileRequestList) {
        ToStringConsoleOutput output = new ToStringConsoleOutput();
        new JsonIngestBatcherReporter(output.getPrintStream())
                .report(fileRequestList, queryType, new TableStatusProvider(tableIndex));
        return output.toString();
    }
}
