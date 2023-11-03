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

package sleeper.clients.status.report.ingest.batcher;

import sleeper.clients.status.report.StatusReporterTestHelper;
import sleeper.clients.testutil.ToStringPrintStream;
import sleeper.core.table.TableIdGenerator;
import sleeper.core.table.TableIdentity;
import sleeper.core.table.TableIdentityProvider;
import sleeper.core.table.TableIndex;
import sleeper.ingest.batcher.FileIngestRequest;
import sleeper.ingest.job.status.IngestJobStatus;

import java.time.Instant;
import java.util.List;
import java.util.stream.Collectors;

public class IngestBatcherReporterTestHelper {
    private IngestBatcherReporterTestHelper() {
    }

    public static final TableIdentity TEST_TABLE = TableIdentity.uniqueIdAndName("test-table-id", "test-table");

    public static List<FileIngestRequest> onePendingAndTwoBatchedFiles() {
        return List.of(
                FileIngestRequest.builder().file("file1.parquet")
                        .fileSizeBytes(123L)
                        .tableId("test-table-id")
                        .receivedTime(Instant.parse("2023-09-12T13:28:00Z")).build(),
                FileIngestRequest.builder().file("file2.parquet")
                        .fileSizeBytes(456L)
                        .tableId("test-table-id")
                        .receivedTime(Instant.parse("2023-09-12T13:25:00Z"))
                        .jobId("test-job-1").build(),
                FileIngestRequest.builder().file("file3.parquet")
                        .fileSizeBytes(789L)
                        .tableId(TableIdGenerator.fromRandomSeed(0).generateString())
                        .receivedTime(Instant.parse("2023-09-12T13:25:00Z"))
                        .jobId("test-job-1").build()
        );
    }

    public static List<FileIngestRequest> multiplePendingFiles() {
        return List.of(
                FileIngestRequest.builder().file("file1.parquet")
                        .fileSizeBytes(123L)
                        .tableId("test-table-id")
                        .receivedTime(Instant.parse("2023-09-12T13:23:00Z")).build(),
                FileIngestRequest.builder().file("file2.parquet")
                        .fileSizeBytes(456L)
                        .tableId("test-table-id")
                        .receivedTime(Instant.parse("2023-09-12T13:25:00Z"))
                        .build(),
                FileIngestRequest.builder().file("file3.parquet")
                        .fileSizeBytes(789L)
                        .tableId(TableIdGenerator.fromRandomSeed(0).generateString())
                        .receivedTime(Instant.parse("2023-09-12T13:28:00Z"))
                        .build()
        );
    }

    public static List<FileIngestRequest> filesWithLargeAndDecimalSizes() {
        return List.of(
                FileIngestRequest.builder().file("file1.parquet")
                        .fileSizeBytes(1_200L)
                        .tableId("test-table-id")
                        .receivedTime(Instant.parse("2023-09-12T13:28:00Z")).build(),
                FileIngestRequest.builder().file("file2.parquet")
                        .fileSizeBytes(12_300_000L)
                        .tableId("test-table-id")
                        .receivedTime(Instant.parse("2023-09-12T13:25:00Z"))
                        .jobId("test-job-1").build(),
                FileIngestRequest.builder().file("file3.parquet")
                        .fileSizeBytes(123_400_000_000L)
                        .tableId("test-table-id")
                        .receivedTime(Instant.parse("2023-09-12T13:23:00Z"))
                        .jobId("test-job-1").build()
        );
    }

    public static String replaceBracketedJobIds(List<IngestJobStatus> job, String example) {
        return StatusReporterTestHelper.replaceBracketedJobIds(job.stream()
                .map(IngestJobStatus::getJobId)
                .collect(Collectors.toList()), example);
    }

    public static String getStandardReport(TableIndex tableIndex, BatcherQuery.Type queryType, List<FileIngestRequest> fileRequestList) {
        ToStringPrintStream output = new ToStringPrintStream();
        new StandardIngestBatcherReporter(output.getPrintStream())
                .report(fileRequestList, queryType, new TableIdentityProvider(tableIndex));
        return output.toString();
    }

    public static String getJsonReport(TableIndex tableIndex, BatcherQuery.Type queryType, List<FileIngestRequest> fileRequestList) {
        ToStringPrintStream output = new ToStringPrintStream();
        new JsonIngestBatcherReporter(output.getPrintStream())
                .report(fileRequestList, queryType, new TableIdentityProvider(tableIndex));
        return output.toString();
    }
}
