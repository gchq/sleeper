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
import sleeper.ingest.batcher.FileIngestRequest;
import sleeper.ingest.job.status.IngestJobStatus;

import java.time.Instant;
import java.util.List;
import java.util.stream.Collectors;

public class IngestBatcherStatusReporterTestHelper {
    private IngestBatcherStatusReporterTestHelper() {
    }

    public static List<FileIngestRequest> onePendingAndTwoBatchedFiles() {
        return List.of(
                FileIngestRequest.builder().file("file1.parquet")
                        .fileSizeBytes(123L)
                        .tableName("test-table")
                        .receivedTime(Instant.parse("2023-09-12T13:28:00Z")).build(),
                FileIngestRequest.builder().file("file2.parquet")
                        .fileSizeBytes(456L)
                        .tableName("test-table")
                        .receivedTime(Instant.parse("2023-09-12T13:25:00Z"))
                        .jobId("test-job-1").build(),
                FileIngestRequest.builder().file("file3.parquet")
                        .fileSizeBytes(789L)
                        .tableName("test-table")
                        .receivedTime(Instant.parse("2023-09-12T13:25:00Z"))
                        .jobId("test-job-1").build()
        );
    }

    public static String replaceBracketedJobIds(List<IngestJobStatus> job, String example) {
        return StatusReporterTestHelper.replaceBracketedJobIds(job.stream()
                .map(IngestJobStatus::getJobId)
                .collect(Collectors.toList()), example);
    }

    public static String getStandardReport(BatcherQuery query, List<FileIngestRequest> fileRequestList) {
        ToStringPrintStream output = new ToStringPrintStream();
        new StandardIngestBatcherStatusReporter(output.getPrintStream()).report(fileRequestList, query);
        return output.toString();
    }

    public static String getJsonReport(BatcherQuery query, List<FileIngestRequest> fileRequestList) {
        ToStringPrintStream output = new ToStringPrintStream();
        new JsonIngestBatcherStatusReporter(output.getPrintStream()).report(fileRequestList, query);
        return output.toString();
    }
}
