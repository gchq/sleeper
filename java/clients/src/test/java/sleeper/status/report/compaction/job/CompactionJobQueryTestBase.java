/*
 * Copyright 2022 Crown Copyright
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

package sleeper.status.report.compaction.job;

import sleeper.ToStringPrintStream;
import sleeper.compaction.job.CompactionJobStatusStore;
import sleeper.compaction.job.CompactionJobTestDataHelper;
import sleeper.compaction.job.TestCompactionJobStatus;
import sleeper.compaction.job.status.CompactionJobStatus;
import sleeper.console.TestConsoleInput;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.List;

import static org.mockito.Mockito.mock;

public class CompactionJobQueryTestBase {
    protected static final String tableName = "test-table";
    protected final CompactionJobStatusStore statusStore = mock(CompactionJobStatusStore.class);
    protected final CompactionJobTestDataHelper dataHelper = new CompactionJobTestDataHelper();
    protected final CompactionJobStatus exampleStatus1 = TestCompactionJobStatus.created(
            dataHelper.singleFileCompaction(), Instant.parse("2022-09-22T13:33:12.001Z"));
    protected final CompactionJobStatus exampleStatus2 = TestCompactionJobStatus.created(
            dataHelper.singleFileCompaction(), Instant.parse("2022-09-22T13:53:12.001Z"));
    protected final List<CompactionJobStatus> exampleStatusList = Arrays.asList(exampleStatus1, exampleStatus2);
    protected final ToStringPrintStream printStream = new ToStringPrintStream();
    protected final TestConsoleInput consoleInput = new TestConsoleInput(printStream.consoleOut());

    protected List<CompactionJobStatus> queryStatuses(CompactionJobStatusReporter.QueryType queryType) {
        return queryStatusesWithParams(queryType, null);
    }

    protected List<CompactionJobStatus> queryStatusesWithParams(CompactionJobStatusReporter.QueryType queryType, String queryParameters) {
        return queryStatuses(queryType, queryParameters, Clock.systemUTC());
    }

    protected List<CompactionJobStatus> queryStatusesAtTime(CompactionJobStatusReporter.QueryType queryType, Instant time) {
        return queryStatuses(queryType, null,
                Clock.fixed(time, ZoneId.of("UTC")));
    }

    private List<CompactionJobStatus> queryStatuses(CompactionJobStatusReporter.QueryType queryType, String queryParameters, Clock clock) {
        return CompactionJobStatusReportArguments.builder()
                .instanceId("test-instance").tableName(tableName)
                .reporter(new StandardCompactionJobStatusReporter())
                .queryType(queryType)
                .queryParameters(queryParameters)
                .build().buildQuery(clock, consoleInput.consoleIn(), printStream.consoleOut()).run(statusStore);
    }
}
