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

package sleeper.status.report.compactionjobstatus;

import org.apache.commons.io.IOUtils;
import org.junit.Before;
import sleeper.compaction.job.CompactionJob;
import sleeper.compaction.job.CompactionJobRecordsProcessed;
import sleeper.compaction.job.CompactionJobSummary;
import sleeper.compaction.job.CompactionJobTestDataHelper;
import sleeper.compaction.job.status.CompactionJobCreatedStatus;
import sleeper.compaction.job.status.CompactionJobFinishedStatus;
import sleeper.compaction.job.status.CompactionJobStartedStatus;
import sleeper.compaction.job.status.CompactionJobStatus;
import sleeper.status.report.compactionjob.CompactionJobStatusReporter;
import sleeper.status.report.compactionjob.CompactionJobStatusReporter.QueryType;
import sleeper.status.report.filestatus.FilesStatusReportTest;

import java.io.IOException;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.nio.charset.Charset;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

public abstract class StatusReporterTest {
    protected CompactionJobTestDataHelper dataHelper;
    public static final String DEFAULT_TASK_ID = "task-id";

    @Before
    public void setup() {
        dataHelper = new CompactionJobTestDataHelper();
    }

    protected static String example(String path) throws IOException {
        URL url = FilesStatusReportTest.class.getClassLoader().getResource(path);
        return IOUtils.toString(Objects.requireNonNull(url), Charset.defaultCharset());
    }

    protected static CompactionJobStatus jobCreated(CompactionJob job, Instant creationTime) {
        return CompactionJobStatus.created(job, creationTime);
    }

    protected static CompactionJobStatus jobStarted(CompactionJob job, String taskId, Instant creationTime, Instant startTime, Instant startUpdateTime) {
        return CompactionJobStatus.builder().jobId(job.getId())
                .createdStatus(CompactionJobCreatedStatus.from(job, creationTime))
                .startedStatus(CompactionJobStartedStatus.updateAndStartTimeWithTaskId(startUpdateTime, startTime, taskId))
                .build();
    }

    protected static CompactionJobStatus jobFinished(CompactionJob job, String taskId, Instant creationTime, Instant startTime, Instant startUpdateTime, Instant finishedTime) {
        CompactionJobSummary summary = new CompactionJobSummary(
                new CompactionJobRecordsProcessed(600L, 300L), startUpdateTime, finishedTime);
        return CompactionJobStatus.builder().jobId(job.getId())
                .createdStatus(CompactionJobCreatedStatus.from(job, creationTime))
                .startedStatus(CompactionJobStartedStatus.updateAndStartTimeWithTaskId(startUpdateTime, startTime, taskId))
                .finishedStatus(CompactionJobFinishedStatus.updateTimeAndSummaryWithTaskId(finishedTime, summary, taskId))
                .build();
    }

    public String verboseReportString(Function<PrintStream, CompactionJobStatusReporter> getReporter, List<CompactionJobStatus> statusList,
                                      QueryType queryType) throws UnsupportedEncodingException {
        return CompactionJobStatusReporter.asString(getReporter, statusList, queryType);
    }
}
