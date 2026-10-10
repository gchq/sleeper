/*
 * Copyright 2022-2026 Crown Copyright
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

package sleeper.clients.report.job.query;

import sleeper.clients.report.arguments.JobTrackerReportOptions;
import sleeper.clients.testutil.TestConsoleInput;
import sleeper.clients.testutil.ToStringConsoleOutput;
import sleeper.compaction.core.job.CompactionJob;
import sleeper.compaction.core.job.CompactionJobTestDataHelper;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TableProperty;
import sleeper.core.tracker.compaction.job.InMemoryCompactionJobTracker;
import sleeper.core.tracker.compaction.job.query.CompactionJobStatus;
import sleeper.core.tracker.compaction.job.update.CompactionJobCommittedEvent;
import sleeper.core.tracker.compaction.job.update.CompactionJobCreatedEvent;
import sleeper.core.tracker.compaction.job.update.CompactionJobFinishedEvent;
import sleeper.core.tracker.compaction.job.update.CompactionJobStartedEvent;
import sleeper.core.tracker.job.run.JobRunSummary;
import sleeper.core.tracker.job.run.RowsProcessed;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;

import static sleeper.compaction.core.job.CompactionJobStatusFromJobTestData.compactionJobCreated;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.schema.SchemaTestHelper.createSchemaWithKey;

public class JobQueryTestBase {
    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final TableProperties tableProperties = createTableProperties();
    protected static final String TABLE_NAME = "test-table";
    protected final String tableId = tableProperties.get(TABLE_ID);
    protected final InMemoryCompactionJobTracker tracker = new InMemoryCompactionJobTracker();
    private final CompactionJobTestDataHelper dataHelper = CompactionJobTestDataHelper.forTable(instanceProperties, tableProperties);
    protected final CompactionJob exampleJob1 = dataHelper.singleFileCompaction("job1");
    protected final CompactionJob exampleJob2 = dataHelper.singleFileCompaction("job2");
    protected final CompactionJobStatus exampleStatus1 = compactionJobCreated(
            exampleJob1, Instant.parse("2022-11-30T08:33:12.001Z"));
    protected final CompactionJobStatus exampleStatus2 = compactionJobCreated(
            exampleJob2, Instant.parse("2022-11-30T08:53:12.001Z"));
    protected final List<CompactionJobStatus> exampleStatusList = Arrays.asList(exampleStatus2, exampleStatus1);
    protected final ToStringConsoleOutput out = new ToStringConsoleOutput();
    protected final TestConsoleInput in = new TestConsoleInput(out.consoleOut());

    protected void createExampleJobs() {
        tracker.jobCreated(exampleJob1.createCreatedEvent(), exampleStatus1.getCreateUpdateTime());
        tracker.jobCreated(exampleJob2.createCreatedEvent(), exampleStatus2.getCreateUpdateTime());
    }

    protected List<CompactionJobStatus> createAllQueryJobs() {
        createExampleJobs();
        CompactionJobStatus finished = createFinishedJob("finished-job", tableId,
                Instant.parse("2022-11-30T09:00:00Z"), Instant.parse("2022-11-30T10:00:00Z"));
        createJob("other-table-job", "other-table", Instant.parse("2022-11-30T09:30:00Z"));
        return List.of(finished, exampleStatus2, exampleStatus1);
    }

    protected void createDetailedQueryJobs() {
        createExampleJobs();
        createJob("unrequested-job", tableId, Instant.parse("2022-11-30T09:00:00Z"));
        createJob("other-table-job", "other-table", Instant.parse("2022-11-30T09:30:00Z"));
    }

    protected List<CompactionJobStatus> createRangeQueryJobs(Instant start, Instant end) {
        createExampleJobs();
        CompactionJobStatus insideStart = createFinishedJob("inside-start", tableId, start.plusSeconds(1), start.plusSeconds(2));
        // A completed job before the start detects a missing or incorrect lower bound.
        createFinishedJob("before-range", tableId, start.minusSeconds(2), start.minusSeconds(1));
        // An unfinished job after the end detects a missing or incorrect upper bound.
        createJob("after-range", tableId, end.plusSeconds(1));
        createJob("other-table-job", "other-table", exampleStatus1.getCreateUpdateTime());
        return List.of(exampleStatus2, exampleStatus1, insideStart);
    }

    private void createJob(String jobId, String jobTableId, Instant time) {
        tracker.jobCreated(CompactionJobCreatedEvent.builder()
                .jobId(jobId).tableId(jobTableId).partitionId("test-partition").inputFilesCount(1)
                .build(), time);
    }

    private CompactionJobStatus createFinishedJob(String jobId, String jobTableId, Instant start, Instant end) {
        createJob(jobId, jobTableId, start);
        tracker.jobStarted(CompactionJobStartedEvent.builder()
                .jobId(jobId).tableId(jobTableId).taskId("test-task").jobRunId("test-run")
                .startTime(start).build());
        tracker.jobFinished(CompactionJobFinishedEvent.builder()
                .jobId(jobId).tableId(jobTableId).taskId("test-task").jobRunId("test-run")
                .summary(new JobRunSummary(RowsProcessed.NONE, start, end)).build());
        tracker.jobCommitted(CompactionJobCommittedEvent.builder()
                .jobId(jobId).tableId(jobTableId).taskId("test-task").jobRunId("test-run")
                .commitTime(end).build());
        return tracker.getJob(jobId).orElseThrow();
    }

    protected List<CompactionJobStatus> queryStatuses(JobQueryType queryType) {
        return queryStatusesWithParams(queryType, null);
    }

    protected List<CompactionJobStatus> queryStatusesWithParams(JobQueryType queryType, String queryParameters) {
        return queryStatuses(queryType, queryParameters, Instant::now);
    }

    protected List<CompactionJobStatus> queryStatusesAtTime(JobQueryType queryType, Instant time) {
        return queryStatuses(queryType, null,
                () -> time);
    }

    protected JobQuery queryFrom(JobQueryType queryType) {
        return queryFrom(queryType, null, Instant::now);
    }

    private List<CompactionJobStatus> queryStatuses(JobQueryType queryType, String queryParameters, Supplier<Instant> timeSupplier) {
        return queryFrom(queryType, queryParameters, timeSupplier).run(tracker, tableId);
    }

    private JobQuery queryFrom(JobQueryType queryType, String queryParameters, Supplier<Instant> timeSupplier) {
        return JobTrackerReportOptions.compactionJobQueryFromParametersOrPrompt(queryType, queryParameters, timeSupplier, in.consoleIn());
    }

    private TableProperties createTableProperties() {
        TableProperties properties = createTestTableProperties(instanceProperties, createSchemaWithKey("key"));
        properties.set(TableProperty.TABLE_NAME, TABLE_NAME);
        return properties;
    }
}
