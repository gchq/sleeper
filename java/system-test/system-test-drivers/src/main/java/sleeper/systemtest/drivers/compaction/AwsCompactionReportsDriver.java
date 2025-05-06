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

package sleeper.systemtest.drivers.compaction;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;

import sleeper.clients.report.CompactionJobStatusReport;
import sleeper.clients.report.CompactionTaskStatusReport;
import sleeper.clients.report.compaction.job.StandardCompactionJobStatusReporter;
import sleeper.clients.report.compaction.task.CompactionTaskQuery;
import sleeper.clients.report.compaction.task.StandardCompactionTaskStatusReporter;
import sleeper.clients.report.job.query.RangeJobsQuery;
import sleeper.compaction.tracker.job.CompactionJobTrackerFactory;
import sleeper.compaction.tracker.task.CompactionTaskTrackerFactory;
import sleeper.core.tracker.compaction.job.CompactionJobTracker;
import sleeper.core.tracker.compaction.job.query.CompactionJobStatus;
import sleeper.core.tracker.compaction.task.CompactionTaskStatus;
import sleeper.core.tracker.compaction.task.CompactionTaskTracker;
import sleeper.systemtest.dsl.instance.SystemTestInstanceContext;
import sleeper.systemtest.dsl.reporting.CompactionReportsDriver;
import sleeper.systemtest.dsl.reporting.ReportingContext;
import sleeper.systemtest.dsl.reporting.SystemTestReport;

import java.time.Instant;
import java.util.List;

public class AwsCompactionReportsDriver implements CompactionReportsDriver {
    private final SystemTestInstanceContext instance;
    private final AmazonDynamoDB dynamoDB;

    public AwsCompactionReportsDriver(SystemTestInstanceContext instance, AmazonDynamoDB dynamoDB) {
        this.instance = instance;
        this.dynamoDB = dynamoDB;
    }

    public SystemTestReport tasksAndJobsReport() {
        return (out, startTime) -> {
            new CompactionTaskStatusReport(taskTracker(),
                    new StandardCompactionTaskStatusReporter(out),
                    CompactionTaskQuery.forPeriod(startTime, Instant.MAX))
                    .run();
            new CompactionJobStatusReport(jobTracker(),
                    new StandardCompactionJobStatusReporter(out),
                    new RangeJobsQuery(instance.getTableStatus(), startTime, Instant.MAX))
                    .run();
        };
    }

    public List<CompactionJobStatus> jobs(ReportingContext reportingContext) {
        return new RangeJobsQuery(instance.getTableStatus(), reportingContext.getRecordingStartTime(), Instant.MAX)
                .run(jobTracker());
    }

    @Override
    public List<CompactionTaskStatus> tasks(ReportingContext reportingContext) {
        return CompactionTaskQuery.forPeriod(reportingContext.getRecordingStartTime(), Instant.MAX)
                .run(taskTracker());
    }

    private CompactionJobTracker jobTracker() {
        return CompactionJobTrackerFactory.getTracker(dynamoDB, instance.getInstanceProperties());
    }

    private CompactionTaskTracker taskTracker() {
        return CompactionTaskTrackerFactory.getTracker(dynamoDB, instance.getInstanceProperties());
    }
}
