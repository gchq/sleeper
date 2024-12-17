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

package sleeper.systemtest.drivers.compaction;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;

import sleeper.clients.status.report.CompactionJobStatusReport;
import sleeper.clients.status.report.CompactionTaskStatusReport;
import sleeper.clients.status.report.compaction.job.StandardCompactionJobStatusReporter;
import sleeper.clients.status.report.compaction.task.CompactionTaskQuery;
import sleeper.clients.status.report.compaction.task.StandardCompactionTaskStatusReporter;
import sleeper.clients.status.report.job.query.RangeJobsQuery;
import sleeper.compaction.core.job.CompactionJobStatusStore;
import sleeper.compaction.core.job.query.CompactionJobStatus;
import sleeper.compaction.core.task.CompactionTaskStatus;
import sleeper.compaction.core.task.CompactionTaskStatusStore;
import sleeper.compaction.status.store.job.CompactionJobStatusStoreFactory;
import sleeper.compaction.status.store.task.CompactionTaskStatusStoreFactory;
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
            new CompactionTaskStatusReport(taskStore(),
                    new StandardCompactionTaskStatusReporter(out),
                    CompactionTaskQuery.forPeriod(startTime, Instant.MAX))
                    .run();
            new CompactionJobStatusReport(jobStore(),
                    new StandardCompactionJobStatusReporter(out),
                    new RangeJobsQuery(instance.getTableStatus(), startTime, Instant.MAX))
                    .run();
        };
    }

    public List<CompactionJobStatus> jobs(ReportingContext reportingContext) {
        return new RangeJobsQuery(instance.getTableStatus(), reportingContext.getRecordingStartTime(), Instant.MAX)
                .run(jobStore());
    }

    @Override
    public List<CompactionTaskStatus> tasks(ReportingContext reportingContext) {
        return CompactionTaskQuery.forPeriod(reportingContext.getRecordingStartTime(), Instant.MAX)
                .run(taskStore());
    }

    private CompactionJobStatusStore jobStore() {
        return CompactionJobStatusStoreFactory.getStatusStore(dynamoDB, instance.getInstanceProperties());
    }

    private CompactionTaskStatusStore taskStore() {
        return CompactionTaskStatusStoreFactory.getStatusStore(dynamoDB, instance.getInstanceProperties());
    }
}
