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

package sleeper.systemtest.drivers.ingest;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.emr.EmrClient;

import sleeper.clients.report.IngestJobStatusReport;
import sleeper.clients.report.IngestTaskStatusReport;
import sleeper.clients.report.ingest.job.PersistentEMRStepCount;
import sleeper.clients.report.ingest.job.StandardIngestJobStatusReporter;
import sleeper.clients.report.ingest.task.IngestTaskQuery;
import sleeper.clients.report.ingest.task.StandardIngestTaskStatusReporter;
import sleeper.clients.report.job.query.RangeJobsQuery;
import sleeper.common.task.QueueMessageCount;
import sleeper.core.tracker.ingest.job.IngestJobTracker;
import sleeper.core.tracker.ingest.job.query.IngestJobStatus;
import sleeper.core.tracker.ingest.task.IngestTaskTracker;
import sleeper.ingest.tracker.job.IngestJobTrackerFactory;
import sleeper.ingest.tracker.task.IngestTaskTrackerFactory;
import sleeper.systemtest.drivers.util.SystemTestClients;
import sleeper.systemtest.dsl.instance.SystemTestInstanceContext;
import sleeper.systemtest.dsl.reporting.IngestReportsDriver;
import sleeper.systemtest.dsl.reporting.ReportingContext;
import sleeper.systemtest.dsl.reporting.SystemTestReport;

import java.time.Instant;
import java.util.List;

public class AwsIngestReportsDriver implements IngestReportsDriver {
    private final SystemTestInstanceContext instance;
    private final DynamoDbClient dynamoClient;
    private final QueueMessageCount.Client queueMessages;
    private final EmrClient emr;

    public AwsIngestReportsDriver(SystemTestInstanceContext instance, SystemTestClients clients) {
        this.instance = instance;
        this.dynamoClient = clients.getDynamo();
        this.queueMessages = QueueMessageCount.withSqsClient(clients.getSqs());
        this.emr = clients.getEmr();
    }

    public SystemTestReport tasksAndJobsReport() {
        return SystemTestReport.join(tasksReport(), jobsReport());
    }

    public SystemTestReport tasksReport() {
        return (out, startTime) -> new IngestTaskStatusReport(taskTracker(),
                new StandardIngestTaskStatusReporter(out),
                IngestTaskQuery.forPeriod(startTime, Instant.MAX))
                .run();
    }

    public SystemTestReport jobsReport() {
        return (out, startTime) -> new IngestJobStatusReport(jobTracker(),
                new RangeJobsQuery(instance.getTableStatus(), startTime, Instant.MAX),
                new StandardIngestJobStatusReporter(out), queueMessages, instance.getInstanceProperties(),
                PersistentEMRStepCount.byStatus(instance.getInstanceProperties(), emr))
                .run();
    }

    public List<IngestJobStatus> jobs(ReportingContext reportingContext) {
        return new RangeJobsQuery(instance.getTableStatus(), reportingContext.getRecordingStartTime(), Instant.MAX)
                .run(jobTracker());
    }

    private IngestJobTracker jobTracker() {
        return IngestJobTrackerFactory.getTracker(dynamoClient, instance.getInstanceProperties());
    }

    private IngestTaskTracker taskTracker() {
        return IngestTaskTrackerFactory.getTracker(dynamoClient, instance.getInstanceProperties());
    }
}
