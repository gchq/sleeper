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

package sleeper.systemtest.drivers.ingest;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.sqs.AmazonSQS;

import sleeper.clients.status.report.IngestJobStatusReport;
import sleeper.clients.status.report.IngestTaskStatusReport;
import sleeper.clients.status.report.ingest.job.PersistentEMRStepCount;
import sleeper.clients.status.report.ingest.job.StandardIngestJobStatusReporter;
import sleeper.clients.status.report.ingest.task.IngestTaskQuery;
import sleeper.clients.status.report.ingest.task.StandardIngestTaskStatusReporter;
import sleeper.clients.status.report.job.query.JobQuery;
import sleeper.clients.status.report.job.query.RangeJobsQuery;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.ingest.job.status.IngestJobStatusStore;
import sleeper.ingest.status.store.job.IngestJobStatusStoreFactory;
import sleeper.ingest.status.store.task.IngestTaskStatusStoreFactory;
import sleeper.ingest.task.IngestTaskStatusStore;
import sleeper.job.common.QueueMessageCount;
import sleeper.systemtest.drivers.instance.SleeperInstanceContext;
import sleeper.systemtest.drivers.instance.SystemTestReport;

import java.time.Instant;

public class IngestReportsDriver {
    private final IngestJobStatusStore ingestJobStatusStore;
    private final IngestTaskStatusStore ingestTaskStatusStore;
    private final SleeperInstanceContext instance;
    private final QueueMessageCount.Client queueClient;
    private final AmazonElasticMapReduce emrClient;

    public IngestReportsDriver(AmazonDynamoDB dynamoDB, AmazonSQS sqs, AmazonElasticMapReduce emrClient,
                               SleeperInstanceContext instance) {
        InstanceProperties properties = instance.getInstanceProperties();
        this.ingestJobStatusStore = IngestJobStatusStoreFactory.getStatusStore(dynamoDB, properties);
        this.ingestTaskStatusStore = IngestTaskStatusStoreFactory.getStatusStore(dynamoDB, properties);
        this.instance = instance;
        this.queueClient = QueueMessageCount.withSqsClient(sqs);
        this.emrClient = emrClient;
    }

    public SystemTestReport tasksAndJobsReport() {
        return (out, startTime) -> {
            new IngestTaskStatusReport(ingestTaskStatusStore,
                    new StandardIngestTaskStatusReporter(out),
                    IngestTaskQuery.forPeriod(startTime, Instant.MAX))
                    .run();
            new IngestJobStatusReport(ingestJobStatusStore, JobQuery.Type.RANGE,
                    new RangeJobsQuery(instance.getTableName(), startTime, Instant.MAX),
                    new StandardIngestJobStatusReporter(out), queueClient, instance.getInstanceProperties(),
                    PersistentEMRStepCount.byStatus(instance.getInstanceProperties(), emrClient))
                    .run();
        };
    }
}
