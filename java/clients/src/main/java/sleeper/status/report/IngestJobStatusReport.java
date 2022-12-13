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

package sleeper.status.report;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.QueueAttributeName;
import sleeper.ClientUtils;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.console.ConsoleInput;
import sleeper.ingest.job.status.IngestJobStatusStore;
import sleeper.ingest.status.store.job.DynamoDBIngestJobStatusStore;
import sleeper.job.common.CommonJobUtils;
import sleeper.status.report.ingest.job.IngestJobStatusReportArguments;
import sleeper.status.report.ingest.job.IngestJobStatusReporter;
import sleeper.status.report.job.query.JobQuery;

import java.io.IOException;
import java.time.Clock;

import static sleeper.configuration.properties.SystemDefinedInstanceProperty.INGEST_JOB_QUEUE_URL;

public class IngestJobStatusReport {
    private final IngestJobStatusStore statusStore;
    private final IngestJobStatusReportArguments arguments;
    private final IngestJobStatusReporter ingestJobStatusReporter;
    private final AmazonSQS sqsClient;
    private final String jobQueueUrl;

    public IngestJobStatusReport(
            IngestJobStatusStore ingestJobStatusStore,
            IngestJobStatusReportArguments arguments,
            AmazonSQS sqsClient,
            String jobQueueUrl) {
        this.statusStore = ingestJobStatusStore;
        this.arguments = arguments;
        this.ingestJobStatusReporter = arguments.getReporter();
        this.sqsClient = sqsClient;
        this.jobQueueUrl = jobQueueUrl;
    }

    private void run() {
        JobQuery query = arguments.buildQuery(Clock.systemUTC(),
                new ConsoleInput(System.console()));
        if (query == null) {
            return;
        }
        ingestJobStatusReporter.report(
                query.run(statusStore),
                arguments.getQueryType(),
                getNumberOfMessagesInQueue());
    }

    private int getNumberOfMessagesInQueue() {
        return CommonJobUtils.getNumberOfMessagesInQueue(jobQueueUrl, sqsClient)
                .get(QueueAttributeName.ApproximateNumberOfMessages.toString());
    }

    public static void main(String[] args) throws IOException {
        IngestJobStatusReportArguments arguments;
        try {
            arguments = IngestJobStatusReportArguments.from(args);
        } catch (IllegalArgumentException e) {
            System.out.println(e.getMessage());
            IngestJobStatusReportArguments.printUsage(System.out);
            System.exit(1);
            return;
        }

        AmazonS3 amazonS3 = AmazonS3ClientBuilder.defaultClient();
        InstanceProperties instanceProperties = ClientUtils.getInstanceProperties(amazonS3, arguments.getInstanceId());

        AmazonDynamoDB dynamoDBClient = AmazonDynamoDBClientBuilder.defaultClient();
        IngestJobStatusStore statusStore = DynamoDBIngestJobStatusStore.from(dynamoDBClient, instanceProperties);
        AmazonSQS sqsClient = AmazonSQSClientBuilder.defaultClient();
        String jobQueueUrl = instanceProperties.get(INGEST_JOB_QUEUE_URL);
        new IngestJobStatusReport(statusStore, arguments, sqsClient, jobQueueUrl).run();
    }
}
