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

package sleeper.status.report;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.QueueAttributeName;

import sleeper.configuration.properties.InstanceProperties;
import sleeper.console.ConsoleInput;
import sleeper.ingest.job.status.IngestJobStatusStore;
import sleeper.ingest.status.store.job.DynamoDBIngestJobStatusStore;
import sleeper.job.common.CommonJobUtils;
import sleeper.status.report.ingest.job.IngestJobStatusReportArguments;
import sleeper.status.report.ingest.job.IngestJobStatusReporter;
import sleeper.status.report.job.query.JobQuery;
import sleeper.status.report.job.query.JobQueryArgument;
import sleeper.util.ClientUtils;

import java.io.IOException;
import java.time.Clock;

import static sleeper.configuration.properties.SystemDefinedInstanceProperty.INGEST_JOB_QUEUE_URL;
import static sleeper.status.report.ingest.job.IngestJobStatusReportArguments.getReporter;
import static sleeper.util.ClientUtils.optionalArgument;

public class IngestJobStatusReport {
    private final IngestJobStatusStore statusStore;
    private final IngestJobStatusReporter ingestJobStatusReporter;
    private final AmazonSQS sqsClient;
    private final String jobQueueUrl;
    private final JobQuery query;
    private final JobQuery.Type queryType;

    public IngestJobStatusReport(
            IngestJobStatusStore ingestJobStatusStore,
            String tableName, JobQuery.Type queryType, String queryParameters,
            IngestJobStatusReporter reporter, AmazonSQS sqsClient, InstanceProperties properties) {
        this.statusStore = ingestJobStatusStore;
        this.query = JobQuery.fromParametersOrPrompt(tableName, queryType, queryParameters,
                Clock.systemUTC(), new ConsoleInput(System.console()));
        this.queryType = queryType;
        this.ingestJobStatusReporter = reporter;
        this.sqsClient = sqsClient;
        this.jobQueueUrl = properties.get(INGEST_JOB_QUEUE_URL);
    }

    public void run() {
        if (query == null) {
            return;
        }
        ingestJobStatusReporter.report(
                query.run(statusStore), queryType,
                getNumberOfMessagesInQueue());
    }

    private int getNumberOfMessagesInQueue() {
        return CommonJobUtils.getNumberOfMessagesInQueue(jobQueueUrl, sqsClient)
                .get(QueueAttributeName.ApproximateNumberOfMessages.toString());
    }

    public static void main(String[] args) throws IOException {
        try {
            if (args.length < 2 || args.length > 5) {
                throw new IllegalArgumentException("Wrong number of arguments");
            }

            String tableName = args[1];
            IngestJobStatusReporter reporter = getReporter(args, 2);
            JobQuery.Type queryType = JobQueryArgument.readTypeArgument(args, 3);
            String queryParameters = optionalArgument(args, 4).orElse(null);


            AmazonS3 amazonS3 = AmazonS3ClientBuilder.defaultClient();
            InstanceProperties instanceProperties = ClientUtils.getInstanceProperties(amazonS3, args[0]);

            AmazonDynamoDB dynamoDBClient = AmazonDynamoDBClientBuilder.defaultClient();
            IngestJobStatusStore statusStore = DynamoDBIngestJobStatusStore.from(dynamoDBClient, instanceProperties);
            AmazonSQS sqsClient = AmazonSQSClientBuilder.defaultClient();
            new IngestJobStatusReport(statusStore, tableName, queryType, queryParameters, reporter, sqsClient, instanceProperties).run();
        } catch (IllegalArgumentException e) {
            System.out.println(e.getMessage());
            IngestJobStatusReportArguments.printUsage(System.out);
            System.exit(1);
        }
    }
}
