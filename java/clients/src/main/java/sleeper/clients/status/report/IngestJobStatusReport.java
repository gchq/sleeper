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

package sleeper.clients.status.report;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import software.amazon.awssdk.services.emr.EmrClient;

import sleeper.clients.status.report.ingest.job.IngestJobStatusReporter;
import sleeper.clients.status.report.ingest.job.IngestQueueMessages;
import sleeper.clients.status.report.ingest.job.JsonIngestJobStatusReporter;
import sleeper.clients.status.report.ingest.job.PersistentEMRStepCount;
import sleeper.clients.status.report.ingest.job.StandardIngestJobStatusReporter;
import sleeper.clients.status.report.ingest.job.query.IngestJobQueryArgument;
import sleeper.clients.status.report.job.query.JobQuery;
import sleeper.clients.status.report.job.query.RejectedJobsQuery;
import sleeper.clients.util.console.ConsoleInput;
import sleeper.configuration.properties.S3InstanceProperties;
import sleeper.configuration.table.index.DynamoDBTableIndex;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.table.TableStatus;
import sleeper.ingest.job.status.IngestJobStatusStore;
import sleeper.ingest.status.store.job.IngestJobStatusStoreFactory;
import sleeper.task.common.QueueMessageCount;

import java.time.Clock;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import static sleeper.clients.util.ClientUtils.optionalArgument;
import static sleeper.configuration.utils.AwsV1ClientHelper.buildAwsV1Client;
import static sleeper.configuration.utils.AwsV2ClientHelper.buildAwsV2Client;

public class IngestJobStatusReport {
    private static final String DEFAULT_REPORTER = "STANDARD";
    private static final Map<String, IngestJobStatusReporter> REPORTERS = new HashMap<>();

    static {
        REPORTERS.put(DEFAULT_REPORTER, new StandardIngestJobStatusReporter());
        REPORTERS.put("JSON", new JsonIngestJobStatusReporter());
    }

    private final IngestJobStatusStore statusStore;
    private final IngestJobStatusReporter ingestJobStatusReporter;
    private final QueueMessageCount.Client queueClient;
    private final InstanceProperties properties;
    private final JobQuery.Type queryType;
    private final JobQuery query;
    private final Map<String, Integer> persistentEmrStepCount;

    public IngestJobStatusReport(
            IngestJobStatusStore ingestJobStatusStore,
            TableStatus table, JobQuery.Type queryType, String queryParameters,
            IngestJobStatusReporter reporter, QueueMessageCount.Client queueClient, InstanceProperties properties,
            Map<String, Integer> persistentEmrStepCount) {
        this(ingestJobStatusStore, JobQuery.fromParametersOrPrompt(table, queryType, queryParameters,
                Clock.systemUTC(), new ConsoleInput(System.console()), Map.of("n", new RejectedJobsQuery())),
                reporter, queueClient, properties, persistentEmrStepCount);
    }

    public IngestJobStatusReport(
            IngestJobStatusStore ingestJobStatusStore, JobQuery query,
            IngestJobStatusReporter reporter, QueueMessageCount.Client queueClient, InstanceProperties properties,
            Map<String, Integer> persistentEmrStepCount) {
        this.statusStore = ingestJobStatusStore;
        this.query = query;
        this.queryType = query.getType();
        this.ingestJobStatusReporter = reporter;
        this.queueClient = queueClient;
        this.properties = properties;
        this.persistentEmrStepCount = persistentEmrStepCount;
    }

    public void run() {
        if (query == null) {
            return;
        }
        ingestJobStatusReporter.report(
                query.run(statusStore), queryType,
                IngestQueueMessages.from(properties, queueClient),
                persistentEmrStepCount);
    }

    public static void main(String[] args) {
        try {
            if (args.length < 2 || args.length > 5) {
                throw new IllegalArgumentException("Wrong number of arguments");
            }
            String instanceId = args[0];
            String tableName = args[1];
            IngestJobStatusReporter reporter = getReporter(args, 2);
            JobQuery.Type queryType = IngestJobQueryArgument.readTypeArgument(args, 3);
            String queryParameters = optionalArgument(args, 4).orElse(null);

            AmazonS3 s3Client = buildAwsV1Client(AmazonS3ClientBuilder.standard());
            AmazonDynamoDB dynamoDBClient = buildAwsV1Client(AmazonDynamoDBClientBuilder.standard());
            AmazonSQS sqsClient = buildAwsV1Client(AmazonSQSClientBuilder.standard());
            try (EmrClient emrClient = buildAwsV2Client(EmrClient.builder())) {
                InstanceProperties instanceProperties = S3InstanceProperties.loadGivenInstanceId(s3Client, instanceId);
                DynamoDBTableIndex tableIndex = new DynamoDBTableIndex(instanceProperties, dynamoDBClient);
                TableStatus table = tableIndex.getTableByName(tableName)
                        .orElseThrow(() -> new IllegalArgumentException("Table does not exist: " + tableName));
                IngestJobStatusStore statusStore = IngestJobStatusStoreFactory.getStatusStore(dynamoDBClient, instanceProperties);
                new IngestJobStatusReport(statusStore, table, queryType, queryParameters,
                        reporter, QueueMessageCount.withSqsClient(sqsClient), instanceProperties,
                        PersistentEMRStepCount.byStatus(instanceProperties, emrClient)).run();
            } finally {
                s3Client.shutdown();
                dynamoDBClient.shutdown();
                sqsClient.shutdown();
            }
        } catch (IllegalArgumentException e) {
            System.out.println(e.getMessage());
            printUsage();
            System.exit(1);
        }
    }

    private static void printUsage() {
        System.out.println("" +
                "Usage: <instance-id> <table-name> <report-type-standard-or-json> <optional-query-type> <optional-query-parameters> \n" +
                "Query types are:\n" +
                "-a (Return all jobs)\n" +
                "-d (Detailed, provide a jobId)\n" +
                "-n (Rejected jobs)\n" +
                "-r (Provide startRange and endRange separated by commas in format yyyyMMddhhmmss)\n" +
                "-u (Unfinished jobs)");
    }

    private static IngestJobStatusReporter getReporter(String[] args, int index) {
        String reporterType = optionalArgument(args, index)
                .map(str -> str.toUpperCase(Locale.ROOT))
                .orElse(DEFAULT_REPORTER);
        if (!REPORTERS.containsKey(reporterType)) {
            throw new IllegalArgumentException("Output type not supported: " + reporterType);
        }
        return REPORTERS.get(reporterType);
    }
}
