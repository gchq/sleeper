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

package sleeper.clients.report;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.emr.EmrClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sts.StsClient;

import sleeper.clients.report.ingest.job.IngestJobStatusReporter;
import sleeper.clients.report.ingest.job.IngestQueueMessages;
import sleeper.clients.report.ingest.job.JsonIngestJobStatusReporter;
import sleeper.clients.report.ingest.job.PersistentEmrStepCount;
import sleeper.clients.report.ingest.job.StandardIngestJobStatusReporter;
import sleeper.clients.report.ingest.job.query.IngestJobQueryArgument;
import sleeper.clients.report.job.query.JobQuery;
import sleeper.clients.report.job.query.RejectedJobsQuery;
import sleeper.clients.util.console.ConsoleInput;
import sleeper.common.task.QueueMessageCount;
import sleeper.configuration.properties.S3InstanceProperties;
import sleeper.configuration.table.index.DynamoDBTableIndex;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.table.TableStatus;
import sleeper.core.tracker.ingest.job.IngestJobTracker;
import sleeper.ingest.tracker.job.IngestJobTrackerFactory;

import java.time.Clock;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import static sleeper.clients.util.ClientUtils.optionalArgument;
import static sleeper.configuration.utils.AwsV2ClientHelper.buildAwsV2Client;

/**
 * Creates reports on the status of ingest and bulk import jobs. Takes a {@link JobQuery} and outputs information about
 * the jobs matching that query.
 */
public class IngestJobStatusReport {
    private static final String DEFAULT_REPORTER = "STANDARD";
    private static final Map<String, IngestJobStatusReporter> REPORTERS = new HashMap<>();

    static {
        REPORTERS.put(DEFAULT_REPORTER, new StandardIngestJobStatusReporter());
        REPORTERS.put("JSON", new JsonIngestJobStatusReporter());
    }

    private final IngestJobTracker tracker;
    private final IngestJobStatusReporter reporter;
    private final QueueMessageCount.Client queueClient;
    private final InstanceProperties properties;
    private final JobQuery.Type queryType;
    private final JobQuery query;
    private final Map<String, Integer> persistentEmrStepCount;

    public IngestJobStatusReport(
            IngestJobTracker tracker, JobQuery query,
            IngestJobStatusReporter reporter, QueueMessageCount.Client queueClient, InstanceProperties properties,
            Map<String, Integer> persistentEmrStepCount) {
        this.tracker = tracker;
        this.query = query;
        this.queryType = query.getType();
        this.reporter = reporter;
        this.queueClient = queueClient;
        this.properties = properties;
        this.persistentEmrStepCount = persistentEmrStepCount;
    }

    /**
     * Creates a query for ingest and bulk import jobs to include in a report.
     *
     * @param  table           the Sleeper table to include jobs for
     * @param  queryType       the type of query
     * @param  queryParameters parameters for the query, as specified on the command line
     * @param  clock           a clock to get the current time, to read relative time ranges
     * @param  input           the console input, to prompt for further parameters
     * @return                 the query
     */
    public static JobQuery queryfromParametersOrPrompt(
            TableStatus table, JobQuery.Type queryType, String queryParameters, Clock clock, ConsoleInput input) {
        return JobQuery.fromParametersOrPrompt(table, queryType, queryParameters, clock, input,
                Map.of("n", new RejectedJobsQuery()));
    }

    /**
     * Creates a report.
     */
    public void run() {
        if (query == null) {
            return;
        }
        reporter.report(
                query.run(tracker), queryType,
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

            try (S3Client s3Client = buildAwsV2Client(S3Client.builder());
                    DynamoDbClient dynamoClient = buildAwsV2Client(DynamoDbClient.builder());
                    SqsClient sqsClient = buildAwsV2Client(SqsClient.builder());
                    EmrClient emrClient = buildAwsV2Client(EmrClient.builder());
                    StsClient stsClient = buildAwsV2Client(StsClient.builder())) {
                String accountName = stsClient.getCallerIdentity().account();
                InstanceProperties instanceProperties = S3InstanceProperties.loadGivenAccountAndInstanceId(s3Client, accountName, instanceId);
                DynamoDBTableIndex tableIndex = new DynamoDBTableIndex(instanceProperties, dynamoClient);
                TableStatus table = tableIndex.getTableByName(tableName)
                        .orElseThrow(() -> new IllegalArgumentException("Table does not exist: " + tableName));
                IngestJobTracker tracker = IngestJobTrackerFactory.getTracker(dynamoClient, instanceProperties);
                JobQuery query = IngestJobStatusReport.queryfromParametersOrPrompt(table, queryType, queryParameters, Clock.systemUTC(), ConsoleInput.stdIn());
                new IngestJobStatusReport(tracker, query, reporter,
                        QueueMessageCount.withSqsClient(sqsClient), instanceProperties,
                        PersistentEmrStepCount.byStatus(instanceProperties, emrClient)).run();
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
