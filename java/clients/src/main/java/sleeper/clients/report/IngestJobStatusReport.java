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

import sleeper.clients.report.arguments.JobTrackerReportOptions;
import sleeper.clients.report.ingest.job.IngestJobStatusReporter;
import sleeper.clients.report.ingest.job.IngestQueueMessages;
import sleeper.clients.report.ingest.job.PersistentEmrStepCount;
import sleeper.clients.report.job.query.JobQuery;
import sleeper.clients.util.console.ConsoleInput;
import sleeper.common.task.QueueMessageCount;
import sleeper.configuration.properties.S3InstanceProperties;
import sleeper.configuration.table.index.DynamoDBTableIndex;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.table.TableStatus;
import sleeper.core.tracker.ingest.job.IngestJobTracker;
import sleeper.core.util.cli.CommandArguments;
import sleeper.core.util.cli.CommandLineUsage;
import sleeper.ingest.tracker.job.IngestJobTrackerFactory;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static sleeper.configuration.utils.AwsV2ClientHelper.buildAwsV2Client;

/**
 * Creates reports on the status of ingest and bulk import jobs. Takes a {@link JobQuery} and outputs information about
 * the jobs matching that query.
 */
public class IngestJobStatusReport {

    private final IngestJobTracker tracker;
    private final IngestJobStatusReporter reporter;
    private final QueueMessageCount.Client queueClient;
    private final InstanceProperties properties;
    private final TableStatus tableStatus;
    private final JobQuery query;
    private final Map<String, Integer> persistentEmrStepCount;

    public IngestJobStatusReport(
            IngestJobTracker tracker, TableStatus tableStatus, JobQuery query,
            IngestJobStatusReporter reporter, QueueMessageCount.Client queueClient, InstanceProperties properties,
            Map<String, Integer> persistentEmrStepCount) {
        this.tracker = tracker;
        this.query = query;
        this.reporter = reporter;
        this.queueClient = queueClient;
        this.properties = properties;
        this.tableStatus = tableStatus;
        this.persistentEmrStepCount = persistentEmrStepCount;
    }

    /**
     * Creates a report.
     */
    public void run() {
        if (query == null) {
            return;
        }
        reporter.report(
                query.run(tracker, tableStatus.getTableUniqueId()), query.getType(),
                IngestQueueMessages.from(properties, queueClient),
                persistentEmrStepCount);
    }

    public static void main(String[] args) {
        Arguments reportArgs = CommandArguments.parseAndValidateOrExit(USAGE, args,
                cmdArgs -> readArguments(cmdArgs, Instant::now, ConsoleInput.stdIn()));

        try (S3Client s3Client = buildAwsV2Client(S3Client.builder());
                DynamoDbClient dynamoClient = buildAwsV2Client(DynamoDbClient.builder());
                SqsClient sqsClient = buildAwsV2Client(SqsClient.builder());
                EmrClient emrClient = buildAwsV2Client(EmrClient.builder());
                StsClient stsClient = buildAwsV2Client(StsClient.builder())) {
            String accountName = stsClient.getCallerIdentity().account();
            InstanceProperties instanceProperties = S3InstanceProperties.loadGivenAccountAndInstanceId(s3Client, accountName, reportArgs.instanceId());
            DynamoDBTableIndex tableIndex = new DynamoDBTableIndex(instanceProperties, dynamoClient);
            TableStatus table = tableIndex.getTableByName(reportArgs.tableName())
                    .orElseThrow(() -> new IllegalArgumentException("Table does not exist: " + reportArgs.tableName()));
            IngestJobTracker tracker = IngestJobTrackerFactory.getTracker(dynamoClient, instanceProperties);
            new IngestJobStatusReport(tracker, table, reportArgs.query(), reportArgs.reporter(),
                    QueueMessageCount.withSqsClient(sqsClient), instanceProperties,
                    PersistentEmrStepCount.byStatus(instanceProperties, emrClient)).run();
        }
    }

    public static final CommandLineUsage USAGE = CommandLineUsage.builder()
            .positionalArguments(List.of("instance-id", "table-name"))
            .options(JobTrackerReportOptions.INGEST_OPTIONS)
            .helpSummary("" +
                    "A report on ingest jobs within a Sleeper instance.\n" +
                    "\n" +
                    "The jobs to report on are chosen with one of the query type options. " +
                    "Only one may be set at a time. If none is set, you will be prompted to choose one.")
            .build();

    /**
     * Reads the arguments from the command line and builds the query.
     *
     * @param  arguments    the parsed command line arguments
     * @param  timeSupplier a supplier of the current time, to read relative time ranges
     * @param  input        the console input, to prompt for further parameters
     * @return              the arguments
     */
    public static Arguments readArguments(CommandArguments arguments, Supplier<Instant> timeSupplier, ConsoleInput input) {
        return new Arguments(arguments.getString("instance-id"),
                arguments.getString("table-name"),
                JobTrackerReportOptions.INGEST_OUTPUT_FORMAT.read(arguments),
                JobTrackerReportOptions.readIngestJobQuery(arguments, timeSupplier, input));
    }

    /**
     * Holds the arguments for the ingest job status report command.
     *
     * @param instanceId the Sleeper instance ID
     * @param tableName  the table name
     * @param reporter   the reporter format, either STANDARD or JSON
     * @param query      the query to execute for the report
     */
    public record Arguments(String instanceId, String tableName, IngestJobStatusReporter reporter, JobQuery query) {
    }
}
