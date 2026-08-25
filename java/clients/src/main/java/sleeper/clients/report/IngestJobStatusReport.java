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
import sleeper.clients.report.job.query.JobQuery;
import sleeper.clients.report.job.query.RangeJobsQuery;
import sleeper.clients.report.job.query.RejectedJobsQuery;
import sleeper.clients.util.console.ConsoleInput;
import sleeper.common.task.QueueMessageCount;
import sleeper.configuration.properties.S3InstanceProperties;
import sleeper.configuration.table.index.DynamoDBTableIndex;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.table.TableStatus;
import sleeper.core.tracker.ingest.job.IngestJobTracker;
import sleeper.core.util.cli.CommandArguments;
import sleeper.core.util.cli.CommandArgumentsException;
import sleeper.core.util.cli.CommandLineUsage;
import sleeper.core.util.cli.CommandOption;
import sleeper.ingest.tracker.job.IngestJobTrackerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Clock;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;
import java.util.stream.Stream;

import static sleeper.configuration.utils.AwsV2ClientHelper.buildAwsV2Client;

/**
 * Creates reports on the status of ingest and bulk import jobs. Takes a {@link JobQuery} and outputs information about
 * the jobs matching that query.
 */
public class IngestJobStatusReport {
    private static final String DEFAULT_REPORTER = "STANDARD";
    private static final Map<String, IngestJobStatusReporter> REPORTERS = new HashMap<>();

    private static List<JobQuery.Type> requiresQueryParams = List.of(JobQuery.Type.DETAILED, JobQuery.Type.RANGE);

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
        Arguments reportArgs = CommandArguments.parseAndValidateOrExit(USAGE, args, IngestJobStatusReport::readArguments);

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
            JobQuery query = IngestJobStatusReport.queryfromParametersOrPrompt(table, reportArgs.queryType(), reportArgs.queryParameters(), Clock.systemUTC(), ConsoleInput.stdIn());
            new IngestJobStatusReport(tracker, query, REPORTERS.get(reportArgs.reportType()),
                    QueueMessageCount.withSqsClient(sqsClient), instanceProperties,
                    PersistentEmrStepCount.byStatus(instanceProperties, emrClient)).run();
        }
    }

    public static final CommandLineUsage USAGE = CommandLineUsage.builder()
            .positionalArguments(List.of("instance-id", "table-name"))
            .options(List.of(CommandOption.longOption("report-type"),
                    CommandOption.shortFlag('a', "all-jobs"),
                    CommandOption.shortFlag('d', "detailed"),
                    CommandOption.shortFlag('n', "rejected-jobs"),
                    CommandOption.shortFlag('r', "range"),
                    CommandOption.shortFlag('u', "unfinished-jobs"),
                    CommandOption.longOption("query-params")))
            .helpSummary("" +
                    "Creates a report listing all the status of the ingest jobs within a Sleeper instance.\n" +
                    "\n" +
                    "--report-type <type>\n" +
                    "Output format. One of STANDARD, JSON. Defaults to STANDARD.\n" +
                    "\n" +
                    "Available query types for the report are:\n" +
                    "[If none, set will default to all, only one can be used at any one time]\n" +
                    "-a [Returns all jobs]\n" +
                    "-d [Detailed, requires a jobId as an optional query parameter]\n" +
                    "-n [Rejected jobs]\n" +
                    "-r [Range, start and end points requires as optional query parameter in following format yyyyMMddhhmmss]\n" +
                    "-u [Unfinished jobs]\n" +
                    "\n" +
                    "--query-params <params>\n" +
                    "Additional parameters required for several query types")
            .build();

    /**
     * Reads the arguments from the command line.
     *
     * @param  arguments the parsed command line arguments
     * @return           the arguments
     */
    public static Arguments readArguments(CommandArguments arguments) {

        return new Arguments(
                arguments.getString("instance-id"),
                arguments.getString("table-name"),
                arguments.getOptionalString("report-type")
                        .map(s -> s.toUpperCase(Locale.ROOT))
                        .orElse(DEFAULT_REPORTER),
                determineQueryType(arguments),
                arguments.getOptionalString("query-parameters").orElse(""));
    }

    /**
     * Holds the arguments for the ingest job status report command.
     *
     * @param instanceId      the Sleeper instance ID
     * @param tableName       the table name
     * @param reportType      the output format, either STANDARD or JSON
     * @param queryType       the type of query to execute for the ingest report
     * @param queryParameters option parameters for several of the report types
     */
    public record Arguments(String instanceId, String tableName, String reportType, JobQuery.Type queryType, String queryParameters) {
        public Arguments {
            if (!REPORTERS.containsKey(reportType)) {
                throw new CommandArgumentsException("Report type not supported: " + reportType + ". Valid types: " + String.join(", ", REPORTERS.keySet()));
            }
            if (requiresQueryParams.contains(queryType)) {
                if (queryParameters.equals(null) || "".equals(queryParameters)) {
                    throw new CommandArgumentsException("Query parameters are required for the query type: " + queryType);
                } else if (queryType.equals(JobQuery.Type.RANGE)) {
                    String[] params = queryParameters.split(",");
                    SimpleDateFormat dateInputFormat = new SimpleDateFormat(RangeJobsQuery.DATE_FORMAT);
                    dateInputFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
                    try {
                        Date start = dateInputFormat.parse(params[0]);
                        Date end = dateInputFormat.parse(params[1]);
                        if (end.before(start)) {
                            throw new CommandArgumentsException("Range end is before rage start. Range start: " + params[0] + ", range end: " + params[1]);
                        }
                    } catch (ParseException e) {
                        throw new CommandArgumentsException("Range parameters don't match expected format: " + RangeJobsQuery.DATE_FORMAT);
                    }
                }
            }

        }
    }

    private static JobQuery.Type determineQueryType(CommandArguments args) {
        Boolean allJobsFlag = args.isFlagSet("a");
        Boolean detailedFlag = args.isFlagSet("d");
        Boolean rejectedJobsFlag = args.isFlagSet("n");
        Boolean rangeFlag = args.isFlagSet("r");
        Boolean unfinishedJobsFlag = args.isFlagSet("u");

        if (Stream.of(allJobsFlag, detailedFlag, rejectedJobsFlag, rangeFlag, unfinishedJobsFlag)
                .filter(flag -> flag.equals(Boolean.TRUE)).count() > 1) {
            Stream<Boolean> setFlags = Stream.of(allJobsFlag, detailedFlag, rejectedJobsFlag, rangeFlag, unfinishedJobsFlag).filter(b -> Boolean.TRUE);
            StringBuilder outStr = new StringBuilder();
            setFlags.forEach(flag -> outStr.append(flag.getClass().getName()));
            throw new CommandArgumentsException("Too many report mode flags are set, maximum  of 1. Flags set: " + outStr.toString());
        }
        if (allJobsFlag) {
            return JobQuery.Type.ALL;
        }
        if (detailedFlag) {
            return JobQuery.Type.DETAILED;
        }
        if (rejectedJobsFlag) {
            return JobQuery.Type.REJECTED;
        }
        if (rangeFlag) {
            return JobQuery.Type.RANGE;
        }
        if (unfinishedJobsFlag) {
            return JobQuery.Type.UNFINISHED;
        }
        //Default to return all
        return JobQuery.Type.ALL;
    }
}
