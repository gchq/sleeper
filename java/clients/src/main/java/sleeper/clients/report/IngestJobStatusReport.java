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
            JobQuery query = IngestJobStatusReport.queryfromParametersOrPrompt(table, reportArgs.queryType(),
                    reportArgs.startTime() + "," + reportArgs.endTime(),
                    Clock.systemUTC(), ConsoleInput.stdIn());
            new IngestJobStatusReport(tracker, query, REPORTERS.get(reportArgs.outputType()),
                    QueueMessageCount.withSqsClient(sqsClient), instanceProperties,
                    PersistentEmrStepCount.byStatus(instanceProperties, emrClient)).run();
        }
    }

    public static final CommandLineUsage USAGE = CommandLineUsage.builder()
            .positionalArguments(List.of("instance-id", "table-name"))
            .options(List.of(CommandOption.longOption("output-type"),
                    CommandOption.shortFlag('a', "all"),
                    CommandOption.shortOption('d', "detailed"),
                    CommandOption.shortFlag('n', "rejected"),
                    CommandOption.shortFlag('r', "range"),
                    CommandOption.longOption("start-time"),
                    CommandOption.longOption("end-time"),
                    CommandOption.shortFlag('u', "unfinished")))
            .helpSummary("" +
                    "A report on ingest jobs within a Sleeper instance.\n" +
                    "\n" +
                    "--output-type <type>\n" +
                    "Output format. One of STANDARD, JSON. Defaults to STANDARD.\n" +
                    "\n" +
                    "Available query types for the report are (Choose 1):\n " +
                    "-a --all\n" +
                    "Returns all jobs.\n" +
                    "\n" +
                    "-d --detailed <jobId>\n" +
                    "Returns a detailed report for the job ID provided.\n" +
                    "\n" +
                    "-n --rejected\n" +
                    "Returns all rejected jobs.\n" +
                    "\n" +
                    "-r --range --start-time <startTime> --end-time <endTime>\n" +
                    "Returns all jobs within a given range. If not set, defaults to 4 hours.\n" +
                    "Alternatively, can be declared with --start-time and --end-time in the following format yyyyMMddhhmmss.\n" +
                    "\n" +
                    "-u --unfinished\n" +
                    "Returns all unfinished jobs.")
            .build();

    /**
     * Reads the arguments from the command line.
     *
     * @param  arguments the parsed command line arguments
     * @return           the arguments
     */
    public static Arguments readArguments(CommandArguments arguments) {
        JobQuery.Type jobType = determineQueryType(arguments);
        String jobId = null;
        String startTime = null;
        String endTime = null;

        if (jobType.equals(JobQuery.Type.DETAILED)) {
            if (arguments.getOptionalString("d").isPresent()) {
                jobId = arguments.getString("d");
            } else if (arguments.getOptionalString("detailed").isPresent()) {
                jobId = arguments.getString("detailed");
            } else {
                throw new CommandArgumentsException("Additional paramter of Job ID is required for the detailed query type.");
            }
        }

        if (jobType.equals(JobQuery.Type.RANGE)) {
            if (arguments.getOptionalString("start-time").isPresent()) {
                startTime = arguments.getString("start-time");
            }
            if (arguments.getOptionalString("end-time").isPresent()) {
                endTime = arguments.getString("end-time");
            }

            if (startTime == null && endTime != null) {
                throw new CommandArgumentsException("Missing paramter of start-time which is required for the ranged query type.");
            }
            if (endTime == null && startTime != null) {
                throw new CommandArgumentsException("Missing paramter of end-time which is required for the ranged query type.");
            }
        }

        return new Arguments(arguments.getString("instance-id"),
                arguments.getString("table-name"),
                arguments.getOptionalString("output-type")
                        .map(s -> s.toUpperCase(Locale.ROOT))
                        .orElse(DEFAULT_REPORTER),
                jobType,
                jobId,
                startTime,
                endTime);
    }

    /**
     * Holds the arguments for the ingest job status report command.
     *
     * @param instanceId the Sleeper instance ID
     * @param tableName  the table name
     * @param outputType the output format, either STANDARD or JSON
     * @param queryType  the type of query to execute for the ingest report
     * @param jobId      optional jobID for the detailed query
     * @param startTime  optional start time for range query
     * @param endTime    optional end time for range query
     */
    public record Arguments(String instanceId, String tableName, String outputType, JobQuery.Type queryType,
            String jobId, String startTime, String endTime) {
        public Arguments {
            if (!REPORTERS.containsKey(outputType)) {
                throw new CommandArgumentsException("Output type not supported: " + outputType + ". Valid types: " + String.join(", ", REPORTERS.keySet()));
            }
            if (queryType.equals(JobQuery.Type.RANGE) && startTime != null && endTime != null) {
                SimpleDateFormat dateInputFormat = new SimpleDateFormat(RangeJobsQuery.DATE_FORMAT);
                dateInputFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
                try {
                    Date start = dateInputFormat.parse(startTime);
                    Date end = dateInputFormat.parse(endTime);
                    if (end.before(start)) {
                        throw new CommandArgumentsException("Range end is before range start. Range start: " + startTime + ", range end: " + endTime);
                    }
                } catch (ParseException e) {
                    throw new CommandArgumentsException("Range parameters don't match expected format: " + RangeJobsQuery.DATE_FORMAT);
                }
            }
        }
    }

    private static JobQuery.Type determineQueryType(CommandArguments args) {
        Boolean allType = args.isFlagSet("a") || args.isFlagSet("all");
        Boolean detailedType = args.getOptionalString("d").isPresent() || args.getOptionalString("detailed").isPresent();
        Boolean rejectedType = args.isFlagSet("n") || args.isFlagSet("rejected");
        Boolean rangeType = args.isFlagSet("r") || args.isFlagSet("range");
        Boolean unfinishedType = args.isFlagSet("u") || args.isFlagSet("unfinished");

        if (Stream.of(allType, detailedType, rejectedType, rangeType, unfinishedType)
                .filter(flag -> flag.equals(Boolean.TRUE)).count() > 1) {
            Stream<Boolean> setFlags = Stream.of(allType, detailedType, rejectedType, rangeType, unfinishedType).filter(b -> Boolean.TRUE);
            StringBuilder outStr = new StringBuilder();
            setFlags.forEach(flag -> outStr.append(flag.getClass().getName()));
            throw new CommandArgumentsException("Too many report mode flags are set, maximum  of 1. Flags set: " + outStr.toString());
        }
        if (allType) {
            return JobQuery.Type.ALL;
        }
        if (detailedType) {
            return JobQuery.Type.DETAILED;
        }
        if (rejectedType) {
            return JobQuery.Type.REJECTED;
        }
        if (rangeType) {
            return JobQuery.Type.RANGE;
        }
        if (unfinishedType) {
            return JobQuery.Type.UNFINISHED;
        }
        //Default to return all
        return JobQuery.Type.ALL;
    }
}
