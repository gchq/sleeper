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

import java.time.Clock;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.stream.Collectors.joining;
import static sleeper.configuration.utils.AwsV2ClientHelper.buildAwsV2Client;

/**
 * Creates reports on the status of ingest and bulk import jobs. Takes a {@link JobQuery} and outputs information about
 * the jobs matching that query.
 */
public class IngestJobStatusReport {
    private static final IngestJobStatusReporter STANDARD_REPORTER = new StandardIngestJobStatusReporter();
    private static final IngestJobStatusReporter JSON_REPORTER = new JsonIngestJobStatusReporter();
    private static final ReportTypeArgument<IngestJobStatusReporter> REPORT_TYPE = ReportTypeArgument
            .withDefault("STANDARD", STANDARD_REPORTER)
            .addReporter("JSON", JSON_REPORTER)
            .build();
    /**
     * The query type options, and the query type each one selects. Declared in the order they appear in the usage,
     * which is the order they are reported in if the user sets more than one.
     */
    private static final Map<String, JobQuery.Type> QUERY_TYPE_BY_OPTION = createQueryTypeByOption();

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
     * Creates a query for ingest and bulk import jobs to include in a report.
     *
     * @param  queryType       the type of query
     * @param  queryParameters parameters for the query, as specified on the command line
     * @param  clock           a clock to get the current time, to read relative time ranges
     * @param  input           the console input, to prompt for further parameters
     * @return                 the query
     */
    public static JobQuery queryfromParametersOrPrompt(
            JobQuery.Type queryType, String queryParameters, Clock clock, ConsoleInput input) {
        return JobQuery.fromParametersOrPrompt(queryType, queryParameters, clock, input, Map.of("n", new RejectedJobsQuery()));
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
            JobQuery query = createQuery(reportArgs, Clock.systemUTC(), ConsoleInput.stdIn());
            new IngestJobStatusReport(tracker, table, query, reportArgs.reporter(),
                    QueueMessageCount.withSqsClient(sqsClient), instanceProperties,
                    PersistentEmrStepCount.byStatus(instanceProperties, emrClient)).run();
        }
    }

    public static final CommandLineUsage USAGE = CommandLineUsage.builder()
            .positionalArguments(List.of("instance-id", "table-name"))
            .options(List.of(
                    CommandOption.shortFlag('a', "all"),
                    CommandOption.shortOption('d', "detailed"),
                    CommandOption.longOption("end-time"),
                    CommandOption.shortFlag('r', "range"),
                    CommandOption.shortFlag('n', "rejected"),
                    ReportTypeArgument.option(),
                    CommandOption.longOption("start-time"),
                    CommandOption.shortFlag('u', "unfinished")))
            .helpSummary("" +
                    "A report on ingest jobs within a Sleeper instance.\n" +
                    "\n" +
                    "The jobs to report on are chosen with one of the query type options, " +
                    "which are --all, --detailed, --range, --rejected and --unfinished. " +
                    "Only one may be set at a time. If none is set, you will be prompted to choose one.\n" +
                    "\n" +
                    "--all, -a\n" +
                    "Reports on all jobs.\n" +
                    "\n" +
                    "--detailed, -d <job-ids>\n" +
                    "Reports in detail on the jobs with the given IDs. Separate several IDs with commas.\n" +
                    "\n" +
                    "--end-time <time>\n" +
                    "End of the period to report on, in the format " + RangeJobsQuery.DATE_FORMAT + ". " +
                    "Must be set together with --start-time, and only applies to the --range query type.\n" +
                    "\n" +
                    "--range, -r\n" +
                    "Reports on all jobs in a time period. Defaults to the last 4 hours, " +
                    "or set the period with --start-time and --end-time.\n" +
                    "\n" +
                    "--rejected, -n\n" +
                    "Reports on all rejected jobs.\n" +
                    "\n" +
                    REPORT_TYPE.helpText() + "\n" +
                    "\n" +
                    "--start-time <time>\n" +
                    "Start of the period to report on, in the format " + RangeJobsQuery.DATE_FORMAT + ". " +
                    "Must be set together with --end-time, and only applies to the --range query type.\n" +
                    "\n" +
                    "--unfinished, -u\n" +
                    "Reports on all unfinished jobs.")
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
        Instant startTime = null;
        Instant endTime = null;

        switch (jobType) {
            case DETAILED:
                // The query type is only DETAILED when this option was set, and the option always takes a value.
                // The value can still be empty if it was set like "--detailed=", which would report on no jobs.
                jobId = arguments.getString("detailed");
                if (jobId.isEmpty()) {
                    throw new CommandArgumentsException("Expected a value for option: detailed");
                }
                break;
            case RANGE:
                Optional<String> optionalStart = arguments.getOptionalString("start-time");
                Optional<String> optionalEnd = arguments.getOptionalString("end-time");

                if (optionalStart.isPresent() && optionalEnd.isPresent()) {
                    startTime = readTime("start-time", optionalStart.get());
                    endTime = readTime("end-time", optionalEnd.get());
                    if (endTime.isBefore(startTime)) {
                        throw new CommandArgumentsException("Range end is before range start. Range start: " + optionalStart.get() + ", range end: " + optionalEnd.get());
                    }
                } else if (optionalStart.isEmpty() && optionalEnd.isPresent()) {
                    throw new CommandArgumentsException("Missing parameter of start-time which is required for the Range query type.");
                } else if (optionalStart.isPresent() && optionalEnd.isEmpty()) {
                    throw new CommandArgumentsException("Missing parameter of end-time which is required for the Range query type.");
                }
                break;
            default:
                break;
        }

        // Below error message to be removed as part of work for ticket number: https://github.com/gchq/sleeper/issues/8061
        if (!jobType.equals(JobQuery.Type.RANGE) &&
                (arguments.getOptionalString("start-time").isPresent() ||
                        arguments.getOptionalString("end-time").isPresent())) {
            throw new CommandArgumentsException("Range time flags, start-time and end-time are not valid for following query type: " + jobType);
        }

        return new Arguments(arguments.getString("instance-id"),
                arguments.getString("table-name"),
                REPORT_TYPE.read(arguments),
                jobType,
                jobId,
                startTime,
                endTime);
    }

    /**
     * Determines which query type the user asked for. Exactly one query type option may be set. If none is set, the
     * user is prompted for one, unless a time was given for a range.
     *
     * @param  arguments the parsed command line arguments
     * @return           the query type
     */
    private static JobQuery.Type determineQueryType(CommandArguments arguments) {
        List<JobQuery.Type> setTypes = QUERY_TYPE_BY_OPTION.entrySet().stream()
                .filter(entry -> isOptionSet(arguments, entry.getKey()))
                .map(Map.Entry::getValue)
                .toList();
        if (setTypes.size() > 1) {
            throw new CommandArgumentsException("Too many query type flags are set, maximum of 1. Flags set: " +
                    setTypes.stream().map(JobQuery.Type::name).collect(joining(", ")));
        }
        if (!setTypes.isEmpty()) {
            return setTypes.get(0);
        }
        // Additional step to trigger range query if no flag presented, but start-time or end-time present.
        // Either one on its own is an error, but it is reported when the range is read, so that the user is told
        // which one is missing rather than that the time they did set is invalid for some other query type.
        // Likely to be refactored when including range as an option with the Query Types rather than a separate one
        // See ticket: https://github.com/gchq/sleeper/issues/8061
        if (arguments.getOptionalString("start-time").isPresent()
                || arguments.getOptionalString("end-time").isPresent()) {
            return JobQuery.Type.RANGE;
        }
        return JobQuery.Type.PROMPT;
    }

    private static Instant readTime(String option, String value) {
        try {
            return RangeJobsQuery.parseTime(value);
        } catch (IllegalArgumentException e) {
            throw new CommandArgumentsException(
                    option + " parameter doesn't match expected format: " + RangeJobsQuery.DATE_FORMAT);
        }
    }

    private static boolean isOptionSet(CommandArguments arguments, String option) {
        return arguments.isFlagSet(option) || arguments.getOptionalString(option).isPresent();
    }

    private static Map<String, JobQuery.Type> createQueryTypeByOption() {
        Map<String, JobQuery.Type> queryTypeByOption = new LinkedHashMap<>();
        queryTypeByOption.put("all", JobQuery.Type.ALL);
        queryTypeByOption.put("detailed", JobQuery.Type.DETAILED);
        queryTypeByOption.put("range", JobQuery.Type.RANGE);
        queryTypeByOption.put("rejected", JobQuery.Type.REJECTED);
        queryTypeByOption.put("unfinished", JobQuery.Type.UNFINISHED);
        return queryTypeByOption;
    }

    /**
     * Creates the query for the jobs to report on. The times for a range are read when the arguments are validated,
     * so the range is built directly rather than passing the times as parameters to be read again.
     *
     * @param  args  the arguments read from the command line
     * @param  clock a clock to get the current time, to read relative time ranges
     * @param  input the console input, to prompt for further parameters
     * @return       the query
     */
    public static JobQuery createQuery(Arguments args, Clock clock, ConsoleInput input) {
        switch (args.queryType()) {
            case RANGE:
                if (args.startTime() == null) {
                    return RangeJobsQuery.forDefaultPeriod(clock);
                }
                return new RangeJobsQuery(args.startTime(), args.endTime());
            default:
                return queryfromParametersOrPrompt(args.queryType(), args.jobId(), clock, input);
        }
    }

    /**
     * Holds the arguments for the ingest job status report command.
     *
     * @param instanceId the Sleeper instance ID
     * @param tableName  the table name
     * @param reporter   the reporter format, either STANDARD or JSON
     * @param queryType  the type of query to execute for the ingest report
     * @param jobId      optional job IDs separated by commas for the detailed query
     * @param startTime  optional start time for range query
     * @param endTime    optional end time for range query
     */
    public record Arguments(String instanceId, String tableName, IngestJobStatusReporter reporter, JobQuery.Type queryType,
            String jobId, Instant startTime, Instant endTime) {
    }
}
