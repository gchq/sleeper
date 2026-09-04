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
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sts.StsClient;

import sleeper.clients.report.compaction.job.CompactionJobStatusReporter;
import sleeper.clients.report.compaction.job.JsonCompactionJobStatusReporter;
import sleeper.clients.report.compaction.job.StandardCompactionJobStatusReporter;
import sleeper.clients.report.job.query.JobQuery;
import sleeper.clients.util.console.ConsoleInput;
import sleeper.compaction.tracker.job.CompactionJobTrackerFactory;
import sleeper.configuration.properties.S3InstanceProperties;
import sleeper.configuration.table.index.DynamoDBTableIndex;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.table.TableStatus;
import sleeper.core.tracker.compaction.job.CompactionJobTracker;
import sleeper.core.util.cli.CommandArguments;
import sleeper.core.util.cli.CommandArgumentsException;
import sleeper.core.util.cli.CommandLineUsage;
import sleeper.core.util.cli.CommandOption;

import java.time.Clock;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static sleeper.configuration.utils.AwsV2ClientHelper.buildAwsV2Client;

/**
 * Creates reports on the status of compaction jobs. Takes a {@link JobQuery} and outputs information about the jobs
 * matching that query.
 */
public class CompactionJobStatusReport {
    private static final String DEFAULT_REPORTER = "STANDARD";
    private static final Map<String, CompactionJobStatusReporter> REPORTERS = new HashMap<>();

    static {
        REPORTERS.put(DEFAULT_REPORTER, new StandardCompactionJobStatusReporter());
        REPORTERS.put("JSON", new JsonCompactionJobStatusReporter());
    }

    private final CompactionJobStatusReporter compactionJobStatusReporter;
    private final CompactionJobTracker compactionJobTracker;
    private final JobQuery.Type queryType;
    private final JobQuery query;

    public CompactionJobStatusReport(
            CompactionJobTracker compactionJobTracker,
            CompactionJobStatusReporter reporter,
            JobQuery query) {
        this.compactionJobTracker = compactionJobTracker;
        this.compactionJobStatusReporter = reporter;
        this.query = query;
        this.queryType = query.getType();
    }

    /**
     * Creates the report.
     */
    public void run() {
        if (query == null) {
            return;
        }
        compactionJobStatusReporter.report(query.run(compactionJobTracker), queryType);
    }

    public static void main(String[] args) {
        Arguments reportArgs = CommandArguments.parseAndValidateOrExit(USAGE, args, CompactionJobStatusReport::readArguments);

        try (S3Client s3Client = buildAwsV2Client(S3Client.builder());
                DynamoDbClient dynamoClient = buildAwsV2Client(DynamoDbClient.builder());
                StsClient stsClient = buildAwsV2Client(StsClient.builder())) {
            String accountName = stsClient.getCallerIdentity().account();
            InstanceProperties instanceProperties = S3InstanceProperties.loadGivenAccountAndInstanceId(s3Client, accountName, reportArgs.instanceId());
            DynamoDBTableIndex tableIndex = new DynamoDBTableIndex(instanceProperties, dynamoClient);
            TableStatus table = tableIndex.getTableByName(reportArgs.tableName())
                    .orElseThrow(() -> new IllegalArgumentException("Table does not exist: " + reportArgs.tableName()));
            CompactionJobTracker tracker = CompactionJobTrackerFactory.getTracker(dynamoClient, instanceProperties);
            JobQuery query = JobQuery.fromParametersOrPrompt(table, reportArgs.queryType(),
                    reportArgs.startTime() + "," + reportArgs.endTime(),
                    Clock.systemUTC(), ConsoleInput.stdIn());
            new CompactionJobStatusReport(tracker, REPORTERS.get(reportArgs.reportType().toUpperCase(Locale.ROOT)), query).run();
        }

    }

    public static final CommandLineUsage USAGE = CommandLineUsage.builder()
            .positionalArguments(List.of("instance-id", "table-name", "report-type"))
            .options(List.of(CommandOption.shortFlag('a', "all"),
                    CommandOption.shortOption('d', "detailed"),
                    CommandOption.shortFlag('r', "range"),
                    CommandOption.shortFlag('u', "unfinished"),
                    CommandOption.longOption("start-time"),
                    CommandOption.longOption("end-time")))
            .helpSummary("" +
                    "A report on the status of the compaction jobs within a sleeper instance.\n" +
                    "\n" +
                    "--report-type\n" +
                    "Format of the report. One of STANDARD or JSON. Defaults to STANDARD.\n" +
                    "\n" +
                    "Available query types for the report are:\n" +
                    "[Defaults to all if none set]" +
                    "\n" +
                    "-a --all \n" +
                    "Returns all the jobs for the report.\n" +
                    "\n" +
                    "-d --detailed <jobId>\n" +
                    "Returns a detailed report for the job ID provided." +
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
        JobQuery.Type queryType = null; //Need method
        String jobId = null; // Need to set
        String startTime = null; // Need to set
        String endTime = null; // Need to set
        return new Arguments(arguments.getString("instance-id"),
                arguments.getString("table-name"),
                arguments.getString("report-type"),
                queryType,
                jobId,
                startTime,
                endTime);
    }

    /**
     * Holds the arguments for the compaction job status report command.
     *
     * @param instanceId the Sleeper instance ID
     * @param tableName  the table name
     * @param reportType the report format, either STANDARD or JSON
     * @param queryType  the type of query to execute for the compaction status report
     * @param jobId      optional jobId for the detailed query
     * @param startTime  optional start time for the range query
     * @param endTime    optional end time for the range query
     */
    public record Arguments(String instanceId, String tableName, String reportType, JobQuery.Type queryType, String jobId, String startTime, String endTime) {
        public Arguments {
            if (!REPORTERS.containsKey(reportType.toUpperCase(Locale.ROOT))) {
                throw new CommandArgumentsException("Report type not supported: " + reportType + ". Valid types: " + String.join(", ", REPORTERS.keySet()));
            }
        }
    }
}
