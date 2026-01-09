/*
 * Copyright 2022-2025 Crown Copyright
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

import sleeper.clients.report.compaction.job.CompactionJobStatusReporter;
import sleeper.clients.report.compaction.job.JsonCompactionJobStatusReporter;
import sleeper.clients.report.compaction.job.StandardCompactionJobStatusReporter;
import sleeper.clients.report.job.query.JobQuery;
import sleeper.clients.report.job.query.JobQueryArgument;
import sleeper.clients.util.console.ConsoleInput;
import sleeper.compaction.tracker.job.CompactionJobTrackerFactory;
import sleeper.configuration.properties.S3InstanceProperties;
import sleeper.configuration.table.index.DynamoDBTableIndex;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.table.TableStatus;
import sleeper.core.tracker.compaction.job.CompactionJobTracker;

import java.time.Clock;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import static sleeper.clients.util.ClientUtils.optionalArgument;
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
            TableStatus table, JobQuery.Type queryType) {
        this(compactionJobTracker, reporter, table, queryType, "");
    }

    public CompactionJobStatusReport(
            CompactionJobTracker compactionJobTracker,
            CompactionJobStatusReporter reporter,
            TableStatus table, JobQuery.Type queryType, String queryParameters) {
        this(compactionJobTracker, reporter,
                JobQuery.fromParametersOrPrompt(table, queryType, queryParameters,
                        Clock.systemUTC(), new ConsoleInput(System.console())));
    }

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
        try {
            if (args.length < 2 || args.length > 5) {
                throw new IllegalArgumentException("Wrong number of arguments");
            }
            String instanceId = args[0];
            String tableName = args[1];
            CompactionJobStatusReporter reporter = getReporter(args, 2);
            JobQuery.Type queryType = JobQueryArgument.readTypeArgument(args, 3);
            String queryParameters = optionalArgument(args, 4).orElse(null);

            try (S3Client s3Client = buildAwsV2Client(S3Client.builder());
                    DynamoDbClient dynamoClient = buildAwsV2Client(DynamoDbClient.builder())) {
                InstanceProperties instanceProperties = S3InstanceProperties.loadGivenInstanceId(s3Client, instanceId);
                DynamoDBTableIndex tableIndex = new DynamoDBTableIndex(instanceProperties, dynamoClient);
                TableStatus table = tableIndex.getTableByName(tableName)
                        .orElseThrow(() -> new IllegalArgumentException("Table does not exist: " + tableName));
                CompactionJobTracker tracker = CompactionJobTrackerFactory.getTracker(dynamoClient, instanceProperties);
                new CompactionJobStatusReport(tracker, reporter, table, queryType, queryParameters).run();
            }
        } catch (IllegalArgumentException e) {
            System.err.println(e.getMessage());
            printUsage();
            System.exit(1);
        }
    }

    private static void printUsage() {
        System.err.println("Usage: <instance-id> <table-name> <report-type-standard-or-json> <optional-query-type> <optional-query-parameters> \n" +
                "Query types are:\n" +
                "-a (Return all jobs)\n" +
                "-d (Detailed, provide a jobId)\n" +
                "-r (Provide startRange and endRange separated by commas in format yyyyMMddhhmmss)\n" +
                "-u (Unfinished jobs)");
    }

    private static CompactionJobStatusReporter getReporter(String[] args, int index) {
        String reporterType = optionalArgument(args, index)
                .map(str -> str.toUpperCase(Locale.ROOT))
                .orElse(DEFAULT_REPORTER);
        if (!REPORTERS.containsKey(reporterType)) {
            throw new IllegalArgumentException("Output type not supported: " + reporterType);
        }
        return REPORTERS.get(reporterType);
    }
}
