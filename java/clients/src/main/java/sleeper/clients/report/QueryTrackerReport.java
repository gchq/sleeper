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

import sleeper.clients.report.query.JsonQueryTrackerReporter;
import sleeper.clients.report.query.QueryTrackerReporter;
import sleeper.clients.report.query.StandardQueryTrackerReporter;
import sleeper.clients.report.query.TrackerQuery;
import sleeper.clients.report.query.TrackerQueryPrompt;
import sleeper.clients.util.console.ConsoleInput;
import sleeper.configuration.properties.S3InstanceProperties;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.query.core.tracker.QueryTrackerStore;
import sleeper.query.runner.tracker.DynamoDBQueryTracker;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import static sleeper.clients.util.ClientUtils.optionalArgument;
import static sleeper.configuration.utils.AwsV2ClientHelper.buildAwsV2Client;

/**
 * Creates reports on the status of queries made against tables in a Sleeper instance.
 */
public class QueryTrackerReport {
    private static final String DEFAULT_REPORTER = "STANDARD";
    private static final Map<String, QueryTrackerReporter> REPORTERS = new HashMap<>();
    private static final Map<String, TrackerQuery> QUERY_TYPES = new HashMap<>();

    static {
        REPORTERS.put(DEFAULT_REPORTER, new StandardQueryTrackerReporter());
        REPORTERS.put("JSON", new JsonQueryTrackerReporter());
        QUERY_TYPES.put("-a", TrackerQuery.ALL);
        QUERY_TYPES.put("-q", TrackerQuery.QUEUED);
        QUERY_TYPES.put("-i", TrackerQuery.IN_PROGRESS);
        QUERY_TYPES.put("-c", TrackerQuery.COMPLETED);
        QUERY_TYPES.put("-f", TrackerQuery.FAILED);
    }

    private final QueryTrackerReporter reporter;
    private final QueryTrackerStore queryTrackerStore;
    private final TrackerQuery queryType;

    public QueryTrackerReport(QueryTrackerStore queryTrackerStore, TrackerQuery queryType, QueryTrackerReporter reporter) {
        this.queryTrackerStore = queryTrackerStore;
        this.queryType = queryType;
        this.reporter = reporter;
    }

    /**
     * Creates a report.
     */
    public void run() {
        reporter.report(queryType, queryType.run(queryTrackerStore));
    }

    public static void main(String[] args) {
        try (S3Client s3Client = buildAwsV2Client(S3Client.builder());
                DynamoDbClient dynamoClient = buildAwsV2Client(DynamoDbClient.builder())) {
            if (args.length < 2 || args.length > 3) {
                throw new IllegalArgumentException("Wrong number of arguments");
            }
            String instanceId = args[0];
            QueryTrackerReporter reporter = getReporter(args, 1);
            TrackerQuery queryType = optionalArgument(args, 2)
                    .map(QueryTrackerReport::readTypeArgument)
                    .orElseGet(QueryTrackerReport::promptForQueryType);
            InstanceProperties instanceProperties = S3InstanceProperties.loadGivenInstanceId(s3Client, instanceId);
            QueryTrackerStore queryTrackerStore = new DynamoDBQueryTracker(instanceProperties, dynamoClient);
            new QueryTrackerReport(queryTrackerStore, queryType, reporter).run();
        } catch (IllegalArgumentException e) {
            System.out.println(e.getMessage());
            printUsage();
            System.exit(1);
        }
    }

    private static QueryTrackerReporter getReporter(String[] args, int index) {
        String reporterType = optionalArgument(args, index)
                .map(str -> str.toUpperCase(Locale.ROOT))
                .orElse(DEFAULT_REPORTER);
        if (!REPORTERS.containsKey(reporterType)) {
            throw new IllegalArgumentException("Output type not supported: " + reporterType);
        }
        return REPORTERS.get(reporterType);
    }

    private static TrackerQuery readTypeArgument(String type) {
        if (QUERY_TYPES.containsKey(type)) {
            return QUERY_TYPES.get(type);
        } else {
            System.out.println("Invalid query type: " + type);
            return promptForQueryType();
        }
    }

    private static TrackerQuery promptForQueryType() {
        return TrackerQueryPrompt.from(new ConsoleInput(System.console()));
    }

    private static void printUsage() {
        System.out.println(
                "Usage: <instance-id> <report-type-standard-or-json> <optional-query-type> \n" +
                        "Query types are:\n" +
                        "-a (Return all queries)\n" +
                        "-q (Queued queries)\n" +
                        "-i (In progress queries)\n" +
                        "-c (Completed queries)\n" +
                        "-f (Failed queries)");
    }
}
