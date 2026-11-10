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

import sleeper.clients.report.arguments.OutputFormatArgument;
import sleeper.clients.report.query.JsonQueryTrackerReporter;
import sleeper.clients.report.query.QueryTrackerQuery;
import sleeper.clients.report.query.QueryTrackerQueryPrompt;
import sleeper.clients.report.query.QueryTrackerReporter;
import sleeper.clients.report.query.StandardQueryTrackerReporter;
import sleeper.clients.util.console.ConsoleInput;
import sleeper.configuration.properties.S3InstanceProperties;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.util.cli.CommandArguments;
import sleeper.core.util.cli.CommandLineUsage;
import sleeper.core.util.cli.CommandOption;
import sleeper.query.core.tracker.QueryTrackerStore;
import sleeper.query.runner.tracker.DynamoDBQueryTracker;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

import static sleeper.configuration.utils.AwsV2ClientHelper.buildAwsV2Client;

/**
 * Creates reports on the status of queries made against tables in a Sleeper instance.
 */
public class QueryTrackerReport {

    private final QueryTrackerReporter reporter;
    private final QueryTrackerStore queryTrackerStore;
    private final QueryTrackerQuery queryType;
    private final String queryId;

    public QueryTrackerReport(QueryTrackerStore queryTrackerStore, QueryTrackerQuery queryType, String queryId, QueryTrackerReporter reporter) {
        this.queryTrackerStore = queryTrackerStore;
        this.queryType = queryType;
        this.queryId = queryId;
        this.reporter = reporter;
    }

    /**
     * Creates a report.
     */
    public void run() {
        reporter.report(queryType, queryType.run(queryTrackerStore, queryId));
    }

    public static void main(String[] args) {
        Arguments reportArgs = CommandArguments.parseAndValidateOrExit(USAGE, args,
                cmdArgs -> readArguments(cmdArgs, ConsoleInput.stdIn()));

        try (S3Client s3Client = buildAwsV2Client(S3Client.builder());
                DynamoDbClient dynamoClient = buildAwsV2Client(DynamoDbClient.builder());
                StsClient stsClient = buildAwsV2Client(StsClient.builder())) {
            String accountName = stsClient.getCallerIdentity().account();
            InstanceProperties instanceProperties = S3InstanceProperties.loadGivenAccountAndInstanceId(s3Client, accountName, reportArgs.instanceId());
            QueryTrackerStore queryTrackerStore = new DynamoDBQueryTracker(instanceProperties, dynamoClient);
            new QueryTrackerReport(queryTrackerStore, reportArgs.query(), reportArgs.queryId(), reportArgs.reporter()).run();
        }
    }

    public static final OutputFormatArgument<QueryTrackerReporter> OUTPUT_FORMAT = OutputFormatArgument
            .<QueryTrackerReporter>withDefault("STANDARD", new StandardQueryTrackerReporter())
            .addReporter("JSON", new JsonQueryTrackerReporter())
            .build();

    public static final CommandLineUsage USAGE = CommandLineUsage.builder()
            .positionalArguments(List.of("instance-id"))
            .options(Stream.concat(
                    QueryTrackerQuery.options().stream(),
                    Stream.of(OUTPUT_FORMAT.option()))
                    .sorted(Comparator.comparing(CommandOption::longName))
                    .toList())
            .helpSummary("" +
                    "A report on queries held in the query tracker of a Sleeper instance.\n" +
                    "\n" +
                    "The queries to report on are chosen with one of the report type options. " +
                    "Only one may be set at a time. If none is set, you will be prompted to choose one.")
            .build();

    /**
     * Reads the arguments from the command line and builds the query.
     *
     * @param  arguments the parsed command line arguments
     * @param  input     the console input, to prompt for the query type if it was not set
     * @return           the arguments
     */
    public static Arguments readArguments(CommandArguments arguments, ConsoleInput input) {
        QueryTrackerQuery query = QueryTrackerQuery.readOneOf(arguments)
                .orElseGet(() -> QueryTrackerQueryPrompt.from(input));
        String queryId = query == QueryTrackerQuery.FOR_QUERY
                ? arguments.getOptionalString(QueryTrackerQuery.FOR_QUERY.option().longName())
                        .filter(id -> !id.isBlank())
                        .orElseGet(() -> QueryTrackerQueryPrompt.promptQueryId(input))
                : null;
        return new Arguments(arguments.getString("instance-id"),
                OUTPUT_FORMAT.read(arguments),
                query, queryId);
    }

    /**
     * Holds the arguments for the query tracker report command.
     *
     * @param instanceId the Sleeper instance ID
     * @param reporter   the reporter format, either STANDARD or JSON
     * @param query      the query to execute against the query tracker
     * @param queryId    the ID of the query to report on, only set when reporting on a single query
     */
    public record Arguments(String instanceId, QueryTrackerReporter reporter, QueryTrackerQuery query, String queryId) {
    }
}
