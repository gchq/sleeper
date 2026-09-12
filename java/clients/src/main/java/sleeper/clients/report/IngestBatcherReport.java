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

import sleeper.clients.report.ingest.batcher.BatcherQuery;
import sleeper.clients.report.ingest.batcher.IngestBatcherReporter;
import sleeper.clients.report.ingest.batcher.JsonIngestBatcherReporter;
import sleeper.clients.report.ingest.batcher.StandardIngestBatcherReporter;
import sleeper.clients.util.console.ConsoleInput;
import sleeper.configuration.properties.S3InstanceProperties;
import sleeper.configuration.properties.S3TableProperties;
import sleeper.configuration.table.index.DynamoDBTableIndex;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.table.TableStatusProvider;
import sleeper.core.util.cli.CommandArguments;
import sleeper.core.util.cli.CommandArgumentsException;
import sleeper.core.util.cli.CommandLineUsage;
import sleeper.core.util.cli.CommandOption;
import sleeper.ingest.batcher.core.IngestBatcherStore;
import sleeper.ingest.batcher.store.DynamoDBIngestBatcherStore;

import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static sleeper.configuration.utils.AwsV2ClientHelper.buildAwsV2Client;

/**
 * Creates reports on files submitted to the ingest batcher.
 */
public class IngestBatcherReport {
    private static final String DEFAULT_REPORTER = "STANDARD";
    private static final Map<String, IngestBatcherReporter> REPORTERS = new HashMap<>();

    static {
        REPORTERS.put(DEFAULT_REPORTER, new StandardIngestBatcherReporter());
        REPORTERS.put("JSON", new JsonIngestBatcherReporter());
    }

    private final IngestBatcherStore batcherStore;
    private final IngestBatcherReporter reporter;
    private final BatcherQuery.Type queryType;
    private final BatcherQuery query;
    private final TableStatusProvider tableProvider;

    public IngestBatcherReport(
            IngestBatcherStore batcherStore, IngestBatcherReporter reporter,
            BatcherQuery query, TableStatusProvider tableProvider) {
        this.batcherStore = batcherStore;
        this.reporter = reporter;
        this.query = query;
        this.queryType = query.getType();
        this.tableProvider = tableProvider;
    }

    /**
     * Creates a report.
     */
    public void run() {
        if (query == null) {
            return;
        }
        reporter.report(query.run(batcherStore), queryType, tableProvider);
    }

    public static void main(String[] args) {

        Arguments reportArgs = CommandArguments.parseAndValidateOrExit(USAGE, args, IngestBatcherReport::readArguments);

        try (S3Client s3Client = buildAwsV2Client(S3Client.builder());
                DynamoDbClient dynamoClient = buildAwsV2Client(DynamoDbClient.builder());
                StsClient stsClient = buildAwsV2Client(StsClient.builder())) {
            String accountName = stsClient.getCallerIdentity().account();
            InstanceProperties instanceProperties = S3InstanceProperties.loadGivenAccountAndInstanceId(s3Client, accountName, reportArgs.instanceId());
            IngestBatcherStore store = new DynamoDBIngestBatcherStore(dynamoClient, instanceProperties,
                    S3TableProperties.createProvider(instanceProperties, s3Client, dynamoClient));
            new IngestBatcherReport(store, REPORTERS.get(reportArgs.reportType()), BatcherQuery.from(reportArgs.queryType(), ConsoleInput.stdIn()),
                    new TableStatusProvider(new DynamoDBTableIndex(instanceProperties, dynamoClient)))
                    .run();
        }
    }

    public static final CommandLineUsage USAGE = CommandLineUsage.builder()
            .positionalArguments(List.of("instance-id", "report-type"))
            .options(List.of(
                    CommandOption.shortFlag('a', "all"),
                    CommandOption.shortFlag('p', "pending")))
            .helpSummary("Creates a report about all the ingest batches.\n" +
                    "\n" +
                    "--report-type <type>\n" +
                    "Format of the report. One of STANDARD or JSON. Defaults to STANDARD.\n" +
                    "\n" +
                    "Available query types for the report are:\n" +
                    "[Defaults to ALL, if neither is set]\n" +
                    "\n" +
                    "-a, --all\n" +
                    "Returns all the batches for the report.\n" +
                    "\n" +
                    "-p, --pending\n" +
                    "Returns only the pending batches as part of the report.")
            .build();

    /**
     * Reads the arguments from the command line.
     *
     * @param  arguments the parsed command line arguments
     * @return           the arguments
     */
    public static Arguments readArguments(CommandArguments arguments) {
        return new Arguments(arguments.getString("instance-id"),
                arguments.getString("report-type"),
                determineQueryType(arguments));
    }

    private static BatcherQuery.Type determineQueryType(CommandArguments args) {
        Boolean allFlag = args.isFlagSet("all");
        Boolean pendingFlag = args.isFlagSet("pending");
        if (allFlag && pendingFlag) {
            throw new CommandArgumentsException("Both query type mode flags are set, please only set 1.");
        } else {
            if (pendingFlag) {
                return BatcherQuery.Type.PENDING;
            } else {
                return BatcherQuery.Type.ALL;
            }
        }
    }

    /**
     * Holds the arguments for the ingest batcher report command.
     *
     * @param instanceId the Sleeper instance id
     * @param reportType the output format, either STANDARD or JSON
     * @param queryType  the type of query to execute for the ingest batcher report
     */
    public record Arguments(String instanceId, String reportType, BatcherQuery.Type queryType) {
        public Arguments {
            if (!REPORTERS.containsKey(reportType.toUpperCase(Locale.ROOT))) {
                throw new CommandArgumentsException("Report type not supported: " + reportType + ". Valid types: " + String.join(", ", REPORTERS.keySet()));
            }
        }
    }
}
