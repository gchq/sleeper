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
import sleeper.clients.report.ingest.batcher.BatcherQuery;
import sleeper.clients.report.ingest.batcher.BatcherQueryPrompt;
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
import sleeper.core.util.cli.CommandLineUsage;
import sleeper.core.util.cli.CommandOption;
import sleeper.ingest.batcher.core.IngestBatcherStore;
import sleeper.ingest.batcher.store.DynamoDBIngestBatcherStore;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

import static sleeper.configuration.utils.AwsV2ClientHelper.buildAwsV2Client;

/**
 * Creates reports on files submitted to the ingest batcher.
 */
public class IngestBatcherReport {

    private final IngestBatcherStore batcherStore;
    private final IngestBatcherReporter reporter;
    private final BatcherQuery query;
    private final TableStatusProvider tableProvider;

    public IngestBatcherReport(
            IngestBatcherStore batcherStore, IngestBatcherReporter reporter,
            BatcherQuery query, TableStatusProvider tableProvider) {
        this.batcherStore = batcherStore;
        this.reporter = reporter;
        this.query = query;
        this.tableProvider = tableProvider;
    }

    /**
     * Creates a report.
     */
    public void run() {
        reporter.report(query.run(batcherStore), query, tableProvider);
    }

    public static void main(String[] args) {
        Arguments reportArgs = CommandArguments.parseAndValidateOrExit(USAGE, args,
                cmdArgs -> readArguments(cmdArgs, ConsoleInput.stdIn()));

        try (S3Client s3Client = buildAwsV2Client(S3Client.builder());
                DynamoDbClient dynamoClient = buildAwsV2Client(DynamoDbClient.builder());
                StsClient stsClient = buildAwsV2Client(StsClient.builder())) {
            String accountName = stsClient.getCallerIdentity().account();
            InstanceProperties instanceProperties = S3InstanceProperties.loadGivenAccountAndInstanceId(s3Client, accountName, reportArgs.instanceId());
            IngestBatcherStore store = new DynamoDBIngestBatcherStore(dynamoClient, instanceProperties,
                    S3TableProperties.createProvider(instanceProperties, s3Client, dynamoClient));
            new IngestBatcherReport(store, reportArgs.reporter(), reportArgs.query(),
                    new TableStatusProvider(new DynamoDBTableIndex(instanceProperties, dynamoClient)))
                    .run();
        }
    }

    public static final OutputFormatArgument<IngestBatcherReporter> OUTPUT_FORMAT = OutputFormatArgument
            .<IngestBatcherReporter>withDefault("STANDARD", new StandardIngestBatcherReporter())
            .addReporter("JSON", new JsonIngestBatcherReporter())
            .build();

    public static final CommandLineUsage USAGE = CommandLineUsage.builder()
            .positionalArguments(List.of("instance-id"))
            .options(Stream.concat(
                    BatcherQuery.options().stream(),
                    Stream.of(OUTPUT_FORMAT.option()))
                    .sorted(Comparator.comparing(CommandOption::longName))
                    .toList())
            .helpSummary("" +
                    "A report on files tracked by the ingest batcher of a Sleeper instance. These are files submitted " +
                    "for ingest or bulk import, which the batcher assigns to jobs. The report shows which job each " +
                    "file has been added to, if any.\n" +
                    "\n" +
                    "The files to report on are chosen with one of the query type options. " +
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
        return new Arguments(arguments.getString("instance-id"),
                OUTPUT_FORMAT.read(arguments),
                BatcherQuery.readOneOf(arguments)
                        .orElseGet(() -> BatcherQueryPrompt.from(input)));
    }

    /**
     * Holds the arguments for the ingest batcher report command.
     *
     * @param instanceId the Sleeper instance ID
     * @param reporter   the reporter format, either STANDARD or JSON
     * @param query      the query to execute against the ingest batcher store
     */
    public record Arguments(String instanceId, IngestBatcherReporter reporter, BatcherQuery query) {
    }
}
