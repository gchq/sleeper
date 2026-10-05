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
import sleeper.clients.report.filestatus.CsvFileStatusReporter;
import sleeper.clients.report.filestatus.FileStatusCollector;
import sleeper.clients.report.filestatus.FileStatusReporter;
import sleeper.clients.report.filestatus.JsonFileStatusReporter;
import sleeper.clients.report.filestatus.StandardFileStatusReporter;
import sleeper.clients.report.filestatus.TableFilesStatus;
import sleeper.configuration.properties.S3InstanceProperties;
import sleeper.configuration.properties.S3TableProperties;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TablePropertiesProvider;
import sleeper.core.statestore.StateStore;
import sleeper.core.util.cli.CommandArguments;
import sleeper.core.util.cli.CommandLineUsage;
import sleeper.core.util.cli.CommandOption;
import sleeper.core.util.cli.CommandOption.NumArgs;
import sleeper.statestore.StateStoreFactory;

import java.util.List;

import static sleeper.configuration.utils.AwsV2ClientHelper.buildAwsV2Client;

/**
 * Creates reports on the files in a Sleeper table.
 */
public class FilesStatusReport {

    private static final OutputFormatArgument<FileStatusReporter> OUTPUT_FORMAT = OutputFormatArgument
            .<FileStatusReporter>withDefault("STANDARD", new StandardFileStatusReporter())
            .addReporter("JSON", new JsonFileStatusReporter())
            .addReporter("CSV", new CsvFileStatusReporter())
            .build();

    private final int maxNumberOfFilesWithNoReferencesToCount;
    private final boolean verbose;
    private final FileStatusReporter fileStatusReporter;
    private final FileStatusCollector fileStatusCollector;

    public FilesStatusReport(
            StateStore stateStore, int maxNumberOfFilesWithNoReferencesToCount, boolean verbose,
            FileStatusReporter fileStatusReporter) {
        this.maxNumberOfFilesWithNoReferencesToCount = maxNumberOfFilesWithNoReferencesToCount;
        this.verbose = verbose;
        this.fileStatusReporter = fileStatusReporter;
        this.fileStatusCollector = new FileStatusCollector(stateStore);
    }

    /**
     * Creates a report.
     */
    public void run() {
        TableFilesStatus tableStatus = fileStatusCollector.run(maxNumberOfFilesWithNoReferencesToCount);
        fileStatusReporter.report(tableStatus, verbose);
    }

    public static final CommandLineUsage USAGE = CommandLineUsage.builder()
            .positionalArguments(List.of("instance-id", "table-name"))
            .options(List.of(
                    OUTPUT_FORMAT.option(),
                    CommandOption.withLongName("max-no-ref-files")
                            .numArgs(NumArgs.ONE)
                            .helpText("Maximum number of files with no references to count. Defaults to 1000.")
                            .argsHelpText("<number>")
                            .build(),
                    CommandOption.withLongName("verbose")
                            .helpText("If set, the report will include detailed file information.")
                            .build()))
            .helpSummary("Creates a report on the status of files in a Sleeper table.")
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
                arguments.getIntegerOrDefault("max-no-ref-files", 1000),
                arguments.isFlagSet("verbose"),
                OUTPUT_FORMAT.read(arguments));
    }

    /**
     * Holds the arguments for the files status report command.
     *
     * @param instanceId    the Sleeper instance ID
     * @param tableName     the name of the table to report on
     * @param maxNoRefFiles the maximum number of files with no references to count
     * @param verbose       if true, the report will include detailed file information
     * @param reporterType  the output format, one of STANDARD, JSON, CSV
     */
    public record Arguments(
            String instanceId,
            String tableName,
            int maxNoRefFiles,
            boolean verbose,
            FileStatusReporter reporter) {
    }

    public static void main(String[] rawArgs) {
        Arguments args = CommandArguments.parseAndValidateOrExit(USAGE, rawArgs, FilesStatusReport::readArguments);

        try (S3Client s3Client = buildAwsV2Client(S3Client.builder());
                DynamoDbClient dynamoClient = buildAwsV2Client(DynamoDbClient.builder());
                StsClient stsClient = buildAwsV2Client(StsClient.builder())) {
            String accountName = stsClient.getCallerIdentity().account();
            InstanceProperties instanceProperties = S3InstanceProperties.loadGivenAccountAndInstanceId(s3Client, accountName, args.instanceId());
            TablePropertiesProvider tablePropertiesProvider = S3TableProperties.createProvider(instanceProperties, s3Client, dynamoClient);
            StateStoreFactory stateStoreFactory = new StateStoreFactory(instanceProperties, s3Client, dynamoClient);
            StateStore stateStore = stateStoreFactory.getStateStore(tablePropertiesProvider.getByName(args.tableName()));
            new FilesStatusReport(stateStore, args.maxNoRefFiles(), args.verbose(), args.reporter()).run();
        }
    }
}
