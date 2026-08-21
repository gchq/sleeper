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

import sleeper.clients.report.filestatus.CVSFileStatusReporter;
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
import sleeper.core.util.cli.CommandArgumentsException;
import sleeper.core.util.cli.CommandLineUsage;
import sleeper.core.util.cli.CommandOption;
import sleeper.statestore.StateStoreFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static sleeper.configuration.utils.AwsV2ClientHelper.buildAwsV2Client;

/**
 * Creates reports on the files in a Sleeper table.
 */
public class FilesStatusReport {
    private final int maxNumberOfFilesWithNoReferencesToCount;
    private final boolean verbose;
    private final FileStatusReporter fileStatusReporter;
    private final FileStatusCollector fileStatusCollector;

    private static final String DEFAULT_STATUS_REPORTER = "STANDARD";
    private static final Map<String, FileStatusReporter> FILE_STATUS_REPORTERS = new HashMap<>();

    static {
        FILE_STATUS_REPORTERS.put(DEFAULT_STATUS_REPORTER, new StandardFileStatusReporter());
        FILE_STATUS_REPORTERS.put("JSON", new JsonFileStatusReporter());
        FILE_STATUS_REPORTERS.put("CSV", new CVSFileStatusReporter());
    }

    public FilesStatusReport(StateStore stateStore, int maxNumberOfFilesWithNoReferencesToCount, boolean verbose) {
        this(stateStore, maxNumberOfFilesWithNoReferencesToCount, verbose, DEFAULT_STATUS_REPORTER);
    }

    public FilesStatusReport(
            StateStore stateStore, int maxNumberOfFilesWithNoReferencesToCount, boolean verbose, String outputType) {
        this(stateStore, maxNumberOfFilesWithNoReferencesToCount, verbose, getReporter(outputType));
    }

    public FilesStatusReport(
            StateStore stateStore, int maxNumberOfFilesWithNoReferencesToCount, boolean verbose,
            FileStatusReporter fileStatusReporter) {
        this.maxNumberOfFilesWithNoReferencesToCount = maxNumberOfFilesWithNoReferencesToCount;
        this.verbose = verbose;
        this.fileStatusReporter = fileStatusReporter;
        this.fileStatusCollector = new FileStatusCollector(stateStore);
    }

    private static FileStatusReporter getReporter(String outputType) {
        if (!FILE_STATUS_REPORTERS.containsKey(outputType)) {
            throw new IllegalArgumentException("Output type not supported " + outputType);
        }
        return FILE_STATUS_REPORTERS.get(outputType);
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
                    CommandOption.longOption("max-no-ref-files"),
                    CommandOption.longFlag("verbose"),
                    CommandOption.longOption("report-type")))
            .helpSummary("" +
                    "Creates a report on the status of files in a Sleeper table.\n" +
                    "\n" +
                    "--max-no-ref-files <number>\n" +
                    "Maximum number of files with no references to count. Defaults to 1000.\n" +
                    "\n" +
                    "--report-type <type>\n" +
                    "Output format. One of STANDARD, JSON, CSV. Defaults to STANDARD.\n" +
                    "\n" +
                    "--verbose\n" +
                    "If set, the report will include detailed file information.")
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
                arguments.getOptionalString("report-type")
                        .map(s -> s.toUpperCase(Locale.ROOT))
                        .orElse(DEFAULT_STATUS_REPORTER));
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
            String reporterType) {

        public Arguments {
            if (!FILE_STATUS_REPORTERS.containsKey(reporterType)) {
                throw new CommandArgumentsException("Report type not supported: " + reporterType + ". Valid types: " + String.join(", ", FILE_STATUS_REPORTERS.keySet()));
            }
        }
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
            new FilesStatusReport(stateStore, args.maxNoRefFiles(), args.verbose(), args.reporterType()).run();
        }
    }
}
