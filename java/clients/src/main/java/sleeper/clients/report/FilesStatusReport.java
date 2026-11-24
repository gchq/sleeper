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
import sleeper.clients.report.filestatus.CompactionJobTrackerLookup;
import sleeper.clients.report.filestatus.CsvFileStatusReporter;
import sleeper.clients.report.filestatus.FileStatusCollector;
import sleeper.clients.report.filestatus.FileStatusDetailLevel;
import sleeper.clients.report.filestatus.FileStatusReporter;
import sleeper.clients.report.filestatus.JsonFileStatusReporter;
import sleeper.clients.report.filestatus.StandardFileStatusReporter;
import sleeper.clients.report.filestatus.TableFilesStatus;
import sleeper.compaction.tracker.job.CompactionJobTrackerFactory;
import sleeper.configuration.properties.S3InstanceProperties;
import sleeper.configuration.properties.S3TableProperties;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TablePropertiesProvider;
import sleeper.core.statestore.StateStore;
import sleeper.core.tracker.compaction.job.CompactionJobTracker;
import sleeper.core.util.cli.CommandArguments;
import sleeper.core.util.cli.CommandArgumentsException;
import sleeper.core.util.cli.CommandLineUsage;
import sleeper.core.util.cli.CommandOption;
import sleeper.core.util.cli.CommandOption.NumArgs;
import sleeper.statestore.StateStoreFactory;

import java.time.Instant;
import java.util.List;
import java.util.Locale;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static java.util.stream.Collectors.joining;
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
    private final FileStatusDetailLevel detailLevel;
    private final FileStatusReporter fileStatusReporter;
    private final FileStatusCollector fileStatusCollector;
    private final CompactionJobTrackerLookup jobTracker;

    public FilesStatusReport(
            StateStore stateStore, int maxNumberOfFilesWithNoReferencesToCount, FileStatusDetailLevel detailLevel,
            FileStatusReporter fileStatusReporter, CompactionJobTrackerLookup jobTracker, Supplier<Instant> timeSupplier) {
        this.maxNumberOfFilesWithNoReferencesToCount = maxNumberOfFilesWithNoReferencesToCount;
        this.detailLevel = detailLevel;
        this.fileStatusReporter = fileStatusReporter;
        this.fileStatusCollector = new FileStatusCollector(stateStore, timeSupplier);
        this.jobTracker = jobTracker;
    }

    /**
     * Creates a report.
     */
    public void run() {
        TableFilesStatus tableStatus = fileStatusCollector.run(maxNumberOfFilesWithNoReferencesToCount);
        fileStatusReporter.report(tableStatus, detailLevel, jobTracker);
    }

    public static final CommandLineUsage USAGE = CommandLineUsage.builder()
            .positionalArguments(List.of("instance-id", "table-name"))
            .options(List.of(
                    CommandOption.withLongName("detail")
                            .numArgs(NumArgs.ONE)
                            .helpText("""
                                    Level of detail to include in the report. Defaults to stats_only. One of:
                                    stats_only: statistics on the files in the table.
                                    compaction_jobs: statistics, and compaction jobs assigned to files.
                                    references: statistics, and all files and their references.
                                    full: statistics, all files and their references, and compaction jobs assigned to files.""")
                            .argsHelpText("<level>")
                            .build(),
                    OUTPUT_FORMAT.option(),
                    CommandOption.withLongName("max-no-ref-files")
                            .numArgs(NumArgs.ONE)
                            .helpText("Maximum number of files with no references to count. Defaults to 1000.")
                            .argsHelpText("<number>")
                            .build()))
            .helpSummary("Creates a report on the status of files in a Sleeper table.\n" +
                    "\n" +
                    "Note that further information on compaction jobs is available in the compaction " +
                    "job status report, which can show full details of compactions that have run.")
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
                arguments.getOptionalString("detail")
                        .map(FilesStatusReport::readDetailLevel)
                        .orElse(FileStatusDetailLevel.STATS_ONLY),
                OUTPUT_FORMAT.read(arguments));
    }

    /**
     * Holds the arguments for the files status report command.
     *
     * @param instanceId    the Sleeper instance ID
     * @param tableName     the name of the table to report on
     * @param maxNoRefFiles the maximum number of files with no references to count
     * @param detailLevel   the level of detail to include in the report
     * @param reporter      the reporter for the given output format
     */
    public record Arguments(
            String instanceId,
            String tableName,
            int maxNoRefFiles,
            FileStatusDetailLevel detailLevel,
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
            CompactionJobTracker jobTracker = CompactionJobTrackerFactory.getTracker(dynamoClient, instanceProperties);
            new FilesStatusReport(stateStore, args.maxNoRefFiles(), args.detailLevel(), args.reporter(), CompactionJobTrackerLookup.from(jobTracker), Instant::now).run();
        }
    }

    private static FileStatusDetailLevel readDetailLevel(String level) {
        try {
            return FileStatusDetailLevel.valueOf(level.toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            throw new CommandArgumentsException(
                    "Detail level not supported: " + level + ". Valid levels: " + validDetailLevels());
        }
    }

    private static String validDetailLevels() {
        return Stream.of(FileStatusDetailLevel.values()).map(level -> level.name().toLowerCase(Locale.ROOT)).collect(joining(", "));
    }
}
