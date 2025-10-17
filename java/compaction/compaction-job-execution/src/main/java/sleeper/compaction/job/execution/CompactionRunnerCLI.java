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
package sleeper.compaction.job.execution;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.transfer.s3.S3TransferManager;

import sleeper.compaction.core.job.CompactionJob;
import sleeper.compaction.core.job.CompactionJobSerDe;
import sleeper.compaction.core.job.CompactionRunner;
import sleeper.compaction.core.task.CompactionRunnerFactory;
import sleeper.configuration.jars.S3UserJarsLoader;
import sleeper.configuration.properties.S3InstanceProperties;
import sleeper.configuration.properties.S3TableProperties;
import sleeper.core.iterator.IteratorCreationException;
import sleeper.core.partition.PartitionTree;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.range.Region;
import sleeper.core.range.RegionSerDe;
import sleeper.core.schema.Schema;
import sleeper.core.schema.SchemaSerDe;
import sleeper.core.statestore.StateStore;
import sleeper.core.util.ObjectFactory;
import sleeper.core.util.ObjectFactoryException;
import sleeper.core.util.cli.CommandArguments;
import sleeper.core.util.cli.CommandArgumentsException;
import sleeper.core.util.cli.CommandLineUsage;
import sleeper.core.util.cli.CommandOption;
import sleeper.core.util.cli.CommandOption.NumArgs;
import sleeper.parquet.utils.HadoopConfigurationProvider;
import sleeper.parquet.utils.TableHadoopConfigurationProvider;
import sleeper.sketches.store.S3SketchesStore;
import sleeper.statestore.StateStoreFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static sleeper.configuration.utils.AwsV2ClientHelper.buildAwsV2Client;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;

/**
 * A command line tool to run a compaction job locally. Can be useful for debugging.
 */
public class CompactionRunnerCLI {

    private final TablePropertiesProvider tablePropertiesProvider;
    private final CompactionRunnerFactory runnerFactory;
    private final RegionSupplier regionSupplier;

    private CompactionRunnerCLI(TablePropertiesProvider tablePropertiesProvider, CompactionRunnerFactory runnerFactory, RegionSupplier regionSupplier) {
        this.tablePropertiesProvider = tablePropertiesProvider;
        this.runnerFactory = runnerFactory;
        this.regionSupplier = regionSupplier;
    }

    public static CompactionRunnerCLI createForInstance(
            String instanceId, S3Client s3Client, S3TransferManager s3TransferManager, DynamoDbClient dynamoClient) throws ObjectFactoryException {
        InstanceProperties instanceProperties = S3InstanceProperties.loadGivenInstanceId(s3Client, instanceId);
        return new CompactionRunnerCLI(
                S3TableProperties.createProvider(instanceProperties, s3Client, dynamoClient)::getById,
                new DefaultCompactionRunnerFactory(
                        new S3UserJarsLoader(instanceProperties, s3Client).buildObjectFactory(),
                        TableHadoopConfigurationProvider.forClient(instanceProperties),
                        new S3SketchesStore(s3Client, s3TransferManager)),
                (tableProperties, partitionId) -> {
                    StateStore stateStore = new StateStoreFactory(instanceProperties, s3Client, dynamoClient).getStateStore(tableProperties);
                    PartitionTree partitionTree = new PartitionTree(stateStore.getAllPartitions());
                    return partitionTree.getPartition(partitionId).getRegion();
                });
    }

    public static CompactionRunnerCLI createForFiles(Path schemaPath, Path regionPath, S3Client s3Client, S3TransferManager s3TransferManager) throws IOException {
        Schema schema = readSchemaFile(schemaPath);
        Region region = readRegionFile(schema, regionPath);
        TableProperties tableProperties = new TableProperties(new InstanceProperties());
        tableProperties.setSchema(schema);
        return createForFiles(tableProperties, region, s3Client, s3TransferManager);
    }

    public static CompactionRunnerCLI createForFiles(TableProperties baseTableProperties, Region region, S3Client s3Client, S3TransferManager s3TransferManager) {
        return new CompactionRunnerCLI(
                tableId -> {
                    TableProperties properties = TableProperties.copyOf(baseTableProperties);
                    properties.set(TABLE_ID, tableId);
                    properties.set(TABLE_NAME, "unknown");
                    return properties;
                },
                new DefaultCompactionRunnerFactory(
                        ObjectFactory.noUserJars(),
                        HadoopConfigurationProvider.getConfigurationForClient(),
                        new S3SketchesStore(s3Client, s3TransferManager)),
                (tableProperties, partitionId) -> region);
    }

    public void runNTimes(CompactionJob job, int times) throws IOException, IteratorCreationException {
        TableProperties tableProperties = tablePropertiesProvider.getTableProperties(job.getTableId());
        Region region = regionSupplier.getPartitionRegion(tableProperties, job.getPartitionId());
        CompactionRunner runner = runnerFactory.createCompactor(job, tableProperties);
        for (int i = 0; i < times; i++) {
            runner.compact(job, tableProperties, region);
        }
    }

    public static void main(String[] rawArgs) throws IOException, ObjectFactoryException, IteratorCreationException {
        CommandLineUsage usage = CommandLineUsage.builder()
                .positionalArguments(List.of("job.json path"))
                .helpSummary("""
                        Runs a compaction locally. Intended for debugging. You can either load the full configuration
                        from a running instance with --load-instance <instance id>, or set the table schema with
                        --schema <schema.json path>. When using a schema file, the remaining configuration will be set
                        to defaults.""")
                .options(List.of(
                        CommandOption.shortOption('r', "repetitions", NumArgs.ONE),
                        CommandOption.longOption("load-instance", NumArgs.ONE),
                        CommandOption.longOption("schema", NumArgs.ONE),
                        CommandOption.longOption("region", NumArgs.ONE)))
                .build();
        Arguments args = CommandArguments.parseAndValidateOrExit(usage, rawArgs, arguments -> new Arguments(
                Path.of(arguments.getString("job.json path")),
                arguments.getIntegerOrDefault("repetitions", 1),
                arguments.getOptionalString("load-instance").orElse(null),
                arguments.getOptionalString("schema").map(Path::of).orElse(null),
                arguments.getOptionalString("region").map(Path::of).orElse(null)));

        String jobJson = Files.readString(args.jobJsonPath());
        CompactionJob job = new CompactionJobSerDe().fromJson(jobJson);

        try (S3Client s3Client = buildAwsV2Client(S3Client.builder());
                DynamoDbClient dynamoClient = buildAwsV2Client(DynamoDbClient.builder());
                S3AsyncClient s3AsyncClient = buildAwsV2Client(S3AsyncClient.crtBuilder());
                S3TransferManager s3TransferManager = S3TransferManager.builder().s3Client(s3AsyncClient).build()) {

            CompactionRunnerCLI cli;
            if (args.instanceId() != null) {
                cli = createForInstance(args.instanceId(), s3Client, s3TransferManager, dynamoClient);
            } else {
                cli = createForFiles(args.schemaPath(), args.regionPath(), s3Client, s3TransferManager);
            }
            cli.runNTimes(job, args.repetitions());
        }
    }

    private static Schema readSchemaFile(Path path) throws IOException {
        String json = Files.readString(path);
        return new SchemaSerDe().fromJson(json);
    }

    private static Region readRegionFile(Schema schema, Path path) throws IOException {
        if (path == null) {
            return Region.coveringAllValuesOfAllRowKeys(schema);
        } else {
            String json = Files.readString(path);
            return new RegionSerDe(schema).fromJson(json);
        }
    }

    public interface TablePropertiesProvider {
        TableProperties getTableProperties(String tableId);
    }

    public interface RegionSupplier {
        Region getPartitionRegion(TableProperties tableProperties, String partitionId);
    }

    public record Arguments(Path jobJsonPath, int repetitions, String instanceId, Path schemaPath, Path regionPath) {

        public Arguments {
            if (instanceId == null && schemaPath == null) {
                throw new CommandArgumentsException("Expected --load-instance <instance id> or --schema <schema.json path>");
            }
        }
    }

}
