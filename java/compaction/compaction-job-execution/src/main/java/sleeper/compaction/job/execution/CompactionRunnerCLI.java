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

import org.apache.hadoop.conf.Configuration;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.transfer.s3.S3TransferManager;

import sleeper.compaction.core.job.CompactionJob;
import sleeper.compaction.core.job.CompactionJobSerDe;
import sleeper.compaction.core.job.CompactionRunner;
import sleeper.configuration.jars.S3UserJarsLoader;
import sleeper.configuration.properties.S3InstanceProperties;
import sleeper.configuration.properties.S3TableProperties;
import sleeper.core.iterator.IteratorCreationException;
import sleeper.core.partition.Partition;
import sleeper.core.partition.PartitionTree;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.statestore.StateStore;
import sleeper.core.util.ObjectFactory;
import sleeper.core.util.ObjectFactoryException;
import sleeper.core.util.cli.CommandArguments;
import sleeper.core.util.cli.CommandLineUsage;
import sleeper.core.util.cli.CommandOption;
import sleeper.core.util.cli.CommandOption.NumArgs;
import sleeper.parquet.utils.HadoopConfigurationProvider;
import sleeper.sketches.store.S3SketchesStore;
import sleeper.sketches.store.SketchesStore;
import sleeper.statestore.StateStoreFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static sleeper.configuration.utils.AwsV2ClientHelper.buildAwsV2Client;

/**
 * A command line tool to run a compaction job locally. Can be useful for debugging.
 */
public class CompactionRunnerCLI {

    private CompactionRunnerCLI() {
    }

    public static void main(String[] args) throws IOException, ObjectFactoryException, IteratorCreationException {
        CommandLineUsage usage = CommandLineUsage.builder()
                .positionalArguments(List.of("job.json path"))
                .helpSummary("""
                        Runs a compaction locally. Intended for debugging. You can either load the full configuration
                        from a running instance with --load-instance <instance id>, or set the table schema with
                        --schema <schema.json path>. When using a schema file, the remaining configuration will be set
                        to defaults.""")
                .options(List.of(
                        CommandOption.shortOption('r', "repetitions", NumArgs.ONE),
                        CommandOption.shortOption('i', "load-instance", NumArgs.ONE),
                        CommandOption.shortOption('s', "schema", NumArgs.ONE)))
                .build();
        CommandArguments arguments = CommandArguments.parseAndValidateOrExit(usage, args);

        String instanceId = arguments.getOptionalString("load-instance").orElse(null);
        Path jobJsonPath = Path.of(arguments.getString("job.json path"));
        int repetitions = arguments.getIntegerOrDefault("repetitions", 1);

        String jobJson = Files.readString(jobJsonPath);
        CompactionJob job = new CompactionJobSerDe().fromJson(jobJson);

        try (S3Client s3Client = buildAwsV2Client(S3Client.builder());
                DynamoDbClient dynamoClient = buildAwsV2Client(DynamoDbClient.builder());
                S3AsyncClient s3AsyncClient = buildAwsV2Client(S3AsyncClient.crtBuilder());
                S3TransferManager s3TransferManager = S3TransferManager.builder().s3Client(s3AsyncClient).build()) {
            InstanceProperties instanceProperties = S3InstanceProperties.loadGivenInstanceId(s3Client, instanceId);
            TableProperties tableProperties = S3TableProperties.createStore(instanceProperties, s3Client, dynamoClient).loadById(job.getTableId());
            ObjectFactory objectFactory = new S3UserJarsLoader(instanceProperties, s3Client).buildObjectFactory();
            Configuration hadoopConf = HadoopConfigurationProvider.getConfigurationForClient(instanceProperties, tableProperties);
            SketchesStore sketchesStore = new S3SketchesStore(s3Client, s3TransferManager);
            StateStore stateStore = new StateStoreFactory(instanceProperties, s3Client, dynamoClient).getStateStore(tableProperties);
            PartitionTree partitionTree = new PartitionTree(stateStore.getAllPartitions());
            Partition partition = partitionTree.getPartition(job.getPartitionId());

            DefaultCompactionRunnerFactory runnerFactory = new DefaultCompactionRunnerFactory(objectFactory, hadoopConf, sketchesStore);
            CompactionRunner runner = runnerFactory.createCompactor(job, tableProperties);

            for (int i = 0; i < repetitions; i++) {
                runner.compact(job, tableProperties, partition.getRegion());
            }
        }
    }

}
