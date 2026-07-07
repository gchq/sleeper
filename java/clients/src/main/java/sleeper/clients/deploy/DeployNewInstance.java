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
package sleeper.clients.deploy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.regions.PartitionMetadata;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.regions.providers.DefaultAwsRegionProviderChain;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.ecr.EcrClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sts.StsClient;

import sleeper.clients.table.AddTableClient;
import sleeper.clients.util.FileReader;
import sleeper.clients.util.cdk.CdkCommand;
import sleeper.configuration.properties.S3TableProperties;
import sleeper.core.deploy.SleeperInstanceConfiguration;
import sleeper.core.properties.PropertiesUtils;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.model.SleeperInternalCdkApp;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertiesStore;
import sleeper.core.schema.SchemaSerDe;
import sleeper.core.statestore.StateStoreProvider;
import sleeper.core.util.cli.CommandArguments;
import sleeper.core.util.cli.CommandArgumentsException;
import sleeper.core.util.cli.CommandLineUsage;
import sleeper.core.util.cli.CommandOption;
import sleeper.statestore.StateStoreFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static sleeper.core.properties.instance.CommonProperty.ID;
import static sleeper.core.properties.instance.CommonProperty.SUBNETS;
import static sleeper.core.properties.instance.CommonProperty.VPC_ID;

public class DeployNewInstance {
    private static final Logger LOGGER = LoggerFactory.getLogger(DeployNewInstance.class);

    private final InstanceDeployer deployInstance;
    private final StoreFactory storeFactory;
    private final SleeperInstanceConfiguration deployInstanceConfiguration;
    private final SleeperInternalCdkApp cdkApp;
    private final boolean ignoreTableFiles;
    private final boolean deployPaused;

    public DeployNewInstance(InstanceDeployer deployInstance,
            StoreFactory storeFactory,
            SleeperInstanceConfiguration deployInstanceConfiguration,
            SleeperInternalCdkApp cdkApp, boolean ignoreTableFiles, boolean deployPaused) {
        this.deployInstance = deployInstance;
        this.storeFactory = storeFactory;
        this.deployInstanceConfiguration = deployInstanceConfiguration;
        this.cdkApp = cdkApp;
        this.ignoreTableFiles = ignoreTableFiles;
        this.deployPaused = deployPaused;
    }

    public static final CommandLineUsage USAGE = CommandLineUsage.builder()
            .systemArguments(List.of("scriptsDirectory"))
            .positionalArguments(List.of("scriptsDirectory", "instanceId", "vpcId", "subnetIds"))
            .options(List.of(
                    CommandOption.longOption("instance-properties"),
                    CommandOption.longOption("config-dir"),
                    CommandOption.longFlag("ignoreTableFiles"),
                    CommandOption.longFlag("paused")))
            .helpSummary("" +
                    "Deploys a new instance of Sleeper.\n" +
                    "\n" +
                    "--instance-properties <file>\n" +
                    "Path to an instance properties file.\n" +
                    "One of --instance-properties and --config-dir must be set but not both.\n" +
                    "\n" +
                    "--config-dir <dir>\n" +
                    "Path to a directory containing an instance.properties file.\n" +
                    "One of --instance-properties and --config-dir must be set but not both.\n" +
                    "\n" +
                    "--ignoreTableFiles\n" +
                    "If set, the instance will be deployed on it's own. Otherwise tables will be created based on " +
                    "any relevent table.properties files found in the specified --config-dir. This flag cannot be used " +
                    "without the --config-dir optional argument.\n" +
                    "\n" +
                    "--paused\n" +
                    "If set, the instance will be deployed paused. Periodic background processes will not run until " +
                    "the instance is manually resumed.")
            .build();

    public static SleeperInstanceConfiguration loadConfiguration(Arguments args, FileReader files) {
        InstanceProperties instanceProperties = InstanceProperties.createWithoutValidation(
                PropertiesUtils.loadProperties(FileReader.readFile(files, args.resolvePropertiesFile())));
        if (args.ignoreTableFiles()) {
            return SleeperInstanceConfiguration.withNoTables(instanceProperties);
        }

        //TODO, this is wrong and doesn't load the table properties correctly
        TableProperties tableProperties = new TableProperties(instanceProperties,
                PropertiesUtils.loadProperties(FileReader.readFile(files, args.configDir().resolve("table.properties"))));
        tableProperties.setSchema(new SchemaSerDe().fromJson(
                FileReader.readFile(files, args.configDir().resolve("schema.json"))));

        return new SleeperInstanceConfiguration(instanceProperties, List.of(tableProperties));
    }

    public static SleeperInstanceConfiguration loadConfiguration2(Arguments args, FileReader files) {
        if (args.ignoreTableFiles()) {
            return SleeperInstanceConfiguration.fromLocalConfiguration(args.resolvePropertiesFile());
        }

        return SleeperInstanceConfiguration.fromLocalConfigurationDirectory(args.configDir());
    }

    public static Arguments readArguments(CommandArguments arguments) {
        return new Arguments(
                Path.of(arguments.getString("scriptsDirectory")),
                arguments.getString("instanceId"),
                arguments.getString("vpcId"),
                arguments.getString("subnetIds"),
                arguments.getOptionalString("instance-properties").map(Path::of).orElse(null),
                arguments.getOptionalString("config-dir").map(Path::of).orElse(null),
                arguments.isFlagSet("ignoreTableFiles"),
                arguments.isFlagSet("paused"));
    }

    public static void main(String[] rawArgs) throws IOException, InterruptedException {
        Arguments args = CommandArguments.parseAndValidateOrExit(USAGE, rawArgs, a -> readArguments(a));

        Path scriptsDirectory = Path.of(rawArgs[0]);
        boolean deployPaused = args.deployPaused();
        try (S3Client s3Client = S3Client.create();
                DynamoDbClient dynamoClient = DynamoDbClient.create();
                StsClient stsClient = StsClient.create();
                EcrClient ecrClient = EcrClient.create()) {
            String accountName = stsClient.getCallerIdentity().account();
            Region region = DefaultAwsRegionProviderChain.builder().build().getRegion();
            PartitionMetadata partitionMetadata = PartitionMetadata.of(region);

            SleeperInstanceConfiguration config = loadConfiguration(args, Files::readString);

            config.getInstanceProperties().set(ID, args.instanceId());
            config.getInstanceProperties().set(VPC_ID, args.vpcId());
            config.getInstanceProperties().set(SUBNETS, args.subnetIds());

            new DeployNewInstance(DeployInstance.fromScriptsDirectory(scriptsDirectory, accountName, region, partitionMetadata, s3Client, ecrClient),
                    StoreFactory.withAwsClients(s3Client, dynamoClient),
                    config, SleeperInternalCdkApp.STANDARD, args.ignoreTableFiles(), deployPaused).deploy();
        }
    }

    public void deploy() throws IOException, InterruptedException {
        deployInstanceConfiguration.validate();

        deployInstance.deploy(DeployInstanceRequest.builder()
                .instanceConfig(deployInstanceConfiguration)
                .cdkCommand(deployPaused ? CdkCommand.deployNewPaused() : CdkCommand.deployNew())
                .cdkApp(cdkApp)
                .build());

        if (!ignoreTableFiles) {
            InstanceProperties instanceProperties = deployInstanceConfiguration.getInstanceProperties();

            for (TableProperties tableProperties : deployInstanceConfiguration.getTableProperties()) {
                LOGGER.info("Adding table " + tableProperties.getStatus());
                new AddTableClient(tableProperties,
                        storeFactory.createTableStore(instanceProperties),
                        storeFactory.createStateStore(instanceProperties))
                        .run();
            }
        }
        LOGGER.info("Finished deployment of new instance");
    }

    public record Arguments(
            Path scriptsDirectory,
            String instanceId,
            String vpcId,
            String subnetIds,
            Path propertiesFile,
            Path configDir,
            boolean ignoreTableFiles,
            boolean deployPaused) {

        public Arguments {
            if (propertiesFile == null && configDir == null) {
                throw new CommandArgumentsException("Either --instance-properties or --config-dir must be provided");
            }

            if (propertiesFile != null && configDir != null) {
                throw new CommandArgumentsException("Cannot use both --instance-properties and --config-dir");
            }

            if (propertiesFile != null) {
                ignoreTableFiles = true;
            }
        }

        public Path resolvePropertiesFile() {
            return propertiesFile != null ? propertiesFile : configDir.resolve("instance.properties");
        }
    }

    @FunctionalInterface
    public interface InstancePropertiesLoader {
        InstanceProperties load(String instanceId);
    }

    public interface StoreFactory {
        TablePropertiesStore createTableStore(InstanceProperties instanceProperties);

        StateStoreProvider createStateStore(InstanceProperties instanceProperties);

        static StoreFactory withAwsClients(S3Client s3Client, DynamoDbClient dynamoClient) {
            return new StoreFactory() {
                public TablePropertiesStore createTableStore(InstanceProperties p) {
                    return S3TableProperties.createStore(p, s3Client, dynamoClient);
                }

                public StateStoreProvider createStateStore(InstanceProperties p) {
                    return StateStoreFactory.createProvider(p, s3Client, dynamoClient);
                }
            };
        }
    }
}
