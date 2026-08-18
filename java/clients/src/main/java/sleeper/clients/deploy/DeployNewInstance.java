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
import sleeper.clients.util.cdk.CdkCommand;
import sleeper.configuration.properties.S3InstanceProperties;
import sleeper.configuration.properties.S3TableProperties;
import sleeper.core.deploy.SleeperInstanceConfiguration;
import sleeper.core.properties.PropertiesUtils;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.model.SleeperInternalCdkApp;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertiesStore;
import sleeper.core.statestore.StateStoreProvider;
import sleeper.core.util.cli.CommandArguments;
import sleeper.core.util.cli.CommandArgumentsException;
import sleeper.core.util.cli.CommandLineUsage;
import sleeper.core.util.cli.CommandOption;
import sleeper.statestore.StateStoreFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;

import static sleeper.core.properties.instance.CommonProperty.ID;
import static sleeper.core.properties.instance.CommonProperty.SUBNETS;
import static sleeper.core.properties.instance.CommonProperty.VPC_ID;

public class DeployNewInstance {
    private static final Logger LOGGER = LoggerFactory.getLogger(DeployNewInstance.class);

    private final InstanceDeployer deployInstance;
    private final StoreFactory storeFactory;
    private final InstancePropertiesLoader instancePropertiesLoader;
    private final SleeperInternalCdkApp cdkApp;
    private final CdkCommand cdkCommand;
    private final SleeperInstanceConfiguration expectedInstanceConfiguration;

    private DeployNewInstance(Builder builder) {
        this.deployInstance = Objects.requireNonNull(builder.deployInstance, "deployInstance must not be null");
        this.storeFactory = Objects.requireNonNull(builder.storeFactory, "storeFactory must not be null");
        this.instancePropertiesLoader = Objects.requireNonNull(builder.instancePropertiesLoader, "instancePropertiesLoader must not be null");
        this.cdkApp = Objects.requireNonNull(builder.cdkApp, "cdkApp must not be null");
        this.cdkCommand = builder.buildCdkCommand();
        this.expectedInstanceConfiguration = Objects.requireNonNull(builder.expectedInstanceConfiguration, "expectedInstanceConfiguration must not be null");
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final CommandLineUsage USAGE = CommandLineUsage.builder()
            .systemArguments(List.of("scriptsDirectory"))
            .positionalArguments(List.of("scriptsDirectory", "instanceId", "vpcId", "subnetIds"))
            .options(List.of(
                    CommandOption.longOption("properties-file"),
                    CommandOption.longOption("config-dir"),
                    CommandOption.longFlag("paused")))
            .helpSummary("" +
                    "Deploys a new instance of Sleeper.\n" +
                    "\n" +
                    "--config-dir <dir>\n" +
                    "Path to a directory containing an instance.properties file. Passed to the CDK in the context variable \"configurationDir\".\n" +
                    "One of --properties-file and --config-dir must be set but not both.\n" +
                    "\n" +
                    "--paused\n" +
                    "If set, the instance will be deployed paused. This will set the CDK context variable " +
                    "\"deployPaused\". Periodic background processes will not run until the instance is manually " +
                    "resumed.\n" +
                    "\n" +
                    "--properties-file <file>\n" +
                    "Path to an instance properties file. Passed to the CDK in the context variable \"propertiesFile\".\n" +
                    "One of --properties-file and --config-dir must be set but not both.")
            .build();

    public static Arguments readArguments(CommandArguments arguments) {
        return new Arguments(
                Path.of(arguments.getString("scriptsDirectory")),
                arguments.getString("instanceId"),
                arguments.getString("vpcId"),
                arguments.getString("subnetIds"),
                arguments.getOptionalString("properties-file").map(Path::of).orElse(null),
                arguments.getOptionalString("config-dir").map(Path::of).orElse(null),
                arguments.isFlagSet("paused"));
    }

    public static SleeperInstanceConfiguration loadConfiguration(Arguments args) throws IOException {
        SleeperInstanceConfiguration config;
        if (args.propertiesFile() != null) {
            config = SleeperInstanceConfiguration.fromLocalConfiguration(args.propertiesFile());
        } else {
            config = SleeperInstanceConfiguration.fromLocalConfigurationDirectory(args.configDir());
        }

        config.getInstanceProperties().set(ID, args.instanceId());
        config.getInstanceProperties().set(VPC_ID, args.vpcId());
        config.getInstanceProperties().set(SUBNETS, args.subnetIds());

        return config;
    }

    public static void main(String[] rawArgs) throws IOException, InterruptedException {
        Arguments args = CommandArguments.parseAndValidateOrExit(USAGE, rawArgs, a -> readArguments(a));

        try (S3Client s3Client = S3Client.create();
                DynamoDbClient dynamoClient = DynamoDbClient.create();
                StsClient stsClient = StsClient.create();
                EcrClient ecrClient = EcrClient.create()) {
            String accountName = stsClient.getCallerIdentity().account();
            Region region = DefaultAwsRegionProviderChain.builder().build().getRegion();
            PartitionMetadata partitionMetadata = PartitionMetadata.of(region);

            SleeperInstanceConfiguration config = loadConfiguration(args);

            DeployNewInstance.builder()
                    .deployInstance(DeployInstance.fromScriptsDirectory(args.scriptsDirectory(), accountName, region, partitionMetadata, s3Client, ecrClient))
                    .storeFactory(StoreFactory.withAwsClients(s3Client, dynamoClient))
                    .instancePropertiesLoader(instanceId -> S3InstanceProperties.loadGivenAccountAndInstanceId(s3Client, accountName, instanceId))
                    .expectedInstanceConfiguration(config)
                    .cdkApp(SleeperInternalCdkApp.STANDARD)
                    .propertiesFile(args.propertiesFile())
                    .configDir(args.configDir())
                    .deployPaused(args.deployPaused())
                    .build().deploy();
        }
    }

    public void deploy() throws IOException, InterruptedException {
        InstanceProperties instanceProperties = expectedInstanceConfiguration.getInstanceProperties();

        deployInstance.deploy(DeployInstanceRequest.builder()
                .instanceConfig(expectedInstanceConfiguration)
                .cdkCommand(cdkCommand)
                .cdkApp(cdkApp)
                .build());

        if (!expectedInstanceConfiguration.getTableProperties().isEmpty()) {
            InstanceProperties deployedProperties = instancePropertiesLoader.load(instanceProperties.get(ID));

            for (TableProperties tableProperties : expectedInstanceConfiguration.getTableProperties()) {
                TableProperties deployedTableProperties = new TableProperties(deployedProperties,
                        PropertiesUtils.loadProperties(tableProperties.saveAsString()));
                LOGGER.info("Adding table " + deployedTableProperties.getStatus());
                new AddTableClient(deployedTableProperties,
                        storeFactory.createTableStore(deployedProperties),
                        storeFactory.createStateStore(deployedProperties))
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
            boolean deployPaused) {

        public Arguments {
            if (propertiesFile == null && configDir == null) {
                throw new CommandArgumentsException("Either --properties-file or --config-dir must be provided");
            }

            if (propertiesFile != null && configDir != null) {
                throw new CommandArgumentsException("Cannot use both --properties-file and --config-dir");
            }
        }
    }

    public static final class Builder {
        private InstanceDeployer deployInstance;
        private StoreFactory storeFactory;
        private InstancePropertiesLoader instancePropertiesLoader;
        private SleeperInternalCdkApp cdkApp;
        private SleeperInstanceConfiguration expectedInstanceConfiguration;
        private Path propertiesFile;
        private Path configDir;
        private boolean deployPaused = false;

        private Builder() {

        }

        public Builder deployInstance(InstanceDeployer deployInstance) {
            this.deployInstance = deployInstance;
            return this;
        }

        public Builder storeFactory(StoreFactory storeFactory) {
            this.storeFactory = storeFactory;
            return this;
        }

        public Builder instancePropertiesLoader(InstancePropertiesLoader instancePropertiesLoader) {
            this.instancePropertiesLoader = instancePropertiesLoader;
            return this;
        }

        public Builder cdkApp(SleeperInternalCdkApp cdkApp) {
            this.cdkApp = cdkApp;
            return this;
        }

        public Builder expectedInstanceConfiguration(SleeperInstanceConfiguration expectedInstanceConfiguration) {
            this.expectedInstanceConfiguration = expectedInstanceConfiguration;
            return this;
        }

        public Builder propertiesFile(Path propertiesFile) {
            this.propertiesFile = propertiesFile;
            return this;
        }

        public Builder configDir(Path configDir) {
            this.configDir = configDir;
            return this;
        }

        public Builder deployPaused(boolean deployPaused) {
            this.deployPaused = deployPaused;
            return this;
        }

        private CdkCommand buildCdkCommand() {
            CdkCommand command = CdkCommand.deployNew();
            if (propertiesFile != null) {
                command = command.withPropertiesFile(propertiesFile);
            }
            if (configDir != null) {
                command = command.withConfigurationDirectory(configDir);
            }
            if (deployPaused) {
                command = command.toBuilder().deployPaused(true).build();
            }
            if (expectedInstanceConfiguration != null) {
                InstanceProperties instanceProperties = expectedInstanceConfiguration.getInstanceProperties();
                command = command.toBuilder()
                        .instanceId(instanceProperties.get(ID))
                        .vpcId(instanceProperties.get(VPC_ID))
                        .subnets(instanceProperties.get(SUBNETS))
                        .build();
            }
            return command;
        }

        public DeployNewInstance build() {
            return new DeployNewInstance(this);
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
