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
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.model.SleeperInternalCdkApp;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.util.cli.CommandArguments;
import sleeper.core.util.cli.CommandArgumentsException;
import sleeper.core.util.cli.CommandLineUsage;
import sleeper.core.util.cli.CommandOption;
import sleeper.statestore.StateStoreFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import static sleeper.core.properties.instance.CommonProperty.ID;
import static sleeper.core.properties.instance.CommonProperty.SUBNETS;
import static sleeper.core.properties.instance.CommonProperty.VPC_ID;

public class DeployNewInstanceWrk {
    private static final Logger LOGGER = LoggerFactory.getLogger(DeployNewInstance.class);

    private final DeployInstance deployInstance;
    private final String accountName;
    private final S3Client s3Client;
    private final DynamoDbClient dynamoClient;
    private final SleeperInstanceConfiguration deployInstanceConfiguration;
    private final SleeperInternalCdkApp cdkApp;
    private final boolean deployPaused;

    private DeployNewInstanceWrk(Builder builder) {
        deployInstance = builder.deployInstance;
        accountName = builder.accountName;
        s3Client = builder.s3Client;
        dynamoClient = builder.dynamoClient;
        deployInstanceConfiguration = builder.deployInstanceConfiguration;
        cdkApp = builder.cdkApp;
        deployPaused = builder.deployPaused;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final CommandLineUsage USAGE = CommandLineUsage.builder()
            .positionalArguments(List.of("instance-id"))
            .positionalArguments(List.of("vpc"))
            .positionalArguments(List.of("subnets"))
            .options(List.of(
                    CommandOption.longOption("instance-properties"),
                    CommandOption.longOption("config-dir"),
                    CommandOption.longFlag("deployPaused")))
            .helpSummary("" +
                    "Deploys a new instance of Sleeper.\n" +
                    "Positional Argumemts:\n" +
                    "Instance ID, VPC, Subnets\n" +
                    "Optional Arguments\n" +
                    "--instance-properties <file>\n" +
                    "Optional path to an instance properties file. If not set, default instance properties will be used.\n" +
                    "\n" +
                    "--config-dir <dir>\n" +
                    "Path to a directory containing instance.properties.")
            .build();

    public static Arguments readArguments(CommandArguments arguments) {
        return new Arguments(
                arguments.getString("instance-id"),
                arguments.getString("vpc"),
                arguments.getString("subnets"),
                arguments.getOptionalString("instance-properties").map(Path::of).orElse(null),
                arguments.getOptionalString("config-dir").map(Path::of).orElse(null),
                arguments.isFlagSet("deployPaused"));
    }

    public static void main(String[] rawArgs) throws IOException, InterruptedException {
        Arguments args = CommandArguments.parseAndValidateOrExit(USAGE, rawArgs, a -> readArguments(a));

        Path scriptsDirectory = Path.of(rawArgs[0]);
        Path instancePropertiesFile = args.resolvePropertiesFile();
        boolean deployPaused = args.deployPaused();
        try (S3Client s3Client = S3Client.create();
                DynamoDbClient dynamoClient = DynamoDbClient.create();
                StsClient stsClient = StsClient.create();
                EcrClient ecrClient = EcrClient.create()) {
            String accountName = stsClient.getCallerIdentity().account();
            Region region = DefaultAwsRegionProviderChain.builder().build().getRegion();
            PartitionMetadata partitionMetadata = PartitionMetadata.of(region);

            SleeperInstanceConfiguration config = SleeperInstanceConfiguration.fromLocalConfiguration(instancePropertiesFile);

            config.getInstanceProperties().set(ID, args.instanceId());
            config.getInstanceProperties().set(VPC_ID, args.vpcId());
            config.getInstanceProperties().set(SUBNETS, args.subnetIds());

            builder()
                    .deployInstance(DeployInstance.fromScriptsDirectory(scriptsDirectory, accountName, region, partitionMetadata, s3Client, ecrClient))
                    .accountName(accountName)
                    .s3Client(s3Client)
                    .dynamoClient(dynamoClient)
                    .deployInstanceConfiguration(config)
                    .deployPaused(deployPaused)
                    .cdkApp(SleeperInternalCdkApp.STANDARD)
                    .build().deploy();
        }
    }

    public void deploy() throws IOException, InterruptedException {
        deployInstanceConfiguration.validate();

        deployInstance.deploy(DeployInstanceRequest.builder()
                .instanceConfig(deployInstanceConfiguration)
                .cdkCommand(deployPaused ? CdkCommand.deployNewPaused() : CdkCommand.deployNew())
                .cdkApp(cdkApp)
                .build());

        InstanceProperties instanceProperties = S3InstanceProperties.loadGivenAccountAndInstanceId(s3Client, accountName, deployInstanceConfiguration.getInstanceId());
        for (TableProperties tableProperties : deployInstanceConfiguration.getTableProperties()) {
            LOGGER.info("Adding table " + tableProperties.getStatus());
            new AddTableClient(tableProperties,
                    S3TableProperties.createStore(instanceProperties, s3Client, dynamoClient),
                    StateStoreFactory.createProvider(instanceProperties, s3Client, dynamoClient))
                    .run();
        }
        LOGGER.info("Finished deployment of new instance");
    }

    public record Arguments(
            String instanceId,
            String vpcId,
            String subnetIds,
            Path propertiesFile,
            Path configDir,
            boolean deployPaused) {

        public Arguments {
            if (instanceId == null) {
                throw new CommandArgumentsException("instance-id must not be null");
            }

            if (vpcId == null) {
                throw new CommandArgumentsException("vpcId must not be null");
            }

            if (subnetIds == null) {
                throw new CommandArgumentsException("subnetIds must not be null");
            }

            if (propertiesFile == null && configDir == null) {
                throw new CommandArgumentsException("Either --instance-properties or --config-dir must be provided");
            }
        }

        public Path resolvePropertiesFile() {
            return propertiesFile != null ? propertiesFile : configDir.resolve("instance.properties");
        }
    }

    public static final class Builder {
        private DeployInstance deployInstance;
        private String accountName;
        private S3Client s3Client;
        private DynamoDbClient dynamoClient;
        private SleeperInstanceConfiguration deployInstanceConfiguration;
        private SleeperInternalCdkApp cdkApp;
        private boolean deployPaused;

        private Builder() {
        }

        public Builder deployInstance(DeployInstance deployInstance) {
            this.deployInstance = deployInstance;
            return this;
        }

        public Builder accountName(String accountName) {
            this.accountName = accountName;
            return this;
        }

        public Builder s3Client(S3Client s3Client) {
            this.s3Client = s3Client;
            return this;
        }

        public Builder dynamoClient(DynamoDbClient dynamoClient) {
            this.dynamoClient = dynamoClient;
            return this;
        }

        public Builder deployInstanceConfiguration(SleeperInstanceConfiguration deployInstanceConfiguration) {
            this.deployInstanceConfiguration = deployInstanceConfiguration;
            return this;
        }

        public Builder cdkApp(SleeperInternalCdkApp cdkApp) {
            this.cdkApp = cdkApp;
            return this;
        }

        public Builder deployPaused(boolean deployPaused) {
            this.deployPaused = deployPaused;
            return this;
        }

        public DeployNewInstanceWrk build() {
            return new DeployNewInstanceWrk(this);
        }

        public void deployWithClients(S3Client s3Client, DynamoDbClient dynamoClient) throws IOException, InterruptedException {
            s3Client(s3Client)
                    .dynamoClient(dynamoClient)
                    .build().deploy();
        }
    }
}
