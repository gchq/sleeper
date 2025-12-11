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
package sleeper.clients.deploy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.regions.providers.AwsRegionProvider;
import software.amazon.awssdk.regions.providers.DefaultAwsRegionProviderChain;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.ecr.EcrClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sts.StsClient;

import sleeper.clients.deploy.container.CheckVersionExistsInEcr;
import sleeper.clients.deploy.container.StackDockerImage;
import sleeper.clients.deploy.container.UploadDockerImages;
import sleeper.clients.deploy.container.UploadDockerImagesToEcr;
import sleeper.clients.deploy.jar.SyncJars;
import sleeper.clients.table.AddTable;
import sleeper.clients.util.cdk.CdkCommand;
import sleeper.clients.util.cdk.InvokeCdk;
import sleeper.clients.util.command.CommandPipelineRunner;
import sleeper.clients.util.command.CommandUtils;
import sleeper.configuration.properties.S3InstanceProperties;
import sleeper.configuration.properties.S3TableProperties;
import sleeper.core.deploy.SleeperInstanceConfiguration;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.statestore.StateStoreFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import static sleeper.clients.util.ClientUtils.optionalArgument;
import static sleeper.core.properties.instance.CommonProperty.ID;
import static sleeper.core.properties.instance.CommonProperty.SUBNETS;
import static sleeper.core.properties.instance.CommonProperty.VPC_ID;

public class DeployNewInstance {
    private static final Logger LOGGER = LoggerFactory.getLogger(DeployNewInstance.class);

    private final S3Client s3Client;
    private final DynamoDbClient dynamoClient;
    private final EcrClient ecrClient;
    private final StsClient stsClient;
    private final AwsRegionProvider regionProvider;
    private final Path scriptsDirectory;
    private final SleeperInstanceConfiguration deployInstanceConfiguration;
    private final List<StackDockerImage> extraDockerImages;
    private final InvokeCdk.Type instanceType;
    private final CommandPipelineRunner runCommand;
    private final boolean deployPaused;
    private final boolean createMultiPlatformBuilder;

    private DeployNewInstance(Builder builder) {
        s3Client = builder.s3Client;
        dynamoClient = builder.dynamoClient;
        ecrClient = builder.ecrClient;
        stsClient = builder.stsClient;
        regionProvider = builder.regionProvider;
        scriptsDirectory = builder.scriptsDirectory;
        deployInstanceConfiguration = builder.deployInstanceConfiguration;
        extraDockerImages = builder.extraDockerImages;
        instanceType = builder.instanceType;
        runCommand = builder.runCommand;
        deployPaused = builder.deployPaused;
        createMultiPlatformBuilder = builder.createMultiPlatformBuilder;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        if (args.length < 4 || args.length > 6) {
            throw new IllegalArgumentException("Usage: <scripts-dir> <instance-id> <vpc> <csv-list-of-subnets> " +
                    "<optional-instance-properties-file> <optional-deploy-paused-flag>");
        }
        try (S3Client s3Client = S3Client.create();
                DynamoDbClient dynamoClient = DynamoDbClient.create();
                StsClient stsClient = StsClient.create();
                EcrClient ecrClient = EcrClient.create()) {
            Path scriptsDirectory = Path.of(args[0]);

            Path instancePropertiesFile = optionalArgument(args, 4).map(Path::of).orElse(null);
            boolean deployPaused = "true".equalsIgnoreCase(optionalArgument(args, 5).orElse("false"));

            SleeperInstanceConfiguration config = SleeperInstanceConfiguration.forNewInstanceDefaultingInstance(
                    instancePropertiesFile, scriptsDirectory.resolve("templates"));

            config.getInstanceProperties().set(ID, args[1]);
            config.getInstanceProperties().set(VPC_ID, args[2]);
            config.getInstanceProperties().set(SUBNETS, args[3]);

            builder().scriptsDirectory(scriptsDirectory)
                    .deployInstanceConfiguration(config)
                    .deployPaused(deployPaused)
                    .instanceType(InvokeCdk.Type.STANDARD)
                    .deployWithClients(s3Client, dynamoClient, ecrClient, stsClient, DefaultAwsRegionProviderChain.builder().build());
        }
    }

    public void deploy() throws IOException, InterruptedException {
        deployInstanceConfiguration.validate();
        DeployInstance deployInstance = new DeployInstance(
                SyncJars.fromScriptsDirectory(s3Client, scriptsDirectory),
                new UploadDockerImagesToEcr(
                        UploadDockerImages.builder()
                                .scriptsDirectory(scriptsDirectory)
                                .deployConfig(DeployConfiguration.fromScriptsDirectory(scriptsDirectory))
                                .commandRunner(runCommand)
                                .createMultiplatformBuilder(createMultiPlatformBuilder)
                                .build(),
                        CheckVersionExistsInEcr.withEcrClient(ecrClient), stsClient.getCallerIdentity().account(), regionProvider.getRegion().id()),
                DeployInstance.WriteLocalProperties.underScriptsDirectory(scriptsDirectory),
                InvokeCdk.builder().scriptsDirectory(scriptsDirectory).runCommand(runCommand).build());

        deployInstance.deploy(DeployInstanceRequest.builder()
                .instanceConfig(deployInstanceConfiguration)
                .cdkCommand(deployPaused ? CdkCommand.deployNewPaused() : CdkCommand.deployNew())
                .extraDockerImages(extraDockerImages)
                .instanceType(instanceType)
                .build());

        InstanceProperties instanceProperties = S3InstanceProperties.loadGivenInstanceId(s3Client, deployInstanceConfiguration.getInstanceId());
        for (TableProperties tableProperties : deployInstanceConfiguration.getTableProperties()) {
            LOGGER.info("Adding table " + tableProperties.getStatus());
            new AddTable(instanceProperties, tableProperties,
                    S3TableProperties.createStore(instanceProperties, s3Client, dynamoClient),
                    StateStoreFactory.createProvider(instanceProperties, s3Client, dynamoClient))
                    .run();
        }
        LOGGER.info("Finished deployment of new instance");
    }

    public static final class Builder {
        private S3Client s3Client;
        private StsClient stsClient;
        private DynamoDbClient dynamoClient;
        private EcrClient ecrClient;
        private AwsRegionProvider regionProvider;
        private Path scriptsDirectory;
        private SleeperInstanceConfiguration deployInstanceConfiguration;
        private List<StackDockerImage> extraDockerImages = List.of();
        private InvokeCdk.Type instanceType;
        private CommandPipelineRunner runCommand = CommandUtils::runCommandInheritIO;
        private boolean deployPaused;
        private boolean createMultiPlatformBuilder = true;

        private Builder() {
        }

        public Builder s3Client(S3Client s3Client) {
            this.s3Client = s3Client;
            return this;
        }

        public Builder stsClient(StsClient stsClient) {
            this.stsClient = stsClient;
            return this;
        }

        public Builder dynamoClient(DynamoDbClient dynamoClient) {
            this.dynamoClient = dynamoClient;
            return this;
        }

        public Builder ecrClient(EcrClient ecrClient) {
            this.ecrClient = ecrClient;
            return this;
        }

        public Builder regionProvider(AwsRegionProvider regionProvider) {
            this.regionProvider = regionProvider;
            return this;
        }

        public Builder scriptsDirectory(Path scriptsDirectory) {
            this.scriptsDirectory = scriptsDirectory;
            return this;
        }

        public Builder deployInstanceConfiguration(SleeperInstanceConfiguration deployInstanceConfiguration) {
            this.deployInstanceConfiguration = deployInstanceConfiguration;
            return this;
        }

        public Builder extraDockerImages(List<StackDockerImage> extraDockerImages) {
            this.extraDockerImages = extraDockerImages;
            return this;
        }

        public Builder instanceType(InvokeCdk.Type instanceType) {
            this.instanceType = instanceType;
            return this;
        }

        public Builder runCommand(CommandPipelineRunner runCommand) {
            this.runCommand = runCommand;
            return this;
        }

        public Builder deployPaused(boolean deployPaused) {
            this.deployPaused = deployPaused;
            return this;
        }

        public Builder createMultiPlatformBuilder(boolean createMultiPlatformBuilder) {
            this.createMultiPlatformBuilder = createMultiPlatformBuilder;
            return this;
        }

        public DeployNewInstance build() {
            return new DeployNewInstance(this);
        }

        public void deployWithClients(
                S3Client s3Client, DynamoDbClient dynamoClient, EcrClient ecrClient, StsClient stsClient,
                AwsRegionProvider regionProvider) throws IOException, InterruptedException {
            s3Client(s3Client)
                    .dynamoClient(dynamoClient)
                    .ecrClient(ecrClient)
                    .stsClient(stsClient)
                    .regionProvider(regionProvider)
                    .build().deploy();
        }
    }
}
