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
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.transfer.s3.S3TransferManager;

import sleeper.clients.deploy.container.EcrRepositoryCreator;
import sleeper.clients.deploy.container.StackDockerImage;
import sleeper.clients.deploy.container.UploadDockerImages;
import sleeper.clients.deploy.container.UploadDockerImagesRequest;
import sleeper.clients.deploy.jar.SyncJars;
import sleeper.clients.deploy.properties.PopulateInstancePropertiesAws;
import sleeper.clients.table.AddTable;
import sleeper.clients.util.ClientUtils;
import sleeper.clients.util.cdk.CdkCommand;
import sleeper.clients.util.cdk.InvokeCdkForInstance;
import sleeper.clients.util.command.CommandPipelineRunner;
import sleeper.clients.util.command.CommandUtils;
import sleeper.configurationv2.properties.S3InstanceProperties;
import sleeper.configurationv2.properties.S3TableProperties;
import sleeper.core.deploy.DeployInstanceConfiguration;
import sleeper.core.deploy.PopulateInstanceProperties;
import sleeper.core.properties.SleeperPropertiesValidationReporter;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.local.SaveLocalProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.statestorev2.StateStoreFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static sleeper.clients.util.ClientUtils.optionalArgument;
import static sleeper.core.properties.instance.CommonProperty.ID;
import static sleeper.core.properties.instance.CommonProperty.SUBNETS;
import static sleeper.core.properties.instance.CommonProperty.VPC_ID;

public class DeployNewInstance {
    private static final Logger LOGGER = LoggerFactory.getLogger(DeployNewInstance.class);

    private final S3Client s3Client;
    private final S3TransferManager s3TransferManager;
    private final DynamoDbClient dynamoClient;
    private final EcrClient ecrClient;
    private final Path scriptsDirectory;
    private final DeployInstanceConfiguration deployInstanceConfiguration;
    private final List<StackDockerImage> extraDockerImages;
    private final InvokeCdkForInstance.Type instanceType;
    private final CommandPipelineRunner runCommand;
    private final boolean deployPaused;

    private DeployNewInstance(Builder builder) {
        s3Client = builder.s3Client;
        s3TransferManager = builder.s3TransferManager;
        dynamoClient = builder.dynamoClient;
        ecrClient = builder.ecrClient;
        scriptsDirectory = builder.scriptsDirectory;
        deployInstanceConfiguration = builder.deployInstanceConfiguration;
        extraDockerImages = builder.extraDockerImages;
        instanceType = builder.instanceType;
        runCommand = builder.runCommand;
        deployPaused = builder.deployPaused;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        if (args.length < 4 || args.length > 6) {
            throw new IllegalArgumentException("Usage: <scripts-dir> <instance-id> <vpc> <csv-list-of-subnets> " +
                    "<optional-instance-properties-file> <optional-deploy-paused-flag>");
        }
        AwsRegionProvider regionProvider = DefaultAwsRegionProviderChain.builder().build();
        try (S3Client s3Client = S3Client.create();
                S3AsyncClient s3AsyncClient = S3AsyncClient.crtCreate();
                S3TransferManager s3TransferManager = S3TransferManager.builder().s3Client(s3AsyncClient).build();
                DynamoDbClient dynamoClient = DynamoDbClient.create();
                StsClient stsClient = StsClient.create();
                EcrClient ecrClient = EcrClient.create()) {
            Path scriptsDirectory = Path.of(args[0]);
            PopulateInstanceProperties populateInstanceProperties = PopulateInstancePropertiesAws.builder(stsClient, regionProvider)
                    .instanceId(args[1]).vpcId(args[2]).subnetIds(args[3])
                    .build();
            Path instancePropertiesFile = optionalArgument(args, 4).map(Path::of).orElse(null);
            boolean deployPaused = "true".equalsIgnoreCase(optionalArgument(args, 5).orElse("false"));
            builder().scriptsDirectory(scriptsDirectory)
                    .deployInstanceConfiguration(DeployInstanceConfiguration.forNewInstanceDefaultingInstance(
                            instancePropertiesFile, populateInstanceProperties, scriptsDirectory.resolve("templates")))
                    .deployPaused(deployPaused)
                    .instanceType(InvokeCdkForInstance.Type.STANDARD)
                    .deployWithClients(s3Client, s3TransferManager, dynamoClient, ecrClient);
        }
    }

    public void deploy() throws IOException, InterruptedException {
        LOGGER.info("-------------------------------------------------------");
        LOGGER.info("Running Deployment");
        LOGGER.info("-------------------------------------------------------");

        Path templatesDirectory = scriptsDirectory.resolve("templates");
        Path generatedDirectory = scriptsDirectory.resolve("generated");
        Path jarsDirectory = scriptsDirectory.resolve("jars");
        String sleeperVersion = Files.readString(templatesDirectory.resolve("version.txt"));

        InstanceProperties instanceProperties = deployInstanceConfiguration.getInstanceProperties();
        String instanceId = instanceProperties.get(ID);
        LOGGER.info("instanceId: {}", instanceId);
        LOGGER.info("vpcId: {}", instanceProperties.get(VPC_ID));
        LOGGER.info("subnetIds: {}", instanceProperties.get(SUBNETS));
        LOGGER.info("templatesDirectory: {}", templatesDirectory);
        LOGGER.info("generatedDirectory: {}", generatedDirectory);
        LOGGER.info("scriptsDirectory: {}", scriptsDirectory);
        LOGGER.info("jarsDirectory: {}", jarsDirectory);
        LOGGER.info("sleeperVersion: {}", sleeperVersion);
        LOGGER.info("deployPaused: {}", deployPaused);
        validate(instanceProperties, deployInstanceConfiguration.getTableProperties());

        SyncJars.builder().s3(s3Client)
                .jarsDirectory(jarsDirectory).instanceProperties(instanceProperties)
                .deleteOldJars(false).build().sync();
        UploadDockerImages.builder()
                .baseDockerDirectory(scriptsDirectory.resolve("docker")).jarsDirectory(jarsDirectory)
                .ecrClient(EcrRepositoryCreator.withEcrClient(ecrClient))
                .build().upload(runCommand,
                        UploadDockerImagesRequest.forNewDeployment(instanceProperties, sleeperVersion)
                                .withExtraImages(extraDockerImages));

        Files.createDirectories(generatedDirectory);
        ClientUtils.clearDirectory(generatedDirectory);
        SaveLocalProperties.saveToDirectory(generatedDirectory, instanceProperties,
                deployInstanceConfiguration.getTableProperties().stream());

        LOGGER.info("-------------------------------------------------------");
        LOGGER.info("Deploying Stacks");
        LOGGER.info("-------------------------------------------------------");
        CdkCommand cdkCommand = deployPaused ? CdkCommand.deployNewPaused() : CdkCommand.deployNew();
        InvokeCdkForInstance.builder()
                .propertiesFile(generatedDirectory.resolve("instance.properties"))
                .jarsDirectory(jarsDirectory).version(sleeperVersion)
                .build().invoke(instanceType, cdkCommand, runCommand);
        instanceProperties = S3InstanceProperties.loadGivenInstanceId(s3Client, instanceId);
        for (TableProperties tableProperties : deployInstanceConfiguration.getTableProperties()) {
            LOGGER.info("Adding table " + tableProperties.getStatus());
            new AddTable(instanceProperties, tableProperties,
                    S3TableProperties.createStore(instanceProperties, s3Client, dynamoClient),
                    StateStoreFactory.createProvider(instanceProperties, s3Client, dynamoClient, s3TransferManager))
                    .run();
        }
        LOGGER.info("Finished deployment of new instance");
    }

    private void validate(InstanceProperties instanceProperties, List<TableProperties> tableProperties) {
        SleeperPropertiesValidationReporter validationReporter = new SleeperPropertiesValidationReporter();
        instanceProperties.validate(validationReporter);
        tableProperties.forEach(properties -> properties.validate(validationReporter));
        validationReporter.throwIfFailed();
    }

    public static final class Builder {
        private S3Client s3Client;
        private S3TransferManager s3TransferManager;
        private DynamoDbClient dynamoClient;
        private EcrClient ecrClient;
        private Path scriptsDirectory;
        private DeployInstanceConfiguration deployInstanceConfiguration;
        private List<StackDockerImage> extraDockerImages = List.of();
        private InvokeCdkForInstance.Type instanceType;
        private CommandPipelineRunner runCommand = CommandUtils::runCommandInheritIO;
        private boolean deployPaused;

        private Builder() {
        }

        public Builder s3Client(S3Client s3Client) {
            this.s3Client = s3Client;
            return this;
        }

        public Builder s3TransferManager(S3TransferManager s3TransferManager) {
            this.s3TransferManager = s3TransferManager;
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

        public Builder scriptsDirectory(Path scriptsDirectory) {
            this.scriptsDirectory = scriptsDirectory;
            return this;
        }

        public Builder deployInstanceConfiguration(DeployInstanceConfiguration deployInstanceConfiguration) {
            this.deployInstanceConfiguration = deployInstanceConfiguration;
            return this;
        }

        public Builder extraDockerImages(List<StackDockerImage> extraDockerImages) {
            this.extraDockerImages = extraDockerImages;
            return this;
        }

        public Builder instanceType(InvokeCdkForInstance.Type instanceType) {
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

        public DeployNewInstance build() {
            return new DeployNewInstance(this);
        }

        public void deployWithClients(
                S3Client s3Client, S3TransferManager s3TransferManager, DynamoDbClient dynamoClient, EcrClient ecrClient) throws IOException, InterruptedException {
            s3Client(s3Client)
                    .s3TransferManager(s3TransferManager)
                    .dynamoClient(dynamoClient)
                    .ecrClient(ecrClient)
                    .build().deploy();
        }
    }
}
