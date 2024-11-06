/*
 * Copyright 2022-2024 Crown Copyright
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

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.regions.providers.AwsRegionProvider;
import software.amazon.awssdk.regions.providers.DefaultAwsRegionProviderChain;
import software.amazon.awssdk.services.ecr.EcrClient;
import software.amazon.awssdk.services.s3.S3Client;

import sleeper.clients.status.update.AddTable;
import sleeper.clients.util.ClientUtils;
import sleeper.clients.util.CommandPipelineRunner;
import sleeper.clients.util.EcrRepositoryCreator;
import sleeper.clients.util.cdk.CdkCommand;
import sleeper.clients.util.cdk.InvokeCdkForInstance;
import sleeper.configuration.properties.S3InstanceProperties;
import sleeper.core.deploy.DeployInstanceConfiguration;
import sleeper.core.deploy.DeployInstanceConfigurationFromTemplates;
import sleeper.core.deploy.PopulateInstanceProperties;
import sleeper.core.properties.SleeperPropertiesValidationReporter;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.local.SaveLocalProperties;
import sleeper.core.properties.table.TableProperties;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static sleeper.clients.util.ClientUtils.optionalArgument;
import static sleeper.core.properties.instance.CommonProperty.ID;
import static sleeper.core.properties.instance.CommonProperty.SUBNETS;
import static sleeper.core.properties.instance.CommonProperty.VPC_ID;
import static sleeper.parquet.utils.HadoopConfigurationProvider.getConfigurationForClient;

public class DeployNewInstance {
    private static final Logger LOGGER = LoggerFactory.getLogger(DeployNewInstance.class);

    private final AmazonS3 s3;
    private final S3Client s3v2;
    private final AmazonDynamoDB dynamoDB;
    private final EcrClient ecr;
    private final Path scriptsDirectory;
    private final DeployInstanceConfiguration deployInstanceConfiguration;
    private final List<StackDockerImage> extraDockerImages;
    private final InvokeCdkForInstance.Type instanceType;
    private final CommandPipelineRunner runCommand;
    private final boolean deployPaused;

    private DeployNewInstance(Builder builder) {
        s3 = builder.s3;
        s3v2 = builder.s3v2;
        dynamoDB = builder.dynamoDB;
        ecr = builder.ecr;
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
        if (args.length < 5 || args.length > 8) {
            throw new IllegalArgumentException("Usage: <scripts-dir> <instance-id> <vpc> <csv-list-of-subnets> <table-name> " +
                    "<optional-instance-properties-file> <optional-deploy-paused-flag> <optional-split-points-file>");
        }
        AWSSecurityTokenService sts = AWSSecurityTokenServiceClientBuilder.defaultClient();
        AmazonS3 s3 = AmazonS3ClientBuilder.defaultClient();
        AmazonDynamoDB dynamoDB = AmazonDynamoDBClientBuilder.defaultClient();
        AwsRegionProvider regionProvider = DefaultAwsRegionProviderChain.builder().build();
        try (S3Client s3v2 = S3Client.create();
                EcrClient ecr = EcrClient.create()) {
            Path scriptsDirectory = Path.of(args[0]);
            PopulateInstanceProperties populateInstanceProperties = PopulateInstancePropertiesAws.builder(sts, regionProvider)
                    .instanceId(args[1]).vpcId(args[2]).subnetIds(args[3])
                    .build();
            String tableNameForTemplate = optionalArgument(args, 4).orElse(null);
            Path instancePropertiesFile = optionalArgument(args, 5).map(Path::of).orElse(null);
            boolean deployPaused = "true".equalsIgnoreCase(optionalArgument(args, 6).orElse("false"));
            Path splitPointsFileForTemplate = optionalArgument(args, 7).map(Path::of).orElse(null);
            DeployInstanceConfigurationFromTemplates fromTemplates = DeployInstanceConfigurationFromTemplates.builder()
                    .templatesDir(scriptsDirectory.resolve("templates"))
                    .tableNameForTemplate(tableNameForTemplate)
                    .splitPointsFileForTemplate(splitPointsFileForTemplate)
                    .build();
            builder().scriptsDirectory(scriptsDirectory)
                    .deployInstanceConfiguration(DeployInstanceConfiguration.forNewInstanceDefaultingInstanceAndTables(instancePropertiesFile, populateInstanceProperties, fromTemplates))
                    .deployPaused(deployPaused)
                    .instanceType(InvokeCdkForInstance.Type.STANDARD)
                    .deployWithClients(s3, s3v2, dynamoDB, ecr);
        } finally {
            sts.shutdown();
            s3.shutdown();
            dynamoDB.shutdown();
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

        SyncJars.builder().s3(s3v2)
                .jarsDirectory(jarsDirectory).instanceProperties(instanceProperties)
                .deleteOldJars(false).build().sync();
        UploadDockerImages.builder()
                .baseDockerDirectory(scriptsDirectory.resolve("docker")).jarsDirectory(jarsDirectory)
                .ecrClient(EcrRepositoryCreator.withEcrClient(ecr))
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
        instanceProperties = S3InstanceProperties.loadGivenInstanceId(s3, instanceId);
        for (TableProperties tableProperties : deployInstanceConfiguration.getTableProperties()) {
            LOGGER.info("Adding table " + tableProperties.getStatus());
            new AddTable(s3, dynamoDB, instanceProperties, tableProperties, getConfigurationForClient(instanceProperties, tableProperties)).run();
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
        private AmazonS3 s3;
        private S3Client s3v2;
        private AmazonDynamoDB dynamoDB;
        private EcrClient ecr;
        private Path scriptsDirectory;
        private DeployInstanceConfiguration deployInstanceConfiguration;
        private List<StackDockerImage> extraDockerImages = List.of();
        private InvokeCdkForInstance.Type instanceType;
        private CommandPipelineRunner runCommand = ClientUtils::runCommandInheritIO;
        private boolean deployPaused;

        private Builder() {
        }

        public Builder s3(AmazonS3 s3) {
            this.s3 = s3;
            return this;
        }

        public Builder s3v2(S3Client s3v2) {
            this.s3v2 = s3v2;
            return this;
        }

        public Builder dynamoDB(AmazonDynamoDB dynamoDB) {
            this.dynamoDB = dynamoDB;
            return this;
        }

        public Builder ecr(EcrClient ecr) {
            this.ecr = ecr;
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
                AmazonS3 s3, S3Client s3v2, AmazonDynamoDB dynamoDB, EcrClient ecr) throws IOException, InterruptedException {
            s3(s3).s3v2(s3v2).dynamoDB(dynamoDB).ecr(ecr)
                    .build().deploy();
        }
    }
}
