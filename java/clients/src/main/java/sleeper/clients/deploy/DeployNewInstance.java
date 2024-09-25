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
import sleeper.configuration.s3properties.S3InstanceProperties;
import sleeper.core.properties.SleeperPropertiesValidationReporter;
import sleeper.core.properties.deploy.DeployInstanceConfiguration;
import sleeper.core.properties.deploy.DeployInstanceConfigurationFromTemplates;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.local.SaveLocalProperties;
import sleeper.core.properties.table.TableProperties;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.function.Consumer;

import static sleeper.clients.util.ClientUtils.optionalArgument;
import static sleeper.io.parquet.utils.HadoopConfigurationProvider.getConfigurationForClient;

public class DeployNewInstance {
    private static final Logger LOGGER = LoggerFactory.getLogger(DeployNewInstance.class);

    private final AWSSecurityTokenService sts;
    private final AwsRegionProvider regionProvider;
    private final AmazonS3 s3;
    private final S3Client s3v2;
    private final AmazonDynamoDB dynamoDB;
    private final EcrClient ecr;
    private final Path scriptsDirectory;
    private final String instanceId;
    private final String vpcId;
    private final String subnetIds;
    private final DeployInstanceConfiguration deployInstanceConfiguration;
    private final Consumer<InstanceProperties> extraInstanceProperties;
    private final List<StackDockerImage> extraDockerImages;
    private final InvokeCdkForInstance.Type instanceType;
    private final CommandPipelineRunner runCommand;
    private final boolean deployPaused;

    private DeployNewInstance(Builder builder) {
        sts = builder.sts;
        regionProvider = builder.regionProvider;
        s3 = builder.s3;
        s3v2 = builder.s3v2;
        dynamoDB = builder.dynamoDB;
        ecr = builder.ecr;
        scriptsDirectory = builder.scriptsDirectory;
        instanceId = builder.instanceId;
        vpcId = builder.vpcId;
        subnetIds = builder.subnetIds;
        deployInstanceConfiguration = builder.deployInstanceConfiguration;
        extraInstanceProperties = builder.extraInstanceProperties;
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
        Path scriptsDirectory = Path.of(args[0]);
        builder().scriptsDirectory(scriptsDirectory)
                .instanceId(args[1])
                .vpcId(args[2])
                .subnetIds(args[3])
                .deployInstanceConfiguration(DeployInstanceConfigurationFromTemplates.builder()
                        .tableNameForTemplate(args[4])
                        .instancePropertiesPath(optionalArgument(args, 5).map(Path::of).orElse(null))
                        .templatesDir(scriptsDirectory.resolve("templates"))
                        .splitPointsFileForTemplate(optionalArgument(args, 7).map(Path::of).orElse(null))
                        .build().load())
                .deployPaused("true".equalsIgnoreCase(optionalArgument(args, 6).orElse("false")))
                .instanceType(InvokeCdkForInstance.Type.STANDARD)
                .deployWithDefaultClients();
    }

    public void deploy() throws IOException, InterruptedException {
        LOGGER.info("-------------------------------------------------------");
        LOGGER.info("Running Deployment");
        LOGGER.info("-------------------------------------------------------");

        Path templatesDirectory = scriptsDirectory.resolve("templates");
        Path generatedDirectory = scriptsDirectory.resolve("generated");
        Path jarsDirectory = scriptsDirectory.resolve("jars");
        String sleeperVersion = Files.readString(templatesDirectory.resolve("version.txt"));

        LOGGER.info("instanceId: {}", instanceId);
        LOGGER.info("vpcId: {}", vpcId);
        LOGGER.info("subnetIds: {}", subnetIds);
        LOGGER.info("templatesDirectory: {}", templatesDirectory);
        LOGGER.info("generatedDirectory: {}", generatedDirectory);
        LOGGER.info("scriptsDirectory: {}", scriptsDirectory);
        LOGGER.info("jarsDirectory: {}", jarsDirectory);
        LOGGER.info("sleeperVersion: {}", sleeperVersion);
        LOGGER.info("deployPaused: {}", deployPaused);
        InstanceProperties instanceProperties = PopulateInstanceProperties.builder()
                .sts(sts).regionProvider(regionProvider)
                .deployInstanceConfig(deployInstanceConfiguration)
                .instanceId(instanceId).vpcId(vpcId).subnetIds(subnetIds)
                .build().populate();
        extraInstanceProperties.accept(instanceProperties);
        validate(instanceProperties, deployInstanceConfiguration.getTableProperties());

        SyncJars.builder().s3(s3v2)
                .jarsDirectory(jarsDirectory).instanceProperties(instanceProperties)
                .deleteOldJars(false).build().sync();
        UploadDockerImages.builder()
                .baseDockerDirectory(scriptsDirectory.resolve("docker"))
                .ecrClient(EcrRepositoryCreator.withEcrClient(ecr))
                .build().upload(runCommand,
                        StacksForDockerUpload.from(instanceProperties, sleeperVersion),
                        extraDockerImages);

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
        private AWSSecurityTokenService sts;
        private AwsRegionProvider regionProvider;
        private AmazonS3 s3;
        private S3Client s3v2;
        private AmazonDynamoDB dynamoDB;
        private EcrClient ecr;
        private Path scriptsDirectory;
        private String instanceId;
        private String vpcId;
        private String subnetIds;
        private DeployInstanceConfiguration deployInstanceConfiguration;
        private Consumer<InstanceProperties> extraInstanceProperties = properties -> {
        };
        private List<StackDockerImage> extraDockerImages = List.of();
        private InvokeCdkForInstance.Type instanceType;
        private CommandPipelineRunner runCommand = ClientUtils::runCommandInheritIO;
        private boolean deployPaused;

        private Builder() {
        }

        public Builder sts(AWSSecurityTokenService sts) {
            this.sts = sts;
            return this;
        }

        public Builder regionProvider(AwsRegionProvider regionProvider) {
            this.regionProvider = regionProvider;
            return this;
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

        public Builder instanceId(String instanceId) {
            this.instanceId = instanceId;
            return this;
        }

        public Builder vpcId(String vpcId) {
            this.vpcId = vpcId;
            return this;
        }

        public Builder subnetIds(String subnetIds) {
            this.subnetIds = subnetIds;
            return this;
        }

        public Builder deployInstanceConfiguration(DeployInstanceConfiguration deployInstanceConfiguration) {
            this.deployInstanceConfiguration = deployInstanceConfiguration;
            return this;
        }

        public Builder extraInstanceProperties(Consumer<InstanceProperties> extraInstanceProperties) {
            this.extraInstanceProperties = extraInstanceProperties;
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

        public void deployWithDefaultClients() throws IOException, InterruptedException {
            AWSSecurityTokenService sts = AWSSecurityTokenServiceClientBuilder.defaultClient();
            AmazonS3 s3 = AmazonS3ClientBuilder.defaultClient();
            AmazonDynamoDB dynamoDB = AmazonDynamoDBClientBuilder.defaultClient();
            try (S3Client s3v2 = S3Client.create();
                    EcrClient ecr = EcrClient.create()) {
                deployWithClients(
                        sts, DefaultAwsRegionProviderChain.builder().build(),
                        s3, s3v2, dynamoDB, ecr);
            } finally {
                sts.shutdown();
                s3.shutdown();
                dynamoDB.shutdown();
            }
        }

        public void deployWithClients(
                AWSSecurityTokenService sts, AwsRegionProvider regionProvider,
                AmazonS3 s3, S3Client s3v2, AmazonDynamoDB dynamoDB, EcrClient ecr) throws IOException, InterruptedException {
            sts(sts).regionProvider(regionProvider)
                    .s3(s3).s3v2(s3v2).dynamoDB(dynamoDB).ecr(ecr)
                    .build().deploy();
        }
    }
}
