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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.ecr.EcrClient;
import software.amazon.awssdk.services.s3.S3Client;

import sleeper.clients.util.ClientUtils;
import sleeper.clients.util.CommandPipelineRunner;
import sleeper.clients.util.EcrRepositoryCreator;
import sleeper.clients.util.cdk.CdkCommand;
import sleeper.clients.util.cdk.CdkDeploy;
import sleeper.clients.util.cdk.InvokeCdkForInstance;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.local.SaveLocalProperties;
import sleeper.configuration.properties.table.S3TableProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.SleeperVersion;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;

import static sleeper.clients.util.ClientUtils.optionalArgument;
import static sleeper.clients.util.cdk.CdkCommand.deployExisting;
import static sleeper.clients.util.cdk.CdkCommand.deployExistingPaused;

public class DeployExistingInstance {

    private static final Logger LOGGER = LoggerFactory.getLogger(DeployExistingInstance.class);
    private final Path scriptsDirectory;
    private final InstanceProperties properties;
    private final List<TableProperties> tablePropertiesList;
    private final S3Client s3;
    private final EcrClient ecr;
    private final CdkDeploy deployCommand;
    private final CommandPipelineRunner runCommand;

    private DeployExistingInstance(Builder builder) {
        scriptsDirectory = builder.scriptsDirectory;
        properties = builder.properties;
        tablePropertiesList = builder.tablePropertiesList;
        s3 = builder.s3;
        ecr = builder.ecr;
        deployCommand = builder.deployCommand;
        runCommand = builder.runCommand;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        if (args.length < 2 || args.length > 3) {
            throw new IllegalArgumentException("Usage: <scripts-dir> <instance-id> <optional-paused-true-or-false>");
        }

        boolean deployPaused = optionalArgument(args, 2)
                .map(Boolean::parseBoolean)
                .orElse(false);

        AmazonS3 s3 = AmazonS3ClientBuilder.defaultClient();
        AmazonDynamoDB dynamoDB = AmazonDynamoDBClientBuilder.defaultClient();
        try (S3Client s3v2 = S3Client.create();
                EcrClient ecr = EcrClient.create()) {
            builder().clients(s3v2, ecr)
                    .scriptsDirectory(Path.of(args[0]))
                    .instanceId(args[1])
                    .deployCommand(deployPaused ? deployExistingPaused() : deployExisting())
                    .loadPropertiesFromS3(s3, dynamoDB)
                    .build().update();
        } finally {
            s3.shutdown();
            dynamoDB.shutdown();
        }
    }

    public void update() throws IOException, InterruptedException {
        LOGGER.info("-------------------------------------------------------");
        LOGGER.info("Running Deployment");
        LOGGER.info("-------------------------------------------------------");

        // Write properties files for CDK
        Path generatedDirectory = scriptsDirectory.resolve("generated");
        Path jarsDirectory = scriptsDirectory.resolve("jars");
        Files.createDirectories(generatedDirectory);
        ClientUtils.clearDirectory(generatedDirectory);
        SaveLocalProperties.saveToDirectory(generatedDirectory, properties, tablePropertiesList.stream());

        SyncJars.builder().s3(s3)
                .jarsDirectory(jarsDirectory).instanceProperties(properties)
                .deleteOldJars(false)
                .build().sync();

        UploadDockerImages.builder()
                .baseDockerDirectory(scriptsDirectory.resolve("docker"))
                .ecrClient(EcrRepositoryCreator.withEcrClient(ecr))
                .build().upload(runCommand, StacksForDockerUpload.from(properties));

        LOGGER.info("-------------------------------------------------------");
        LOGGER.info("Deploying Stacks");
        LOGGER.info("-------------------------------------------------------");
        InvokeCdkForInstance.builder()
                .propertiesFile(generatedDirectory.resolve("instance.properties"))
                .version(SleeperVersion.getVersion())
                .jarsDirectory(jarsDirectory)
                .build().invokeInferringType(properties, deployCommand, runCommand);

        // We can use RestartTasks here to terminate indefinitely running ECS tasks, in order to get them onto the new
        // version of the jars. That will be part of issues #639 and #640 once graceful termination is implemented.
        // Note we'll need to reload instance properties as the cluster/lambda names may have been updated by the CDK.

        LOGGER.info("Finished deployment of existing instance");
    }

    public static final class Builder {
        private Path scriptsDirectory;
        private String instanceId;
        private InstanceProperties properties;
        private List<TableProperties> tablePropertiesList;
        private S3Client s3;
        private EcrClient ecr;
        private CdkDeploy deployCommand = CdkCommand.deployExisting();
        private CommandPipelineRunner runCommand = ClientUtils::runCommandInheritIO;

        private Builder() {
        }

        public Builder scriptsDirectory(Path scriptsDirectory) {
            this.scriptsDirectory = scriptsDirectory;
            return this;
        }

        public Builder instanceId(String instanceId) {
            this.instanceId = instanceId;
            return this;
        }

        public Builder properties(InstanceProperties properties) {
            this.properties = properties;
            return this;
        }

        public Builder tableProperties(TableProperties... tableProperties) {
            return tablePropertiesList(List.of(tableProperties));
        }

        public Builder tablePropertiesList(List<TableProperties> tablePropertiesList) {
            this.tablePropertiesList = tablePropertiesList;
            return this;
        }

        public Builder clients(S3Client s3, EcrClient ecr) {
            this.s3 = s3;
            this.ecr = ecr;
            return this;
        }

        public Builder deployCommand(CdkDeploy deployCommand) {
            this.deployCommand = deployCommand;
            return this;
        }

        public Builder runCommand(CommandPipelineRunner runCommand) {
            this.runCommand = runCommand;
            return this;
        }

        public Builder loadPropertiesFromS3(AmazonS3 s3, AmazonDynamoDB dynamoDB) {
            properties = new InstanceProperties();
            properties.loadFromS3GivenInstanceId(s3, instanceId);
            tablePropertiesList = S3TableProperties.getStore(properties, s3, dynamoDB)
                    .streamAllTables().collect(Collectors.toList());
            return this;
        }

        public DeployExistingInstance build() {
            return new DeployExistingInstance(this);
        }
    }
}
