/*
 * Copyright 2022-2023 Crown Copyright
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

import com.amazonaws.services.ecr.AmazonECR;
import com.amazonaws.services.ecr.AmazonECRClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3Client;

import sleeper.clients.util.ClientUtils;
import sleeper.clients.util.CommandPipelineRunner;
import sleeper.clients.util.EcrRepositoryCreator;
import sleeper.clients.util.cdk.CdkCommand;
import sleeper.clients.util.cdk.CdkDeploy;
import sleeper.clients.util.cdk.InvokeCdkForInstance;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.local.SaveLocalProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.SleeperVersion;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;

import static sleeper.configuration.properties.table.TableProperties.streamTablesFromS3;

public class DeployExistingInstance {

    private static final Logger LOGGER = LoggerFactory.getLogger(DeployExistingInstance.class);
    private final Path scriptsDirectory;
    private final InstanceProperties properties;
    private final List<TableProperties> tablePropertiesList;
    private final S3Client s3;
    private final AmazonECR ecr;
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
        if (2 != args.length) {
            throw new IllegalArgumentException("Usage: <scripts-dir> <instance-id>");
        }

        AmazonS3 s3 = AmazonS3ClientBuilder.defaultClient();
        AmazonECR ecr = AmazonECRClientBuilder.defaultClient();
        try (S3Client s3v2 = S3Client.create()) {
            builder().clients(s3v2, ecr)
                    .scriptsDirectory(Path.of(args[0]))
                    .instanceId(args[1])
                    .loadPropertiesFromS3(s3)
                    .build().update();
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
        private AmazonECR ecr;
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

        public Builder clients(S3Client s3, AmazonECR ecr) {
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

        public Builder loadPropertiesFromS3(AmazonS3 s3) {
            properties = new InstanceProperties();
            properties.loadFromS3GivenInstanceId(s3, instanceId);
            tablePropertiesList = streamTablesFromS3(s3, properties).collect(Collectors.toList());
            return this;
        }

        public DeployExistingInstance build() {
            return new DeployExistingInstance(this);
        }
    }
}
