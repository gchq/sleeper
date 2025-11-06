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
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.ecr.EcrClient;
import software.amazon.awssdk.services.s3.S3Client;

import sleeper.clients.deploy.container.CheckVersionExistsInEcr;
import sleeper.clients.deploy.container.UploadDockerImages;
import sleeper.clients.deploy.container.UploadDockerImagesToEcr;
import sleeper.clients.deploy.jar.SyncJars;
import sleeper.clients.util.cdk.CdkCommand;
import sleeper.clients.util.cdk.InvokeCdk;
import sleeper.clients.util.command.CommandPipelineRunner;
import sleeper.clients.util.command.CommandUtils;
import sleeper.configuration.properties.S3InstanceProperties;
import sleeper.configuration.properties.S3TableProperties;
import sleeper.core.deploy.DeployInstanceConfiguration;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;

import static sleeper.clients.util.ClientUtils.optionalArgument;

public class DeployExistingInstance {

    private static final Logger LOGGER = LoggerFactory.getLogger(DeployExistingInstance.class);
    private final Path scriptsDirectory;
    private final InstanceProperties properties;
    private final List<TableProperties> tablePropertiesList;
    private final S3Client s3;
    private final EcrClient ecr;
    private final boolean deployPaused;
    private final CommandPipelineRunner runCommand;

    private DeployExistingInstance(Builder builder) {
        scriptsDirectory = builder.scriptsDirectory;
        properties = builder.properties;
        tablePropertiesList = builder.tablePropertiesList;
        s3 = builder.s3;
        ecr = builder.ecr;
        deployPaused = builder.deployPaused;
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

        try (S3Client s3Client = S3Client.create();
                DynamoDbClient dynamoClient = DynamoDbClient.create();
                EcrClient ecrClient = EcrClient.create()) {
            builder().clients(s3Client, ecrClient)
                    .scriptsDirectory(Path.of(args[0]))
                    .instanceId(args[1])
                    .deployPaused(deployPaused)
                    .loadPropertiesFromS3(s3Client, dynamoClient)
                    .build().update();
        }
    }

    public void update() throws IOException, InterruptedException {
        DeployInstance deployInstance = new DeployInstance(
                SyncJars.fromScriptsDirectory(s3, scriptsDirectory),
                new UploadDockerImagesToEcr(
                        UploadDockerImages.builder()
                                .scriptsDirectory(scriptsDirectory)
                                .deployConfig(DeployConfiguration.fromScriptsDirectory(scriptsDirectory))
                                .commandRunner(runCommand)
                                .build(),
                        CheckVersionExistsInEcr.withEcrClient(ecr)),
                DeployInstance.WriteLocalProperties.underScriptsDirectory(scriptsDirectory),
                InvokeCdk.builder().scriptsDirectory(scriptsDirectory).runCommand(runCommand).build());

        deployInstance.deploy(DeployInstanceRequest.builder()
                .instanceConfig(DeployInstanceConfiguration.builder().instanceProperties(properties).tableProperties(tablePropertiesList).build())
                .cdkCommand(deployPaused ? CdkCommand.deployExistingPaused() : CdkCommand.deployExisting())
                .inferInstanceType()
                .build());

        LOGGER.info("Finished deployment of existing instance");
    }

    public static final class Builder {
        private Path scriptsDirectory;
        private String instanceId;
        private InstanceProperties properties;
        private List<TableProperties> tablePropertiesList;
        private S3Client s3;
        private EcrClient ecr;
        private boolean deployPaused;
        private CommandPipelineRunner runCommand = CommandUtils::runCommandInheritIO;

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

        public Builder deployPaused(boolean deployPaused) {
            this.deployPaused = deployPaused;
            return this;
        }

        public Builder runCommand(CommandPipelineRunner runCommand) {
            this.runCommand = runCommand;
            return this;
        }

        public Builder loadPropertiesFromS3(S3Client s3Client, DynamoDbClient dynamoCient) {
            properties = S3InstanceProperties.loadGivenInstanceId(s3Client, instanceId);
            tablePropertiesList = S3TableProperties.createStore(properties, s3Client, dynamoCient)
                    .streamAllTables().collect(Collectors.toList());
            return this;
        }

        public DeployExistingInstance build() {
            return new DeployExistingInstance(this);
        }
    }
}
