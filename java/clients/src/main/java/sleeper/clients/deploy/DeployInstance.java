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
import software.amazon.awssdk.services.ecr.EcrClient;
import software.amazon.awssdk.services.s3.S3Client;

import sleeper.clients.deploy.container.EcrRepositoryCreator;
import sleeper.clients.deploy.container.UploadDockerImages;
import sleeper.clients.deploy.container.UploadDockerImagesToEcr;
import sleeper.clients.deploy.container.UploadDockerImagesToEcrRequest;
import sleeper.clients.deploy.jar.SyncJars;
import sleeper.clients.deploy.jar.SyncJarsRequest;
import sleeper.clients.util.ClientUtils;
import sleeper.clients.util.cdk.CdkDeploy;
import sleeper.clients.util.cdk.InvokeCdkForInstance;
import sleeper.core.deploy.DeployInstanceConfiguration;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.local.SaveLocalProperties;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static sleeper.clients.util.ClientUtils.optionalArgument;
import static sleeper.core.properties.instance.CommonProperty.ID;
import static sleeper.core.properties.instance.CommonProperty.SUBNETS;
import static sleeper.core.properties.instance.CommonProperty.VPC_ID;

public class DeployInstance {
    public static final Logger LOGGER = LoggerFactory.getLogger(DeployInstance.class);

    private final SyncJars syncJars;
    private final UploadDockerImagesToEcr dockerImageUploader;
    private final WriteLocalProperties writeLocalProperties;
    private final InvokeCdkForInstance invokeCdk;

    public DeployInstance(SyncJars syncJars, UploadDockerImagesToEcr dockerImageUploader, WriteLocalProperties writeLocalProperties, InvokeCdkForInstance invokeCdk) {
        this.syncJars = syncJars;
        this.dockerImageUploader = dockerImageUploader;
        this.writeLocalProperties = writeLocalProperties;
        this.invokeCdk = invokeCdk;
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        if (args.length < 2 || args.length > 3) {
            throw new IllegalArgumentException("Usage: <scripts-dir> <instance-properties-path> <optional-paused-true-or-false>");
        }

        Path scriptsDirectory = Path.of(args[0]);
        Path propertiesFile = Path.of(args[1]);
        boolean deployPaused = optionalArgument(args, 2)
                .map(Boolean::parseBoolean)
                .orElse(false);

        try (S3Client s3Client = S3Client.create();
                EcrClient ecrClient = EcrClient.create()) {
            DeployInstance deployInstance = new DeployInstance(
                    SyncJars.fromScriptsDirectory(s3Client, scriptsDirectory),
                    new UploadDockerImagesToEcr(
                            UploadDockerImages.fromScriptsDirectory(scriptsDirectory),
                            EcrRepositoryCreator.withEcrClient(ecrClient)),
                    DeployInstance.writeLocalPropertiesUnderScriptsDirectory(scriptsDirectory),
                    InvokeCdkForInstance.fromScriptsDirectory(scriptsDirectory));

            deployInstance.deploy(DeployInstanceRequest.builder()
                    .instanceConfig(DeployInstanceConfiguration.fromLocalConfiguration(propertiesFile))
                    .cdkCommand(CdkDeploy.builder()
                            .ensureNewInstance(false)
                            .skipVersionCheck(false)
                            .deployPaused(deployPaused)
                            .build())
                    .build());
        }
    }

    public static WriteLocalProperties writeLocalPropertiesUnderScriptsDirectory(Path scriptsDirectory) {
        return writeLocalPropertiesToDirectory(scriptsDirectory.resolve("generated"));
    }

    public static WriteLocalProperties writeLocalPropertiesToDirectory(Path directory) {
        return instanceConfig -> {
            LOGGER.info("Writing instance configuration to local directory: {}", directory);
            Files.createDirectories(directory);
            ClientUtils.clearDirectory(directory);
            SaveLocalProperties.saveToDirectory(directory,
                    instanceConfig.getInstanceProperties(),
                    instanceConfig.getTableProperties().stream());
        };
    }

    public void deploy(DeployInstanceRequest request) throws IOException, InterruptedException {
        LOGGER.info("-------------------------------------------------------");
        LOGGER.info("Running Deployment");
        LOGGER.info("-------------------------------------------------------");

        DeployInstanceConfiguration instanceConfig = request.getInstanceConfig();
        InstanceProperties instanceProperties = instanceConfig.getInstanceProperties();
        LOGGER.info("instanceId: {}", instanceProperties.get(ID));
        LOGGER.info("vpcId: {}", instanceProperties.get(VPC_ID));
        LOGGER.info("subnetIds: {}", instanceProperties.get(SUBNETS));
        syncJars.sync(SyncJarsRequest.from(instanceProperties));
        dockerImageUploader.upload(
                UploadDockerImagesToEcrRequest.forDeployment(instanceProperties)
                        .withExtraImages(request.getExtraDockerImages()));
        writeLocalProperties.write(instanceConfig);
        LOGGER.info("-------------------------------------------------------");
        LOGGER.info("Deploying Stacks");
        LOGGER.info("-------------------------------------------------------");
        invokeCdk.invoke(request.getInstanceType(), request.getCdkCommand());
    }

    public interface WriteLocalProperties {
        void write(DeployInstanceConfiguration instanceConfig) throws IOException;
    }
}
