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
import software.amazon.awssdk.services.ecr.EcrClient;
import software.amazon.awssdk.services.s3.S3Client;

import sleeper.clients.deploy.container.DockerImageConfiguration;
import sleeper.clients.deploy.container.UploadDockerImages;
import sleeper.clients.deploy.container.UploadDockerImagesToEcr;
import sleeper.clients.deploy.container.UploadDockerImagesToEcrRequest;
import sleeper.clients.deploy.jar.SyncJars;
import sleeper.clients.deploy.jar.SyncJarsRequest;
import sleeper.clients.util.ClientUtils;
import sleeper.clients.util.cdk.CdkCommand;
import sleeper.clients.util.cdk.InvokeCdk;
import sleeper.core.deploy.SleeperInstanceConfiguration;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.local.SaveLocalProperties;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static sleeper.core.properties.instance.CommonProperty.ARTEFACTS_DEPLOYMENT_ID;
import static sleeper.core.properties.instance.CommonProperty.ID;
import static sleeper.core.properties.instance.CommonProperty.SUBNETS;
import static sleeper.core.properties.instance.CommonProperty.VPC_ID;
import static sleeper.core.properties.model.SleeperInternalCdkApp.ARTEFACTS;

public class DeployInstance {
    public static final Logger LOGGER = LoggerFactory.getLogger(DeployInstance.class);

    private final SyncJars syncJars;
    private final UploadDockerImagesToEcr dockerImageUploader;
    private final WriteLocalProperties writeLocalProperties;
    private final InvokeCdk invokeCdk;

    public DeployInstance(SyncJars syncJars, UploadDockerImagesToEcr dockerImageUploader, WriteLocalProperties writeLocalProperties, InvokeCdk invokeCdk) {
        this.syncJars = syncJars;
        this.dockerImageUploader = dockerImageUploader;
        this.writeLocalProperties = writeLocalProperties;
        this.invokeCdk = invokeCdk;
    }

    public static DeployInstance fromScriptsDirectory(Path scriptsDirectory, String account, String region, S3Client s3Client, EcrClient ecrClient) throws IOException {
        return new DeployInstance(
                SyncJars.fromScriptsDirectory(s3Client, account, scriptsDirectory),
                new UploadDockerImagesToEcr(
                        UploadDockerImages.fromScriptsDirectory(scriptsDirectory, ecrClient),
                        account, region),
                DeployInstance.WriteLocalProperties.underScriptsDirectory(scriptsDirectory),
                InvokeCdk.fromScriptsDirectory(scriptsDirectory));
    }

    public void deploy(DeployInstanceRequest request) throws IOException, InterruptedException {
        LOGGER.info("-------------------------------------------------------");
        LOGGER.info("Running Deployment");
        LOGGER.info("-------------------------------------------------------");

        SleeperInstanceConfiguration instanceConfig = request.getInstanceConfig();
        InstanceProperties instanceProperties = instanceConfig.getInstanceProperties();
        LOGGER.info("instanceId: {}", instanceProperties.get(ID));
        LOGGER.info("vpcId: {}", instanceProperties.get(VPC_ID));
        LOGGER.info("subnetIds: {}", instanceProperties.get(SUBNETS));
        if (!instanceProperties.isSet(ARTEFACTS_DEPLOYMENT_ID)) {
            invokeCdk.invoke(ARTEFACTS, CdkCommand.deployArtefacts(instanceProperties.get(ID)));
        }
        syncJars.sync(SyncJarsRequest.from(instanceProperties));
        dockerImageUploader.upload(
                UploadDockerImagesToEcrRequest.forDeployment(instanceProperties, request.getCdkApp(), DockerImageConfiguration.getDefault()));
        Path propertiesFile = writeLocalProperties.write(instanceConfig);
        LOGGER.info("-------------------------------------------------------");
        LOGGER.info("Deploying Stacks");
        LOGGER.info("-------------------------------------------------------");
        invokeCdk.invoke(request.getCdkApp(), request.getCdkCommand().withPropertiesFile(propertiesFile));
    }

    public interface WriteLocalProperties {
        Path write(SleeperInstanceConfiguration instanceConfig) throws IOException;

        static WriteLocalProperties underScriptsDirectory(Path scriptsDirectory) {
            return toDirectory(scriptsDirectory.resolve("generated"));
        }

        static WriteLocalProperties toDirectory(Path directory) {
            return instanceConfig -> {
                LOGGER.info("Writing instance configuration to local directory: {}", directory);
                Files.createDirectories(directory);
                ClientUtils.clearDirectory(directory);
                SaveLocalProperties.saveToDirectory(directory,
                        instanceConfig.getInstanceProperties(),
                        instanceConfig.getTableProperties().stream());
                return directory.resolve("instance.properties");
            };
        }
    }
}
