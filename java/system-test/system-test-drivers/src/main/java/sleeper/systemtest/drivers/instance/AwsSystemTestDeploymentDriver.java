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

package sleeper.systemtest.drivers.instance;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.cloudformation.CloudFormationClient;
import software.amazon.awssdk.services.cloudformation.model.CloudFormationException;
import software.amazon.awssdk.services.ecr.EcrClient;
import software.amazon.awssdk.services.s3.S3Client;

import sleeper.clients.deploy.DeployConfiguration;
import sleeper.clients.deploy.container.CheckVersionExistsInEcr;
import sleeper.clients.deploy.container.UploadDockerImages;
import sleeper.clients.deploy.container.UploadDockerImagesToEcr;
import sleeper.clients.deploy.container.UploadDockerImagesToEcrRequest;
import sleeper.clients.deploy.jar.SyncJars;
import sleeper.clients.deploy.jar.SyncJarsRequest;
import sleeper.clients.util.cdk.CdkCommand;
import sleeper.clients.util.cdk.InvokeCdk;
import sleeper.clients.util.command.CommandUtils;
import sleeper.core.deploy.LambdaJar;
import sleeper.core.properties.model.SleeperArtefactsLocation;
import sleeper.systemtest.configuration.SystemTestStandaloneProperties;
import sleeper.systemtest.drivers.util.SystemTestClients;
import sleeper.systemtest.dsl.instance.SystemTestDeploymentDriver;
import sleeper.systemtest.dsl.instance.SystemTestParameters;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static sleeper.clients.util.cdk.InvokeCdk.Type.ARTEFACTS;
import static sleeper.clients.util.cdk.InvokeCdk.Type.SYSTEM_TEST_STANDALONE;
import static sleeper.core.deploy.LambdaJar.CUSTOM_RESOURCES;
import static sleeper.systemtest.configuration.SystemTestProperty.SYSTEM_TEST_ID;
import static sleeper.systemtest.drivers.cdk.DeployNewTestInstance.SYSTEM_TEST_IMAGE;

public class AwsSystemTestDeploymentDriver implements SystemTestDeploymentDriver {
    private static final Logger LOGGER = LoggerFactory.getLogger(AwsSystemTestDeploymentDriver.class);

    private final SystemTestParameters parameters;
    private final S3Client s3;
    private final EcrClient ecr;
    private final CloudFormationClient cloudFormation;

    public AwsSystemTestDeploymentDriver(SystemTestParameters parameters, SystemTestClients clients) {
        this.parameters = parameters;
        this.s3 = clients.getS3();
        this.ecr = clients.getEcr();
        this.cloudFormation = clients.getCloudFormation();
    }

    @Override
    public void saveProperties(SystemTestStandaloneProperties properties) {
        properties.saveToS3(s3);
    }

    @Override
    public SystemTestStandaloneProperties loadProperties() {
        return SystemTestStandaloneProperties.fromS3GivenDeploymentId(s3, parameters.getSystemTestShortId());
    }

    @Override
    public boolean deployIfNotPresent(SystemTestStandaloneProperties properties) {
        try {
            String deploymentId = properties.get(SYSTEM_TEST_ID);
            cloudFormation.describeStacks(builder -> builder.stackName(deploymentId));
            LOGGER.info("Deployment already exists: {}", deploymentId);
            return false;
        } catch (CloudFormationException e) {
            redeploy(properties);
            return true;
        }
    }

    @Override
    public void redeploy(SystemTestStandaloneProperties deployProperties) {
        try {
            uploadJarsAndDockerImages();
            Path generatedDirectory = Files.createDirectories(parameters.getGeneratedDirectory());
            Path propertiesFile = generatedDirectory.resolve("system-test.properties");
            deployProperties.save(propertiesFile);
            InvokeCdk.builder()
                    .jarsDirectory(parameters.getJarsDirectory())
                    .runCommand(CommandUtils::runCommandLogOutput)
                    .build().invoke(SYSTEM_TEST_STANDALONE,
                            CdkCommand.deploySystemTestStandalone(propertiesFile));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void uploadJarsAndDockerImages() throws IOException, InterruptedException {
        InvokeCdk.builder()
                .jarsDirectory(parameters.getJarsDirectory())
                .runCommand(CommandUtils::runCommandLogOutput)
                .build().invoke(ARTEFACTS,
                        CdkCommand.deployArtefacts(parameters.getArtefactsDeploymentId(), List.of(SYSTEM_TEST_IMAGE.getImageName())));
        new SyncJars(s3, parameters.getJarsDirectory())
                .sync(SyncJarsRequest.builder()
                        .bucketName(SleeperArtefactsLocation.getDefaultJarsBucketName(parameters.getArtefactsDeploymentId()))
                        .uploadFilter(jar -> LambdaJar.isFileJar(jar, CUSTOM_RESOURCES))
                        .build());
        if (!parameters.isSystemTestClusterEnabled()) {
            return;
        }
        UploadDockerImagesToEcr dockerUploader = new UploadDockerImagesToEcr(
                UploadDockerImages.builder()
                        .scriptsDirectory(parameters.getScriptsDirectory())
                        .deployConfig(DeployConfiguration.fromScriptsDirectory(parameters.getScriptsDirectory()))
                        .commandRunner(CommandUtils::runCommandLogOutput)
                        .build(),
                CheckVersionExistsInEcr.withEcrClient(ecr), parameters.getAccount(), parameters.getRegion());
        dockerUploader.upload(UploadDockerImagesToEcrRequest.builder()
                .ecrPrefix(SleeperArtefactsLocation.getDefaultEcrRepositoryPrefix(parameters.getArtefactsDeploymentId()))
                .images(List.of(SYSTEM_TEST_IMAGE))
                .build());
    }
}
