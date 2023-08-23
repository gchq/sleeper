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

package sleeper.systemtest.drivers.instance;

import com.amazonaws.services.s3.AmazonS3;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.cloudformation.CloudFormationClient;
import software.amazon.awssdk.services.cloudformation.model.CloudFormationException;
import software.amazon.awssdk.services.s3.S3Client;

import sleeper.clients.deploy.SyncJars;
import sleeper.clients.deploy.UploadDockerImages;
import sleeper.clients.util.ClientUtils;
import sleeper.clients.util.cdk.CdkCommand;
import sleeper.clients.util.cdk.InvokeCdkForInstance;
import sleeper.core.SleeperVersion;
import sleeper.systemtest.configuration.SystemTestStandaloneProperties;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.function.Consumer;

import static sleeper.clients.util.cdk.InvokeCdkForInstance.Type.SYSTEM_TEST_STANDALONE;
import static sleeper.systemtest.configuration.SystemTestProperty.SYSTEM_TEST_ACCOUNT;
import static sleeper.systemtest.configuration.SystemTestProperty.SYSTEM_TEST_BUCKET_NAME;
import static sleeper.systemtest.configuration.SystemTestProperty.SYSTEM_TEST_ID;
import static sleeper.systemtest.configuration.SystemTestProperty.SYSTEM_TEST_JARS_BUCKET;
import static sleeper.systemtest.configuration.SystemTestProperty.SYSTEM_TEST_REGION;
import static sleeper.systemtest.configuration.SystemTestProperty.SYSTEM_TEST_REPO;
import static sleeper.systemtest.configuration.SystemTestProperty.SYSTEM_TEST_VPC_ID;
import static sleeper.systemtest.configuration.SystemTestProperty.WRITE_DATA_ROLE_NAME;

public class SystemTestInstanceContext {
    private static final Logger LOGGER = LoggerFactory.getLogger(SystemTestInstanceContext.class);

    private final SystemTestParameters parameters;
    private final AmazonS3 s3;
    private final S3Client s3v2;
    private final CloudFormationClient cloudFormation;
    private SystemTestStandaloneProperties properties;
    private InstanceDidNotDeployException failure;

    public SystemTestInstanceContext(SystemTestParameters parameters,
                                     AmazonS3 s3, S3Client s3v2, CloudFormationClient cloudFormation) {
        this.parameters = parameters;
        this.s3 = s3;
        this.s3v2 = s3v2;
        this.cloudFormation = cloudFormation;
    }

    public void updateProperties(Consumer<SystemTestStandaloneProperties> config) {
        config.accept(properties);
        try {
            properties.saveToS3(s3);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public SystemTestStandaloneProperties getProperties() {
        return properties;
    }

    public void deployIfMissing() throws InterruptedException {
        if (properties != null) {
            return;
        }
        if (failure != null) {
            throw failure;
        }
        try {
            deployIfMissingNoFailureTracking();
        } catch (RuntimeException | InterruptedException e) {
            failure = new InstanceDidNotDeployException(parameters.getSystemTestDeploymentId(), e);
            throw e;
        }
    }

    private void deployIfMissingNoFailureTracking() throws InterruptedException {
        try {
            String deploymentId = parameters.getSystemTestDeploymentId();
            cloudFormation.describeStacks(builder -> builder.stackName(deploymentId));
            LOGGER.info("Deployment already exists: {}", deploymentId);
        } catch (CloudFormationException e) {
            try {
                uploadJarsAndDockerImages();
                Path propertiesFile = parameters.getGeneratedDirectory().resolve("system-test.properties");
                generateProperties().save(propertiesFile);
                InvokeCdkForInstance.builder()
                        .propertiesFile(propertiesFile)
                        .jarsDirectory(parameters.getJarsDirectory())
                        .version(SleeperVersion.getVersion())
                        .build().invoke(SYSTEM_TEST_STANDALONE,
                                CdkCommand.deploySystemTestStandalone(),
                                ClientUtils::runCommandLogOutput);
            } catch (IOException e1) {
                throw new UncheckedIOException(e1);
            }
        }
        try {
            properties = SystemTestStandaloneProperties.fromS3(s3, parameters.buildSystemTestBucketName());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private SystemTestStandaloneProperties generateProperties() {
        SystemTestStandaloneProperties properties = new SystemTestStandaloneProperties();
        properties.set(SYSTEM_TEST_ID, parameters.getSystemTestDeploymentId());
        properties.set(SYSTEM_TEST_ACCOUNT, parameters.getAccount());
        properties.set(SYSTEM_TEST_REGION, parameters.getRegion());
        properties.set(SYSTEM_TEST_VPC_ID, parameters.getVpcId());
        properties.set(SYSTEM_TEST_JARS_BUCKET, parameters.buildJarsBucketName());
        properties.set(SYSTEM_TEST_REPO, parameters.getSystemTestDeploymentId() + "/system-test");
        return properties;
    }

    private void uploadJarsAndDockerImages() throws IOException, InterruptedException {
        boolean jarsChanged = SyncJars.builder().s3(s3v2)
                .jarsDirectory(parameters.getJarsDirectory())
                .bucketName(parameters.buildJarsBucketName())
                .region(parameters.getRegion())
                .uploadFilter(jar -> {
                    String filename = String.valueOf(jar.getFileName());
                    return filename.startsWith("system-test") || filename.startsWith("cdk-custom-resources");
                })
                .deleteOldJars(false).build().sync();
        UploadDockerImages.builder()
                .baseDockerDirectory(parameters.getDockerDirectory())
                .uploadDockerImagesScript(parameters.getScriptsDirectory().resolve("deploy/uploadDockerImages.sh"))
                .id(parameters.getSystemTestDeploymentId())
                .account(parameters.getAccount())
                .region(parameters.getRegion())
                .skipIf(!jarsChanged)
                .stacks("SystemTestStack")
                .build().upload(ClientUtils::runCommandLogOutput);
    }

    public String getSystemTestBucketName() {
        return properties.get(SYSTEM_TEST_BUCKET_NAME);
    }

    public String getSystemTestWriterRoleName() {
        return properties.get(WRITE_DATA_ROLE_NAME);
    }
}
