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

import com.amazonaws.services.ecr.AmazonECR;
import com.amazonaws.services.s3.AmazonS3;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.cloudformation.CloudFormationClient;
import software.amazon.awssdk.services.cloudformation.model.CloudFormationException;
import software.amazon.awssdk.services.s3.S3Client;

import sleeper.cdk.jars.BuiltJar;
import sleeper.clients.deploy.StacksForDockerUpload;
import sleeper.clients.deploy.SyncJars;
import sleeper.clients.deploy.UploadDockerImages;
import sleeper.clients.util.ClientUtils;
import sleeper.clients.util.EcrRepositoryCreator;
import sleeper.clients.util.cdk.CdkCommand;
import sleeper.clients.util.cdk.InvokeCdkForInstance;
import sleeper.core.SleeperVersion;
import sleeper.systemtest.configuration.SystemTestStandaloneProperties;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.function.Consumer;

import static sleeper.cdk.jars.BuiltJar.CUSTOM_RESOURCES;
import static sleeper.clients.util.cdk.InvokeCdkForInstance.Type.SYSTEM_TEST_STANDALONE;
import static sleeper.systemtest.configuration.SystemTestProperty.MAX_ENTRIES_RANDOM_LIST;
import static sleeper.systemtest.configuration.SystemTestProperty.MAX_ENTRIES_RANDOM_MAP;
import static sleeper.systemtest.configuration.SystemTestProperty.MAX_RANDOM_INT;
import static sleeper.systemtest.configuration.SystemTestProperty.MAX_RANDOM_LONG;
import static sleeper.systemtest.configuration.SystemTestProperty.MIN_RANDOM_INT;
import static sleeper.systemtest.configuration.SystemTestProperty.MIN_RANDOM_LONG;
import static sleeper.systemtest.configuration.SystemTestProperty.RANDOM_BYTE_ARRAY_LENGTH;
import static sleeper.systemtest.configuration.SystemTestProperty.RANDOM_STRING_LENGTH;
import static sleeper.systemtest.configuration.SystemTestProperty.SYSTEM_TEST_ACCOUNT;
import static sleeper.systemtest.configuration.SystemTestProperty.SYSTEM_TEST_BUCKET_NAME;
import static sleeper.systemtest.configuration.SystemTestProperty.SYSTEM_TEST_CLUSTER_ENABLED;
import static sleeper.systemtest.configuration.SystemTestProperty.SYSTEM_TEST_ID;
import static sleeper.systemtest.configuration.SystemTestProperty.SYSTEM_TEST_JARS_BUCKET;
import static sleeper.systemtest.configuration.SystemTestProperty.SYSTEM_TEST_REGION;
import static sleeper.systemtest.configuration.SystemTestProperty.SYSTEM_TEST_REPO;
import static sleeper.systemtest.configuration.SystemTestProperty.SYSTEM_TEST_VPC_ID;
import static sleeper.systemtest.configuration.SystemTestProperty.WRITE_DATA_ROLE_NAME;

public class SystemTestDeploymentContext {
    private static final Logger LOGGER = LoggerFactory.getLogger(SystemTestDeploymentContext.class);

    private final SystemTestParameters parameters;
    private final AmazonS3 s3;
    private final S3Client s3v2;
    private final AmazonECR ecr;
    private final CloudFormationClient cloudFormation;
    private SystemTestStandaloneProperties properties;
    private InstanceDidNotDeployException failure;

    public SystemTestDeploymentContext(SystemTestParameters parameters, AmazonS3 s3, S3Client s3v2,
                                       AmazonECR ecr, CloudFormationClient cloudFormation) {
        this.parameters = parameters;
        this.s3 = s3;
        this.s3v2 = s3v2;
        this.ecr = ecr;
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
            failure = new InstanceDidNotDeployException(parameters.getSystemTestShortId(), e);
            throw e;
        }
    }

    public void resetProperties() {
        updateProperties(properties ->
                properties.getPropertiesIndex().getUserDefined().stream()
                        .filter(property -> property.isEditable() && !property.isRunCDKDeployWhenChanged())
                        .forEach(properties::unset));
    }

    public boolean isSystemTestClusterEnabled() {
        return parameters.isSystemTestClusterEnabled() && properties.getBoolean(SYSTEM_TEST_CLUSTER_ENABLED);
    }

    private void deployIfMissingNoFailureTracking() throws InterruptedException {
        try {
            String deploymentId = parameters.getSystemTestShortId();
            cloudFormation.describeStacks(builder -> builder.stackName(deploymentId));
            LOGGER.info("Deployment already exists: {}", deploymentId);
            properties = loadProperties();
            redeployIfNeeded();
        } catch (CloudFormationException e) {
            deploy(generateProperties());
        }
    }

    private void redeployIfNeeded() throws InterruptedException {
        boolean redeployNeeded = false;
        if (parameters.isSystemTestClusterEnabled() && !properties.getBoolean(SYSTEM_TEST_CLUSTER_ENABLED)) {
            properties.set(SYSTEM_TEST_CLUSTER_ENABLED, "true");
            LOGGER.info("System test cluster not present, deploying");
            redeployNeeded = true;
        }
        if (parameters.isForceRedeploySystemTest()) {
            LOGGER.info("Forcing redeploy");
            redeployNeeded = true;
        }
        if (redeployNeeded) {
            deploy(properties);
        }
    }

    private void deploy(SystemTestStandaloneProperties deployProperties) throws InterruptedException {
        try {
            uploadJarsAndDockerImages();
            Path generatedDirectory = Files.createDirectories(parameters.getGeneratedDirectory());
            Path propertiesFile = generatedDirectory.resolve("system-test.properties");
            deployProperties.save(propertiesFile);
            InvokeCdkForInstance.builder()
                    .propertiesFile(propertiesFile)
                    .jarsDirectory(parameters.getJarsDirectory())
                    .version(SleeperVersion.getVersion())
                    .build().invoke(SYSTEM_TEST_STANDALONE,
                            CdkCommand.deploySystemTestStandalone(),
                            ClientUtils::runCommandLogOutput);
            properties = loadProperties();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private SystemTestStandaloneProperties loadProperties() {
        try {
            return SystemTestStandaloneProperties.fromS3(s3, parameters.buildSystemTestBucketName());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private SystemTestStandaloneProperties generateProperties() {
        SystemTestStandaloneProperties properties = new SystemTestStandaloneProperties();
        properties.set(SYSTEM_TEST_ID, parameters.getSystemTestShortId());
        properties.set(SYSTEM_TEST_ACCOUNT, parameters.getAccount());
        properties.set(SYSTEM_TEST_REGION, parameters.getRegion());
        properties.set(SYSTEM_TEST_VPC_ID, parameters.getVpcId());
        properties.set(SYSTEM_TEST_JARS_BUCKET, parameters.buildJarsBucketName());
        properties.set(SYSTEM_TEST_REPO, parameters.buildSystemTestECRRepoName());
        properties.set(SYSTEM_TEST_CLUSTER_ENABLED, String.valueOf(parameters.isSystemTestClusterEnabled()));
        properties.set(MIN_RANDOM_INT, "0");
        properties.set(MAX_RANDOM_INT, "100000000");
        properties.set(MIN_RANDOM_LONG, "0");
        properties.set(MAX_RANDOM_LONG, "10000000000");
        properties.set(RANDOM_STRING_LENGTH, "10");
        properties.set(RANDOM_BYTE_ARRAY_LENGTH, "10");
        properties.set(MAX_ENTRIES_RANDOM_MAP, "10");
        properties.set(MAX_ENTRIES_RANDOM_LIST, "10");
        return properties;
    }

    private void uploadJarsAndDockerImages() throws IOException, InterruptedException {
        SyncJars.builder().s3(s3v2)
                .jarsDirectory(parameters.getJarsDirectory())
                .bucketName(parameters.buildJarsBucketName())
                .region(parameters.getRegion())
                .uploadFilter(jar -> BuiltJar.isFileJar(jar, CUSTOM_RESOURCES))
                .deleteOldJars(false).build().sync();
        if (!parameters.isSystemTestClusterEnabled()) {
            return;
        }
        UploadDockerImages.builder()
                .baseDockerDirectory(parameters.getDockerDirectory())
                .ecrClient(EcrRepositoryCreator.withEcrClient(ecr))
                .build().upload(ClientUtils::runCommandLogOutput, StacksForDockerUpload.builder()
                        .ecrPrefix(parameters.getSystemTestShortId())
                        .account(parameters.getAccount())
                        .region(parameters.getRegion())
                        .version(SleeperVersion.getVersion())
                        .stacks(List.of("SystemTestStack")).build());
    }

    public String getSystemTestBucketName() {
        return properties.get(SYSTEM_TEST_BUCKET_NAME);
    }

    public String getSystemTestWriterRoleName() {
        return properties.get(WRITE_DATA_ROLE_NAME);
    }
}
