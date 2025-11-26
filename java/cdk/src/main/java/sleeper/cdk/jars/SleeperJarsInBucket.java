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
package sleeper.cdk.jars;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awscdk.services.s3.IBucket;
import software.amazon.awssdk.services.s3.S3Client;

import sleeper.core.deploy.LambdaJar;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.model.LambdaDeployType;

import java.util.HashMap;
import java.util.Map;

import static sleeper.core.properties.instance.CommonProperty.ECR_REPOSITORY_PREFIX;
import static sleeper.core.properties.instance.CommonProperty.JARS_BUCKET;
import static sleeper.core.properties.instance.CommonProperty.LAMBDA_DEPLOY_TYPE;

/**
 * Finds jars to deploy lambda functions. This will finds the latest version of each jar in a versioned S3 bucket. The
 * deployment will be done against a specific version of each jar. It will only check the bucket once for each jar, and
 * you can reuse the same object for multiple Sleeper instances.
 */
public class SleeperJarsInBucket {

    public static final Logger LOGGER = LoggerFactory.getLogger(SleeperJarsInBucket.class);

    private final GetVersionId getVersionId;
    private final String bucketName;
    private final LambdaDeployType deployType;
    private final String ecrRepositoryPrefix;
    private final Map<LambdaJar, String> latestVersionIdByJar = new HashMap<>();

    private SleeperJarsInBucket(GetVersionId getVersionId, String bucketName, LambdaDeployType deployType, String ecrRepositoryPrefix) {
        this.getVersionId = getVersionId;
        this.bucketName = bucketName;
        this.deployType = deployType;
        this.ecrRepositoryPrefix = ecrRepositoryPrefix;
    }

    public static SleeperJarsInBucket from(S3Client s3, InstanceProperties instanceProperties) {
        return from(GetVersionId.fromJarsBucket(s3, instanceProperties), instanceProperties);
    }

    public static SleeperJarsInBucket from(GetVersionId getVersionId, InstanceProperties instanceProperties) {
        return new SleeperJarsInBucket(getVersionId,
                instanceProperties.get(JARS_BUCKET),
                instanceProperties.getEnumValue(LAMBDA_DEPLOY_TYPE, LambdaDeployType.class),
                instanceProperties.get(ECR_REPOSITORY_PREFIX));
    }

    public String bucketName() {
        return bucketName;
    }

    public SleeperLambdaCode lambdaCode(IBucket bucketConstruct) {
        return new SleeperLambdaCode(this, deployType, bucketConstruct);
    }

    public String getLatestVersionId(LambdaJar jar) {
        return latestVersionIdByJar.computeIfAbsent(jar, getVersionId::getVersionId);
    }

    public String getRepositoryName(LambdaJar jar) {
        return ecrRepositoryPrefix + "/" + jar.getImageName();
    }

    public interface GetVersionId {
        String getVersionId(LambdaJar jar);

        static GetVersionId fromJarsBucket(S3Client s3Client, InstanceProperties instanceProperties) {
            String bucketName = instanceProperties.get(JARS_BUCKET);
            return jar -> {
                String versionId = s3Client.headObject(builder -> builder.bucket(bucketName).key(jar.getFilename())).versionId();
                LOGGER.info("Found latest version ID for jar {}: {}", jar.getFilename(), versionId);
                return versionId;
            };
        }
    }
}
