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
package sleeper.cdk.jars;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awscdk.services.s3.IBucket;
import software.amazon.awssdk.services.s3.S3Client;

import sleeper.core.deploy.LambdaJar;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.validation.LambdaDeployType;

import java.util.HashMap;
import java.util.Map;

import static sleeper.core.properties.instance.CommonProperty.ECR_REPOSITORY_PREFIX;
import static sleeper.core.properties.instance.CommonProperty.JARS_BUCKET;
import static sleeper.core.properties.instance.CommonProperty.LAMBDA_DEPLOY_TYPE;

public class BuiltJars {

    public static final Logger LOGGER = LoggerFactory.getLogger(BuiltJars.class);

    private final S3Client s3;
    private final String bucketName;
    private final LambdaDeployType deployType;
    private final String ecrRepositoryPrefix;
    private final Map<LambdaJar, String> latestVersionIdByJar = new HashMap<>();

    private BuiltJars(S3Client s3, String bucketName, LambdaDeployType deployType, String ecrRepositoryPrefix) {
        this.s3 = s3;
        this.bucketName = bucketName;
        this.deployType = deployType;
        this.ecrRepositoryPrefix = ecrRepositoryPrefix;
    }

    public static BuiltJars from(S3Client s3, InstanceProperties instanceProperties) {
        return new BuiltJars(s3,
                instanceProperties.get(JARS_BUCKET),
                instanceProperties.getEnumValue(LAMBDA_DEPLOY_TYPE, LambdaDeployType.class),
                instanceProperties.get(ECR_REPOSITORY_PREFIX));
    }

    public String bucketName() {
        return bucketName;
    }

    public LambdaCode lambdaCode(LambdaJar jar, IBucket bucketConstruct) {
        return new LambdaCode(this, jar, deployType, bucketConstruct);
    }

    public String getLatestVersionId(LambdaJar jar) {
        return latestVersionIdByJar.computeIfAbsent(jar,
                missingJar -> {
                    String versionId = s3.headObject(builder -> builder.bucket(bucketName).key(missingJar.getFilename())).versionId();
                    LOGGER.info("Found latest version ID for jar {}: {}", missingJar.getFilename(), versionId);
                    return versionId;
                });
    }

    public String getRepositoryName(LambdaJar jar) {
        return ecrRepositoryPrefix + "/" + jar.getImageName();
    }
}
