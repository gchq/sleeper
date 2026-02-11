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
package sleeper.cdk.artefacts;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awscdk.services.s3.Bucket;
import software.amazon.awscdk.services.s3.IBucket;
import software.amazon.awssdk.services.s3.S3Client;
import software.constructs.Construct;

import sleeper.cdk.lambda.SleeperLambdaCode;
import sleeper.core.deploy.LambdaJar;
import sleeper.core.properties.instance.InstanceProperties;

import java.util.HashMap;
import java.util.Map;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.VERSION;
import static sleeper.core.properties.instance.CommonProperty.ECR_REPOSITORY_PREFIX;
import static sleeper.core.properties.instance.CommonProperty.JARS_BUCKET;

/**
 * Finds jars to deploy lambda functions. This will finds the latest version of each jar in a versioned S3 bucket. The
 * deployment will be done against a specific version of each jar. It will only check the bucket once for each jar, and
 * you can reuse the same object for multiple Sleeper instances.
 */
public class SleeperJarsInBucket {

    public static final Logger LOGGER = LoggerFactory.getLogger(SleeperJarsInBucket.class);

    private final GetVersionId getVersionId;
    private final InstanceProperties instanceProperties;
    private final Map<LambdaJar, String> latestVersionIdByJar = new HashMap<>();

    private SleeperJarsInBucket(GetVersionId getVersionId, InstanceProperties instanceProperties) {
        this.getVersionId = getVersionId;
        this.instanceProperties = instanceProperties;
    }

    public static SleeperJarsInBucket from(S3Client s3, InstanceProperties instanceProperties) {
        return from(GetVersionId.fromJarsBucket(s3, instanceProperties), instanceProperties);
    }

    public static SleeperJarsInBucket from(GetVersionId getVersionId, InstanceProperties instanceProperties) {
        return new SleeperJarsInBucket(getVersionId, instanceProperties);
    }

    public IBucket createJarsBucketReference(Construct scope, String id) {
        return Bucket.fromBucketName(scope, id, instanceProperties.get(JARS_BUCKET));
    }

    public SleeperLambdaCode lambdaCode(IBucket bucketConstruct) {
        return SleeperLambdaCode.from(instanceProperties, new SleeperArtefactsFromProperties(instanceProperties, this), bucketConstruct);
    }

    public String getLatestVersionId(LambdaJar jar) {
        return latestVersionIdByJar.computeIfAbsent(jar, getVersionId::getVersionId);
    }

    public String getRepositoryName(LambdaJar jar) {
        return instanceProperties.get(ECR_REPOSITORY_PREFIX) + "/" + jar.getImageName();
    }

    /**
     * Checks the version ID of the latest version of a given jar in the jars bucket. When we provide the CDK with a
     * specific S3 version ID, it can tell when the jar has changed even if it still has the same filename.
     */
    public interface GetVersionId {
        String getVersionId(LambdaJar jar);

        static GetVersionId fromJarsBucket(S3Client s3Client, InstanceProperties instanceProperties) {
            return jar -> {
                String filename = jar.getFilename(instanceProperties.get(VERSION));
                String bucketName = instanceProperties.get(JARS_BUCKET);
                LOGGER.info("Checking version ID for jar: {}", filename);
                String versionId = s3Client.headObject(builder -> builder.bucket(bucketName).key(filename)).versionId();
                LOGGER.info("Found latest version ID for jar {}: {}", filename, versionId);
                return versionId;
            };
        }
    }
}
