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
package sleeper.clients.deploy.jar;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.BucketCannedACL;
import software.amazon.awssdk.services.s3.model.BucketLocationConstraint;
import software.amazon.awssdk.services.s3.model.BucketVersioningStatus;

import sleeper.core.properties.instance.InstanceProperties;

import static sleeper.core.properties.instance.CommonProperty.JARS_BUCKET;
import static sleeper.core.properties.instance.CommonProperty.REGION;

/**
 * A tool to create a jars bucket that matches how it is created by the CDK. Usually only used for testing.
 */
public class JarsBucketCreator {
    public static final Logger LOGGER = LoggerFactory.getLogger(JarsBucketCreator.class);

    private final InstanceProperties instanceProperties;
    private final S3Client s3Client;

    public JarsBucketCreator(InstanceProperties instanceProperties, S3Client s3Client) {
        this.instanceProperties = instanceProperties;
        this.s3Client = s3Client;
    }

    public void create() {
        String bucketName = instanceProperties.get(JARS_BUCKET);

        LOGGER.info("Creating jars bucket: {}", bucketName);
        s3Client.createBucket(builder -> builder
                .bucket(bucketName)
                .acl(BucketCannedACL.PRIVATE)
                .createBucketConfiguration(configBuilder -> configBuilder
                        .locationConstraint(bucketLocationConstraint(instanceProperties.get(REGION)))));
        s3Client.putPublicAccessBlock(builder -> builder
                .bucket(bucketName)
                .publicAccessBlockConfiguration(configBuilder -> configBuilder
                        .blockPublicAcls(true)
                        .ignorePublicAcls(true)
                        .blockPublicPolicy(true)
                        .restrictPublicBuckets(true)));

        // We enable versioning so that the CDK is able to update the functions when the code changes in the bucket.
        // See the following:
        // https://www.define.run/posts/cdk-not-updating-lambda/
        // https://awsteele.com/blog/2020/12/24/aws-lambda-latest-is-dangerous.html
        // https://docs.aws.amazon.com/cdk/api/v1/java/software/amazon/awscdk/services/lambda/Version.html
        s3Client.putBucketVersioning(builder -> builder
                .bucket(bucketName)
                .versioningConfiguration(config -> config.status(BucketVersioningStatus.ENABLED)));
    }

    private static BucketLocationConstraint bucketLocationConstraint(String region) {
        // The us-east-1 region is returned as UNKNOWN_TO_SDK_VERSION, which incorrectly serialises as a string "null".
        BucketLocationConstraint constraint = BucketLocationConstraint.fromValue(region);
        if (constraint == BucketLocationConstraint.UNKNOWN_TO_SDK_VERSION) {
            return null;
        } else {
            return constraint;
        }
    }

}
