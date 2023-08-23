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

package sleeper.systemtest.cdk;

import software.amazon.awscdk.NestedStack;
import software.amazon.awscdk.RemovalPolicy;
import software.amazon.awscdk.Tags;
import software.amazon.awscdk.services.s3.BlockPublicAccess;
import software.amazon.awscdk.services.s3.Bucket;
import software.amazon.awscdk.services.s3.BucketEncryption;
import software.amazon.awscdk.services.s3.IBucket;
import software.constructs.Construct;

import sleeper.systemtest.configuration.SystemTestProperty;
import sleeper.systemtest.configuration.SystemTestPropertySetter;
import sleeper.systemtest.configuration.SystemTestStandaloneProperties;

import java.util.Locale;

import static sleeper.systemtest.configuration.SystemTestProperty.SYSTEM_TEST_BUCKET_NAME;

public class SystemTestBucketStack extends NestedStack {

    private final IBucket bucket;

    public SystemTestBucketStack(Construct scope, String id,
                                 SystemTestStandaloneProperties properties) {
        this(scope, id, properties.get(SystemTestProperty.SYSTEM_TEST_ID), properties);
    }

    public SystemTestBucketStack(Construct scope, String id,
                                 String deploymentId, SystemTestPropertySetter propertySetter) {
        super(scope, id);
        String bucketName = String.join("-", "sleeper", deploymentId,
                "system", "test").toLowerCase(Locale.ROOT);
        propertySetter.set(SYSTEM_TEST_BUCKET_NAME, bucketName);
        bucket = Bucket.Builder.create(this, "SystemTestBucket")
                .bucketName(bucketName)
                .versioned(false)
                .encryption(BucketEncryption.S3_MANAGED)
                .blockPublicAccess(BlockPublicAccess.BLOCK_ALL)
                .removalPolicy(RemovalPolicy.DESTROY)
                .autoDeleteObjects(true)
                .build();
        Tags.of(this).add("DeploymentStack", getNode().getId());
    }

    public IBucket getBucket() {
        return bucket;
    }
}
