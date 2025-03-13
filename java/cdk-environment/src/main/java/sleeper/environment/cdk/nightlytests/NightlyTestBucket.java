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
package sleeper.environment.cdk.nightlytests;

import software.amazon.awscdk.RemovalPolicy;
import software.amazon.awscdk.services.s3.BlockPublicAccess;
import software.amazon.awscdk.services.s3.Bucket;
import software.amazon.awscdk.services.s3.BucketEncryption;
import software.amazon.awscdk.services.s3.IBucket;
import software.constructs.Construct;

import sleeper.environment.cdk.config.AppContext;
import sleeper.environment.cdk.config.AppParameters;
import sleeper.environment.cdk.config.OptionalStringParameter;

import static sleeper.environment.cdk.config.AppParameters.INSTANCE_ID;

public class NightlyTestBucket {
    public static final OptionalStringParameter NIGHTLY_TEST_BUCKET = AppParameters.NIGHTLY_TEST_BUCKET;

    private final IBucket bucket;

    public NightlyTestBucket(Construct scope) {
        AppContext context = AppContext.of(scope);

        bucket = context.get(NIGHTLY_TEST_BUCKET)
                .map(bucketName -> Bucket.fromBucketName(scope, "TestBucket", bucketName))
                .orElseGet(() -> Bucket.Builder.create(scope, "TestBucket")
                        .bucketName("sleeper-" + context.get(INSTANCE_ID) + "-tests")
                        .versioned(false)
                        .encryption(BucketEncryption.S3_MANAGED)
                        .blockPublicAccess(BlockPublicAccess.BLOCK_ALL)
                        .removalPolicy(RemovalPolicy.RETAIN_ON_UPDATE_OR_DELETE)
                        .build());
    }

    public IBucket getBucket() {
        return bucket;
    }

}
