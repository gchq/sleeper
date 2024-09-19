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

package sleeper.systemtest.cdk;

import software.amazon.awscdk.NestedStack;
import software.amazon.awscdk.RemovalPolicy;
import software.amazon.awscdk.Tags;
import software.amazon.awscdk.services.s3.BlockPublicAccess;
import software.amazon.awscdk.services.s3.Bucket;
import software.amazon.awscdk.services.s3.BucketEncryption;
import software.amazon.awscdk.services.s3.IBucket;
import software.constructs.Construct;

import sleeper.cdk.Utils;
import sleeper.systemtest.configuration.SystemTestProperties;
import sleeper.systemtest.configuration.SystemTestStandaloneProperties;

import java.util.List;
import java.util.Locale;

import static sleeper.configuration.properties.instance.CommonProperty.ID;
import static sleeper.configuration.properties.instance.IngestProperty.INGEST_SOURCE_BUCKET;
import static sleeper.systemtest.configuration.SystemTestProperty.SYSTEM_TEST_BUCKET_NAME;
import static sleeper.systemtest.configuration.SystemTestProperty.SYSTEM_TEST_ID;

public class SystemTestBucketStack extends NestedStack {

    private final IBucket bucket;

    public SystemTestBucketStack(Construct scope, String id, SystemTestStandaloneProperties properties) {
        super(scope, id);
        String bucketName = SystemTestStandaloneProperties.buildSystemTestBucketName(properties.get(SYSTEM_TEST_ID));
        properties.set(SYSTEM_TEST_BUCKET_NAME, bucketName);
        bucket = createBucket("SystemTestBucket", bucketName);
        Tags.of(this).add("DeploymentStack", id);
    }

    public SystemTestBucketStack(Construct scope, String id, SystemTestProperties properties) {
        super(scope, id);
        String bucketName = String.join("-", "sleeper", properties.get(ID),
                "system", "test", "ingest").toLowerCase(Locale.ROOT);
        properties.set(SYSTEM_TEST_BUCKET_NAME, bucketName);
        properties.addToListIfMissing(INGEST_SOURCE_BUCKET, List.of(bucketName));
        bucket = createBucket("SystemTestIngestBucket", bucketName);
        Utils.addStackTagIfSet(this, properties);
    }

    private IBucket createBucket(String id, String bucketName) {
        return Bucket.Builder.create(this, id)
                .bucketName(bucketName)
                .versioned(false)
                .encryption(BucketEncryption.S3_MANAGED)
                .blockPublicAccess(BlockPublicAccess.BLOCK_ALL)
                .removalPolicy(RemovalPolicy.DESTROY)
                .autoDeleteObjects(true)
                .build();
    }

    public IBucket getBucket() {
        return bucket;
    }
}
