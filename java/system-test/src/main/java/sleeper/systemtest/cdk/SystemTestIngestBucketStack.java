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
import software.amazon.awscdk.services.s3.BlockPublicAccess;
import software.amazon.awscdk.services.s3.Bucket;
import software.amazon.awscdk.services.s3.BucketEncryption;
import software.constructs.Construct;

import sleeper.cdk.Utils;
import sleeper.configuration.properties.instance.InstanceProperties;

import java.util.Locale;

import static sleeper.configuration.properties.instance.CommonProperty.ID;
import static sleeper.configuration.properties.instance.IngestProperty.INGEST_SOURCE_BUCKET;

public class SystemTestIngestBucketStack extends NestedStack {

    public SystemTestIngestBucketStack(Construct scope, String id, InstanceProperties systemTestProperties) {
        super(scope, id);
        String systemTestIngestBucketName = String.join("-", "sleeper", systemTestProperties.get(ID),
                "system", "test", "ingest").toLowerCase(Locale.ROOT);
        systemTestProperties.set(INGEST_SOURCE_BUCKET, systemTestIngestBucketName);
        Bucket.Builder.create(this, "SystemTestIngestBucket")
                .bucketName(systemTestIngestBucketName)
                .versioned(false)
                .encryption(BucketEncryption.S3_MANAGED)
                .blockPublicAccess(BlockPublicAccess.BLOCK_ALL)
                .removalPolicy(RemovalPolicy.DESTROY)
                .autoDeleteObjects(true)
                .build();
        Utils.addStackTagIfSet(this, systemTestProperties);
    }
}
