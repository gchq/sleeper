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
package sleeper.cdk.stack.bulkimport;

import software.amazon.awscdk.NestedStack;
import software.amazon.awscdk.RemovalPolicy;
import software.amazon.awscdk.services.s3.BlockPublicAccess;
import software.amazon.awscdk.services.s3.Bucket;
import software.amazon.awscdk.services.s3.BucketEncryption;
import software.amazon.awscdk.services.s3.IBucket;
import software.constructs.Construct;

import sleeper.configuration.properties.instance.InstanceProperties;

import java.util.Locale;

import static sleeper.configuration.properties.instance.CommonProperty.ID;
import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.BULK_IMPORT_BUCKET;

public class BulkImportBucketStack extends NestedStack {
    private final IBucket importBucket;

    public BulkImportBucketStack(Construct scope,
                                 String id,
                                 InstanceProperties instanceProperties) {
        super(scope, id);
        importBucket = Bucket.Builder.create(this, "BulkImportBucket")
                .bucketName(String.join("-", "sleeper", instanceProperties.get(ID),
                        "bulk-import").toLowerCase(Locale.ROOT))
                .blockPublicAccess(BlockPublicAccess.BLOCK_ALL)
                .versioned(false)
                .autoDeleteObjects(true)
                .removalPolicy(RemovalPolicy.DESTROY)
                .encryption(BucketEncryption.S3_MANAGED)
                .build();
        instanceProperties.set(BULK_IMPORT_BUCKET, importBucket.getBucketName());
    }

    public IBucket getImportBucket() {
        return importBucket;
    }
}
