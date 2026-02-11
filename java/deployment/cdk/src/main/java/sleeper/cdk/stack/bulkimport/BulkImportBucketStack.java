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
package sleeper.cdk.stack.bulkimport;

import software.amazon.awscdk.NestedStack;
import software.amazon.awscdk.RemovalPolicy;
import software.amazon.awscdk.services.s3.BlockPublicAccess;
import software.amazon.awscdk.services.s3.Bucket;
import software.amazon.awscdk.services.s3.BucketEncryption;
import software.amazon.awscdk.services.s3.IBucket;
import software.constructs.Construct;

import sleeper.cdk.artefacts.SleeperJarVersionIdsCache;
import sleeper.cdk.stack.SleeperCoreStacks;
import sleeper.cdk.util.Utils;
import sleeper.core.properties.instance.InstanceProperties;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.BULK_IMPORT_BUCKET;

public class BulkImportBucketStack extends NestedStack {
    private final IBucket importBucket;

    public BulkImportBucketStack(Construct scope, String id, InstanceProperties instanceProperties, SleeperCoreStacks coreStacks, SleeperJarVersionIdsCache jars) {
        super(scope, id);
        String bucketName = String.join("-", "sleeper",
                Utils.cleanInstanceId(instanceProperties), "bulk-import");
        importBucket = Bucket.Builder.create(this, "BulkImportBucket")
                .bucketName(bucketName)
                .blockPublicAccess(BlockPublicAccess.BLOCK_ALL)
                .versioned(false)
                .removalPolicy(RemovalPolicy.DESTROY)
                .encryption(BucketEncryption.S3_MANAGED)
                .build();
        importBucket.grantWrite(coreStacks.getIngestByQueuePolicyForGrants());
        instanceProperties.set(BULK_IMPORT_BUCKET, importBucket.getBucketName());
        coreStacks.addAutoDeleteS3Objects(this, importBucket);
    }

    public IBucket getImportBucket() {
        return importBucket;
    }
}
