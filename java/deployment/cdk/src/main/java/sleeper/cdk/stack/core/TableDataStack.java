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

package sleeper.cdk.stack.core;

import software.amazon.awscdk.NestedStack;
import software.amazon.awscdk.RemovalPolicy;
import software.amazon.awscdk.services.iam.IGrantable;
import software.amazon.awscdk.services.s3.BlockPublicAccess;
import software.amazon.awscdk.services.s3.Bucket;
import software.amazon.awscdk.services.s3.BucketEncryption;
import software.amazon.awscdk.services.s3.IBucket;
import software.constructs.Construct;

import sleeper.cdk.artefacts.SleeperJarsInBucket;
import sleeper.cdk.util.Utils;
import sleeper.core.properties.instance.InstanceProperties;

import static sleeper.cdk.util.Utils.removalPolicy;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;

public class TableDataStack extends NestedStack {

    private final IBucket dataBucket;

    public TableDataStack(
            Construct scope, String id, InstanceProperties instanceProperties,
            LoggingStack loggingStack, ManagedPoliciesStack policiesStack, AutoDeleteS3ObjectsStack autoDeleteS3ObjectsStack, SleeperJarsInBucket jars) {
        super(scope, id);

        RemovalPolicy removalPolicy = removalPolicy(instanceProperties);

        String bucketName = String.join("-", "sleeper",
                Utils.cleanInstanceId(instanceProperties), "table-data");
        dataBucket = Bucket.Builder
                .create(this, "TableDataBucket")
                .bucketName(bucketName)
                .versioned(false)
                .blockPublicAccess(BlockPublicAccess.BLOCK_ALL)
                .encryption(BucketEncryption.S3_MANAGED)
                .removalPolicy(removalPolicy)
                .build();

        if (removalPolicy == RemovalPolicy.DESTROY) {
            autoDeleteS3ObjectsStack.addAutoDeleteS3Objects(this, dataBucket);
        }

        instanceProperties.set(DATA_BUCKET, dataBucket.getBucketName());

        dataBucket.grantReadWrite(policiesStack.getDirectIngestPolicyForGrants());
        dataBucket.grantRead(policiesStack.getClearInstancePolicyForGrants());
        dataBucket.grantDelete(policiesStack.getClearInstancePolicyForGrants());
        Utils.addTags(this, instanceProperties);
    }

    public IBucket getDataBucket() {
        return dataBucket;
    }

    public void grantRead(IGrantable grantee) {
        dataBucket.grantRead(grantee);
    }

    public void grantReadWrite(IGrantable grantee) {
        dataBucket.grantReadWrite(grantee);
    }

    public void grantReadDelete(IGrantable grantee) {
        dataBucket.grantRead(grantee);
        dataBucket.grantDelete(grantee);
    }
}
