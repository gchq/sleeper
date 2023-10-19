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

package sleeper.cdk.stack;

import software.amazon.awscdk.NestedStack;
import software.amazon.awscdk.RemovalPolicy;
import software.amazon.awscdk.services.iam.IGrantable;
import software.amazon.awscdk.services.s3.BlockPublicAccess;
import software.amazon.awscdk.services.s3.Bucket;
import software.amazon.awscdk.services.s3.BucketEncryption;
import software.amazon.awscdk.services.s3.IBucket;
import software.constructs.Construct;

import sleeper.configuration.properties.instance.InstanceProperties;

import java.util.Locale;

import static sleeper.cdk.Utils.removalPolicy;
import static sleeper.cdk.stack.IngestStack.addIngestSourceRoleReferences;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.configuration.properties.instance.CommonProperty.ID;

public class TableDataStack extends NestedStack {

    private final IBucket dataBucket;

    public TableDataStack(Construct scope, String id, InstanceProperties instanceProperties) {
        super(scope, id);

        String instanceId = instanceProperties.get(ID);
        RemovalPolicy removalPolicy = removalPolicy(instanceProperties);

        dataBucket = Bucket.Builder
                .create(this, "TableDataBucket")
                .bucketName(String.join("-", "sleeper", instanceId, "table-data").toLowerCase(Locale.ROOT))
                .versioned(false)
                .blockPublicAccess(BlockPublicAccess.BLOCK_ALL)
                .encryption(BucketEncryption.S3_MANAGED)
                .removalPolicy(removalPolicy).autoDeleteObjects(removalPolicy == RemovalPolicy.DESTROY)
                .build();

        instanceProperties.set(DATA_BUCKET, dataBucket.getBucketName());

        addIngestSourceRoleReferences(this, "DataWriterForIngest", instanceProperties)
                .forEach(dataBucket::grantReadWrite);
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
}
