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

package sleeper.cdk.stack.core;

import software.amazon.awscdk.NestedStack;
import software.amazon.awscdk.RemovalPolicy;
import software.amazon.awscdk.services.dynamodb.Attribute;
import software.amazon.awscdk.services.dynamodb.AttributeType;
import software.amazon.awscdk.services.dynamodb.BillingMode;
import software.amazon.awscdk.services.dynamodb.Table;
import software.amazon.awscdk.services.iam.IGrantable;
import software.constructs.Construct;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.statestore.s3.S3StateStore;

import java.util.Locale;

import static sleeper.cdk.util.Utils.removalPolicy;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.REVISION_TABLENAME;
import static sleeper.core.properties.instance.CommonProperty.ID;
import static sleeper.core.properties.instance.CommonProperty.S3_STATE_STORE_DYNAMO_POINT_IN_TIME_RECOVERY;

public class S3StateStoreStack extends NestedStack {
    private final Table revisionTable;
    private final TableDataStack dataStack;

    public S3StateStoreStack(
            Construct scope, String id, InstanceProperties instanceProperties, TableDataStack dataStack) {
        super(scope, id);
        this.dataStack = dataStack;
        RemovalPolicy removalPolicy = removalPolicy(instanceProperties);

        // Dynamo table to store latest revision version
        Attribute partitionKeyRevisionTable = Attribute.builder()
                .name(S3StateStore.TABLE_ID)
                .type(AttributeType.STRING)
                .build();
        Attribute sortKeyRevisionTable = Attribute.builder()
                .name(S3StateStore.REVISION_ID_KEY)
                .type(AttributeType.STRING)
                .build();

        this.revisionTable = Table.Builder
                .create(this, "DynamoDBRevisionTable")
                .tableName(String.join("-", "sleeper", instanceProperties.get(ID), "table", "revisions").toLowerCase(Locale.ROOT))
                .removalPolicy(removalPolicy)
                .billingMode(BillingMode.PAY_PER_REQUEST)
                .partitionKey(partitionKeyRevisionTable)
                .sortKey(sortKeyRevisionTable)
                .pointInTimeRecovery(instanceProperties.getBoolean(S3_STATE_STORE_DYNAMO_POINT_IN_TIME_RECOVERY))
                .build();
        instanceProperties.set(REVISION_TABLENAME, this.revisionTable.getTableName());
    }

    public void grantAccess(StateStoreGrants grants, IGrantable grantee) {
        if (grants.canWriteAny()) {
            revisionTable.grantReadWriteData(grantee);
            dataStack.getDataBucket().grantReadWrite(grantee); // TODO Only needs access to keys starting with 'table-name/statestore'
        } else if (grants.canReadAny()) {
            revisionTable.grantReadData(grantee);
            dataStack.getDataBucket().grantRead(grantee); // TODO Only needs access to keys starting with 'table-name/statestore'
        }
    }
}
