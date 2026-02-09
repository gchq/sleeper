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
import software.amazon.awscdk.services.dynamodb.Attribute;
import software.amazon.awscdk.services.dynamodb.AttributeType;
import software.amazon.awscdk.services.dynamodb.BillingMode;
import software.amazon.awscdk.services.dynamodb.ITable;
import software.amazon.awscdk.services.dynamodb.PointInTimeRecoverySpecification;
import software.amazon.awscdk.services.dynamodb.Table;
import software.amazon.awscdk.services.iam.IGrantable;
import software.constructs.Construct;

import sleeper.cdk.util.Utils;
import sleeper.configuration.table.index.DynamoDBTableIndex;
import sleeper.core.properties.instance.InstanceProperties;

import static sleeper.cdk.util.Utils.removalPolicy;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.TABLE_ID_INDEX_DYNAMO_TABLENAME;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.TABLE_NAME_INDEX_DYNAMO_TABLENAME;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.TABLE_ONLINE_INDEX_DYNAMO_TABLENAME;
import static sleeper.core.properties.instance.CommonProperty.ID;
import static sleeper.core.properties.instance.TableStateProperty.TABLE_INDEX_DYNAMO_POINT_IN_TIME_RECOVERY;

public final class TableIndexStack extends NestedStack {

    private final ITable indexByNameDynamoTable;
    private final ITable indexByIdDynamoTable;
    private final ITable indexByOnlineDynamoTable;

    public TableIndexStack(Construct scope, String id, InstanceProperties instanceProperties,
            ManagedPoliciesStack policiesStack) {
        super(scope, id);
        String instanceId = instanceProperties.get(ID);
        RemovalPolicy removalPolicy = removalPolicy(instanceProperties);

        indexByNameDynamoTable = Table.Builder
                .create(this, "IndexByNameDynamoDBTable")
                .tableName(String.join("-", "sleeper", instanceId, "table-index-by-name"))
                .removalPolicy(removalPolicy)
                .billingMode(BillingMode.PAY_PER_REQUEST)
                .partitionKey(Attribute.builder()
                        .name(DynamoDBTableIndex.TABLE_NAME_FIELD)
                        .type(AttributeType.STRING)
                        .build())
                .pointInTimeRecoverySpecification(PointInTimeRecoverySpecification.builder()
                        .pointInTimeRecoveryEnabled(instanceProperties.getBoolean(TABLE_INDEX_DYNAMO_POINT_IN_TIME_RECOVERY))
                        .build())
                .build();
        instanceProperties.set(TABLE_NAME_INDEX_DYNAMO_TABLENAME, indexByNameDynamoTable.getTableName());

        indexByIdDynamoTable = Table.Builder
                .create(this, "IndexByIdDynamoDBTable")
                .tableName(String.join("-", "sleeper", instanceId, "table-index-by-id"))
                .removalPolicy(removalPolicy)
                .billingMode(BillingMode.PAY_PER_REQUEST)
                .partitionKey(Attribute.builder()
                        .name(DynamoDBTableIndex.TABLE_ID_FIELD)
                        .type(AttributeType.STRING)
                        .build())
                .pointInTimeRecoverySpecification(PointInTimeRecoverySpecification.builder()
                        .pointInTimeRecoveryEnabled(instanceProperties.getBoolean(TABLE_INDEX_DYNAMO_POINT_IN_TIME_RECOVERY))
                        .build())
                .build();
        instanceProperties.set(TABLE_ID_INDEX_DYNAMO_TABLENAME, indexByIdDynamoTable.getTableName());

        indexByOnlineDynamoTable = Table.Builder
                .create(this, "IndexByOnlineDynamoDBTable")
                .tableName(String.join("-", "sleeper", instanceId, "table-index-by-online"))
                .removalPolicy(removalPolicy)
                .billingMode(BillingMode.PAY_PER_REQUEST)
                .partitionKey(Attribute.builder()
                        .name(DynamoDBTableIndex.TABLE_ONLINE_FIELD)
                        .type(AttributeType.STRING)
                        .build())
                .sortKey(Attribute.builder()
                        .name(DynamoDBTableIndex.TABLE_NAME_FIELD)
                        .type(AttributeType.STRING)
                        .build())
                .pointInTimeRecoverySpecification(PointInTimeRecoverySpecification.builder()
                        .pointInTimeRecoveryEnabled(instanceProperties.getBoolean(TABLE_INDEX_DYNAMO_POINT_IN_TIME_RECOVERY))
                        .build())
                .build();
        instanceProperties.set(TABLE_ONLINE_INDEX_DYNAMO_TABLENAME, indexByOnlineDynamoTable.getTableName());

        grantRead(policiesStack.getDirectIngestPolicyForGrants());
        grantRead(policiesStack.getIngestByQueuePolicyForGrants());
        grantReadWrite(policiesStack.getEditTablesPolicyForGrants());
        grantReadWrite(policiesStack.getClearInstancePolicyForGrants());
        Utils.addTags(this, instanceProperties);
    }

    public void grantRead(IGrantable grantee) {
        indexByNameDynamoTable.grantReadData(grantee);
        indexByIdDynamoTable.grantReadData(grantee);
        indexByOnlineDynamoTable.grantReadData(grantee);
    }

    private void grantReadWrite(IGrantable grantee) {
        indexByNameDynamoTable.grantReadWriteData(grantee);
        indexByIdDynamoTable.grantReadWriteData(grantee);
        indexByOnlineDynamoTable.grantReadWriteData(grantee);
    }
}
