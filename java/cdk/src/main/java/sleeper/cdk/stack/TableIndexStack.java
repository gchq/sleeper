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
import software.amazon.awscdk.services.dynamodb.Attribute;
import software.amazon.awscdk.services.dynamodb.AttributeType;
import software.amazon.awscdk.services.dynamodb.BillingMode;
import software.amazon.awscdk.services.dynamodb.ITable;
import software.amazon.awscdk.services.dynamodb.Table;
import software.amazon.awscdk.services.iam.IGrantable;
import software.constructs.Construct;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.table.index.DynamoDBTableIndex;

import static sleeper.cdk.Utils.removalPolicy;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.TABLE_ID_INDEX_DYNAMO_TABLENAME;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.TABLE_NAME_INDEX_DYNAMO_TABLENAME;
import static sleeper.configuration.properties.instance.CommonProperty.ID;
import static sleeper.configuration.properties.instance.CommonProperty.TABLE_INDEX_DYNAMO_POINT_IN_TIME_RECOVERY;

public class TableIndexStack extends NestedStack {

    private final ITable indexByNameDynamoTable;
    private final ITable indexByIdDynamoTable;

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
                .pointInTimeRecovery(instanceProperties.getBoolean(TABLE_INDEX_DYNAMO_POINT_IN_TIME_RECOVERY))
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
                .pointInTimeRecovery(instanceProperties.getBoolean(TABLE_INDEX_DYNAMO_POINT_IN_TIME_RECOVERY))
                .build();
        instanceProperties.set(TABLE_ID_INDEX_DYNAMO_TABLENAME, indexByIdDynamoTable.getTableName());

        indexByNameDynamoTable.grantReadData(policiesStack.getIngestPolicy());
        indexByIdDynamoTable.grantReadData(policiesStack.getIngestPolicy());
    }

    public void grantRead(IGrantable grantee) {
        indexByNameDynamoTable.grantReadData(grantee);
        indexByIdDynamoTable.grantReadData(grantee);
    }
}
