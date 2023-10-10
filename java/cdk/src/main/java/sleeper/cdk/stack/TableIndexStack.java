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
import software.constructs.Construct;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.table.index.dynamodb.DynamoDBTableIndex;

import static sleeper.cdk.Utils.removalPolicy;
import static sleeper.configuration.properties.instance.CommonProperty.DYNAMO_TABLE_INDEX_POINT_IN_TIME_RECOVERY;
import static sleeper.configuration.properties.instance.CommonProperty.ID;
import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.TABLE_ID_INDEX_DYNAMO_TABLENAME;
import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.TABLE_NAME_INDEX_DYNAMO_TABLENAME;

public class TableIndexStack extends NestedStack {

    private final ITable nameIndexTable;
    private final ITable idIndexTable;

    public TableIndexStack(Construct scope, String id, InstanceProperties instanceProperties) {
        super(scope, id);
        String instanceId = instanceProperties.get(ID);
        RemovalPolicy removalPolicy = removalPolicy(instanceProperties);

        nameIndexTable = Table.Builder
                .create(this, "DynamoDBTableNameIndexTable")
                .tableName(String.join("-", "sleeper", instanceId, "table-name-index"))
                .removalPolicy(removalPolicy)
                .billingMode(BillingMode.PAY_PER_REQUEST)
                .partitionKey(Attribute.builder()
                        .name(DynamoDBTableIndex.TABLE_NAME_FIELD)
                        .type(AttributeType.STRING)
                        .build())
                .pointInTimeRecovery(instanceProperties.getBoolean(DYNAMO_TABLE_INDEX_POINT_IN_TIME_RECOVERY))
                .build();
        instanceProperties.set(TABLE_NAME_INDEX_DYNAMO_TABLENAME, nameIndexTable.getTableName());

        idIndexTable = Table.Builder
                .create(this, "DynamoDBTableIdIndexTable")
                .tableName(String.join("-", "sleeper", instanceId, "table-id-index"))
                .removalPolicy(removalPolicy)
                .billingMode(BillingMode.PAY_PER_REQUEST)
                .partitionKey(Attribute.builder()
                        .name(DynamoDBTableIndex.TABLE_ID_FIELD)
                        .type(AttributeType.STRING)
                        .build())
                .pointInTimeRecovery(instanceProperties.getBoolean(DYNAMO_TABLE_INDEX_POINT_IN_TIME_RECOVERY))
                .build();
        instanceProperties.set(TABLE_ID_INDEX_DYNAMO_TABLENAME, idIndexTable.getTableName());
    }

    public ITable getNameIndexTable() {
        return nameIndexTable;
    }

    public ITable getIdIndexTable() {
        return idIndexTable;
    }
}
