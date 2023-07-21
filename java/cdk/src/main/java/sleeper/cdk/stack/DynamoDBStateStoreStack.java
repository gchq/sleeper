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

import software.amazon.awscdk.RemovalPolicy;
import software.amazon.awscdk.customresources.Provider;
import software.amazon.awscdk.services.dynamodb.Attribute;
import software.amazon.awscdk.services.dynamodb.AttributeType;
import software.amazon.awscdk.services.dynamodb.BillingMode;
import software.amazon.awscdk.services.dynamodb.Table;
import software.amazon.awscdk.services.iam.IGrantable;
import software.constructs.Construct;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.statestore.dynamodb.DynamoDBStateStore;

import static sleeper.cdk.Utils.removalPolicy;
import static sleeper.configuration.properties.instance.CommonProperty.ID;
import static sleeper.configuration.properties.table.TableProperty.DYNAMO_STATE_STORE_POINT_IN_TIME_RECOVERY;
import static sleeper.configuration.properties.table.TableProperty.FILE_IN_PARTITION_TABLENAME;
import static sleeper.configuration.properties.table.TableProperty.FILE_LIFECYCLE_TABLENAME;
import static sleeper.configuration.properties.table.TableProperty.PARTITION_TABLENAME;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;

public class DynamoDBStateStoreStack implements StateStoreStack {
    private final Table fileInPartitionTable;
    private final Table fileLifecycleTable;
    private final Table partitionTable;

    public DynamoDBStateStoreStack(Construct scope,
                                   Provider tablesProvider,
                                   InstanceProperties instanceProperties,
                                   TableProperties tableProperties) {
        String instanceId = instanceProperties.get(ID);
        String tableName = tableProperties.get(TABLE_NAME);

        RemovalPolicy removalPolicy = removalPolicy(instanceProperties);

        // DynamoDB table for file in partition information
        Attribute partitionKeyFileInPartitionTable = Attribute.builder()
                .name(DynamoDBStateStore.FILE_NAME)
                .type(AttributeType.STRING)
                .build();
        Attribute sortKeyFileInPartitionTable = Attribute.builder()
                .name(DynamoDBStateStore.PARTITION_ID)
                .type(AttributeType.STRING)
                .build();

        this.fileInPartitionTable = Table.Builder
                .create(scope, tableName + "DynamoDBFileInPartitionTable")
                .tableName(String.join("-", "sleeper", instanceId, "table", tableName, "file-in-partition"))
                .removalPolicy(removalPolicy)
                .billingMode(BillingMode.PAY_PER_REQUEST)
                .partitionKey(partitionKeyFileInPartitionTable)
                .sortKey(sortKeyFileInPartitionTable)
                .pointInTimeRecovery(tableProperties.getBoolean(DYNAMO_STATE_STORE_POINT_IN_TIME_RECOVERY))
                .build();
        tableProperties.set(FILE_IN_PARTITION_TABLENAME, fileInPartitionTable.getTableName());

        fileInPartitionTable.grantReadData(tablesProvider.getOnEventHandler());

        // DynamoDB table for file lifecyle information
        Attribute partitionKeyFileLifecycleTable = Attribute.builder()
                .name(DynamoDBStateStore.FILE_NAME)
                .type(AttributeType.STRING)
                .build();
        this.fileLifecycleTable = Table.Builder
                .create(scope, tableName + "DynamoDBFileLifecycleTable")
                .tableName(String.join("-", "sleeper", instanceId, "table", tableName, "file-lifecycle"))
                .removalPolicy(removalPolicy)
                .billingMode(BillingMode.PAY_PER_REQUEST)
                .partitionKey(partitionKeyFileLifecycleTable)
                .pointInTimeRecovery(tableProperties.getBoolean(DYNAMO_STATE_STORE_POINT_IN_TIME_RECOVERY))
                .build();

        tableProperties.set(FILE_LIFECYCLE_TABLENAME, fileLifecycleTable.getTableName());

        // DynamoDB table for partition information
        Attribute partitionKeyPartitionTable = Attribute.builder()
                .name(DynamoDBStateStore.PARTITION_ID)
                .type(AttributeType.STRING)
                .build();
        this.partitionTable = Table.Builder
                .create(scope, tableName + "DynamoDBPartitionInfoTable")
                .tableName(String.join("-", "sleeper", instanceId, "table", tableName, "partitions"))
                .removalPolicy(removalPolicy)
                .billingMode(BillingMode.PAY_PER_REQUEST)
                .partitionKey(partitionKeyPartitionTable)
                .pointInTimeRecovery(tableProperties.getBoolean(DYNAMO_STATE_STORE_POINT_IN_TIME_RECOVERY))
                .build();

        tableProperties.set(PARTITION_TABLENAME, partitionTable.getTableName());
        partitionTable.grantReadWriteData(tablesProvider.getOnEventHandler());
    }

    @Override
    public void grantReadFileInPartitionMetadata(IGrantable grantee) {
        fileInPartitionTable.grantReadData(grantee);
    }

    @Override
    public void grantReadWriteFileInPartitionMetadata(IGrantable grantee) {
        fileInPartitionTable.grantReadWriteData(grantee);
    }

    @Override
    public void grantReadWriteFileLifecycleMetadata(IGrantable grantee) {
        fileLifecycleTable.grantReadWriteData(grantee);
    }

    @Override
    public void grantReadFileLifecycleMetadata(IGrantable grantee) {
        fileLifecycleTable.grantReadData(grantee);
    }

    @Override
    public void grantReadPartitionMetadata(IGrantable grantee) {
        partitionTable.grantReadData(grantee);
    }

    @Override
    public void grantReadWritePartitionMetadata(IGrantable grantee) {
        partitionTable.grantReadWriteData(grantee);
    }
}
