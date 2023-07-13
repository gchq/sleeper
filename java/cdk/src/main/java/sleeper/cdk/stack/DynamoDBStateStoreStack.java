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

import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.statestore.dynamodb.DynamoDBStateStore;

import static sleeper.cdk.Utils.removalPolicy;
import static sleeper.configuration.properties.CommonProperty.ID;
import static sleeper.configuration.properties.table.TableProperty.ACTIVE_FILEINFO_TABLENAME;
import static sleeper.configuration.properties.table.TableProperty.DYNAMO_STATE_STORE_POINT_IN_TIME_RECOVERY;
import static sleeper.configuration.properties.table.TableProperty.PARTITION_TABLENAME;
import static sleeper.configuration.properties.table.TableProperty.READY_FOR_GC_FILEINFO_TABLENAME;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;

public class DynamoDBStateStoreStack implements StateStoreStack {
    private final Table activeFileInfoTable;
    private final Table readyForGCFileInfoTable;
    private final Table partitionTable;

    public DynamoDBStateStoreStack(Construct scope,
                                   Provider tablesProvider,
                                   InstanceProperties instanceProperties,
                                   TableProperties tableProperties) {
        String instanceId = instanceProperties.get(ID);
        String tableName = tableProperties.get(TABLE_NAME);

        RemovalPolicy removalPolicy = removalPolicy(instanceProperties);

        // DynamoDB table for active file information
        Attribute partitionKeyActiveFileInfoTable = Attribute.builder()
                .name(DynamoDBStateStore.FILE_NAME)
                .type(AttributeType.STRING)
                .build();

        this.activeFileInfoTable = Table.Builder
                .create(scope, tableName + "DynamoDBActiveFileInfoTable")
                .tableName(String.join("-", "sleeper", instanceId, "table", tableName, "active-files"))
                .removalPolicy(removalPolicy)
                .billingMode(BillingMode.PAY_PER_REQUEST)
                .partitionKey(partitionKeyActiveFileInfoTable)
                .pointInTimeRecovery(tableProperties.getBoolean(DYNAMO_STATE_STORE_POINT_IN_TIME_RECOVERY))
                .build();
        tableProperties.set(ACTIVE_FILEINFO_TABLENAME, activeFileInfoTable.getTableName());

        activeFileInfoTable.grantReadData(tablesProvider.getOnEventHandler());

        // DynamoDB table for ready for GC file information
        Attribute partitionKeyReadyForGCFileInfoTable = Attribute.builder()
                .name(DynamoDBStateStore.FILE_NAME)
                .type(AttributeType.STRING)
                .build();
        this.readyForGCFileInfoTable = Table.Builder
                .create(scope, tableName + "DynamoDBReadyForGCFileInfoTable")
                .tableName(String.join("-", "sleeper", instanceId, "table", tableName, "gc-files"))
                .removalPolicy(removalPolicy)
                .billingMode(BillingMode.PAY_PER_REQUEST)
                .partitionKey(partitionKeyReadyForGCFileInfoTable)
                .pointInTimeRecovery(tableProperties.getBoolean(DYNAMO_STATE_STORE_POINT_IN_TIME_RECOVERY))
                .build();

        tableProperties.set(READY_FOR_GC_FILEINFO_TABLENAME, readyForGCFileInfoTable.getTableName());

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
    public void grantReadActiveFileMetadata(IGrantable grantee) {
        activeFileInfoTable.grantReadData(grantee);
    }

    @Override
    public void grantReadWriteActiveFileMetadata(IGrantable grantee) {
        activeFileInfoTable.grantReadWriteData(grantee);
    }

    @Override
    public void grantReadWriteReadyForGCFileMetadata(IGrantable grantee) {
        readyForGCFileInfoTable.grantReadWriteData(grantee);
    }

    @Override
    public void grantWriteReadyForGCFileMetadata(IGrantable grantee) {
        readyForGCFileInfoTable.grantWriteData(grantee);
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
