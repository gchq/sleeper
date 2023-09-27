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
import software.amazon.awscdk.services.dynamodb.Table;
import software.amazon.awscdk.services.iam.IGrantable;
import software.constructs.Construct;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.statestore.dynamodb.DynamoDBStateStore;

import static sleeper.cdk.Utils.removalPolicy;
import static sleeper.configuration.properties.instance.CommonProperty.DYNAMO_STATE_STORE_POINT_IN_TIME_RECOVERY;
import static sleeper.configuration.properties.instance.CommonProperty.ID;
import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.ACTIVE_FILEINFO_TABLENAME;
import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.PARTITION_TABLENAME;
import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.READY_FOR_GC_FILEINFO_TABLENAME;

public class DynamoDBStateStoreStack extends NestedStack implements StateStoreStack {
    private final Table activeFileInfoTable;
    private final Table readyForGCFileInfoTable;
    private final Table partitionTable;

    public DynamoDBStateStoreStack(Construct scope, String id, InstanceProperties instanceProperties) {
        super(scope, id);
        String instanceId = instanceProperties.get(ID);
        RemovalPolicy removalPolicy = removalPolicy(instanceProperties);

        // DynamoDB table for active file information
        Attribute partitionKeyActiveFileInfoTable = Attribute.builder()
                .name(DynamoDBStateStore.TABLE_NAME)
                .type(AttributeType.STRING)
                .build();
        Attribute sortKeyActiveFileInfoTable = Attribute.builder()
                .name(DynamoDBStateStore.FILE_NAME)
                .type(AttributeType.STRING)
                .build();

        this.activeFileInfoTable = Table.Builder
                .create(scope, "DynamoDBActiveFileInfoTable")
                .tableName(String.join("-", "sleeper", instanceId, "active-files"))
                .removalPolicy(removalPolicy)
                .billingMode(BillingMode.PAY_PER_REQUEST)
                .partitionKey(partitionKeyActiveFileInfoTable)
                .sortKey(sortKeyActiveFileInfoTable)
                .pointInTimeRecovery(instanceProperties.getBoolean(DYNAMO_STATE_STORE_POINT_IN_TIME_RECOVERY))
                .build();
        instanceProperties.set(ACTIVE_FILEINFO_TABLENAME, activeFileInfoTable.getTableName());

        // DynamoDB table for ready for GC file information
        Attribute partitionKeyReadyForGCFileInfoTable = Attribute.builder()
                .name(DynamoDBStateStore.TABLE_NAME)
                .type(AttributeType.STRING)
                .build();
        Attribute sortKeyReadyForGCFileInfoTable = Attribute.builder()
                .name(DynamoDBStateStore.FILE_NAME)
                .type(AttributeType.STRING)
                .build();
        this.readyForGCFileInfoTable = Table.Builder
                .create(scope, "DynamoDBReadyForGCFileInfoTable")
                .tableName(String.join("-", "sleeper", instanceId, "gc-files"))
                .removalPolicy(removalPolicy)
                .billingMode(BillingMode.PAY_PER_REQUEST)
                .partitionKey(partitionKeyReadyForGCFileInfoTable)
                .sortKey(sortKeyReadyForGCFileInfoTable)
                .pointInTimeRecovery(instanceProperties.getBoolean(DYNAMO_STATE_STORE_POINT_IN_TIME_RECOVERY))
                .build();

        instanceProperties.set(READY_FOR_GC_FILEINFO_TABLENAME, readyForGCFileInfoTable.getTableName());

        // DynamoDB table for partition information
        Attribute partitionKeyPartitionTable = Attribute.builder()
                .name(DynamoDBStateStore.PARTITION_ID)
                .type(AttributeType.STRING)
                .build();
        this.partitionTable = Table.Builder
                .create(scope, "DynamoDBPartitionInfoTable")
                .tableName(String.join("-", "sleeper", instanceId, "partitions"))
                .removalPolicy(removalPolicy)
                .billingMode(BillingMode.PAY_PER_REQUEST)
                .partitionKey(partitionKeyPartitionTable)
                .pointInTimeRecovery(instanceProperties.getBoolean(DYNAMO_STATE_STORE_POINT_IN_TIME_RECOVERY))
                .build();

        instanceProperties.set(PARTITION_TABLENAME, partitionTable.getTableName());
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
