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
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.ACTIVE_FILEINFO_TABLENAME;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.FILE_REFERENCE_COUNT_TABLENAME;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.PARTITION_TABLENAME;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.READY_FOR_GC_FILEINFO_TABLENAME;
import static sleeper.configuration.properties.instance.CommonProperty.DYNAMO_STATE_STORE_POINT_IN_TIME_RECOVERY;
import static sleeper.configuration.properties.instance.CommonProperty.ID;

public class DynamoDBStateStoreStack extends NestedStack {
    private final Table activeFileInfoTable;
    private final Table readyForGCFileInfoTable;
    private final Table fileReferenceCountTable;
    private final Table partitionTable;

    public DynamoDBStateStoreStack(Construct scope, String id, InstanceProperties instanceProperties,
                                   ManagedPoliciesStack policiesStack) {
        super(scope, id);
        String instanceId = instanceProperties.get(ID);
        RemovalPolicy removalPolicy = removalPolicy(instanceProperties);

        // DynamoDB table for active file information
        Attribute partitionKeyActiveFileInfoTable = Attribute.builder()
                .name(DynamoDBStateStore.TABLE_ID)
                .type(AttributeType.STRING)
                .build();
        Attribute sortKeyActiveFileInfoTable = Attribute.builder()
                .name(DynamoDBStateStore.PARTITION_ID_AND_FILENAME)
                .type(AttributeType.STRING)
                .build();

        activeFileInfoTable = Table.Builder
                .create(this, "DynamoDBActiveFileInfoTable")
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
                .name(DynamoDBStateStore.TABLE_ID)
                .type(AttributeType.STRING)
                .build();
        Attribute sortKeyReadyForGCFileInfoTable = Attribute.builder()
                .name(DynamoDBStateStore.FILE_NAME)
                .type(AttributeType.STRING)
                .build();
        readyForGCFileInfoTable = Table.Builder
                .create(this, "DynamoDBReadyForGCFileInfoTable")
                .tableName(String.join("-", "sleeper", instanceId, "gc-files"))
                .removalPolicy(removalPolicy)
                .billingMode(BillingMode.PAY_PER_REQUEST)
                .partitionKey(partitionKeyReadyForGCFileInfoTable)
                .sortKey(sortKeyReadyForGCFileInfoTable)
                .pointInTimeRecovery(instanceProperties.getBoolean(DYNAMO_STATE_STORE_POINT_IN_TIME_RECOVERY))
                .build();
        instanceProperties.set(READY_FOR_GC_FILEINFO_TABLENAME, readyForGCFileInfoTable.getTableName());

        // DynamoDB table for file reference counts
        Attribute partitionKeyFileReferenceCountTable = Attribute.builder()
                .name(DynamoDBStateStore.TABLE_ID)
                .type(AttributeType.STRING)
                .build();
        Attribute sortKeyFileReferenceCountTable = Attribute.builder()
                .name(DynamoDBStateStore.FILE_NAME)
                .type(AttributeType.STRING)
                .build();
        fileReferenceCountTable = Table.Builder
                .create(this, "DynamoDBFileReferenceCountTable")
                .tableName(String.join("-", "sleeper", instanceId, "file-ref-count"))
                .removalPolicy(removalPolicy)
                .billingMode(BillingMode.PAY_PER_REQUEST)
                .partitionKey(partitionKeyFileReferenceCountTable)
                .sortKey(sortKeyFileReferenceCountTable)
                .pointInTimeRecovery(instanceProperties.getBoolean(DYNAMO_STATE_STORE_POINT_IN_TIME_RECOVERY))
                .build();
        instanceProperties.set(FILE_REFERENCE_COUNT_TABLENAME, fileReferenceCountTable.getTableName());

        // DynamoDB table for partition information
        Attribute partitionKeyPartitionTable = Attribute.builder()
                .name(DynamoDBStateStore.TABLE_ID)
                .type(AttributeType.STRING)
                .build();
        Attribute sortKeyPartitionTable = Attribute.builder()
                .name(DynamoDBStateStore.PARTITION_ID)
                .type(AttributeType.STRING)
                .build();
        partitionTable = Table.Builder
                .create(this, "DynamoDBPartitionInfoTable")
                .tableName(String.join("-", "sleeper", instanceId, "partitions"))
                .removalPolicy(removalPolicy)
                .billingMode(BillingMode.PAY_PER_REQUEST)
                .partitionKey(partitionKeyPartitionTable)
                .sortKey(sortKeyPartitionTable)
                .pointInTimeRecovery(instanceProperties.getBoolean(DYNAMO_STATE_STORE_POINT_IN_TIME_RECOVERY))
                .build();

        instanceProperties.set(PARTITION_TABLENAME, partitionTable.getTableName());
        partitionTable.grantReadData(policiesStack.getIngestPolicy());
        activeFileInfoTable.grantReadWriteData(policiesStack.getIngestPolicy());
    }

    public void grantReadActiveFileMetadata(IGrantable grantee) {
        activeFileInfoTable.grantReadData(grantee);
    }

    public void grantReadWriteActiveFileMetadata(IGrantable grantee) {
        activeFileInfoTable.grantReadWriteData(grantee);
        fileReferenceCountTable.grantReadWriteData(grantee);
    }

    public void grantReadWriteReadyForGCFileMetadata(IGrantable grantee) {
        readyForGCFileInfoTable.grantReadWriteData(grantee);
        fileReferenceCountTable.grantReadWriteData(grantee);
    }

    public void grantReadPartitionMetadata(IGrantable grantee) {
        partitionTable.grantReadData(grantee);
    }

    public void grantReadWritePartitionMetadata(IGrantable grantee) {
        partitionTable.grantReadWriteData(grantee);
    }
}
