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

package sleeper.cdk.stack;

import software.amazon.awscdk.NestedStack;
import software.amazon.awscdk.RemovalPolicy;
import software.amazon.awscdk.services.dynamodb.Attribute;
import software.amazon.awscdk.services.dynamodb.AttributeType;
import software.amazon.awscdk.services.dynamodb.BillingMode;
import software.amazon.awscdk.services.dynamodb.Table;
import software.amazon.awscdk.services.iam.IGrantable;
import software.constructs.Construct;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.statestore.dynamodb.DynamoDBStateStore;

import static sleeper.cdk.util.Utils.removalPolicy;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.ACTIVE_FILES_TABLELENAME;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.FILE_REFERENCE_COUNT_TABLENAME;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.PARTITION_TABLENAME;
import static sleeper.core.properties.instance.CommonProperty.DYNAMO_STATE_STORE_POINT_IN_TIME_RECOVERY;
import static sleeper.core.properties.instance.CommonProperty.ID;

public class DynamoDBStateStoreStack extends NestedStack {
    private final Table activeFilesTable;
    private final Table fileReferenceCountTable;
    private final Table partitionTable;

    public DynamoDBStateStoreStack(
            Construct scope, String id, InstanceProperties instanceProperties) {
        super(scope, id);
        String instanceId = instanceProperties.get(ID);
        RemovalPolicy removalPolicy = removalPolicy(instanceProperties);

        // DynamoDB table for active file information
        Attribute partitionKeyActiveFileReferenceTable = Attribute.builder()
                .name(DynamoDBStateStore.TABLE_ID)
                .type(AttributeType.STRING)
                .build();
        Attribute sortKeyActiveFileReferenceTable = Attribute.builder()
                .name(DynamoDBStateStore.PARTITION_ID_AND_FILENAME)
                .type(AttributeType.STRING)
                .build();

        activeFilesTable = Table.Builder
                .create(this, "DynamoDBActiveFilesTable")
                .tableName(String.join("-", "sleeper", instanceId, "active-files"))
                .removalPolicy(removalPolicy)
                .billingMode(BillingMode.PAY_PER_REQUEST)
                .partitionKey(partitionKeyActiveFileReferenceTable)
                .sortKey(sortKeyActiveFileReferenceTable)
                .pointInTimeRecovery(instanceProperties.getBoolean(DYNAMO_STATE_STORE_POINT_IN_TIME_RECOVERY))
                .build();
        instanceProperties.set(ACTIVE_FILES_TABLELENAME, activeFilesTable.getTableName());

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
    }

    public void grantAccess(StateStoreGrants grants, IGrantable grantee) {
        if (grants.canWriteActiveFiles()) {
            activeFilesTable.grantReadWriteData(grantee);
        } else if (grants.canReadActiveFiles()) {
            activeFilesTable.grantReadData(grantee);
        }
        if (grants.canWriteActiveOrReadyForGCFiles()) {
            fileReferenceCountTable.grantReadWriteData(grantee);
        } else if (grants.canReadActiveOrReadyForGCFiles()) {
            fileReferenceCountTable.grantReadData(grantee);
        }
        if (grants.canWritePartitions()) {
            partitionTable.grantReadWriteData(grantee);
        } else if (grants.canReadPartitions()) {
            partitionTable.grantReadData(grantee);
        }
    }
}
