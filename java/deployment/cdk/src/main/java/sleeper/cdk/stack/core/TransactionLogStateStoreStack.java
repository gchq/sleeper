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
import software.amazon.awscdk.services.dynamodb.Attribute;
import software.amazon.awscdk.services.dynamodb.AttributeType;
import software.amazon.awscdk.services.dynamodb.BillingMode;
import software.amazon.awscdk.services.dynamodb.ITable;
import software.amazon.awscdk.services.dynamodb.StreamViewType;
import software.amazon.awscdk.services.dynamodb.Table;
import software.amazon.awscdk.services.iam.IGrantable;
import software.constructs.Construct;

import sleeper.cdk.util.Utils;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.statestore.transactionlog.DynamoDBTransactionLogStateStore;
import sleeper.statestore.transactionlog.snapshots.DynamoDBTransactionLogSnapshotMetadataStore;

import static sleeper.cdk.util.Utils.removalPolicy;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.TRANSACTION_LOG_ALL_SNAPSHOTS_TABLENAME;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.TRANSACTION_LOG_FILES_TABLENAME;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.TRANSACTION_LOG_LATEST_SNAPSHOTS_TABLENAME;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.TRANSACTION_LOG_PARTITIONS_TABLENAME;
import static sleeper.core.properties.instance.CommonProperty.ID;

public class TransactionLogStateStoreStack extends NestedStack {
    private final Table partitionsLogTable;
    private final Table filesLogTable;
    private final Table latestSnapshotsTable;
    private final Table allSnapshotsTable;
    private final TableDataStack dataStack;

    public TransactionLogStateStoreStack(
            Construct scope, String id, InstanceProperties instanceProperties, TableDataStack dataStack) {
        super(scope, id);
        this.dataStack = dataStack;
        partitionsLogTable = createTransactionLogTable(instanceProperties, "PartitionTransactionLogTable", "partition-transaction-log");
        filesLogTable = createTransactionLogTable(instanceProperties, "FileTransactionLogTable", "file-transaction-log");
        latestSnapshotsTable = createLatestSnapshotsTable(instanceProperties, "TransactionLogLatestSnapshotsTable", "transaction-log-latest-snapshots");
        allSnapshotsTable = createAllSnapshotsTable(instanceProperties, "TransactionLogAllSnapshotsTable", "transaction-log-all-snapshots");
        instanceProperties.set(TRANSACTION_LOG_PARTITIONS_TABLENAME, partitionsLogTable.getTableName());
        instanceProperties.set(TRANSACTION_LOG_FILES_TABLENAME, filesLogTable.getTableName());
        instanceProperties.set(TRANSACTION_LOG_LATEST_SNAPSHOTS_TABLENAME, latestSnapshotsTable.getTableName());
        instanceProperties.set(TRANSACTION_LOG_ALL_SNAPSHOTS_TABLENAME, allSnapshotsTable.getTableName());
        Utils.addTags(this, instanceProperties);
    }

    private Table createTransactionLogTable(InstanceProperties instanceProperties, String id, String name) {
        return Table.Builder
                .create(this, id)
                .tableName(String.join("-", "sleeper", instanceProperties.get(ID), name))
                .removalPolicy(removalPolicy(instanceProperties))
                .billingMode(BillingMode.PAY_PER_REQUEST)
                .partitionKey(Attribute.builder()
                        .name(DynamoDBTransactionLogStateStore.TABLE_ID)
                        .type(AttributeType.STRING)
                        .build())
                .sortKey(Attribute.builder()
                        .name(DynamoDBTransactionLogStateStore.TRANSACTION_NUMBER)
                        .type(AttributeType.NUMBER)
                        .build())
                .stream(StreamViewType.NEW_IMAGE)
                .build();
    }

    private Table createLatestSnapshotsTable(InstanceProperties instanceProperties, String id, String name) {
        return Table.Builder
                .create(this, id)
                .tableName(String.join("-", "sleeper", instanceProperties.get(ID), name))
                .removalPolicy(removalPolicy(instanceProperties))
                .billingMode(BillingMode.PAY_PER_REQUEST)
                .partitionKey(Attribute.builder()
                        .name(DynamoDBTransactionLogSnapshotMetadataStore.TABLE_ID)
                        .type(AttributeType.STRING)
                        .build())
                .build();
    }

    private Table createAllSnapshotsTable(InstanceProperties instanceProperties, String id, String name) {
        return Table.Builder
                .create(this, id)
                .tableName(String.join("-", "sleeper", instanceProperties.get(ID), name))
                .removalPolicy(removalPolicy(instanceProperties))
                .billingMode(BillingMode.PAY_PER_REQUEST)
                .partitionKey(Attribute.builder()
                        .name(DynamoDBTransactionLogSnapshotMetadataStore.TABLE_ID_AND_SNAPSHOT_TYPE)
                        .type(AttributeType.STRING)
                        .build())
                .sortKey(Attribute.builder()
                        .name(DynamoDBTransactionLogSnapshotMetadataStore.TRANSACTION_NUMBER)
                        .type(AttributeType.NUMBER)
                        .build())
                .build();
    }

    public void grantAccess(StateStoreGrants grants, IGrantable grantee) {
        // Snapshots and large transactions are both held in the data bucket
        if (grants.canWriteAny()) {
            dataStack.grantReadWrite(grantee);
        } else if (grants.canReadAny()) {
            dataStack.grantRead(grantee);
        }

        if (grants.canReadAny()) {
            latestSnapshotsTable.grantReadData(grantee);
        }

        if (grants.canWriteFileReferencesOrUnreferencedFiles()) {
            filesLogTable.grantReadWriteData(grantee);
        } else if (grants.canReadFileReferencesOrUnreferencedFiles()) {
            filesLogTable.grantReadData(grantee);
        }

        if (grants.canWritePartitions()) {
            partitionsLogTable.grantReadWriteData(grantee);
        } else if (grants.canReadPartitions()) {
            partitionsLogTable.grantReadData(grantee);
        }
    }

    public void grantCreateSnapshots(IGrantable grantee) {
        filesLogTable.grantReadData(grantee);
        partitionsLogTable.grantReadData(grantee);
        latestSnapshotsTable.grantReadWriteData(grantee);
        allSnapshotsTable.grantReadWriteData(grantee);
        dataStack.grantReadWrite(grantee);
    }

    public void grantReadAllSnapshotsTable(IGrantable grantee) {
        allSnapshotsTable.grantReadData(grantee);
    }

    public void grantClearSnapshots(IGrantable grantee) {
        latestSnapshotsTable.grantReadWriteData(grantee);
        allSnapshotsTable.grantReadWriteData(grantee);
    }

    public void grantDeleteSnapshots(IGrantable grantee) {
        latestSnapshotsTable.grantReadData(grantee);
        allSnapshotsTable.grantReadWriteData(grantee);
        dataStack.grantReadDelete(grantee);
    }

    public void grantDeleteTransactions(IGrantable grantee) {
        filesLogTable.grantReadWriteData(grantee);
        partitionsLogTable.grantReadWriteData(grantee);
        latestSnapshotsTable.grantReadData(grantee);
        allSnapshotsTable.grantReadData(grantee);
        dataStack.grantReadDelete(grantee);
    }

    public ITable getFilesLogTable() {
        return filesLogTable;
    }
}
