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

import software.amazon.awscdk.Duration;
import software.amazon.awscdk.NestedStack;
import software.amazon.awscdk.services.dynamodb.Attribute;
import software.amazon.awscdk.services.dynamodb.AttributeType;
import software.amazon.awscdk.services.dynamodb.BillingMode;
import software.amazon.awscdk.services.dynamodb.Table;
import software.amazon.awscdk.services.iam.IGrantable;
import software.amazon.awscdk.services.iam.ManagedPolicy;
import software.amazon.awscdk.services.lambda.IFunction;
import software.amazon.awscdk.services.lambda.MetricType;
import software.amazon.awscdk.services.lambda.MetricsConfig;
import software.amazon.awscdk.services.lambda.StartingPosition;
import software.amazon.awscdk.services.lambda.eventsources.DynamoEventSource;
import software.amazon.awscdk.services.s3.Bucket;
import software.amazon.awscdk.services.s3.IBucket;
import software.constructs.Construct;

import sleeper.cdk.jars.BuiltJars;
import sleeper.cdk.jars.LambdaCode;
import sleeper.cdk.stack.core.LoggingStack.LogGroupRef;
import sleeper.cdk.util.Utils;
import sleeper.core.deploy.LambdaHandler;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.statestore.transactionlog.DynamoDBTransactionLogStateStore;
import sleeper.statestore.transactionlog.snapshots.DynamoDBTransactionLogSnapshotMetadataStore;

import java.util.List;

import static sleeper.cdk.util.Utils.removalPolicy;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.TRANSACTION_LOG_ALL_SNAPSHOTS_TABLENAME;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.TRANSACTION_LOG_FILES_TABLENAME;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.TRANSACTION_LOG_LATEST_SNAPSHOTS_TABLENAME;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.TRANSACTION_LOG_PARTITIONS_TABLENAME;
import static sleeper.core.properties.instance.CommonProperty.ID;
import static sleeper.core.properties.instance.CommonProperty.JARS_BUCKET;
import static sleeper.core.properties.instance.TableStateProperty.TRACKER_FROM_LOG_LAMBDA_CONCURRENCY_RESERVED;
import static sleeper.core.properties.instance.TableStateProperty.TRACKER_FROM_LOG_LAMBDA_MEMORY;
import static sleeper.core.properties.instance.TableStateProperty.TRACKER_FROM_LOG_LAMBDA_TIMEOUT_SECS;

public class TransactionLogStateStoreStack extends NestedStack {
    private final Table partitionsLogTable;
    private final Table filesLogTable;
    private final Table latestSnapshotsTable;
    private final Table allSnapshotsTable;
    private final TableDataStack dataStack;

    public TransactionLogStateStoreStack(
            Construct scope, String id, InstanceProperties instanceProperties,
            BuiltJars jars, LoggingStack loggingStack, TableDataStack dataStack) {
        super(scope, id);
        this.dataStack = dataStack;
        IBucket jarsBucket = Bucket.fromBucketName(this, "JarsBucket", instanceProperties.get(JARS_BUCKET));
        LambdaCode lambdaCode = jars.lambdaCode(jarsBucket);
        partitionsLogTable = createTransactionLogTable(instanceProperties, "PartitionTransactionLogTable", "partition-transaction-log");
        filesLogTable = createTransactionLogTable(instanceProperties, "FileTransactionLogTable", "file-transaction-log");
        latestSnapshotsTable = createLatestSnapshotsTable(instanceProperties, "TransactionLogLatestSnapshotsTable", "transaction-log-latest-snapshots");
        allSnapshotsTable = createAllSnapshotsTable(instanceProperties, "TransactionLogAllSnapshotsTable", "transaction-log-all-snapshots");
        createFunctionToFollowTransactionLog(instanceProperties, lambdaCode, loggingStack);
        instanceProperties.set(TRANSACTION_LOG_PARTITIONS_TABLENAME, partitionsLogTable.getTableName());
        instanceProperties.set(TRANSACTION_LOG_FILES_TABLENAME, filesLogTable.getTableName());
        instanceProperties.set(TRANSACTION_LOG_LATEST_SNAPSHOTS_TABLENAME, latestSnapshotsTable.getTableName());
        instanceProperties.set(TRANSACTION_LOG_ALL_SNAPSHOTS_TABLENAME, allSnapshotsTable.getTableName());
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

    private void createFunctionToFollowTransactionLog(InstanceProperties instanceProperties, LambdaCode lambdaCode, LoggingStack loggingStack) {
        String instanceId = Utils.cleanInstanceId(instanceProperties);
        String functionName = String.join("-", "sleeper", instanceId, "state-transaction-tracker");
        IFunction lambda = lambdaCode.buildFunction(this, LambdaHandler.STATESTORE_TRACKER, "TransactionLogTracker", builder -> builder
                .functionName(functionName)
                .description("Updates trackers by following the transaction log")
                .environment(Utils.createDefaultEnvironment(instanceProperties))
                .reservedConcurrentExecutions(instanceProperties.getIntOrNull(TRACKER_FROM_LOG_LAMBDA_CONCURRENCY_RESERVED))
                .memorySize(instanceProperties.getInt(TRACKER_FROM_LOG_LAMBDA_MEMORY))
                .timeout(Duration.seconds(instanceProperties.getInt(TRACKER_FROM_LOG_LAMBDA_TIMEOUT_SECS)))
                .logGroup(loggingStack.getLogGroup(LogGroupRef.STATESTORE_TRACKER)));

        lambda.addEventSource(DynamoEventSource.Builder.create(filesLogTable)
                .startingPosition(StartingPosition.LATEST)
                .metricsConfig(MetricsConfig.builder()
                        .metrics(List.of(MetricType.EVENT_COUNT))
                        .build())
                .build());
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

        if (grants.canWriteActiveOrReadyForGCFiles()) {
            filesLogTable.grantReadWriteData(grantee);
        } else if (grants.canReadActiveOrReadyForGCFiles()) {
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

    public void grantReadAllSnapshotsTable(ManagedPolicy grantee) {
        allSnapshotsTable.grantReadData(grantee);
    }

    public void grantClearSnapshots(ManagedPolicy grantee) {
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
}
