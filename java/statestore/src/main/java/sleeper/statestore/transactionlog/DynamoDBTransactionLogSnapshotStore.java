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
package sleeper.statestore.transactionlog;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.Put;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.ReturnConsumedCapacity;
import com.amazonaws.services.dynamodbv2.model.TransactWriteItem;
import com.amazonaws.services.dynamodbv2.model.TransactWriteItemsRequest;
import com.amazonaws.services.dynamodbv2.model.TransactionCanceledException;
import com.amazonaws.services.dynamodbv2.model.Update;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TableProperty;
import sleeper.dynamodb.tools.DynamoDBRecordBuilder;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.TRANSACTION_LOG_ALL_SNAPSHOTS_TABLENAME;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.TRANSACTION_LOG_LATEST_SNAPSHOTS_TABLENAME;
import static sleeper.dynamodb.tools.DynamoDBAttributes.createNumberAttribute;
import static sleeper.dynamodb.tools.DynamoDBAttributes.createStringAttribute;
import static sleeper.dynamodb.tools.DynamoDBAttributes.getLongAttribute;
import static sleeper.dynamodb.tools.DynamoDBAttributes.getStringAttribute;
import static sleeper.dynamodb.tools.DynamoDBUtils.hasConditionalCheckFailure;
import static sleeper.dynamodb.tools.DynamoDBUtils.streamPagedItems;

public class DynamoDBTransactionLogSnapshotStore {
    private static final String DELIMETER = "|";
    public static final String TABLE_ID = DynamoDBTransactionLogStateStore.TABLE_ID;
    public static final String TABLE_ID_AND_SNAPSHOT_TYPE = "TABLE_ID_AND_SNAPSHOT_TYPE";
    private static final String PATH = "PATH";
    public static final String TRANSACTION_NUMBER = DynamoDBTransactionLogStateStore.TRANSACTION_NUMBER;
    private static final String UPDATE_TIME = "UPDATE_TIME";
    private static final String SNAPSHOT_TYPE = "SNAPSHOT_TYPE";
    private static final String FILES_TRANSACTION_NUMBER = "FILES_TRANSACTION_NUMBER";
    private static final String PARTITIONS_TRANSACTION_NUMBER = "PARTITIONS_TRANSACTION_NUMBER";
    private static final String FILES_SNAPSHOT_PATH = "FILES_SNAPSHOT_PATH";
    private static final String PARTITIONS_SNAPSHOT_PATH = "PARTITIONS_SNAPSHOT_PATH";
    private final String allSnapshotsTable;
    private final String latestSnapshotsTable;
    private final String sleeperTableId;
    private final AmazonDynamoDB dynamo;
    private final Supplier<Instant> timeSupplier;

    public DynamoDBTransactionLogSnapshotStore(InstanceProperties instanceProperties, TableProperties tableProperties, AmazonDynamoDB dynamo, Supplier<Instant> timeSupplier) {
        this.allSnapshotsTable = instanceProperties.get(TRANSACTION_LOG_ALL_SNAPSHOTS_TABLENAME);
        this.latestSnapshotsTable = instanceProperties.get(TRANSACTION_LOG_LATEST_SNAPSHOTS_TABLENAME);
        this.sleeperTableId = tableProperties.get(TableProperty.TABLE_ID);
        this.dynamo = dynamo;
        this.timeSupplier = timeSupplier;
    }

    public void saveFiles(String snapshotPath, long transactionNumber) throws DuplicateSnapshotException {
        saveSnapshot(SnapshotType.FILES, snapshotPath, transactionNumber);
    }

    public void savePartitions(String snapshotPath, long transactionNumber) throws DuplicateSnapshotException {
        saveSnapshot(SnapshotType.PARTITIONS, snapshotPath, transactionNumber);
    }

    private void saveSnapshot(SnapshotType snapshotType, String snapshotPath, long transactionNumber) throws DuplicateSnapshotException {
        Instant updateTime = timeSupplier.get();
        try {
            List<TransactWriteItem> writes = List.of(
                    new TransactWriteItem()
                            .withPut(putNewSnapshot(snapshotType, snapshotPath, transactionNumber, updateTime)),
                    new TransactWriteItem()
                            .withUpdate(updateLatestSnapshot(snapshotType, snapshotPath, transactionNumber, updateTime)));
            TransactWriteItemsRequest transactWriteItemsRequest = new TransactWriteItemsRequest()
                    .withTransactItems(writes)
                    .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL);
            dynamo.transactWriteItems(transactWriteItemsRequest);
        } catch (TransactionCanceledException e) {
            if (hasConditionalCheckFailure(e)) {
                throw new DuplicateSnapshotException(snapshotPath, e);
            }
        }
    }

    private Put putNewSnapshot(SnapshotType snapshotType, String snapshotPath, long transactionNumber, Instant updateTime) {
        return new Put()
                .withTableName(allSnapshotsTable)
                .withItem(new DynamoDBRecordBuilder()
                        .string(TABLE_ID_AND_SNAPSHOT_TYPE, tableAndType(allSnapshotsTable, snapshotType))
                        .string(TABLE_ID, sleeperTableId)
                        .string(PATH, snapshotPath)
                        .number(TRANSACTION_NUMBER, transactionNumber)
                        .number(UPDATE_TIME, updateTime.toEpochMilli())
                        .string(SNAPSHOT_TYPE, snapshotType.name())
                        .build());
    }

    private Update updateLatestSnapshot(SnapshotType snapshotType, String snapshotPath, long transactionNumber, Instant updateTime) {
        String pathKey;
        String transactionNumberKey;
        if (snapshotType == SnapshotType.FILES) {
            pathKey = FILES_SNAPSHOT_PATH;
            transactionNumberKey = FILES_TRANSACTION_NUMBER;
        } else {
            pathKey = PARTITIONS_SNAPSHOT_PATH;
            transactionNumberKey = PARTITIONS_TRANSACTION_NUMBER;
        }
        return new Update()
                .withTableName(latestSnapshotsTable)
                .withKey(Map.of(TABLE_ID, createStringAttribute(sleeperTableId)))
                .withUpdateExpression("SET " +
                        "#Path = :path, " +
                        "#TransactionNumber = :transaction_number, " +
                        "#UpdateTime = :update_time")
                .withConditionExpression("#TransactionNumber <> :transaction_number")
                .withExpressionAttributeNames(Map.of(
                        "#Path", pathKey,
                        "#TransactionNumber", transactionNumberKey,
                        "#UpdateTime", UPDATE_TIME))
                .withExpressionAttributeValues(Map.of(
                        ":path", createStringAttribute(snapshotPath),
                        ":transaction_number", createNumberAttribute(transactionNumber),
                        ":update_time", createNumberAttribute(updateTime.toEpochMilli())));
    }

    public List<TransactionLogSnapshot> getFilesSnapshots() {
        return getSnapshots(SnapshotType.FILES)
                .collect(Collectors.toList());
    }

    public List<TransactionLogSnapshot> getPartitionsSnapshots() {
        return getSnapshots(SnapshotType.PARTITIONS)
                .collect(Collectors.toList());
    }

    private Stream<TransactionLogSnapshot> getSnapshots(SnapshotType type) {
        return streamPagedItems(dynamo, new QueryRequest()
                .withTableName(allSnapshotsTable)
                .withConsistentRead(true)
                .withKeyConditionExpression("#TableIdAndType = :table_and_type")
                .withExpressionAttributeNames(Map.of("#TableIdAndType", TABLE_ID_AND_SNAPSHOT_TYPE))
                .withExpressionAttributeValues(new DynamoDBRecordBuilder()
                        .string(":table_and_type", tableAndType(allSnapshotsTable, type))
                        .build())
                .withScanIndexForward(false)
                .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL))
                .map(DynamoDBTransactionLogSnapshotStore::getSnapshotFromItem);
    }

    public Optional<TransactionLogSnapshot> getLatestFilesSnapshot() {
        return getLatestSnapshot(SnapshotType.FILES);
    }

    public Optional<TransactionLogSnapshot> getLatestPartitionsSnapshot() {
        return getLatestSnapshot(SnapshotType.PARTITIONS);
    }

    private Optional<TransactionLogSnapshot> getLatestSnapshot(SnapshotType snapshotType) {
        return streamPagedItems(dynamo, new QueryRequest()
                .withTableName(latestSnapshotsTable)
                .withConsistentRead(true)
                .withKeyConditionExpression("#TableId = :table_id")
                .withExpressionAttributeNames(Map.of("#TableId", TABLE_ID))
                .withExpressionAttributeValues(new DynamoDBRecordBuilder()
                        .string(":table_id", sleeperTableId)
                        .build())
                .withScanIndexForward(false)
                .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL))
                .map(item -> getLatestSnapshotFromItem(item, snapshotType))
                .findFirst();
    }

    private static TransactionLogSnapshot getLatestSnapshotFromItem(Map<String, AttributeValue> item, SnapshotType snapshotType) {
        String pathKey;
        String transactionNumberKey;
        if (snapshotType == SnapshotType.FILES) {
            pathKey = FILES_SNAPSHOT_PATH;
            transactionNumberKey = FILES_TRANSACTION_NUMBER;
        } else {
            pathKey = PARTITIONS_SNAPSHOT_PATH;
            transactionNumberKey = PARTITIONS_TRANSACTION_NUMBER;
        }
        return new TransactionLogSnapshot(getStringAttribute(item, pathKey), snapshotType, getLongAttribute(item, transactionNumberKey, 0));
    }

    private static String tableAndType(String table, SnapshotType type) {
        return table + DELIMETER + type.name();
    }

    private static TransactionLogSnapshot getSnapshotFromItem(Map<String, AttributeValue> item) {
        SnapshotType type = SnapshotType.valueOf(item.get(SNAPSHOT_TYPE).getS());
        return new TransactionLogSnapshot(getStringAttribute(item, PATH), type, getLongAttribute(item, TRANSACTION_NUMBER, 0));
    }
}
