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
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import com.amazonaws.services.dynamodbv2.model.ReturnConsumedCapacity;
import com.amazonaws.services.dynamodbv2.model.TransactWriteItem;
import com.amazonaws.services.dynamodbv2.model.TransactWriteItemsRequest;
import com.amazonaws.services.dynamodbv2.model.TransactionCanceledException;
import com.amazonaws.services.dynamodbv2.model.Update;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TableProperty;
import sleeper.dynamodb.tools.DynamoDBRecordBuilder;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.TRANSACTION_LOG_ALL_SNAPSHOTS_TABLENAME;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.TRANSACTION_LOG_LATEST_SNAPSHOTS_TABLENAME;
import static sleeper.configuration.properties.table.TableProperty.TRANSACTION_LOG_SNAPSHOT_EXPIRY_IN_DAYS;
import static sleeper.dynamodb.tools.DynamoDBAttributes.createNumberAttribute;
import static sleeper.dynamodb.tools.DynamoDBAttributes.createStringAttribute;
import static sleeper.dynamodb.tools.DynamoDBAttributes.getLongAttribute;
import static sleeper.dynamodb.tools.DynamoDBAttributes.getStringAttribute;
import static sleeper.dynamodb.tools.DynamoDBUtils.hasConditionalCheckFailure;
import static sleeper.dynamodb.tools.DynamoDBUtils.streamPagedItems;

public class DynamoDBTransactionLogSnapshotStore {
    private static final String DELIMITER = "|";
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
    private final Duration expiryInDays;
    private final Supplier<Instant> timeSupplier;

    public DynamoDBTransactionLogSnapshotStore(InstanceProperties instanceProperties, TableProperties tableProperties, AmazonDynamoDB dynamo) {
        this(instanceProperties, tableProperties, dynamo, Instant::now);
    }

    public DynamoDBTransactionLogSnapshotStore(InstanceProperties instanceProperties, TableProperties tableProperties, AmazonDynamoDB dynamo, Supplier<Instant> timeSupplier) {
        this.allSnapshotsTable = instanceProperties.get(TRANSACTION_LOG_ALL_SNAPSHOTS_TABLENAME);
        this.latestSnapshotsTable = instanceProperties.get(TRANSACTION_LOG_LATEST_SNAPSHOTS_TABLENAME);
        this.sleeperTableId = tableProperties.get(TableProperty.TABLE_ID);
        this.dynamo = dynamo;
        this.expiryInDays = Duration.ofDays(tableProperties.getLong(TRANSACTION_LOG_SNAPSHOT_EXPIRY_IN_DAYS));
        this.timeSupplier = timeSupplier;
    }

    public void saveSnapshot(TransactionLogSnapshot snapshot) throws DuplicateSnapshotException {
        Instant updateTime = timeSupplier.get();
        try {
            List<TransactWriteItem> writes = List.of(
                    new TransactWriteItem()
                            .withPut(putNewSnapshot(snapshot, updateTime)),
                    new TransactWriteItem()
                            .withUpdate(updateLatestSnapshot(snapshot, updateTime)));
            TransactWriteItemsRequest transactWriteItemsRequest = new TransactWriteItemsRequest()
                    .withTransactItems(writes)
                    .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL);
            dynamo.transactWriteItems(transactWriteItemsRequest);
        } catch (TransactionCanceledException e) {
            if (hasConditionalCheckFailure(e)) {
                throw new DuplicateSnapshotException(snapshot.getPath(), e);
            }
        }
    }

    private Put putNewSnapshot(TransactionLogSnapshot snapshot, Instant updateTime) {
        return new Put()
                .withTableName(allSnapshotsTable)
                .withItem(new DynamoDBRecordBuilder()
                        .string(TABLE_ID_AND_SNAPSHOT_TYPE, tableAndType(sleeperTableId, snapshot.getType()))
                        .string(TABLE_ID, sleeperTableId)
                        .string(PATH, snapshot.getPath())
                        .number(TRANSACTION_NUMBER, snapshot.getTransactionNumber())
                        .number(UPDATE_TIME, updateTime.toEpochMilli())
                        .string(SNAPSHOT_TYPE, snapshot.getType().name())
                        .build());
    }

    private Update updateLatestSnapshot(TransactionLogSnapshot snapshot, Instant updateTime) {
        String pathKey;
        String transactionNumberKey;
        if (snapshot.getType() == SnapshotType.FILES) {
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
                        ":path", createStringAttribute(snapshot.getPath()),
                        ":transaction_number", createNumberAttribute(snapshot.getTransactionNumber()),
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
                        .string(":table_and_type", tableAndType(sleeperTableId, type))
                        .build())
                .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL))
                .map(DynamoDBTransactionLogSnapshotStore::getSnapshotFromItem);
    }

    public LatestSnapshots getLatestSnapshots() {
        QueryResult result = dynamo.query(new QueryRequest()
                .withTableName(latestSnapshotsTable)
                .withKeyConditionExpression("#TableId = :table_id")
                .withExpressionAttributeNames(Map.of("#TableId", TABLE_ID))
                .withExpressionAttributeValues(new DynamoDBRecordBuilder()
                        .string(":table_id", sleeperTableId)
                        .build()));
        if (result.getCount() > 0) {
            return getLatestSnapshotsFromItem(result.getItems().get(0));
        } else {
            return new LatestSnapshots(null, null);
        }
    }

    public Stream<TransactionLogSnapshot> getOldestSnapshots() {
        long expiryDate = timeSupplier.get().toEpochMilli() - expiryInDays.toMillis();
        LatestSnapshots latestSnapshots = getLatestSnapshots();
        return Stream.concat(
                getSnapshotsBefore(latestSnapshots.getFilesTransactionNumber(), SnapshotType.FILES, expiryDate),
                getSnapshotsBefore(latestSnapshots.getPartitionsTransactionNumber(), SnapshotType.PARTITIONS, expiryDate));
    }

    private Stream<TransactionLogSnapshot> getSnapshotsBefore(long latestSnapshotNumber, SnapshotType type, long time) {
        return streamPagedItems(dynamo, new QueryRequest()
                .withTableName(allSnapshotsTable)
                .withKeyConditionExpression("#TableIdAndType = :table_id_and_type AND #TransactionNumber < :latest_transaction")
                .withFilterExpression("#UpdateTime < :expiry_time")
                .withExpressionAttributeNames(Map.of(
                        "#TableIdAndType", TABLE_ID_AND_SNAPSHOT_TYPE,
                        "#UpdateTime", UPDATE_TIME,
                        "#TransactionNumber", TRANSACTION_NUMBER))
                .withExpressionAttributeValues(new DynamoDBRecordBuilder()
                        .string(":table_id_and_type", tableAndType(sleeperTableId, type))
                        .number(":expiry_time", time)
                        .number(":latest_transaction", latestSnapshotNumber)
                        .build()))
                .map(DynamoDBTransactionLogSnapshotStore::getSnapshotFromItem);
    }

    public void deleteSnapshot(TransactionLogSnapshot snapshot) {
        dynamo.deleteItem(allSnapshotsTable, getKeyFromSnapshot(snapshot));
    }

    private Map<String, AttributeValue> getKeyFromSnapshot(TransactionLogSnapshot snapshot) {
        return Map.of(TABLE_ID_AND_SNAPSHOT_TYPE, new AttributeValue().withS(tableAndType(sleeperTableId, snapshot.getType())),
                TRANSACTION_NUMBER, new AttributeValue().withN(snapshot.getTransactionNumber() + ""));
    }

    private static LatestSnapshots getLatestSnapshotsFromItem(Map<String, AttributeValue> item) {
        TransactionLogSnapshot filesSnapshot = null;
        String filesSnapshotPath = getStringAttribute(item, FILES_SNAPSHOT_PATH);
        if (filesSnapshotPath != null) {
            filesSnapshot = new TransactionLogSnapshot(filesSnapshotPath, SnapshotType.FILES,
                    getLongAttribute(item, FILES_TRANSACTION_NUMBER, 0));
        }
        TransactionLogSnapshot partitionsSnapshot = null;
        String partitionsSnapshotPath = getStringAttribute(item, PARTITIONS_SNAPSHOT_PATH);
        if (partitionsSnapshotPath != null) {
            partitionsSnapshot = new TransactionLogSnapshot(partitionsSnapshotPath, SnapshotType.PARTITIONS,
                    getLongAttribute(item, PARTITIONS_TRANSACTION_NUMBER, 0));
        }
        return new LatestSnapshots(filesSnapshot, partitionsSnapshot);
    }

    private static String tableAndType(String table, SnapshotType type) {
        return table + DELIMITER + type.name();
    }

    private static TransactionLogSnapshot getSnapshotFromItem(Map<String, AttributeValue> item) {
        SnapshotType type = SnapshotType.valueOf(item.get(SNAPSHOT_TYPE).getS());
        return new TransactionLogSnapshot(getStringAttribute(item, PATH), type, getLongAttribute(item, TRANSACTION_NUMBER, 0));
    }

    public static class LatestSnapshots {
        private final TransactionLogSnapshot filesSnapshot;
        private final TransactionLogSnapshot partitionsSnapshot;

        public LatestSnapshots(TransactionLogSnapshot filesSnapshot, TransactionLogSnapshot partitionsSnapshot) {
            this.filesSnapshot = filesSnapshot;
            this.partitionsSnapshot = partitionsSnapshot;
        }

        public TransactionLogSnapshot getFilesSnapshot() {
            return filesSnapshot;
        }

        public TransactionLogSnapshot getPartitionsSnapshot() {
            return partitionsSnapshot;
        }

        public long getFilesTransactionNumber() {
            return filesSnapshot != null ? filesSnapshot.getTransactionNumber() : 0L;
        }

        public long getPartitionsTransactionNumber() {
            return partitionsSnapshot != null ? partitionsSnapshot.getTransactionNumber() : 0L;
        }

        @Override
        public int hashCode() {
            return Objects.hash(filesSnapshot, partitionsSnapshot);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!(obj instanceof LatestSnapshots)) {
                return false;
            }
            LatestSnapshots other = (LatestSnapshots) obj;
            return Objects.equals(filesSnapshot, other.filesSnapshot) && Objects.equals(partitionsSnapshot, other.partitionsSnapshot);
        }

        @Override
        public String toString() {
            return "LatestSnapshots{filesSnapshot=" + filesSnapshot + ", partitionsSnapshot=" + partitionsSnapshot + "}";
        }

    }

}
