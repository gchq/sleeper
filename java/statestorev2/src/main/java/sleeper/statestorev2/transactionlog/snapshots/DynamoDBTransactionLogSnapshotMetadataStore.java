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
package sleeper.statestorev2.transactionlog.snapshots;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.Put;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;
import software.amazon.awssdk.services.dynamodb.model.ReturnConsumedCapacity;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItem;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItemsRequest;
import software.amazon.awssdk.services.dynamodb.model.TransactionCanceledException;
import software.amazon.awssdk.services.dynamodb.model.Update;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.statestore.transactionlog.log.TransactionLogRange;
import sleeper.core.table.TableStatus;
import sleeper.dynamodb.toolsv2.DynamoDBRecordBuilder;
import sleeper.statestorev2.transactionlog.DuplicateSnapshotException;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.TRANSACTION_LOG_ALL_SNAPSHOTS_TABLENAME;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.TRANSACTION_LOG_LATEST_SNAPSHOTS_TABLENAME;
import static sleeper.dynamodb.toolsv2.DynamoDBAttributes.createNumberAttribute;
import static sleeper.dynamodb.toolsv2.DynamoDBAttributes.createStringAttribute;
import static sleeper.dynamodb.toolsv2.DynamoDBAttributes.getLongAttribute;
import static sleeper.dynamodb.toolsv2.DynamoDBAttributes.getStringAttribute;
import static sleeper.dynamodb.toolsv2.DynamoDBUtils.hasConditionalCheckFailure;
import static sleeper.dynamodb.toolsv2.DynamoDBUtils.streamPagedItems;
import static sleeper.dynamodb.toolsv2.DynamoDBUtils.streamPagedResults;

/**
 * Stores an index of snapshots derived from a transaction log. The index is backed by DynamoDB.
 */
public class DynamoDBTransactionLogSnapshotMetadataStore {
    public static final Logger LOGGER = LoggerFactory.getLogger(DynamoDBTransactionLogSnapshotMetadataStore.class);

    private static final String DELIMITER = "|";
    public static final String TABLE_ID = "TABLE_ID";
    public static final String TABLE_ID_AND_SNAPSHOT_TYPE = "TABLE_ID_AND_SNAPSHOT_TYPE";
    private static final String PATH = "PATH";
    public static final String TRANSACTION_NUMBER = "TRANSACTION_NUMBER";
    private static final String UPDATE_TIME = "UPDATE_TIME";
    private static final String SNAPSHOT_TYPE = "SNAPSHOT_TYPE";
    private static final String FILES_TRANSACTION_NUMBER = "FILES_TRANSACTION_NUMBER";
    private static final String PARTITIONS_TRANSACTION_NUMBER = "PARTITIONS_TRANSACTION_NUMBER";
    private static final String FILES_SNAPSHOT_PATH = "FILES_SNAPSHOT_PATH";
    private static final String PARTITIONS_SNAPSHOT_PATH = "PARTITIONS_SNAPSHOT_PATH";
    private final String allSnapshotsTable;
    private final String latestSnapshotsTable;
    private final TableStatus sleeperTable;
    private final String sleeperTableId;
    private final DynamoDbClient dynamo;
    private final Supplier<Instant> timeSupplier;

    public DynamoDBTransactionLogSnapshotMetadataStore(InstanceProperties instanceProperties, TableProperties tableProperties, DynamoDbClient dynamo) {
        this(instanceProperties, tableProperties, dynamo, Instant::now);
    }

    public DynamoDBTransactionLogSnapshotMetadataStore(InstanceProperties instanceProperties, TableProperties tableProperties, DynamoDbClient dynamo, Supplier<Instant> timeSupplier) {
        this.allSnapshotsTable = instanceProperties.get(TRANSACTION_LOG_ALL_SNAPSHOTS_TABLENAME);
        this.latestSnapshotsTable = instanceProperties.get(TRANSACTION_LOG_LATEST_SNAPSHOTS_TABLENAME);
        this.sleeperTable = tableProperties.getStatus();
        this.sleeperTableId = sleeperTable.getTableUniqueId();
        this.dynamo = dynamo;
        this.timeSupplier = timeSupplier;
    }

    /**
     * Saves a new snapshot to the index.
     *
     * @param  snapshot                   the metadata of the snapshot to be indexed
     * @throws DuplicateSnapshotException if a snapshot already exists for the given transaction number
     */
    public void saveSnapshot(TransactionLogSnapshotMetadata snapshot) throws DuplicateSnapshotException {
        Instant updateTime = timeSupplier.get();
        try {
            List<TransactWriteItem> writes = List.of(
                    TransactWriteItem.builder()
                            .put(putNewSnapshot(snapshot, updateTime)).build(),
                    TransactWriteItem.builder()
                            .update(updateLatestSnapshot(snapshot, updateTime)).build());
            TransactWriteItemsRequest transactWriteItemsRequest = TransactWriteItemsRequest.builder()
                    .transactItems(writes)
                    .returnConsumedCapacity(ReturnConsumedCapacity.TOTAL).build();
            dynamo.transactWriteItems(transactWriteItemsRequest);
        } catch (TransactionCanceledException e) {
            if (hasConditionalCheckFailure(e)) {
                throw new DuplicateSnapshotException(snapshot.getPath(), e);
            }
        }
    }

    private Put putNewSnapshot(TransactionLogSnapshotMetadata snapshot, Instant updateTime) {
        return Put.builder()
                .tableName(allSnapshotsTable)
                .item(new DynamoDBRecordBuilder()
                        .string(TABLE_ID_AND_SNAPSHOT_TYPE, tableAndType(sleeperTableId, snapshot.getType()))
                        .string(TABLE_ID, sleeperTableId)
                        .string(PATH, snapshot.getPath())
                        .number(TRANSACTION_NUMBER, snapshot.getTransactionNumber())
                        .number(UPDATE_TIME, updateTime.toEpochMilli())
                        .string(SNAPSHOT_TYPE, snapshot.getType().name())
                        .build())
                .build();
    }

    private Update updateLatestSnapshot(TransactionLogSnapshotMetadata snapshot, Instant updateTime) {
        String pathKey;
        String transactionNumberKey;
        if (snapshot.getType() == SnapshotType.FILES) {
            pathKey = FILES_SNAPSHOT_PATH;
            transactionNumberKey = FILES_TRANSACTION_NUMBER;
        } else {
            pathKey = PARTITIONS_SNAPSHOT_PATH;
            transactionNumberKey = PARTITIONS_TRANSACTION_NUMBER;
        }
        return Update.builder()
                .tableName(latestSnapshotsTable)
                .key(Map.of(TABLE_ID, createStringAttribute(sleeperTableId)))
                .updateExpression("SET " +
                        "#Path = :path, " +
                        "#TransactionNumber = :transaction_number, " +
                        "#UpdateTime = :update_time")
                .conditionExpression("#TransactionNumber <> :transaction_number")
                .expressionAttributeNames(Map.of(
                        "#Path", pathKey,
                        "#TransactionNumber", transactionNumberKey,
                        "#UpdateTime", UPDATE_TIME))
                .expressionAttributeValues(Map.of(
                        ":path", createStringAttribute(snapshot.getPath()),
                        ":transaction_number", createNumberAttribute(snapshot.getTransactionNumber()),
                        ":update_time", createNumberAttribute(updateTime.toEpochMilli())))
                .build();
    }

    public List<TransactionLogSnapshotMetadata> getFilesSnapshots() {
        return getSnapshots(SnapshotType.FILES)
                .collect(Collectors.toList());
    }

    public List<TransactionLogSnapshotMetadata> getPartitionsSnapshots() {
        return getSnapshots(SnapshotType.PARTITIONS)
                .collect(Collectors.toList());
    }

    private Stream<TransactionLogSnapshotMetadata> getSnapshots(SnapshotType type) {
        return streamPagedItems(dynamo, QueryRequest.builder()
                .tableName(allSnapshotsTable)
                .consistentRead(true)
                .keyConditionExpression("#TableIdAndType = :table_and_type")
                .expressionAttributeNames(Map.of("#TableIdAndType", TABLE_ID_AND_SNAPSHOT_TYPE))
                .expressionAttributeValues(new DynamoDBRecordBuilder()
                        .string(":table_and_type", tableAndType(sleeperTableId, type))
                        .build())
                .returnConsumedCapacity(ReturnConsumedCapacity.TOTAL).build())
                .map(DynamoDBTransactionLogSnapshotMetadataStore::getSnapshotFromItem);
    }

    /**
     * Retrieves metadata on the latest snapshots in the index for the given Sleeper table.
     *
     * @return metadata describing the latest snapshots for the Sleeper table
     */
    public LatestSnapshots getLatestSnapshots() {
        QueryResponse response = dynamo.query(QueryRequest.builder()
                .tableName(latestSnapshotsTable)
                .keyConditionExpression("#TableId = :table_id")
                .expressionAttributeNames(Map.of("#TableId", TABLE_ID))
                .expressionAttributeValues(new DynamoDBRecordBuilder()
                        .string(":table_id", sleeperTableId)
                        .build())
                .build());
        if (response.count() > 0) {
            return getLatestSnapshotsFromItem(response.items().get(0));
        } else {
            return LatestSnapshots.empty();
        }
    }

    /**
     * Retrieves the latest snapshot of a given type that was made against a transaction number in the given range.
     *
     * @param  type  the snapshot type
     * @param  range the range of transactions
     * @return       the snapshot metadata, pointing to the latest snapshot file, if any
     */
    public Optional<TransactionLogSnapshotMetadata> getLatestSnapshotInRange(SnapshotType type, TransactionLogRange range) {
        if (range.isMaxTransactionBounded()) {
            QueryResponse response = dynamo.query(QueryRequest.builder()
                    .tableName(allSnapshotsTable)
                    .keyConditionExpression("#TableIdAndType = :table_id_and_type AND #Number BETWEEN :startInclusive AND :endInclusive")
                    .expressionAttributeNames(Map.of("#TableIdAndType", TABLE_ID_AND_SNAPSHOT_TYPE, "#Number", TRANSACTION_NUMBER))
                    .expressionAttributeValues(new DynamoDBRecordBuilder()
                            .string(":table_id_and_type", tableAndType(sleeperTableId, type))
                            .number(":startInclusive", range.startInclusive())
                            .number(":endInclusive", range.endExclusive() - 1)
                            .build())
                    .scanIndexForward(false)
                    .build());
            return response.items().stream().findFirst().map(item -> getSnapshotFromItem(item));
        } else {
            return getLatestSnapshots().getSnapshot(type)
                    .filter(metadata -> metadata.getTransactionNumber() >= range.startInclusive());
        }
    }

    private static LatestSnapshots getLatestSnapshotsFromItem(Map<String, AttributeValue> item) {
        TransactionLogSnapshotMetadata filesSnapshot = null;
        String filesSnapshotPath = getStringAttribute(item, FILES_SNAPSHOT_PATH);
        if (filesSnapshotPath != null) {
            filesSnapshot = new TransactionLogSnapshotMetadata(filesSnapshotPath, SnapshotType.FILES,
                    getLongAttribute(item, FILES_TRANSACTION_NUMBER, 0));
        }
        TransactionLogSnapshotMetadata partitionsSnapshot = null;
        String partitionsSnapshotPath = getStringAttribute(item, PARTITIONS_SNAPSHOT_PATH);
        if (partitionsSnapshotPath != null) {
            partitionsSnapshot = new TransactionLogSnapshotMetadata(partitionsSnapshotPath, SnapshotType.PARTITIONS,
                    getLongAttribute(item, PARTITIONS_TRANSACTION_NUMBER, 0));
        }
        return new LatestSnapshots(filesSnapshot, partitionsSnapshot);
    }

    private static String tableAndType(String table, SnapshotType type) {
        return table + DELIMITER + type.name();
    }

    private static TransactionLogSnapshotMetadata getSnapshotFromItem(Map<String, AttributeValue> item) {
        SnapshotType type = SnapshotType.valueOf(item.get(SNAPSHOT_TYPE).s());
        return new TransactionLogSnapshotMetadata(getStringAttribute(item, PATH), type, getLongAttribute(item, TRANSACTION_NUMBER, 0));
    }

    /**
     * Retrieves the latest snapshots older than a time. Note that this excludes latest snapshots.
     *
     * @param  time the time used to decide which snapshots to retrieve
     * @return      the latest snapshots that were last updated before the provided time
     */
    public LatestSnapshots getLatestSnapshotsBefore(Instant time) {
        return new LatestSnapshots(
                getLatestSnapshotBefore(SnapshotType.FILES, time).orElse(null),
                getLatestSnapshotBefore(SnapshotType.PARTITIONS, time).orElse(null));
    }

    /**
     * Retrieves metadata of snapshots older than an expiry date, excluding the latest snapshots.
     *
     * @param  expiryDate the time used to decide which snapshots to retrieve
     * @return            a stream of snapshots that were last updated before the provided time
     */
    public Stream<TransactionLogSnapshotMetadata> getExpiredSnapshots(Instant expiryDate) {
        LatestSnapshots latestSnapshots = getLatestSnapshots();
        long latestFilesTransactionNumber = latestSnapshots.getFilesSnapshot()
                .map(TransactionLogSnapshotMetadata::getTransactionNumber)
                .orElse(0L);
        long latestPartitionsTransactionNumber = latestSnapshots.getPartitionsSnapshot()
                .map(TransactionLogSnapshotMetadata::getTransactionNumber)
                .orElse(0L);
        return Stream.concat(
                getExpiredSnapshotsExcludingLatest(latestFilesTransactionNumber, SnapshotType.FILES, expiryDate),
                getExpiredSnapshotsExcludingLatest(latestPartitionsTransactionNumber, SnapshotType.PARTITIONS, expiryDate));
    }

    /**
     * Deletes all snapshot metadata for this Sleeper table. Used when deleting a Sleeper table.
     */
    public void deleteAllSnapshots() {
        deleteAllSnapshots(SnapshotType.FILES);
        deleteAllSnapshots(SnapshotType.PARTITIONS);
        dynamo.deleteItem(
                DeleteItemRequest.builder().tableName(latestSnapshotsTable)
                        .expressionAttributeValues(new DynamoDBRecordBuilder()
                                .string(TABLE_ID, sleeperTableId)
                                .build())
                        .build());
    }

    private void deleteAllSnapshots(SnapshotType type) {
        streamPagedResults(dynamo, QueryRequest.builder()
                .tableName(allSnapshotsTable)
                .keyConditionExpression("#TableIdAndType = :table_and_type")
                .expressionAttributeNames(Map.of("#TableIdAndType", TABLE_ID_AND_SNAPSHOT_TYPE))
                .expressionAttributeValues(new DynamoDBRecordBuilder()
                        .string(":table_and_type", tableAndType(sleeperTableId, type))
                        .build())
                .build())
                .flatMap(response -> response.items().stream())
                .parallel()
                .map(item -> Map.of(
                        TABLE_ID_AND_SNAPSHOT_TYPE, item.get(TABLE_ID_AND_SNAPSHOT_TYPE),
                        TRANSACTION_NUMBER, item.get(TRANSACTION_NUMBER)))
                .forEach(key -> dynamo.deleteItem(DeleteItemRequest.builder()
                        .tableName(allSnapshotsTable)
                        .key(key)
                        .build()));
    }

    private Stream<TransactionLogSnapshotMetadata> getExpiredSnapshotsExcludingLatest(long latestSnapshotNumber, SnapshotType type, Instant time) {
        return getSnapshotsBefore(type, time, request -> {
        }).filter(snapshot -> snapshot.getTransactionNumber() != latestSnapshotNumber);
    }

    private Optional<TransactionLogSnapshotMetadata> getLatestSnapshotBefore(SnapshotType type, Instant time) {
        return getSnapshotsBefore(type, time, request -> request.toBuilder()
                .scanIndexForward(false)
                .limit(1).build())
                .findFirst();
    }

    private Stream<TransactionLogSnapshotMetadata> getSnapshotsBefore(SnapshotType type, Instant time, Consumer<QueryRequest> config) {
        QueryRequest request = QueryRequest.builder()
                .tableName(allSnapshotsTable)
                .keyConditionExpression("#TableIdAndType = :table_id_and_type")
                .filterExpression("#UpdateTime < :expiry_time")
                .expressionAttributeNames(Map.of(
                        "#TableIdAndType", TABLE_ID_AND_SNAPSHOT_TYPE,
                        "#UpdateTime", UPDATE_TIME))
                .expressionAttributeValues(new DynamoDBRecordBuilder()
                        .string(":table_id_and_type", tableAndType(sleeperTableId, type))
                        .number(":expiry_time", time.toEpochMilli())
                        .build())
                .build();
        config.accept(request);
        return streamPagedItems(dynamo, request)
                .map(DynamoDBTransactionLogSnapshotMetadataStore::getSnapshotFromItem);
    }

    void deleteSnapshot(TransactionLogSnapshotMetadata snapshot) {
        dynamo.deleteItem(DeleteItemRequest.builder()
                .tableName(allSnapshotsTable)
                .key(getKeyFromSnapshot(snapshot))
                .build());
    }

    private Map<String, AttributeValue> getKeyFromSnapshot(TransactionLogSnapshotMetadata snapshot) {
        return Map.of(TABLE_ID_AND_SNAPSHOT_TYPE, AttributeValue.builder().s(tableAndType(sleeperTableId, snapshot.getType())).build(),
                TRANSACTION_NUMBER, AttributeValue.builder().n(snapshot.getTransactionNumber() + "").build());
    }

}
