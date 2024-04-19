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
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.ReturnConsumedCapacity;

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

import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.TRANSACTION_LOG_SNAPSHOT_TABLENAME;
import static sleeper.dynamodb.tools.DynamoDBAttributes.getLongAttribute;
import static sleeper.dynamodb.tools.DynamoDBAttributes.getStringAttribute;
import static sleeper.dynamodb.tools.DynamoDBUtils.streamPagedItems;

public class DynamoDBTransactionLogSnapshotStore implements TransactionLogSnapshotStore {
    private static final String DELIMETER = "|";
    private static final String TABLE_ID = DynamoDBTransactionLogStateStore.TABLE_ID;
    public static final String TABLE_ID_AND_SNAPSHOT_TYPE = "TABLE_ID_AND_SNAPSHOT_TYPE";
    private static final String PATH = "PATH";
    public static final String TRANSACTION_NUMBER = DynamoDBTransactionLogStateStore.TRANSACTION_NUMBER;
    private static final String UPDATE_TIME = "UPDATE_TIME";
    private static final String SNAPSHOT_TYPE = "SNAPSHOT_TYPE";
    private final String tableName;
    private final String sleeperTableId;
    private final AmazonDynamoDB dynamo;
    private final Supplier<Instant> timeSupplier;

    DynamoDBTransactionLogSnapshotStore(InstanceProperties instanceProperties, TableProperties tableProperties, AmazonDynamoDB dynamo, Supplier<Instant> timeSupplier) {
        this.tableName = instanceProperties.get(TRANSACTION_LOG_SNAPSHOT_TABLENAME);
        this.sleeperTableId = tableProperties.get(TableProperty.TABLE_ID);
        this.dynamo = dynamo;
        this.timeSupplier = timeSupplier;
    }

    @Override
    public void saveFiles(String snapshotPath, long transactionNumber) throws DuplicateSnapshotException {
        saveSnapshot(SnapshotType.FILES, snapshotPath, transactionNumber);
    }

    @Override
    public void savePartitions(String snapshotPath, long transactionNumber) throws DuplicateSnapshotException {
        saveSnapshot(SnapshotType.PARTITIONS, snapshotPath, transactionNumber);
    }

    private void saveSnapshot(SnapshotType snapshotType, String snapshotPath, long transactionNumber) throws DuplicateSnapshotException {
        Instant updateTime = timeSupplier.get();
        try {
            dynamo.putItem(new PutItemRequest()
                    .withTableName(tableName)
                    .withItem(new DynamoDBRecordBuilder()
                            .string(TABLE_ID_AND_SNAPSHOT_TYPE, tableAndType(tableName, snapshotType))
                            .string(TABLE_ID, sleeperTableId)
                            .string(PATH, snapshotPath)
                            .number(TRANSACTION_NUMBER, transactionNumber)
                            .number(UPDATE_TIME, updateTime.toEpochMilli())
                            .string(SNAPSHOT_TYPE, snapshotType.name())
                            .build())
                    .withConditionExpression("attribute_not_exists(#Path)")
                    .withExpressionAttributeNames(Map.of("#Path", PATH)));
        } catch (ConditionalCheckFailedException e) {
            throw new DuplicateSnapshotException(snapshotPath, e);
        }
    }

    @Override
    public List<TransactionLogSnapshot> getFilesSnapshots() {
        return getSnapshots(SnapshotType.FILES)
                .collect(Collectors.toList());
    }

    @Override
    public List<TransactionLogSnapshot> getPartitionsSnapshots() {
        return getSnapshots(SnapshotType.PARTITIONS)
                .collect(Collectors.toList());
    }

    private Stream<TransactionLogSnapshot> getSnapshots(SnapshotType type) {
        return streamPagedItems(dynamo, new QueryRequest()
                .withTableName(tableName)
                .withConsistentRead(true)
                .withKeyConditionExpression("#TableIdAndType = :table_and_type")
                .withExpressionAttributeNames(Map.of("#TableIdAndType", TABLE_ID_AND_SNAPSHOT_TYPE))
                .withExpressionAttributeValues(new DynamoDBRecordBuilder()
                        .string(":table_and_type", tableAndType(tableName, type))
                        .build())
                .withScanIndexForward(false)
                .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL))
                .map(this::getSnapshotFromItem);
    }

    @Override
    public Optional<TransactionLogSnapshot> getLatestFilesSnapshot() {
        return getSnapshots(SnapshotType.FILES).findFirst();
    }

    @Override
    public Optional<TransactionLogSnapshot> getLatestPartitionsSnapshot() {
        return getSnapshots(SnapshotType.PARTITIONS).findFirst();
    }

    private String tableAndType(String table, SnapshotType type) {
        return table + DELIMETER + type.name();
    }

    private TransactionLogSnapshot getSnapshotFromItem(Map<String, AttributeValue> item) {
        SnapshotType type = SnapshotType.valueOf(item.get(SNAPSHOT_TYPE).getS());
        return new TransactionLogSnapshot(getStringAttribute(item, PATH), type, getLongAttribute(item, TRANSACTION_NUMBER, 0));
    }
}
