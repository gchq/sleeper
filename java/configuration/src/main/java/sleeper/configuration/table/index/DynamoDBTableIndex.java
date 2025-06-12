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

package sleeper.configuration.table.index;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ComparisonOperator;
import software.amazon.awssdk.services.dynamodb.model.Condition;
import software.amazon.awssdk.services.dynamodb.model.ConsumedCapacity;
import software.amazon.awssdk.services.dynamodb.model.Delete;
import software.amazon.awssdk.services.dynamodb.model.Put;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;
import software.amazon.awssdk.services.dynamodb.model.ReturnConsumedCapacity;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItem;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItemsRequest;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItemsResponse;
import software.amazon.awssdk.services.dynamodb.model.TransactionCanceledException;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.table.TableAlreadyExistsException;
import sleeper.core.table.TableIndex;
import sleeper.core.table.TableNotFoundException;
import sleeper.core.table.TableStatus;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.TABLE_ID_INDEX_DYNAMO_TABLENAME;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.TABLE_NAME_INDEX_DYNAMO_TABLENAME;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.TABLE_ONLINE_INDEX_DYNAMO_TABLENAME;
import static sleeper.core.properties.instance.TableStateProperty.TABLE_INDEX_DYNAMO_STRONGLY_CONSISTENT_READS;
import static sleeper.dynamodb.toolsv2.DynamoDBAttributes.createStringAttribute;
import static sleeper.dynamodb.toolsv2.DynamoDBUtils.streamPagedItems;

/**
 * The DynamoDB implementation of the Sleeper table index. Records which tables exist in a Sleeper instance, their
 * names, and which are online or offline.
 */
public class DynamoDBTableIndex implements TableIndex {
    private static final Logger LOGGER = LoggerFactory.getLogger(DynamoDBTableIndex.class);

    public static final String TABLE_NAME_FIELD = DynamoDBTableIdFormat.TABLE_NAME_FIELD;
    public static final String TABLE_ID_FIELD = DynamoDBTableIdFormat.TABLE_ID_FIELD;
    public static final String TABLE_ONLINE_FIELD = DynamoDBTableIdFormat.ONLINE_FIELD;

    private final DynamoDbClient dynamoDB;
    private final String nameIndexDynamoTableName;
    private final String idIndexDynamoTableName;
    private final String onlineIndexDynamoTableName;
    private final boolean stronglyConsistent;

    public DynamoDBTableIndex(InstanceProperties instanceProperties, DynamoDbClient dynamoDB) {
        this.dynamoDB = dynamoDB;
        this.nameIndexDynamoTableName = instanceProperties.get(TABLE_NAME_INDEX_DYNAMO_TABLENAME);
        this.idIndexDynamoTableName = instanceProperties.get(TABLE_ID_INDEX_DYNAMO_TABLENAME);
        this.onlineIndexDynamoTableName = instanceProperties.get(TABLE_ONLINE_INDEX_DYNAMO_TABLENAME);
        this.stronglyConsistent = instanceProperties.getBoolean(TABLE_INDEX_DYNAMO_STRONGLY_CONSISTENT_READS);
    }

    @Override
    public void create(TableStatus table) throws TableAlreadyExistsException {
        Map<String, AttributeValue> idItem = DynamoDBTableIdFormat.getItem(table);
        TransactWriteItemsRequest request = TransactWriteItemsRequest.builder()
                .returnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
                .transactItems(
                        TransactWriteItem.builder()
                                .put(Put.builder()
                                        .tableName(nameIndexDynamoTableName)
                                        .item(idItem)
                                        .conditionExpression("attribute_not_exists(#tablename)")
                                        .expressionAttributeNames(Map.of("#tablename", TABLE_NAME_FIELD))
                                        .build())
                                .build(),
                        TransactWriteItem.builder()
                                .put(Put.builder()
                                        .tableName(idIndexDynamoTableName)
                                        .item(idItem)
                                        .conditionExpression("attribute_not_exists(#tableid)")
                                        .expressionAttributeNames(Map.of("#tableid", TABLE_ID_FIELD))
                                        .build())
                                .build(),
                        TransactWriteItem.builder()
                                .put(Put.builder()
                                        .tableName(onlineIndexDynamoTableName)
                                        .item(idItem)
                                        .conditionExpression("attribute_not_exists(#tablename)")
                                        .expressionAttributeNames(Map.of("#tablename", TABLE_NAME_FIELD)).build())
                                .build())
                .build();
        try {
            TransactWriteItemsResponse result = dynamoDB.transactWriteItems(request);
            List<ConsumedCapacity> consumedCapacity = result.consumedCapacity();
            double totalCapacity = consumedCapacity.stream().mapToDouble(ConsumedCapacity::capacityUnits).sum();
            LOGGER.debug("Created table {}, capacity consumed = {}", table, totalCapacity);
        } catch (TransactionCanceledException e) {
            if (anyCheckFailed(e)) {
                throw new TableAlreadyExistsException(table, e);
            }
            throw e;
        }
    }

    @Override
    public Stream<TableStatus> streamAllTables() {
        return streamPagedItems(dynamoDB,
                ScanRequest.builder()
                        .tableName(onlineIndexDynamoTableName)
                        .consistentRead(stronglyConsistent)
                        .build())
                .map(DynamoDBTableIdFormat::readItem);
    }

    @Override
    public Stream<TableStatus> streamOnlineTables() {
        return streamPagedItems(dynamoDB,
                QueryRequest.builder()
                        .tableName(onlineIndexDynamoTableName)
                        .keyConditionExpression("#online = :true")
                        .expressionAttributeNames(Map.of("#online", TABLE_ONLINE_FIELD))
                        .expressionAttributeValues(Map.of(":true", createStringAttribute("true")))
                        .consistentRead(stronglyConsistent)
                        .build())
                .map(DynamoDBTableIdFormat::readItem);
    }

    @Override
    public Optional<TableStatus> getTableByName(String tableName) {
        QueryResponse result = dynamoDB.query(QueryRequest.builder()
                .tableName(nameIndexDynamoTableName)
                .consistentRead(stronglyConsistent)
                .keyConditions(Map.of(TABLE_NAME_FIELD, Condition.builder()
                        .attributeValueList(createStringAttribute(tableName))
                        .comparisonOperator(ComparisonOperator.EQ)
                        .build()))
                .build());
        return result.items().stream().map(DynamoDBTableIdFormat::readItem).findFirst();
    }

    @Override
    public Optional<TableStatus> getTableByUniqueId(String tableUniqueId) {
        QueryResponse result = dynamoDB.query(QueryRequest.builder()
                .tableName(idIndexDynamoTableName)
                .consistentRead(stronglyConsistent)
                .keyConditions(Map.of(TABLE_ID_FIELD, Condition.builder()
                        .attributeValueList(createStringAttribute(tableUniqueId))
                        .comparisonOperator(ComparisonOperator.EQ).build()))
                .build());
        return result.items().stream().map(DynamoDBTableIdFormat::readItem).findFirst();
    }

    @Override
    public void delete(TableStatus table) {
        TransactWriteItemsRequest request = TransactWriteItemsRequest.builder()
                .returnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
                .transactItems(
                        TransactWriteItem.builder()
                                .delete(Delete.builder()
                                        .tableName(nameIndexDynamoTableName)
                                        .key(DynamoDBTableIdFormat.getNameKey(table))
                                        .conditionExpression("attribute_exists(#tablename)")
                                        .expressionAttributeNames(Map.of("#tablename", TABLE_NAME_FIELD))
                                        .build())
                                .build(),
                        TransactWriteItem.builder()
                                .delete(Delete.builder()
                                        .tableName(idIndexDynamoTableName)
                                        .key(DynamoDBTableIdFormat.getIdKey(table))
                                        .conditionExpression("attribute_exists(#tableid)")
                                        .expressionAttributeNames(Map.of("#tableid", TABLE_ID_FIELD))
                                        .build())
                                .build(),
                        TransactWriteItem.builder()
                                .delete(Delete.builder()
                                        .tableName(onlineIndexDynamoTableName)
                                        .key(DynamoDBTableIdFormat.getOnlineKey(table))
                                        .conditionExpression("attribute_exists(#tablename)")
                                        .expressionAttributeNames(Map.of("#tablename", TABLE_NAME_FIELD))
                                        .build())
                                .build())
                .build();
        try {
            TransactWriteItemsResponse result = dynamoDB.transactWriteItems(request);
            List<ConsumedCapacity> consumedCapacity = result.consumedCapacity();
            double totalCapacity = consumedCapacity.stream().mapToDouble(ConsumedCapacity::capacityUnits).sum();
            LOGGER.debug("Deleted table {}, capacity consumed = {}", table, totalCapacity);
        } catch (TransactionCanceledException e) {
            int nameDeleteIndex = 0;
            int idDeleteIndex = 1;
            int onlineDeleteIndex = 2;
            if (isCheckFailed(e, nameDeleteIndex)) {
                throw TableNotFoundException.withTableName(table.getTableName(), e);
            }
            if (isCheckFailed(e, idDeleteIndex)) {
                throw TableNotFoundException.withTableId(table.getTableUniqueId(), e);
            }
            if (isCheckFailed(e, onlineDeleteIndex)) {
                throw TableNotFoundException.withTableName(table.getTableName(), e);
            }
            throw e;
        }
    }

    @Override
    public void update(TableStatus table) {
        TableStatus oldStatus = getTableByUniqueId(table.getTableUniqueId())
                .orElseThrow(() -> TableNotFoundException.withTableId(table.getTableUniqueId()));
        update(oldStatus, table);
    }

    /**
     * Updates a table index entry.
     *
     * @param oldStatus the entry before the update
     * @param newStatus the entry after the update
     */
    public void update(TableStatus oldStatus, TableStatus newStatus) {
        if (oldStatus.equals(newStatus)) {
            LOGGER.debug("No changes detected for table {}, skipping update", oldStatus);
            return;
        }
        boolean nameChanged = !oldStatus.getTableName().equals(newStatus.getTableName());
        boolean onlineChanged = !(oldStatus.isOnline() == newStatus.isOnline());
        Map<String, AttributeValue> idItem = DynamoDBTableIdFormat.getItem(newStatus);
        List<TransactWriteItem> writeItems = new ArrayList<>();
        List<TransactWriteItem> deleteItems = new ArrayList<>();

        writeItems.add(TransactWriteItem.builder()
                .put(Put.builder()
                        .tableName(onlineIndexDynamoTableName)
                        .item(idItem)
                        .conditionExpression("attribute_not_exists(#tableonline) and attribute_not_exists(#tablename)")
                        .expressionAttributeNames(Map.of("#tableonline", TABLE_ONLINE_FIELD, "#tablename", TABLE_NAME_FIELD))
                        .build())
                .build());
        if (onlineChanged || nameChanged) {
            deleteItems.add(TransactWriteItem.builder()
                    .delete(Delete.builder()
                            .tableName(onlineIndexDynamoTableName)
                            .key(DynamoDBTableIdFormat.getOnlineKey(oldStatus))
                            .conditionExpression("attribute_exists(#tableonline) and attribute_exists(#tablename)")
                            .expressionAttributeNames(Map.of("#tableonline", TABLE_ONLINE_FIELD, "#tablename", TABLE_NAME_FIELD))
                            .build())
                    .build());
        }
        Put namePut;
        if (nameChanged) {
            deleteItems.add(TransactWriteItem.builder()
                    .delete(Delete.builder()
                            .tableName(nameIndexDynamoTableName)
                            .key(DynamoDBTableIdFormat.getNameKey(oldStatus))
                            .conditionExpression("attribute_exists(#tablename)")
                            .expressionAttributeNames(Map.of("#tablename", TABLE_NAME_FIELD))
                            .build())
                    .build());
            namePut = Put.builder()
                    .tableName(nameIndexDynamoTableName)
                    .item(idItem)
                    .conditionExpression("attribute_not_exists(#tablename)")
                    .expressionAttributeNames(Map.of("#tablename", TABLE_NAME_FIELD))
                    .build();
        } else {
            namePut = Put.builder()
                    .tableName(nameIndexDynamoTableName)
                    .item(idItem)
                    .conditionExpression("attribute_exists(#tablename)")
                    .expressionAttributeNames(Map.of("#tablename", TABLE_NAME_FIELD))
                    .build();
        }
        writeItems.add(TransactWriteItem.builder()
                .put(namePut)
                .build());
        writeItems.add(TransactWriteItem.builder()
                .put(Put.builder()
                        .tableName(idIndexDynamoTableName)
                        .item(idItem)
                        .conditionExpression("attribute_exists(#tableid)")
                        .expressionAttributeNames(Map.of("#tableid", TABLE_ID_FIELD))
                        .build())
                .build());
        writeItems.addAll(deleteItems);
        TransactWriteItemsRequest request = TransactWriteItemsRequest.builder()
                .returnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
                .transactItems(writeItems).build();
        try {
            TransactWriteItemsResponse result = dynamoDB.transactWriteItems(request);
            List<ConsumedCapacity> consumedCapacity = result.consumedCapacity();
            double totalCapacity = consumedCapacity.stream().mapToDouble(ConsumedCapacity::capacityUnits).sum();
            LOGGER.debug("Updated table {}, capacity consumed = {}", newStatus, totalCapacity);
        } catch (TransactionCanceledException e) {
            int onlinePutIndex = 0;
            int namePutIndex = 1;
            int idPutIndex = 2;
            int onlineDeleteIndex = 3;
            int nameDeleteIndex = 4;
            if (isCheckFailed(e, onlinePutIndex)) {
                throw new TableAlreadyExistsException(getTableByName(newStatus.getTableName())
                        .orElseThrow(() -> TableNotFoundException.withTableName(newStatus.getTableName())), e);
            }
            if (isCheckFailed(e, namePutIndex)) {
                if (nameChanged) {
                    throw new TableAlreadyExistsException(getTableByName(newStatus.getTableName())
                            .orElseThrow(() -> TableNotFoundException.withTableName(newStatus.getTableName())), e);
                } else {
                    throw TableNotFoundException.withTableName(newStatus.getTableName(), e);
                }
            }
            if (isCheckFailed(e, idPutIndex)) {
                throw TableNotFoundException.withTableId(newStatus.getTableUniqueId(), e);
            }
            if (isCheckFailed(e, onlineDeleteIndex)) {
                throw TableNotFoundException.withTableName(oldStatus.getTableName(), e);
            }
            if (isCheckFailed(e, nameDeleteIndex)) {
                throw TableNotFoundException.withTableName(oldStatus.getTableName(), e);
            }
            throw e;
        }
    }

    private static boolean anyCheckFailed(TransactionCanceledException e) {
        return e.cancellationReasons().stream()
                .anyMatch(reason -> "ConditionalCheckFailed".equals(reason.code()));
    }

    private static boolean isCheckFailed(TransactionCanceledException e, int index) {
        return index < e.cancellationReasons().size() &&
                "ConditionalCheckFailed".equals(e.cancellationReasons().get(index).code());
    }
}
