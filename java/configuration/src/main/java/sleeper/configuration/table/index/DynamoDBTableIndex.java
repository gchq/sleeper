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

package sleeper.configuration.table.index;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.amazonaws.services.dynamodbv2.model.ConsumedCapacity;
import com.amazonaws.services.dynamodbv2.model.Delete;
import com.amazonaws.services.dynamodbv2.model.Put;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import com.amazonaws.services.dynamodbv2.model.ReturnConsumedCapacity;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.TransactWriteItem;
import com.amazonaws.services.dynamodbv2.model.TransactWriteItemsRequest;
import com.amazonaws.services.dynamodbv2.model.TransactWriteItemsResult;
import com.amazonaws.services.dynamodbv2.model.TransactionCanceledException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import static sleeper.core.properties.instance.CommonProperty.TABLE_INDEX_DYNAMO_STRONGLY_CONSISTENT_READS;
import static sleeper.dynamodb.tools.DynamoDBAttributes.createStringAttribute;
import static sleeper.dynamodb.tools.DynamoDBUtils.streamPagedItems;

/**
 * The DynamoDB implementation of the Sleeper table index. Records which tables exist in a Sleeper instance, their
 * names, and which are online or offline.
 */
public class DynamoDBTableIndex implements TableIndex {
    private static final Logger LOGGER = LoggerFactory.getLogger(DynamoDBTableIndex.class);

    public static final String TABLE_NAME_FIELD = DynamoDBTableIdFormat.TABLE_NAME_FIELD;
    public static final String TABLE_ID_FIELD = DynamoDBTableIdFormat.TABLE_ID_FIELD;
    public static final String TABLE_ONLINE_FIELD = DynamoDBTableIdFormat.ONLINE_FIELD;

    private final AmazonDynamoDB dynamoDB;
    private final String nameIndexDynamoTableName;
    private final String idIndexDynamoTableName;
    private final String onlineIndexDynamoTableName;
    private final boolean stronglyConsistent;

    public DynamoDBTableIndex(InstanceProperties instanceProperties, AmazonDynamoDB dynamoDB) {
        this.dynamoDB = dynamoDB;
        this.nameIndexDynamoTableName = instanceProperties.get(TABLE_NAME_INDEX_DYNAMO_TABLENAME);
        this.idIndexDynamoTableName = instanceProperties.get(TABLE_ID_INDEX_DYNAMO_TABLENAME);
        this.onlineIndexDynamoTableName = instanceProperties.get(TABLE_ONLINE_INDEX_DYNAMO_TABLENAME);
        this.stronglyConsistent = instanceProperties.getBoolean(TABLE_INDEX_DYNAMO_STRONGLY_CONSISTENT_READS);
    }

    @Override
    public void create(TableStatus table) throws TableAlreadyExistsException {
        Map<String, AttributeValue> idItem = DynamoDBTableIdFormat.getItem(table);
        TransactWriteItemsRequest request = new TransactWriteItemsRequest()
                .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
                .withTransactItems(
                        new TransactWriteItem().withPut(new Put()
                                .withTableName(nameIndexDynamoTableName)
                                .withItem(idItem)
                                .withConditionExpression("attribute_not_exists(#tablename)")
                                .withExpressionAttributeNames(Map.of("#tablename", TABLE_NAME_FIELD))),
                        new TransactWriteItem().withPut(new Put()
                                .withTableName(idIndexDynamoTableName)
                                .withItem(idItem)
                                .withConditionExpression("attribute_not_exists(#tableid)")
                                .withExpressionAttributeNames(Map.of("#tableid", TABLE_ID_FIELD))),
                        new TransactWriteItem().withPut(new Put()
                                .withTableName(onlineIndexDynamoTableName)
                                .withItem(idItem)
                                .withConditionExpression("attribute_not_exists(#tablename)")
                                .withExpressionAttributeNames(Map.of("#tablename", TABLE_NAME_FIELD))));
        try {
            TransactWriteItemsResult result = dynamoDB.transactWriteItems(request);
            List<ConsumedCapacity> consumedCapacity = result.getConsumedCapacity();
            double totalCapacity = consumedCapacity.stream().mapToDouble(ConsumedCapacity::getCapacityUnits).sum();
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
                new ScanRequest()
                        .withTableName(onlineIndexDynamoTableName)
                        .withConsistentRead(stronglyConsistent))
                .map(DynamoDBTableIdFormat::readItem);
    }

    @Override
    public Stream<TableStatus> streamOnlineTables() {
        return streamPagedItems(dynamoDB,
                new QueryRequest()
                        .withTableName(onlineIndexDynamoTableName)
                        .withKeyConditionExpression("#online = :true")
                        .withExpressionAttributeNames(Map.of("#online", TABLE_ONLINE_FIELD))
                        .withExpressionAttributeValues(Map.of(":true", createStringAttribute("true")))
                        .withConsistentRead(stronglyConsistent))
                .map(DynamoDBTableIdFormat::readItem);
    }

    @Override
    public Optional<TableStatus> getTableByName(String tableName) {
        QueryResult result = dynamoDB.query(new QueryRequest()
                .withTableName(nameIndexDynamoTableName)
                .withConsistentRead(stronglyConsistent)
                .addKeyConditionsEntry(TABLE_NAME_FIELD, new Condition()
                        .withAttributeValueList(createStringAttribute(tableName))
                        .withComparisonOperator(ComparisonOperator.EQ)));
        return result.getItems().stream().map(DynamoDBTableIdFormat::readItem).findFirst();
    }

    @Override
    public Optional<TableStatus> getTableByUniqueId(String tableUniqueId) {
        QueryResult result = dynamoDB.query(new QueryRequest()
                .withTableName(idIndexDynamoTableName)
                .withConsistentRead(stronglyConsistent)
                .addKeyConditionsEntry(TABLE_ID_FIELD, new Condition()
                        .withAttributeValueList(createStringAttribute(tableUniqueId))
                        .withComparisonOperator(ComparisonOperator.EQ)));
        return result.getItems().stream().map(DynamoDBTableIdFormat::readItem).findFirst();
    }

    @Override
    public void delete(TableStatus table) {
        TransactWriteItemsRequest request = new TransactWriteItemsRequest()
                .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
                .withTransactItems(
                        new TransactWriteItem().withDelete(new Delete()
                                .withTableName(nameIndexDynamoTableName)
                                .withKey(DynamoDBTableIdFormat.getNameKey(table))
                                .withConditionExpression("attribute_exists(#tablename)")
                                .withExpressionAttributeNames(Map.of("#tablename", TABLE_NAME_FIELD))),
                        new TransactWriteItem().withDelete(new Delete()
                                .withTableName(idIndexDynamoTableName)
                                .withKey(DynamoDBTableIdFormat.getIdKey(table))
                                .withConditionExpression("attribute_exists(#tableid)")
                                .withExpressionAttributeNames(Map.of("#tableid", TABLE_ID_FIELD))),
                        new TransactWriteItem().withDelete(new Delete()
                                .withTableName(onlineIndexDynamoTableName)
                                .withKey(DynamoDBTableIdFormat.getOnlineKey(table))
                                .withConditionExpression("attribute_exists(#tablename)")
                                .withExpressionAttributeNames(Map.of("#tablename", TABLE_NAME_FIELD))));
        try {
            TransactWriteItemsResult result = dynamoDB.transactWriteItems(request);
            List<ConsumedCapacity> consumedCapacity = result.getConsumedCapacity();
            double totalCapacity = consumedCapacity.stream().mapToDouble(ConsumedCapacity::getCapacityUnits).sum();
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

        writeItems.add(new TransactWriteItem().withPut(new Put()
                .withTableName(onlineIndexDynamoTableName)
                .withItem(idItem)
                .withConditionExpression("attribute_not_exists(#tableonline) and attribute_not_exists(#tablename)")
                .withExpressionAttributeNames(Map.of("#tableonline", TABLE_ONLINE_FIELD, "#tablename", TABLE_NAME_FIELD))));
        if (onlineChanged || nameChanged) {
            deleteItems.add(new TransactWriteItem().withDelete(new Delete()
                    .withTableName(onlineIndexDynamoTableName)
                    .withKey(DynamoDBTableIdFormat.getOnlineKey(oldStatus))
                    .withConditionExpression("attribute_exists(#tableonline) and attribute_exists(#tablename)")
                    .withExpressionAttributeNames(Map.of("#tableonline", TABLE_ONLINE_FIELD, "#tablename", TABLE_NAME_FIELD))));
        }
        Put namePut = new Put()
                .withTableName(nameIndexDynamoTableName)
                .withItem(idItem);
        if (nameChanged) {
            deleteItems.add(new TransactWriteItem().withDelete(new Delete()
                    .withTableName(nameIndexDynamoTableName)
                    .withKey(DynamoDBTableIdFormat.getNameKey(oldStatus))
                    .withConditionExpression("attribute_exists(#tablename)")
                    .withExpressionAttributeNames(Map.of("#tablename", TABLE_NAME_FIELD))));
            namePut.withConditionExpression("attribute_not_exists(#tablename)")
                    .withExpressionAttributeNames(Map.of("#tablename", TABLE_NAME_FIELD));
        } else {
            namePut.withConditionExpression("attribute_exists(#tablename)")
                    .withExpressionAttributeNames(Map.of("#tablename", TABLE_NAME_FIELD));
        }
        writeItems.add(new TransactWriteItem().withPut(namePut));
        writeItems.add(new TransactWriteItem().withPut(new Put()
                .withTableName(idIndexDynamoTableName)
                .withItem(idItem)
                .withConditionExpression("attribute_exists(#tableid)")
                .withExpressionAttributeNames(Map.of("#tableid", TABLE_ID_FIELD))));
        writeItems.addAll(deleteItems);
        TransactWriteItemsRequest request = new TransactWriteItemsRequest()
                .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
                .withTransactItems(writeItems);
        try {
            TransactWriteItemsResult result = dynamoDB.transactWriteItems(request);
            List<ConsumedCapacity> consumedCapacity = result.getConsumedCapacity();
            double totalCapacity = consumedCapacity.stream().mapToDouble(ConsumedCapacity::getCapacityUnits).sum();
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
        return e.getCancellationReasons().stream()
                .anyMatch(reason -> "ConditionalCheckFailed".equals(reason.getCode()));
    }

    private static boolean isCheckFailed(TransactionCanceledException e, int index) {
        return index < e.getCancellationReasons().size() &&
                "ConditionalCheckFailed".equals(e.getCancellationReasons().get(index).getCode());
    }
}
