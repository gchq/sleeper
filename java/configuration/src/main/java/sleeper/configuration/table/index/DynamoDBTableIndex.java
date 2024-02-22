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
import com.amazonaws.services.dynamodbv2.model.CancellationReason;
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

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.core.table.TableAlreadyExistsException;
import sleeper.core.table.TableStatus;
import sleeper.core.table.TableIndex;
import sleeper.core.table.TableNotFoundException;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.TABLE_ID_INDEX_DYNAMO_TABLENAME;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.TABLE_NAME_INDEX_DYNAMO_TABLENAME;
import static sleeper.configuration.properties.instance.CommonProperty.TABLE_INDEX_DYNAMO_STRONGLY_CONSISTENT_READS;
import static sleeper.dynamodb.tools.DynamoDBAttributes.createStringAttribute;
import static sleeper.dynamodb.tools.DynamoDBUtils.streamPagedItems;

public class DynamoDBTableIndex implements TableIndex {
    private static final Logger LOGGER = LoggerFactory.getLogger(DynamoDBTableIndex.class);

    public static final String TABLE_NAME_FIELD = DynamoDBTableIdFormat.TABLE_NAME_FIELD;
    public static final String TABLE_ID_FIELD = DynamoDBTableIdFormat.TABLE_ID_FIELD;

    private final AmazonDynamoDB dynamoDB;
    private final String nameIndexDynamoTableName;
    private final String idIndexDynamoTableName;
    private final boolean stronglyConsistent;

    public DynamoDBTableIndex(InstanceProperties instanceProperties, AmazonDynamoDB dynamoDB) {
        this.dynamoDB = dynamoDB;
        this.nameIndexDynamoTableName = instanceProperties.get(TABLE_NAME_INDEX_DYNAMO_TABLENAME);
        this.idIndexDynamoTableName = instanceProperties.get(TABLE_ID_INDEX_DYNAMO_TABLENAME);
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
                                .withExpressionAttributeNames(Map.of("#tableid", TABLE_ID_FIELD))));
        try {
            TransactWriteItemsResult result = dynamoDB.transactWriteItems(request);
            List<ConsumedCapacity> consumedCapacity = result.getConsumedCapacity();
            double totalCapacity = consumedCapacity.stream().mapToDouble(ConsumedCapacity::getCapacityUnits).sum();
            LOGGER.debug("Created table {}, capacity consumed = {}", table, totalCapacity);
        } catch (TransactionCanceledException e) {
            CancellationReason nameIndexReason = e.getCancellationReasons().get(0);
            if (isCheckFailed(nameIndexReason)) {
                throw new TableAlreadyExistsException(table);
            } else {
                throw e;
            }
        }
    }

    @Override
    public Stream<TableStatus> streamAllTables() {
        return streamPagedItems(dynamoDB,
                new ScanRequest()
                        .withTableName(nameIndexDynamoTableName)
                        .withConsistentRead(stronglyConsistent))
                .map(DynamoDBTableIdFormat::readItem)
                .sorted(Comparator.comparing(TableStatus::getTableName));
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
                                .withExpressionAttributeNames(Map.of("#tableid", TABLE_ID_FIELD))));
        try {
            TransactWriteItemsResult result = dynamoDB.transactWriteItems(request);
            List<ConsumedCapacity> consumedCapacity = result.getConsumedCapacity();
            double totalCapacity = consumedCapacity.stream().mapToDouble(ConsumedCapacity::getCapacityUnits).sum();
            LOGGER.debug("Deleted table {}, capacity consumed = {}", table, totalCapacity);
        } catch (TransactionCanceledException e) {
            CancellationReason nameNotFoundReason = e.getCancellationReasons().get(0);
            CancellationReason idNotFoundReason = e.getCancellationReasons().get(1);
            if (isCheckFailed(nameNotFoundReason)) {
                throw TableNotFoundException.withTableName(table.getTableName());
            } else if (isCheckFailed(idNotFoundReason)) {
                throw TableNotFoundException.withTableId(table.getTableUniqueId());
            } else {
                throw e;
            }
        }
    }

    @Override
    public void update(TableStatus table) {
        TableStatus oldId = getTableByUniqueId(table.getTableUniqueId())
                .orElseThrow(() -> TableNotFoundException.withTableId(table.getTableUniqueId()));
        update(oldId, table);
    }

    public void update(TableStatus oldTable, TableStatus newTable) {
        Map<String, AttributeValue> idItem = DynamoDBTableIdFormat.getItem(newTable);
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
                                .withConditionExpression("attribute_exists(#tableid)")
                                .withExpressionAttributeNames(Map.of("#tableid", TABLE_ID_FIELD))),
                        new TransactWriteItem().withDelete(new Delete()
                                .withTableName(nameIndexDynamoTableName)
                                .withKey(DynamoDBTableIdFormat.getNameKey(oldTable))
                                .withConditionExpression("attribute_exists(#tablename)")
                                .withExpressionAttributeNames(Map.of("#tablename", TABLE_NAME_FIELD))));
        try {
            TransactWriteItemsResult result = dynamoDB.transactWriteItems(request);
            List<ConsumedCapacity> consumedCapacity = result.getConsumedCapacity();
            double totalCapacity = consumedCapacity.stream().mapToDouble(ConsumedCapacity::getCapacityUnits).sum();
            LOGGER.debug("Updated table {}, capacity consumed = {}", newTable, totalCapacity);
        } catch (TransactionCanceledException e) {
            CancellationReason nameAlreadyExistsReason = e.getCancellationReasons().get(0);
            CancellationReason idNotFoundReason = e.getCancellationReasons().get(1);
            CancellationReason oldNameNotFoundReason = e.getCancellationReasons().get(2);
            if (isCheckFailed(nameAlreadyExistsReason)) {
                throw new TableAlreadyExistsException(getTableByName(newTable.getTableName())
                        .orElseThrow(() -> TableNotFoundException.withTableName(newTable.getTableName())));
            } else if (isCheckFailed(idNotFoundReason)) {
                throw TableNotFoundException.withTableId(newTable.getTableUniqueId());
            } else if (isCheckFailed(oldNameNotFoundReason)) {
                throw TableNotFoundException.withTableName(oldTable.getTableName());
            } else {
                throw e;
            }
        }
    }

    private static boolean isCheckFailed(CancellationReason reason) {
        return "ConditionalCheckFailed".equals(reason.getCode());
    }
}
