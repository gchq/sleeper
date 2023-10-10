/*
 * Copyright 2022-2023 Crown Copyright
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

package sleeper.table.store.dynamodb;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.CancellationReason;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.amazonaws.services.dynamodbv2.model.ConsumedCapacity;
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
import sleeper.core.table.TableId;
import sleeper.core.table.TableIdGenerator;
import sleeper.core.table.TableIndex;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.TABLE_ID_INDEX_DYNAMO_TABLENAME;
import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.TABLE_NAME_INDEX_DYNAMO_TABLENAME;
import static sleeper.dynamodb.tools.DynamoDBAttributes.createStringAttribute;
import static sleeper.dynamodb.tools.DynamoDBUtils.streamPagedItems;

public class DynamoDBTableIndex implements TableIndex {
    private static final Logger LOGGER = LoggerFactory.getLogger(DynamoDBTableIndex.class);

    public static final String TABLE_NAME_FIELD = DynamoDBTableIdFormat.TABLE_NAME_FIELD;
    public static final String TABLE_ID_FIELD = DynamoDBTableIdFormat.TABLE_ID_FIELD;

    private final AmazonDynamoDB dynamoDB;
    private final String nameIndexDynamoTableName;
    private final String idIndexDynamoTableName;
    private final TableIdGenerator idGenerator = new TableIdGenerator();

    public DynamoDBTableIndex(AmazonDynamoDB dynamoDB, InstanceProperties instanceProperties) {
        this.dynamoDB = dynamoDB;
        this.nameIndexDynamoTableName = instanceProperties.get(TABLE_NAME_INDEX_DYNAMO_TABLENAME);
        this.idIndexDynamoTableName = instanceProperties.get(TABLE_ID_INDEX_DYNAMO_TABLENAME);
    }

    @Override
    public TableId createTable(String tableName) throws TableAlreadyExistsException {
        TableId id = TableId.idAndName(idGenerator.generateString(), tableName);
        TransactWriteItemsRequest request = new TransactWriteItemsRequest()
                .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
                .withTransactItems(
                        new TransactWriteItem().withPut(new Put()
                                .withTableName(nameIndexDynamoTableName)
                                .withItem(DynamoDBTableIdFormat.getItem(id))
                                .withConditionExpression("attribute_not_exists(#tablename)")
                                .withExpressionAttributeNames(Map.of("#tablename", TABLE_NAME_FIELD))),
                        new TransactWriteItem().withPut(new Put()
                                .withTableName(idIndexDynamoTableName)
                                .withItem(DynamoDBTableIdFormat.getItem(id))
                                .withConditionExpression("attribute_not_exists(#tableid)")
                                .withExpressionAttributeNames(Map.of("#tableid", TABLE_ID_FIELD))));
        try {
            TransactWriteItemsResult result = dynamoDB.transactWriteItems(request);
            List<ConsumedCapacity> consumedCapacity = result.getConsumedCapacity();
            double totalCapacity = consumedCapacity.stream().mapToDouble(ConsumedCapacity::getCapacityUnits).sum();
            LOGGER.debug("Created table {} with ID {}, capacity consumed = {}",
                    tableName, id.getTableId(), totalCapacity);
            return id;
        } catch (TransactionCanceledException e) {
            CancellationReason nameIndexReason = e.getCancellationReasons().get(0);
            if ("ConditionalCheckFailed".equals(nameIndexReason.getCode())) {
                throw new TableAlreadyExistsException(tableName);
            } else {
                throw e;
            }
        }
    }

    @Override
    public Stream<TableId> streamAllTables() {
        return streamPagedItems(dynamoDB, new ScanRequest().withTableName(nameIndexDynamoTableName))
                .map(DynamoDBTableIdFormat::readItem)
                .sorted(Comparator.comparing(TableId::getTableName));
    }

    @Override
    public Optional<TableId> getTableByName(String tableName) {
        QueryResult result = dynamoDB.query(new QueryRequest()
                .withTableName(nameIndexDynamoTableName)
                .addKeyConditionsEntry(TABLE_NAME_FIELD, new Condition()
                        .withAttributeValueList(createStringAttribute(tableName))
                        .withComparisonOperator(ComparisonOperator.EQ)));
        return result.getItems().stream().map(DynamoDBTableIdFormat::readItem).findFirst();
    }

    @Override
    public Optional<TableId> getTableById(String tableId) {
        QueryResult result = dynamoDB.query(new QueryRequest()
                .withTableName(idIndexDynamoTableName)
                .addKeyConditionsEntry(TABLE_ID_FIELD, new Condition()
                        .withAttributeValueList(createStringAttribute(tableId))
                        .withComparisonOperator(ComparisonOperator.EQ)));
        return result.getItems().stream().map(DynamoDBTableIdFormat::readItem).findFirst();
    }
}
