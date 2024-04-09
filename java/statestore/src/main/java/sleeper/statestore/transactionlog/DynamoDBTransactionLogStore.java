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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TableProperty;
import sleeper.core.statestore.transactionlog.DuplicateTransactionNumberException;
import sleeper.core.statestore.transactionlog.StateStoreTransaction;
import sleeper.core.statestore.transactionlog.TransactionLogEntry;
import sleeper.core.statestore.transactionlog.TransactionLogStore;
import sleeper.core.statestore.transactionlog.transactions.TransactionSerDe;
import sleeper.core.statestore.transactionlog.transactions.TransactionType;
import sleeper.dynamodb.tools.DynamoDBRecordBuilder;

import java.util.Map;
import java.util.stream.Stream;

import static sleeper.dynamodb.tools.DynamoDBAttributes.getLongAttribute;
import static sleeper.dynamodb.tools.DynamoDBAttributes.getNumberAttribute;
import static sleeper.dynamodb.tools.DynamoDBAttributes.getStringAttribute;
import static sleeper.dynamodb.tools.DynamoDBUtils.streamPagedItems;

class DynamoDBTransactionLogStore implements TransactionLogStore {
    public static final Logger LOGGER = LoggerFactory.getLogger(DynamoDBTransactionLogStore.class);

    private static final String TABLE_ID = DynamoDBTransactionLogStateStore.TABLE_ID;
    private static final String TRANSACTION_NUMBER = DynamoDBTransactionLogStateStore.TRANSACTION_NUMBER;
    private static final String TYPE = "TYPE";
    private static final String BODY = "BODY";

    private final String logTableName;
    private final String sleeperTableId;
    private final AmazonDynamoDB dynamo;
    private final TransactionSerDe serDe;

    DynamoDBTransactionLogStore(
            String logTableName, TableProperties tableProperties, AmazonDynamoDB dynamo) {
        this.logTableName = logTableName;
        this.sleeperTableId = tableProperties.get(TableProperty.TABLE_ID);
        this.dynamo = dynamo;
        this.serDe = new TransactionSerDe(tableProperties.getSchema());
    }

    @Override
    public void addTransaction(TransactionLogEntry entry) throws DuplicateTransactionNumberException {
        long transactionNumber = entry.getTransactionNumber();
        StateStoreTransaction<?> transaction = entry.getTransaction();
        try {
            dynamo.putItem(new PutItemRequest()
                    .withTableName(logTableName)
                    .withItem(new DynamoDBRecordBuilder()
                            .string(TABLE_ID, sleeperTableId)
                            .number(TRANSACTION_NUMBER, transactionNumber)
                            .string(TYPE, TransactionType.getType(transaction).name())
                            .string(BODY, serDe.toJson(transaction))
                            .build())
                    .withConditionExpression("attribute_not_exists(#Number)")
                    .withExpressionAttributeNames(Map.of("#Number", TRANSACTION_NUMBER)));
        } catch (ConditionalCheckFailedException e) {
            throw new DuplicateTransactionNumberException(transactionNumber, e);
        }
    }

    @Override
    public Stream<TransactionLogEntry> readTransactionsAfter(long lastTransactionNumber) {
        return streamPagedItems(dynamo, new QueryRequest()
                .withTableName(logTableName)
                .withConsistentRead(true)
                .withKeyConditionExpression("#TableId = :table_id AND #Number > :number")
                .withExpressionAttributeNames(Map.of("#TableId", TABLE_ID, "#Number", TRANSACTION_NUMBER))
                .withExpressionAttributeValues(new DynamoDBRecordBuilder()
                        .string(":table_id", sleeperTableId)
                        .number(":number", lastTransactionNumber)
                        .build())
                .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL))
                .map(this::readTransaction);
    }

    private TransactionLogEntry readTransaction(Map<String, AttributeValue> item) {
        long number = getLongAttribute(item, TRANSACTION_NUMBER, -1);
        TransactionType type = readType(item);
        StateStoreTransaction<?> transaction = serDe.toTransaction(type, getStringAttribute(item, BODY));
        return new TransactionLogEntry(number, transaction);
    }

    private TransactionType readType(Map<String, AttributeValue> item) {
        String typeName = getStringAttribute(item, TYPE);
        try {
            return TransactionType.valueOf(typeName);
        } catch (RuntimeException e) {
            LOGGER.warn("Found unrecognised transaction type for table {} transaction {}: {}",
                    getStringAttribute(item, TABLE_ID), getNumberAttribute(item, TRANSACTION_NUMBER), typeName, e);
            throw e;
        }
    }

}
