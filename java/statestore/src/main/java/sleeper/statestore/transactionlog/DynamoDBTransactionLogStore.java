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
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.ReturnConsumedCapacity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TableProperty;
import sleeper.core.statestore.transactionlog.StateStoreTransaction;
import sleeper.core.statestore.transactionlog.TransactionLogStore;
import sleeper.core.statestore.transactionlog.transactions.TransactionSerDe;
import sleeper.dynamodb.tools.DynamoDBRecordBuilder;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.TRANSACTION_LOG_TABLENAME;
import static sleeper.dynamodb.tools.DynamoDBUtils.streamPagedItems;

class DynamoDBTransactionLogStore implements TransactionLogStore {
    public static final Logger LOGGER = LoggerFactory.getLogger(DynamoDBTransactionLogStore.class);

    private static final String TABLE_ID = DynamoDBTransactionLogStateStore.TABLE_ID;
    private static final String TRANSACTION_NUMBER = DynamoDBTransactionLogStateStore.TRANSACTION_NUMBER;
    private static final String TYPE = "TYPE";
    private static final String BODY = "BODY";

    private final InstanceProperties instanceProperties;
    private final TableProperties tableProperties;
    private final AmazonDynamoDB dynamo;
    private final TransactionSerDe serDe;

    DynamoDBTransactionLogStore(
            InstanceProperties instanceProperties, TableProperties tableProperties, AmazonDynamoDB dynamo) {
        this.instanceProperties = instanceProperties;
        this.tableProperties = tableProperties;
        this.dynamo = dynamo;
        this.serDe = new TransactionSerDe(tableProperties.getSchema());
    }

    @Override
    public void addTransaction(StateStoreTransaction transaction, long transactionNumber) {
        dynamo.putItem(new PutItemRequest()
                .withTableName(instanceProperties.get(TRANSACTION_LOG_TABLENAME))
                .withItem(new DynamoDBRecordBuilder()
                        .string(TABLE_ID, tableProperties.get(TableProperty.TABLE_ID))
                        .number(TRANSACTION_NUMBER, transactionNumber)
                        .string(TYPE, transaction.getClass().getName())
                        .string(BODY, serDe.toJson(transaction))
                        .build())
                .withConditionExpression("attribute_not_exists(#Number)")
                .withExpressionAttributeNames(Map.of("#Number", TRANSACTION_NUMBER)));
    }

    @Override
    public Stream<StateStoreTransaction> readTransactionsAfter(long lastTransactionNumber) {
        return streamPagedItems(dynamo, new QueryRequest()
                .withTableName(instanceProperties.get(TRANSACTION_LOG_TABLENAME))
                .withConsistentRead(true)
                .withKeyConditionExpression("#TableId = :table_id AND #Number > :number")
                .withExpressionAttributeNames(Map.of("#TableId", TABLE_ID, "#Number", TRANSACTION_NUMBER))
                .withExpressionAttributeValues(new DynamoDBRecordBuilder()
                        .string(":table_id", tableProperties.get(TableProperty.TABLE_ID))
                        .number(":number", lastTransactionNumber)
                        .build())
                .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL))
                .flatMap(item -> readTransaction(item).stream());
    }

    private Optional<StateStoreTransaction> readTransaction(Map<String, AttributeValue> item) {
        String className = item.get(TYPE).getS();
        try {
            Class<?> type = Class.forName(className);
            if (!StateStoreTransaction.class.isAssignableFrom(type)) {
                LOGGER.warn("Found non-transaction type for table {} transaction {}: {}",
                        item.get(TABLE_ID).getS(), item.get(TRANSACTION_NUMBER).getS(), className);
                return Optional.empty();
            }
            Class<? extends StateStoreTransaction> transactionType = (Class<? extends StateStoreTransaction>) type;
            return Optional.of(serDe.toTransaction(transactionType, item.get(BODY).getS()));
        } catch (ClassNotFoundException e) {
            LOGGER.warn("Found unrecognised transaction type for table {} transaction {}: {}",
                    item.get(TABLE_ID).getS(), item.get(TRANSACTION_NUMBER).getS(), className);
            return Optional.empty();
        }
    }

}
