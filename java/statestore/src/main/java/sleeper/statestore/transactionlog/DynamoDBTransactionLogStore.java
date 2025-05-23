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
package sleeper.statestore.transactionlog;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.ReturnConsumedCapacity;
import com.amazonaws.services.s3.AmazonS3;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.statestore.transactionlog.log.DuplicateTransactionNumberException;
import sleeper.core.statestore.transactionlog.log.TransactionLogDeletionTracker;
import sleeper.core.statestore.transactionlog.log.TransactionLogEntry;
import sleeper.core.statestore.transactionlog.log.TransactionLogRange;
import sleeper.core.statestore.transactionlog.log.TransactionLogStore;
import sleeper.core.statestore.transactionlog.transaction.StateStoreTransaction;
import sleeper.core.statestore.transactionlog.transaction.TransactionSerDe;
import sleeper.core.statestore.transactionlog.transaction.TransactionSerDeProvider;
import sleeper.core.statestore.transactionlog.transaction.TransactionType;
import sleeper.core.table.TableStatus;
import sleeper.dynamodb.tools.DynamoDBRecordBuilder;

import java.time.Instant;
import java.util.Map;
import java.util.stream.Stream;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.TRANSACTION_LOG_FILES_TABLENAME;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.TRANSACTION_LOG_PARTITIONS_TABLENAME;
import static sleeper.dynamodb.tools.DynamoDBAttributes.getInstantAttribute;
import static sleeper.dynamodb.tools.DynamoDBAttributes.getLongAttribute;
import static sleeper.dynamodb.tools.DynamoDBAttributes.getNumberAttribute;
import static sleeper.dynamodb.tools.DynamoDBAttributes.getStringAttribute;
import static sleeper.dynamodb.tools.DynamoDBUtils.streamPagedItems;

/**
 * Stores a transaction log in DynamoDB and S3. If a transaction is too big to fit in a DynamoDB item, the body of the
 * transaction is stored in S3.
 */
public class DynamoDBTransactionLogStore implements TransactionLogStore {
    public static final Logger LOGGER = LoggerFactory.getLogger(DynamoDBTransactionLogStore.class);

    public static final String TABLE_ID = DynamoDBTransactionLogStateStore.TABLE_ID;
    public static final String TRANSACTION_NUMBER = DynamoDBTransactionLogStateStore.TRANSACTION_NUMBER;
    public static final String UPDATE_TIME = "UPDATE_TIME";
    public static final String TYPE = "TYPE";
    public static final String BODY = "BODY";
    public static final String BODY_S3_KEY = "BODY_S3_KEY";

    private final String transactionDescription;
    private final String logTableName;
    private final TableStatus sleeperTable;
    private final AmazonDynamoDB dynamoClient;
    private final S3TransactionBodyStore transactionBodyStore;
    private final TransactionSerDe serDe;

    /**
     * Creates an instance of this class to store file transactions for a given Sleeper table.
     *
     * @param  instanceProperties the instance properties
     * @param  tableProperties    the table properties
     * @param  dynamoClient       the DynamoDB client
     * @param  s3Client           the S3 client
     * @return                    the store
     */
    public static DynamoDBTransactionLogStore forFiles(
            InstanceProperties instanceProperties, TableProperties tableProperties,
            AmazonDynamoDB dynamoClient, AmazonS3 s3Client) {
        return new DynamoDBTransactionLogStore(
                "file", instanceProperties.get(TRANSACTION_LOG_FILES_TABLENAME),
                instanceProperties, tableProperties, dynamoClient, s3Client);
    }

    /**
     * Creates an instance of this class to store partition transactions for a given Sleeper table.
     *
     * @param  instanceProperties the instance properties
     * @param  tableProperties    the table properties
     * @param  dynamoClient       the DynamoDB client
     * @param  s3Client           the S3 client
     * @return                    the store
     */
    public static DynamoDBTransactionLogStore forPartitions(
            InstanceProperties instanceProperties, TableProperties tableProperties,
            AmazonDynamoDB dynamoClient, AmazonS3 s3Client) {
        return new DynamoDBTransactionLogStore(
                "partition", instanceProperties.get(TRANSACTION_LOG_PARTITIONS_TABLENAME),
                instanceProperties, tableProperties, dynamoClient, s3Client);
    }

    private DynamoDBTransactionLogStore(
            String transactionDescription, String logTableName,
            InstanceProperties instanceProperties, TableProperties tableProperties,
            AmazonDynamoDB dynamoClient, AmazonS3 s3Client) {
        this.transactionDescription = transactionDescription;
        this.logTableName = logTableName;
        this.sleeperTable = tableProperties.getStatus();
        this.dynamoClient = dynamoClient;
        this.transactionBodyStore = new S3TransactionBodyStore(instanceProperties, s3Client, TransactionSerDeProvider.forOneTable(tableProperties));
        this.serDe = new TransactionSerDe(tableProperties.getSchema());
    }

    @Override
    public void addTransaction(TransactionLogEntry entry) throws DuplicateTransactionNumberException {
        long transactionNumber = entry.getTransactionNumber();
        try {
            dynamoClient.putItem(new PutItemRequest()
                    .withTableName(logTableName)
                    .withItem(new DynamoDBRecordBuilder()
                            .string(TABLE_ID, sleeperTable.getTableUniqueId())
                            .number(TRANSACTION_NUMBER, transactionNumber)
                            .number(UPDATE_TIME, entry.getUpdateTime().toEpochMilli())
                            .string(TYPE, entry.getTransactionType().name())
                            .apply(builder -> entry.withSerialisedTransactionOrObjectKey(serDe,
                                    serialisedTransaction -> builder.string(BODY, serialisedTransaction),
                                    key -> builder.string(BODY_S3_KEY, key)))
                            .build())
                    .withConditionExpression("attribute_not_exists(#Number)")
                    .withExpressionAttributeNames(Map.of("#Number", TRANSACTION_NUMBER)));
        } catch (ConditionalCheckFailedException e) {
            throw new DuplicateTransactionNumberException(transactionNumber, e);
        }
    }

    @Override
    public Stream<TransactionLogEntry> readTransactions(TransactionLogRange range) {
        QueryRequest request = new QueryRequest()
                .withTableName(logTableName)
                .withConsistentRead(true)
                .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL);
        if (range.isMaxTransactionBounded()) {
            request.withKeyConditionExpression("#TableId = :table_id AND #Number BETWEEN :startInclusive AND :endInclusive")
                    .withExpressionAttributeNames(Map.of("#TableId", TABLE_ID, "#Number", TRANSACTION_NUMBER))
                    .withExpressionAttributeValues(new DynamoDBRecordBuilder()
                            .string(":table_id", sleeperTable.getTableUniqueId())
                            .number(":startInclusive", range.startInclusive())
                            .number(":endInclusive", range.endExclusive() - 1)
                            .build());
        } else {
            request.withKeyConditionExpression("#TableId = :table_id AND #Number >= :startInclusive")
                    .withExpressionAttributeNames(Map.of("#TableId", TABLE_ID, "#Number", TRANSACTION_NUMBER))
                    .withExpressionAttributeValues(new DynamoDBRecordBuilder()
                            .string(":table_id", sleeperTable.getTableUniqueId())
                            .number(":startInclusive", range.startInclusive())
                            .build());
        }
        return streamPagedItems(dynamoClient, request)
                .map(this::readTransaction);
    }

    @Override
    public void deleteTransactionsAtOrBefore(long maxTransactionNumber) {
        LOGGER.info("Deleting {} transactions from Sleeper table {}", transactionDescription, sleeperTable);
        TransactionLogDeletionTracker deletionTracker = new TransactionLogDeletionTracker(transactionDescription);
        streamPagedItems(dynamoClient, new QueryRequest()
                .withTableName(logTableName)
                .withConsistentRead(true)
                .withKeyConditionExpression("#TableId = :table_id AND #Number <= :number")
                .withExpressionAttributeNames(Map.of("#TableId", TABLE_ID, "#Number", TRANSACTION_NUMBER))
                .withExpressionAttributeValues(new DynamoDBRecordBuilder()
                        .string(":table_id", sleeperTable.getTableUniqueId())
                        .number(":number", maxTransactionNumber)
                        .build())
                .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL))
                .forEach(item -> {
                    long transactionNumber = getLongAttribute(item, TRANSACTION_NUMBER, 0L);
                    AttributeValue s3Key = item.get(BODY_S3_KEY);
                    if (s3Key != null) {
                        transactionBodyStore.delete(s3Key.getS());
                        deletionTracker.deletedLargeTransactionBody(transactionNumber);
                    }
                    dynamoClient.deleteItem(logTableName, getKey(item));
                    deletionTracker.deletedFromLog(transactionNumber);
                });
        LOGGER.info("Finished deletion. {}", deletionTracker.summary());
    }

    private TransactionLogEntry readTransaction(Map<String, AttributeValue> item) {
        long number = getLongAttribute(item, TRANSACTION_NUMBER, -1);
        Instant updateTime = getInstantAttribute(item, UPDATE_TIME);
        TransactionType type = readType(item);
        String bodyS3Key = getStringAttribute(item, BODY_S3_KEY);
        if (bodyS3Key != null) {
            return new TransactionLogEntry(number, updateTime, type, bodyS3Key);
        } else {
            String body = getStringAttribute(item, BODY);
            StateStoreTransaction<?> transaction = serDe.toTransaction(type, body);
            return new TransactionLogEntry(number, updateTime, transaction);
        }
    }

    private TransactionType readType(Map<String, AttributeValue> item) {
        String typeName = getStringAttribute(item, TYPE);
        try {
            return TransactionType.valueOf(typeName);
        } catch (RuntimeException e) {
            LOGGER.warn("Found unrecognised transaction type for table {} {} transaction {}: {}",
                    getStringAttribute(item, TABLE_ID), transactionDescription, getNumberAttribute(item, TRANSACTION_NUMBER), typeName);
            throw e;
        }
    }

    private static Map<String, AttributeValue> getKey(Map<String, AttributeValue> item) {
        return Map.of(TABLE_ID, item.get(TABLE_ID),
                TRANSACTION_NUMBER, item.get(TRANSACTION_NUMBER));
    }
}
