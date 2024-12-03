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
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.DeleteObjectRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.statestore.transactionlog.DuplicateTransactionNumberException;
import sleeper.core.statestore.transactionlog.StateStoreTransaction;
import sleeper.core.statestore.transactionlog.TransactionLogDeletionTracker;
import sleeper.core.statestore.transactionlog.TransactionLogEntry;
import sleeper.core.statestore.transactionlog.TransactionLogStore;
import sleeper.core.statestore.transactionlog.transactions.TransactionSerDe;
import sleeper.core.statestore.transactionlog.transactions.TransactionType;
import sleeper.core.table.TableStatus;
import sleeper.core.util.LoggedDuration;
import sleeper.dynamodb.tools.DynamoDBRecordBuilder;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
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

    private static final String TABLE_ID = DynamoDBTransactionLogStateStore.TABLE_ID;
    private static final String TRANSACTION_NUMBER = DynamoDBTransactionLogStateStore.TRANSACTION_NUMBER;
    private static final String UPDATE_TIME = "UPDATE_TIME";
    private static final String TYPE = "TYPE";
    private static final String BODY = "BODY";
    private static final String BODY_S3_KEY = "BODY_S3_KEY";

    private final String transactionDescription;
    private final String logTableName;
    private final String dataBucket;
    private final String transactionsPrefix;
    private final TableStatus sleeperTable;
    private final AmazonDynamoDB dynamo;
    private final AmazonS3 s3;
    private final TransactionSerDe serDe;

    @SuppressWarnings("checkstyle:MissingJavadocMethod")
    public static DynamoDBTransactionLogStore forFiles(
            InstanceProperties instanceProperties, TableProperties tableProperties,
            AmazonDynamoDB dynamo, AmazonS3 s3) {
        return new DynamoDBTransactionLogStore(
                "file", instanceProperties.get(TRANSACTION_LOG_FILES_TABLENAME),
                instanceProperties, tableProperties, dynamo, s3);
    }

    @SuppressWarnings("checkstyle:MissingJavadocMethod")
    public static DynamoDBTransactionLogStore forPartitions(
            InstanceProperties instanceProperties, TableProperties tableProperties,
            AmazonDynamoDB dynamo, AmazonS3 s3) {
        return new DynamoDBTransactionLogStore(
                "partition", instanceProperties.get(TRANSACTION_LOG_PARTITIONS_TABLENAME),
                instanceProperties, tableProperties, dynamo, s3);
    }

    private DynamoDBTransactionLogStore(
            String transactionDescription, String logTableName,
            InstanceProperties instanceProperties, TableProperties tableProperties,
            AmazonDynamoDB dynamo, AmazonS3 s3) {
        this.transactionDescription = transactionDescription;
        this.logTableName = logTableName;
        this.dataBucket = instanceProperties.get(DATA_BUCKET);
        this.sleeperTable = tableProperties.getStatus();
        this.transactionsPrefix = sleeperTable.getTableUniqueId() + "/statestore/transactions/";
        this.dynamo = dynamo;
        this.s3 = s3;
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
                            .string(TABLE_ID, sleeperTable.getTableUniqueId())
                            .number(TRANSACTION_NUMBER, transactionNumber)
                            .number(UPDATE_TIME, entry.getUpdateTime().toEpochMilli())
                            .string(TYPE, TransactionType.getType(transaction).name())
                            .apply(builder -> setBodyDirectlyOrInS3IfTooBig(builder, entry, serDe.toJson(transaction)))
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
                        .string(":table_id", sleeperTable.getTableUniqueId())
                        .number(":number", lastTransactionNumber)
                        .build())
                .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL))
                .map(this::readTransaction);
    }

    @Override
    public void deleteTransactionsAtOrBefore(long maxTransactionNumber) {
        LOGGER.info("Deleting {} transactions from Sleeper table {}", transactionDescription, sleeperTable);
        TransactionLogDeletionTracker deletionTracker = new TransactionLogDeletionTracker(transactionDescription);
        streamPagedItems(dynamo, new QueryRequest()
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
                    boolean deleteFromS3 = item.get(BODY_S3_KEY) != null;
                    if (deleteFromS3) {
                        s3.deleteObject(new DeleteObjectRequest(dataBucket, item.get(BODY_S3_KEY).getS()));
                        deletionTracker.deletedLargeTransactionBody(transactionNumber);
                    }
                    dynamo.deleteItem(logTableName, getKey(item));
                    deletionTracker.deletedFromLog(transactionNumber);
                });
        LOGGER.info("Finished deletion. {}", deletionTracker.summary());
    }

    private void setBodyDirectlyOrInS3IfTooBig(DynamoDBRecordBuilder builder, TransactionLogEntry entry, String body) {
        // Max DynamoDB item size is 400KB. Leave some space for the rest of the item.
        // DynamoDB uses UTF-8 encoding for strings.
        long lengthInBytes = body.getBytes(StandardCharsets.UTF_8).length;
        if (lengthInBytes < 1024 * 350) {
            builder.string(BODY, body);
        } else {
            // Use a random UUID to avoid conflicting when another process is adding a transaction with the same number
            String key = transactionsPrefix + entry.getTransactionNumber() + "-" + UUID.randomUUID().toString() + ".json";
            LOGGER.info("Found large {} transaction, saving to data bucket instead of DynamoDB at {}", transactionDescription, key);
            builder.string(BODY_S3_KEY, key);
            Instant startTime = Instant.now();
            s3.putObject(dataBucket, key, body);
            LOGGER.info("Saved to S3 in {}", LoggedDuration.withShortOutput(startTime, Instant.now()));
        }
    }

    private TransactionLogEntry readTransaction(Map<String, AttributeValue> item) {
        long number = getLongAttribute(item, TRANSACTION_NUMBER, -1);
        Instant updateTime = getInstantAttribute(item, UPDATE_TIME);
        TransactionType type = readType(item);
        String body;
        String bodyS3Key = getStringAttribute(item, BODY_S3_KEY);
        if (bodyS3Key != null) {
            LOGGER.debug("Reading large {} transaction from data bucket at {}", transactionDescription, bodyS3Key);
            body = s3.getObjectAsString(dataBucket, bodyS3Key);
        } else {
            body = getStringAttribute(item, BODY);
        }
        StateStoreTransaction<?> transaction = serDe.toTransaction(type, body);
        return new TransactionLogEntry(number, updateTime, transaction);
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
