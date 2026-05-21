/*
 * Copyright 2022-2026 Crown Copyright
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.ReturnConsumedCapacity;
import software.amazon.awssdk.services.s3.S3Client;

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
import java.util.Objects;
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
    private final DynamoDbClient dynamoClient;
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
            DynamoDbClient dynamoClient, S3Client s3Client) {
        return builder()
                .transactionDescription("file")
                .logTableName(instanceProperties.get(TRANSACTION_LOG_FILES_TABLENAME))
                .instanceProperties(instanceProperties)
                .tableProperties(tableProperties)
                .dynamoClient(dynamoClient)
                .s3Client(s3Client)
                .build();
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
            DynamoDbClient dynamoClient, S3Client s3Client) {
        return builder()
                .transactionDescription("partition")
                .logTableName(instanceProperties.get(TRANSACTION_LOG_PARTITIONS_TABLENAME))
                .instanceProperties(instanceProperties)
                .tableProperties(tableProperties)
                .dynamoClient(dynamoClient)
                .s3Client(s3Client)
                .build();
    }

    public static Builder builder() {
        return new Builder();
    }

    private DynamoDBTransactionLogStore(Builder builder) {
        InstanceProperties instanceProperties = Objects.requireNonNull(builder.instanceProperties, "instanceProperties must not be null");
        TableProperties tableProperties = Objects.requireNonNull(builder.tableProperties, "tableProperties must not be null");
        S3Client s3Client = Objects.requireNonNull(builder.s3Client, "s3Client must not be null");
        this.transactionDescription = Objects.requireNonNull(builder.transactionDescription, "transactionDescription must not be null");
        this.logTableName = Objects.requireNonNull(builder.logTableName, "logTableName must not be null");
        this.sleeperTable = tableProperties.getStatus();
        this.dynamoClient = Objects.requireNonNull(builder.dynamoClient, "dynamoClient must not be null");
        this.transactionBodyStore = new S3TransactionBodyStore(instanceProperties, s3Client,
                builder.isForDeleteOnly ? null : TransactionSerDeProvider.forOneTable(tableProperties));
        this.serDe = builder.isForDeleteOnly ? null : new TransactionSerDe(tableProperties.getSchema());
    }

    @Override
    public void addTransaction(TransactionLogEntry entry) throws DuplicateTransactionNumberException {
        long transactionNumber = entry.getTransactionNumber();
        try {
            dynamoClient.putItem(PutItemRequest.builder()
                    .tableName(logTableName)
                    .item(new DynamoDBRecordBuilder()
                            .string(TABLE_ID, sleeperTable.getTableUniqueId())
                            .number(TRANSACTION_NUMBER, transactionNumber)
                            .number(UPDATE_TIME, entry.getUpdateTime().toEpochMilli())
                            .string(TYPE, entry.getTransactionType().name())
                            .apply(builder -> entry.withSerialisedTransactionOrObjectKey(serDe,
                                    serialisedTransaction -> builder.string(BODY, serialisedTransaction),
                                    key -> builder.string(BODY_S3_KEY, key)))
                            .build())
                    .conditionExpression("attribute_not_exists(#Number)")
                    .expressionAttributeNames(Map.of("#Number", TRANSACTION_NUMBER)).build());
        } catch (ConditionalCheckFailedException e) {
            throw new DuplicateTransactionNumberException(transactionNumber, e);
        }
    }

    @Override
    public Stream<TransactionLogEntry> readTransactions(TransactionLogRange range) {
        QueryRequest.Builder request = QueryRequest.builder()
                .tableName(logTableName)
                .consistentRead(true)
                .returnConsumedCapacity(ReturnConsumedCapacity.TOTAL);
        if (range.isMaxTransactionBounded()) {
            request.keyConditionExpression("#TableId = :table_id AND #Number BETWEEN :startInclusive AND :endInclusive")
                    .expressionAttributeNames(Map.of("#TableId", TABLE_ID, "#Number", TRANSACTION_NUMBER))
                    .expressionAttributeValues(new DynamoDBRecordBuilder()
                            .string(":table_id", sleeperTable.getTableUniqueId())
                            .number(":startInclusive", range.startInclusive())
                            .number(":endInclusive", range.endExclusive() - 1)
                            .build());
        } else {
            request.keyConditionExpression("#TableId = :table_id AND #Number >= :startInclusive")
                    .expressionAttributeNames(Map.of("#TableId", TABLE_ID, "#Number", TRANSACTION_NUMBER))
                    .expressionAttributeValues(new DynamoDBRecordBuilder()
                            .string(":table_id", sleeperTable.getTableUniqueId())
                            .number(":startInclusive", range.startInclusive())
                            .build());
        }
        return streamPagedItems(dynamoClient, request.build())
                .map(this::readTransaction);
    }

    @Override
    public void deleteTransactionsAtOrBefore(long maxTransactionNumber) {
        LOGGER.info("Deleting {} transactions from Sleeper table {}", transactionDescription, sleeperTable);
        TransactionLogDeletionTracker deletionTracker = new TransactionLogDeletionTracker(transactionDescription);
        streamPagedItems(dynamoClient, QueryRequest.builder()
                .tableName(logTableName)
                .consistentRead(true)
                .keyConditionExpression("#TableId = :table_id AND #Number <= :number")
                .expressionAttributeNames(Map.of("#TableId", TABLE_ID, "#Number", TRANSACTION_NUMBER))
                .expressionAttributeValues(new DynamoDBRecordBuilder()
                        .string(":table_id", sleeperTable.getTableUniqueId())
                        .number(":number", maxTransactionNumber)
                        .build())
                .returnConsumedCapacity(ReturnConsumedCapacity.TOTAL).build())
                .forEach(item -> {
                    long transactionNumber = getLongAttribute(item, TRANSACTION_NUMBER, 0L);
                    AttributeValue s3Key = item.get(BODY_S3_KEY);
                    if (s3Key != null) {
                        transactionBodyStore.delete(s3Key.s());
                        deletionTracker.deletedLargeTransactionBody(transactionNumber);
                    }
                    dynamoClient.deleteItem(DeleteItemRequest.builder()
                            .tableName(logTableName)
                            .key(getKey(item))
                            .build());
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

    /**
     * Builder for Dynamo DB transaction log store.
     */
    public static final class Builder {
        String transactionDescription;
        String logTableName;
        InstanceProperties instanceProperties;
        TableProperties tableProperties;
        DynamoDbClient dynamoClient;
        S3Client s3Client;
        boolean isForDeleteOnly = false;

        private Builder() {

        }

        /**
         * Sets the transaction description.
         *
         * @param  transactionDescription the description of the transaction
         * @return                        the builder
         */
        public Builder transactionDescription(String transactionDescription) {
            this.transactionDescription = transactionDescription;
            return this;
        }

        /**
         * Sets the log table name.
         *
         * @param  logTableName the logTableName
         * @return              the builder
         */
        public Builder logTableName(String logTableName) {
            this.logTableName = logTableName;
            return this;
        }

        /**
         * Sets the instance properties.
         *
         * @param  instanceProperties the instance properties
         * @return                    the builder
         */
        public Builder instanceProperties(InstanceProperties instanceProperties) {
            this.instanceProperties = instanceProperties;
            return this;
        }

        /**
         * Sets the table properties.
         *
         * @param  tableProperties the table properties
         * @return                 the builder
         */
        public Builder tableProperties(TableProperties tableProperties) {
            this.tableProperties = tableProperties;
            return this;
        }

        /**
         * Sets the Dynamo DB client.
         *
         * @param  dynamoClient the Dynamo DB client
         * @return              the builder
         */
        public Builder dynamoClient(DynamoDbClient dynamoClient) {
            this.dynamoClient = dynamoClient;
            return this;
        }

        /**
         * Sets the S3 client.
         *
         * @param  s3Client the S3 client
         * @return          the builder
         */
        public Builder s3Client(S3Client s3Client) {
            this.s3Client = s3Client;
            return this;
        }

        /**
         * Sets if this log store is only going to be used for deletion.
         * If it is only used for deletion the the SerDes don't need to be created.
         * This allows us to delete tables even with invalid schema in S3.
         *
         * @param  isForDeleteOnly denotes is the log store will only be used for deletion
         * @return                 the builder
         */
        public Builder isForDeleteOnly(boolean isForDeleteOnly) {
            this.isForDeleteOnly = isForDeleteOnly;
            return this;
        }

        public DynamoDBTransactionLogStore build() {
            return new DynamoDBTransactionLogStore(this);
        }
    }
}
