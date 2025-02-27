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
package sleeper.statestore.lambda.transaction;

import com.amazonaws.services.lambda.runtime.events.models.dynamodb.AttributeValue;
import com.amazonaws.services.lambda.runtime.events.models.dynamodb.Record;

import sleeper.core.statestore.transactionlog.log.TransactionLogEntry;
import sleeper.core.statestore.transactionlog.transaction.StateStoreTransaction;
import sleeper.core.statestore.transactionlog.transaction.TransactionSerDeProvider;
import sleeper.core.statestore.transactionlog.transaction.TransactionType;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static sleeper.statestore.transactionlog.DynamoDBTransactionLogStore.BODY;
import static sleeper.statestore.transactionlog.DynamoDBTransactionLogStore.BODY_S3_KEY;
import static sleeper.statestore.transactionlog.DynamoDBTransactionLogStore.TABLE_ID;
import static sleeper.statestore.transactionlog.DynamoDBTransactionLogStore.TRANSACTION_NUMBER;
import static sleeper.statestore.transactionlog.DynamoDBTransactionLogStore.TYPE;
import static sleeper.statestore.transactionlog.DynamoDBTransactionLogStore.UPDATE_TIME;

/**
 * Maps between transaction log entries and their DynamoDB Stream event objects.
 */
public class DynamoDBStreamTransactionLogEntryMapper {

    private final TransactionSerDeProvider serDeProvider;

    public DynamoDBStreamTransactionLogEntryMapper(TransactionSerDeProvider serDeProvider) {
        this.serDeProvider = serDeProvider;
    }

    /**
     * Reads a DynamoDB Stream record for a transaction log entry.
     *
     * @param  record the record
     * @return        the log entry
     */
    public TransactionLogEntryHandle toTransactionLogEntry(Record record) {
        return toTransactionLogEntryOrThrow(record);
    }

    /**
     * Reads a list of DynamoDB Stream records to produce a stream of transaction log entries, wrapped as handles.
     *
     * @param  records a list of records
     * @return         a stream of log entries
     */
    public Stream<TransactionLogEntryHandle> toTransactionLogEntries(List<? extends Record> records) {
        return records.stream().map(record -> toTransactionLogEntry(record));
    }

    private TransactionLogEntryHandle toTransactionLogEntryOrThrow(Record record) {
        String sequenceNumber = record.getDynamodb().getSequenceNumber();
        Map<String, AttributeValue> image = record.getDynamodb().getNewImage();
        String tableId = image.get(TABLE_ID).getS();
        long transactionNumber = Long.parseLong(image.get(TRANSACTION_NUMBER).getN());
        long updateTimeMillis = Long.parseLong(image.get(UPDATE_TIME).getN());
        Instant updateTime = Instant.ofEpochMilli(updateTimeMillis);
        TransactionType transactionType = TransactionType.valueOf(image.get(TYPE).getS());
        AttributeValue body = image.get(BODY);
        if (body != null) {
            StateStoreTransaction<?> transaction = serDeProvider.getByTableId(tableId).toTransaction(transactionType, body.getS());
            return new TransactionLogEntryHandle(tableId, sequenceNumber,
                    new TransactionLogEntry(transactionNumber, updateTime, transaction));
        } else {
            String bodyKey = image.get(BODY_S3_KEY).getS();
            return new TransactionLogEntryHandle(tableId, sequenceNumber,
                    new TransactionLogEntry(transactionNumber, updateTime, transactionType, bodyKey));
        }
    }
}
