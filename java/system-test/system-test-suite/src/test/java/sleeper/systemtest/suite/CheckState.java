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
package sleeper.systemtest.suite;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

import sleeper.configuration.properties.S3InstanceProperties;
import sleeper.configuration.properties.S3TableProperties;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.transactionlog.log.TransactionBodyStore;
import sleeper.core.statestore.transactionlog.log.TransactionLogEntry;
import sleeper.core.statestore.transactionlog.log.TransactionLogRange;
import sleeper.core.statestore.transactionlog.log.TransactionLogStore;
import sleeper.core.statestore.transactionlog.state.StateStoreFiles;
import sleeper.core.statestore.transactionlog.transaction.StateStoreTransaction;
import sleeper.core.statestore.transactionlog.transaction.TransactionSerDeProvider;
import sleeper.statestore.transactionlog.DynamoDBTransactionLogStore;
import sleeper.statestore.transactionlog.S3TransactionBodyStore;

import java.util.LinkedList;
import java.util.List;

import static java.util.stream.Collectors.toCollection;
import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;

public class CheckState {

    private final List<Entry> filesLog;

    private CheckState(List<Entry> filesLog) {
        this.filesLog = filesLog;
    }

    public static void main(String[] args) {
        CheckState check = load();
        StateStoreFiles state = check.filesStateAtTransaction(10);
        long totalRecords = state.references().mapToLong(FileReference::getNumberOfRecords).sum();
        assertThat(totalRecords).isEqualTo(10_000_000_000L);
    }

    public static CheckState load(TableProperties tableProperties, TransactionLogStore logStore, TransactionBodyStore bodyStore) {
        List<Entry> log = logStore
                .readTransactions(TransactionLogRange.fromMinimum(1))
                .map(entry -> {
                    StateStoreTransaction<?> transaction = entry.getTransactionOrLoadFromPointer(tableProperties.get(TABLE_ID), bodyStore);
                    return new Entry(entry, transaction);
                })
                .collect(toCollection(LinkedList::new));
        return new CheckState(log);
    }

    public StateStoreFiles filesStateAtTransaction(long transactionNumber) {
        StateStoreFiles state = new StateStoreFiles();
        for (Entry entry : filesLog) {
            entry.apply(state);
            if (entry.transactionNumber() == transactionNumber) {
                return state;
            }
        }
        throw new IllegalArgumentException("Transaction number not found: " + transactionNumber);
    }

    private record Entry(TransactionLogEntry original, StateStoreTransaction<?> transaction) {

        public <S> void apply(S state) {
            StateStoreTransaction<S> transaction = castTransaction();
            transaction.apply(state, original.getUpdateTime());
        }

        public <S, T extends StateStoreTransaction<S>> T castTransaction() {
            return (T) transaction;
        }

        public long transactionNumber() {
            return original.getTransactionNumber();
        }
    }

    private static CheckState load() {
        AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();
        AmazonDynamoDB dynamoClient = AmazonDynamoDBClientBuilder.defaultClient();
        InstanceProperties properties = S3InstanceProperties.loadFromBucket(s3Client, System.getenv("CONFIG_BUCKET"));
        TableProperties tableProperties = S3TableProperties.createProvider(properties, s3Client, dynamoClient).getById(System.getenv("TABLE_ID"));
        DynamoDBTransactionLogStore filesLogStore = DynamoDBTransactionLogStore.forFiles(properties, tableProperties, dynamoClient, s3Client);
        S3TransactionBodyStore bodyStore = new S3TransactionBodyStore(properties, s3Client, TransactionSerDeProvider.forOneTable(tableProperties));
        return load(tableProperties, filesLogStore, bodyStore);
    }
}
