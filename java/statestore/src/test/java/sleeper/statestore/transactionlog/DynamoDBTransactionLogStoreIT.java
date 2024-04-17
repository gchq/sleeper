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

import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.google.gson.JsonSyntaxException;
import org.junit.jupiter.api.Test;

import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TableProperty;
import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.transactionlog.DuplicateTransactionNumberException;
import sleeper.core.statestore.transactionlog.StateStoreTransaction;
import sleeper.core.statestore.transactionlog.TransactionLogEntry;
import sleeper.core.statestore.transactionlog.TransactionLogStore;
import sleeper.core.statestore.transactionlog.transactions.ClearFilesTransaction;
import sleeper.core.statestore.transactionlog.transactions.DeleteFilesTransaction;
import sleeper.core.statestore.transactionlog.transactions.TransactionType;
import sleeper.dynamodb.tools.DynamoDBRecordBuilder;

import java.time.Instant;
import java.util.List;

import static java.util.stream.Collectors.toUnmodifiableList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.FILE_TRANSACTION_LOG_TABLENAME;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.PARTITION_TRANSACTION_LOG_TABLENAME;
import static sleeper.configuration.properties.table.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;
import static sleeper.statestore.transactionlog.DynamoDBTransactionLogStateStore.TABLE_ID;
import static sleeper.statestore.transactionlog.DynamoDBTransactionLogStateStore.TRANSACTION_NUMBER;

public class DynamoDBTransactionLogStoreIT extends TransactionLogStateStoreTestBase {

    private static final Instant DEFAULT_UPDATE_TIME = Instant.parse("2024-04-09T14:01:01Z");
    private final Schema schema = schemaWithKey("key");
    private final TableProperties tableProperties = createTestTableProperties(instanceProperties, schema);
    private final TransactionLogStore fileLogStore = fileLogStore();

    @Test
    void shouldAddFirstTransaction() throws Exception {
        // Given
        TransactionLogEntry entry = logEntry(1, new ClearFilesTransaction());

        // When
        fileLogStore.addTransaction(entry);

        // Then
        assertThat(fileLogStore.readTransactionsAfter(0))
                .containsExactly(entry);
    }

    @Test
    void shouldFailToAddTransactionWhenOneAlreadyExistsWithSameNumber() throws Exception {
        // Given
        TransactionLogEntry entry1 = logEntry(1,
                new DeleteFilesTransaction(List.of("file1.parquet")));
        TransactionLogEntry entry2 = logEntry(1,
                new DeleteFilesTransaction(List.of("file2.parquet")));
        fileLogStore.addTransaction(entry1);

        // When / Then
        assertThatThrownBy(() -> fileLogStore.addTransaction(entry2))
                .isInstanceOf(DuplicateTransactionNumberException.class)
                .hasMessage("Unread transaction found. Adding transaction number 1, but it already exists.");
        assertThat(fileLogStore.readTransactionsAfter(0))
                .containsExactly(entry1);
    }

    @Test
    void shouldFailLoadingTransactionWithUnrecognisedType() throws Exception {
        // Given
        dynamoDBClient.putItem(new PutItemRequest()
                .withTableName(instanceProperties.get(FILE_TRANSACTION_LOG_TABLENAME))
                .withItem(new DynamoDBRecordBuilder()
                        .string(TABLE_ID, tableProperties.get(TableProperty.TABLE_ID))
                        .number(TRANSACTION_NUMBER, 1)
                        .string("TYPE", "UNRECOGNISED_TRANSACTION")
                        .string("BODY", "{}")
                        .build()));

        // When / Then
        assertThatThrownBy(() -> fileLogStore.readTransactionsAfter(0).findAny())
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void shouldFailLoadingTransactionWithRecognisedTypeButInvalidJson() throws Exception {
        // Given
        dynamoDBClient.putItem(new PutItemRequest()
                .withTableName(instanceProperties.get(FILE_TRANSACTION_LOG_TABLENAME))
                .withItem(new DynamoDBRecordBuilder()
                        .string(TABLE_ID, tableProperties.get(TableProperty.TABLE_ID))
                        .number(TRANSACTION_NUMBER, 1)
                        .string("TYPE", TransactionType.ADD_FILES.name())
                        .string("BODY", "{")
                        .build()));

        // When / Then
        assertThatThrownBy(() -> fileLogStore.readTransactionsAfter(0).findAny())
                .isInstanceOf(JsonSyntaxException.class);
    }

    @Test
    void shouldStoreFileUpdateTimeInLogEntry() throws Exception {
        // Given
        Instant updateTime = Instant.parse("2024-04-09T14:19:01Z");
        StateStore stateStore = new DynamoDBTransactionLogStateStore(instanceProperties, tableProperties, dynamoDBClient);
        stateStore.fixFileUpdateTime(updateTime);
        PartitionTree partitions = new PartitionsBuilder(schema).singlePartition("root").buildTree();
        stateStore.initialise(partitions.getAllPartitions());
        stateStore.addFile(FileReferenceFactory.from(partitions).rootFile(100));

        // When
        List<TransactionLogEntry> entries = fileLogStore.readTransactionsAfter(0).collect(toUnmodifiableList());

        // Then
        assertThat(entries)
                .extracting(TransactionLogEntry::getUpdateTime)
                .containsExactly(updateTime);
    }

    @Test
    void shouldStorePartitionUpdateTimeInLogEntry() throws Exception {
        // Given
        Instant updateTime = Instant.parse("2024-04-09T14:19:01Z");
        StateStore stateStore = new DynamoDBTransactionLogStateStore(instanceProperties, tableProperties, dynamoDBClient);
        stateStore.fixPartitionUpdateTime(updateTime);
        stateStore.initialise();

        // When
        List<TransactionLogEntry> entries = partitionLogStore().readTransactionsAfter(0).collect(toUnmodifiableList());

        // Then
        assertThat(entries)
                .extracting(TransactionLogEntry::getUpdateTime)
                .containsExactly(updateTime);
    }

    private TransactionLogEntry logEntry(long number, StateStoreTransaction<?> transaction) {
        return new TransactionLogEntry(number, DEFAULT_UPDATE_TIME, transaction);
    }

    private TransactionLogStore fileLogStore() {
        return new DynamoDBTransactionLogStore(
                instanceProperties.get(FILE_TRANSACTION_LOG_TABLENAME),
                tableProperties, dynamoDBClient);
    }

    private TransactionLogStore partitionLogStore() {
        return new DynamoDBTransactionLogStore(
                instanceProperties.get(PARTITION_TRANSACTION_LOG_TABLENAME),
                tableProperties, dynamoDBClient);
    }
}
