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

import com.google.gson.JsonSyntaxException;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.s3.model.S3Object;

import sleeper.core.partition.Partition;
import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.partition.PartitionsBuilderSplitsFirst;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TableProperty;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.StringType;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.transactionlog.log.DuplicateTransactionNumberException;
import sleeper.core.statestore.transactionlog.log.TransactionLogEntry;
import sleeper.core.statestore.transactionlog.log.TransactionLogStore;
import sleeper.core.statestore.transactionlog.transaction.StateStoreTransaction;
import sleeper.core.statestore.transactionlog.transaction.TransactionType;
import sleeper.core.statestore.transactionlog.transaction.impl.ClearFilesTransaction;
import sleeper.core.statestore.transactionlog.transaction.impl.DeleteFilesTransaction;
import sleeper.dynamodb.tools.DynamoDBRecordBuilder;

import java.time.Instant;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.TRANSACTION_LOG_FILES_TABLENAME;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.schema.SchemaTestHelper.createSchemaWithKey;
import static sleeper.core.statestore.FileReferenceTestData.DEFAULT_UPDATE_TIME;
import static sleeper.core.statestore.testutils.StateStoreUpdatesWrapper.update;
import static sleeper.core.statestore.transactionlog.log.TransactionLogRange.toUpdateLocalStateAt;
import static sleeper.core.statestore.transactionlog.log.TransactionLogRange.toUpdateLocalStateToApply;
import static sleeper.statestore.transactionlog.DynamoDBTransactionLogStateStore.TABLE_ID;
import static sleeper.statestore.transactionlog.DynamoDBTransactionLogStateStore.TRANSACTION_NUMBER;

public class DynamoDBTransactionLogStoreIT extends TransactionLogStateStoreTestBase {

    private final Schema schema = createSchemaWithKey("key", new StringType());
    private final TableProperties tableProperties = createTestTableProperties(instanceProperties, schema);
    private final TransactionLogStore fileLogStore = fileLogStore();
    private final TransactionLogStore partitionLogStore = partitionLogStore();

    @Test
    void shouldAddFirstTransaction() throws Exception {
        // Given
        TransactionLogEntry entry = logEntry(1, new ClearFilesTransaction());

        // When
        fileLogStore.addTransaction(entry);

        // Then
        assertThat(fileLogStore.readTransactions(toUpdateLocalStateAt(0)))
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
        assertThat(fileLogStore.readTransactions(toUpdateLocalStateAt(0)))
                .containsExactly(entry1);
    }

    @Test
    void shouldFailLoadingTransactionWithUnrecognisedType() throws Exception {
        // Given
        dynamoClientV2.putItem(PutItemRequest.builder()
                .tableName(instanceProperties.get(TRANSACTION_LOG_FILES_TABLENAME))
                .item(new DynamoDBRecordBuilder()
                        .string(TABLE_ID, tableProperties.get(TableProperty.TABLE_ID))
                        .number(TRANSACTION_NUMBER, 1)
                        .string("TYPE", "UNRECOGNISED_TRANSACTION")
                        .string("BODY", "{}")
                        .build())
                .build());

        // When / Then
        assertThatThrownBy(() -> fileLogStore.readTransactions(toUpdateLocalStateAt(0)).findAny())
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void shouldFailLoadingTransactionWithRecognisedTypeButInvalidJson() throws Exception {
        // Given
        dynamoClientV2.putItem(PutItemRequest.builder()
                .tableName(instanceProperties.get(TRANSACTION_LOG_FILES_TABLENAME))
                .item(new DynamoDBRecordBuilder()
                        .string(TABLE_ID, tableProperties.get(TableProperty.TABLE_ID))
                        .number(TRANSACTION_NUMBER, 1)
                        .string("TYPE", TransactionType.ADD_FILES.name())
                        .string("BODY", "{")
                        .build())
                .build());

        // When / Then
        assertThatThrownBy(() -> fileLogStore.readTransactions(toUpdateLocalStateAt(0)).findAny())
                .isInstanceOf(JsonSyntaxException.class);
    }

    @Test
    void shouldStoreFileUpdateTimeInLogEntry() throws Exception {
        // Given
        Instant updateTime = Instant.parse("2024-04-09T14:19:01Z");
        StateStore stateStore = createStateStore(tableProperties);
        stateStore.fixFileUpdateTime(updateTime);
        PartitionTree partitions = new PartitionsBuilder(schema).singlePartition("root").buildTree();
        update(stateStore).initialise(partitions.getAllPartitions());
        update(stateStore).addFile(FileReferenceFactory.from(partitions).rootFile(100));

        // When
        List<TransactionLogEntry> entries = fileLogStore.readTransactions(toUpdateLocalStateAt(0)).toList();

        // Then
        assertThat(entries)
                .extracting(TransactionLogEntry::getUpdateTime)
                .containsExactly(updateTime);
    }

    @Test
    void shouldStorePartitionUpdateTimeInLogEntry() throws Exception {
        // Given
        Instant updateTime = Instant.parse("2024-04-09T14:19:01Z");
        StateStore stateStore = createStateStore(tableProperties);
        stateStore.fixPartitionUpdateTime(updateTime);
        update(stateStore).initialise(schema);

        // When
        List<TransactionLogEntry> entries = partitionLogStore.readTransactions(toUpdateLocalStateAt(0)).toList();

        // Then
        assertThat(entries)
                .extracting(TransactionLogEntry::getUpdateTime)
                .containsExactly(updateTime);
    }

    @Test
    void shouldDeleteTransactionsAtOrBeforeNumber() throws Exception {
        // Given
        Instant updateTime = Instant.parse("2024-04-09T14:19:01Z");
        StateStore stateStore = createStateStore(tableProperties);
        stateStore.fixFileUpdateTime(updateTime);
        update(stateStore).clearFileData();
        update(stateStore).clearFileData();

        // When
        fileLogStore.deleteTransactionsAtOrBefore(1);

        // Then
        assertThat(fileLogStore.readTransactions(toUpdateLocalStateAt(0))).containsExactly(
                new TransactionLogEntry(2, updateTime, new ClearFilesTransaction()));
    }

    @Test
    void shouldNotDeleteTransactionsWhenNoneAtOrBeforeNumber() throws Exception {
        // Given
        Instant updateTime = Instant.parse("2024-04-09T14:19:01Z");
        StateStore stateStore = createStateStore(tableProperties);
        stateStore.fixFileUpdateTime(updateTime);
        update(stateStore).clearFileData();
        update(stateStore).clearFileData();

        // When
        fileLogStore.deleteTransactionsAtOrBefore(0);

        // Then
        assertThat(fileLogStore.readTransactions(toUpdateLocalStateAt(0))).containsExactly(
                new TransactionLogEntry(1, updateTime, new ClearFilesTransaction()),
                new TransactionLogEntry(2, updateTime, new ClearFilesTransaction()));
    }

    @Test
    void shouldDeleteAllTransactionsWhenAllAtOrBeforeNumber() throws Exception {
        // Given
        Instant updateTime = Instant.parse("2024-04-09T14:19:01Z");
        StateStore stateStore = createStateStore(tableProperties);
        stateStore.fixFileUpdateTime(updateTime);
        update(stateStore).clearFileData();
        update(stateStore).clearFileData();

        // When
        fileLogStore.deleteTransactionsAtOrBefore(2);

        // Then
        assertThat(fileLogStore.readTransactions(toUpdateLocalStateAt(0))).isEmpty();
    }

    @Test
    void shouldStoreTransactionInS3() throws Exception {
        // When we add a transaction too large to fit in a DynamoDB item
        List<String> leafIds = IntStream.range(0, 1000)
                .mapToObj(i -> "" + i)
                .collect(Collectors.toList());
        List<Object> splitPoints = LongStream.range(1, 1000)
                .mapToObj(i -> "split" + String.format("%04d", i))
                .collect(Collectors.toList());
        List<Partition> partitions = PartitionsBuilderSplitsFirst
                .leavesWithSplits(schema, leafIds, splitPoints)
                .anyTreeJoiningAllLeaves().buildList();
        Instant updateTime = Instant.parse("2024-04-09T14:19:01Z");
        StateStore stateStore = createStateStore(tableProperties);
        stateStore.fixPartitionUpdateTime(updateTime);
        update(stateStore).initialise(partitions);

        // Then the transaction is held in S3
        String file = singleFileInDataBucket();
        assertThat(partitionLogStore.readTransactions(toUpdateLocalStateAt(0))).containsExactly(
                new TransactionLogEntry(1, updateTime, TransactionType.INITIALISE_PARTITIONS, file));
    }

    @Test
    void shouldDeleteTransactionStoredInS3() throws Exception {
        // Given a transaction too large to fit in a DynamoDB item
        List<String> leafIds = IntStream.range(0, 1000)
                .mapToObj(i -> "" + i)
                .collect(Collectors.toList());
        List<Object> splitPoints = LongStream.range(1, 1000)
                .mapToObj(i -> "split" + String.format("%04d", i))
                .collect(Collectors.toList());
        Instant updateTime = Instant.parse("2024-04-09T14:19:01Z");
        StateStore stateStore = createStateStore(tableProperties);
        stateStore.fixPartitionUpdateTime(updateTime);
        update(stateStore).initialise(PartitionsBuilderSplitsFirst
                .leavesWithSplits(schema, leafIds, splitPoints)
                .anyTreeJoiningAllLeaves().buildList());

        // When
        partitionLogStore.deleteTransactionsAtOrBefore(1);

        // Then
        assertThat(partitionLogStore.readTransactions(toUpdateLocalStateAt(0))).isEmpty();
        assertThat(filesInDataBucket()).isEmpty();
    }

    @Test
    void shouldReturnTransactionsInBetweenTwoEntries() throws Exception {
        // Given
        TransactionLogEntry entry1 = logEntry(1, new ClearFilesTransaction());
        TransactionLogEntry entry2 = logEntry(2, new ClearFilesTransaction());
        TransactionLogEntry entry3 = logEntry(3, new ClearFilesTransaction());
        fileLogStore.addTransaction(entry1);
        fileLogStore.addTransaction(entry2);
        fileLogStore.addTransaction(entry3);

        // When / Then
        assertThat(fileLogStore.readTransactions(toUpdateLocalStateToApply(1, 3)))
                .containsExactly(entry2);
    }

    @Test
    void shouldThrowExceptionReadingTransactionsBetweenWhenAlreadyUpToDate() throws Exception {
        // Given
        TransactionLogEntry entry1 = logEntry(1, new ClearFilesTransaction());
        TransactionLogEntry entry2 = logEntry(2, new ClearFilesTransaction());
        TransactionLogEntry entry3 = logEntry(3, new ClearFilesTransaction());
        fileLogStore.addTransaction(entry1);
        fileLogStore.addTransaction(entry2);
        fileLogStore.addTransaction(entry3);

        // When / Then
        assertThatThrownBy(() -> fileLogStore.readTransactions(toUpdateLocalStateToApply(2, 2)))
                .isInstanceOf(DynamoDbException.class);
    }

    @Test
    void shouldThrowExceptionReadingTransactionsBetweenWhenTargetTransactionIsBeforeCurrent() throws Exception {
        // Given
        TransactionLogEntry entry1 = logEntry(1, new ClearFilesTransaction());
        TransactionLogEntry entry2 = logEntry(2, new ClearFilesTransaction());
        TransactionLogEntry entry3 = logEntry(3, new ClearFilesTransaction());
        fileLogStore.addTransaction(entry1);
        fileLogStore.addTransaction(entry2);
        fileLogStore.addTransaction(entry3);

        // When / Then
        assertThatThrownBy(() -> fileLogStore.readTransactions(toUpdateLocalStateToApply(3, 2)))
                .isInstanceOf(DynamoDbException.class);
    }

    private TransactionLogEntry logEntry(long number, StateStoreTransaction<?> transaction) {
        return new TransactionLogEntry(number, DEFAULT_UPDATE_TIME, transaction);
    }

    private TransactionLogStore fileLogStore() {
        return DynamoDBTransactionLogStore.forFiles(instanceProperties, tableProperties, dynamoClientV2, s3ClientV2);
    }

    private TransactionLogStore partitionLogStore() {
        return DynamoDBTransactionLogStore.forPartitions(instanceProperties, tableProperties, dynamoClientV2, s3ClientV2);
    }

    private String singleFileInDataBucket() {
        List<String> files = filesInDataBucket();
        if (files.size() != 1) {
            throw new IllegalStateException("Expected one file in data bucket, found: " + files);
        } else {
            return files.get(0);
        }
    }

    private List<String> filesInDataBucket() {
        return s3ClientV2.listObjectsV2Paginator(builder -> builder
                .bucket(instanceProperties.get(DATA_BUCKET))
                .prefix(tableProperties.get(TableProperty.TABLE_ID)))
                .contents().stream().map(S3Object::key)
                .toList();
    }
}
