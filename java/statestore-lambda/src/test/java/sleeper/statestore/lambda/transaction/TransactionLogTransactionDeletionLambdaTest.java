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
package sleeper.statestore.lambda.transaction;

import com.amazonaws.services.lambda.runtime.events.SQSBatchResponse;
import com.amazonaws.services.lambda.runtime.events.SQSBatchResponse.BatchItemFailure;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.amazonaws.services.lambda.runtime.events.SQSEvent.SQSMessage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.properties.PropertiesReloader;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.testutils.FixedTablePropertiesProvider;
import sleeper.core.schema.type.StringType;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.testutils.InMemoryTransactionLogStateStore;
import sleeper.core.statestore.testutils.InMemoryTransactionLogs;
import sleeper.core.statestore.testutils.InMemoryTransactionLogsPerTable;
import sleeper.core.statestore.transactionlog.log.TransactionLogRange;
import sleeper.core.statestore.transactionlog.snapshot.TransactionLogSnapshot;
import sleeper.statestore.transactionlog.TransactionLogTransactionDeleter.GetLatestSnapshotsBefore;
import sleeper.statestore.transactionlog.snapshots.LatestSnapshots;
import sleeper.statestore.transactionlog.snapshots.TransactionLogSnapshotMetadata;

import java.time.Instant;
import java.util.List;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.properties.table.TableProperty.TRANSACTION_LOG_NUMBER_BEHIND_TO_DELETE;
import static sleeper.core.properties.table.TableProperty.TRANSACTION_LOG_SNAPSHOT_MIN_AGE_MINUTES_TO_DELETE_TRANSACTIONS;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.schema.SchemaTestHelper.createSchemaWithKey;
import static sleeper.core.statestore.testutils.StateStoreUpdatesWrapper.update;
import static sleeper.core.testutils.SupplierTestHelper.timePassesAMinuteAtATime;

public class TransactionLogTransactionDeletionLambdaTest {

    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final TableProperties tableProperties = createTestTableProperties(instanceProperties, createSchemaWithKey("key", new StringType()));
    private final InMemoryTransactionLogsPerTable transactionLogsPerTable = new InMemoryTransactionLogsPerTable();
    private final InMemoryTransactionLogs transactionLogs = transactionLogsPerTable.forTable(tableProperties);
    private final StateStore stateStore = InMemoryTransactionLogStateStore.create(tableProperties, transactionLogs);
    private final Supplier<Instant> timeSupplier = timePassesAMinuteAtATime();
    private GetLatestSnapshotsBefore getLatestSnapshotsBefore;

    @BeforeEach
    void setUp() {
        tableProperties.setNumber(TRANSACTION_LOG_SNAPSHOT_MIN_AGE_MINUTES_TO_DELETE_TRANSACTIONS, 0);
        tableProperties.setNumber(TRANSACTION_LOG_NUMBER_BEHIND_TO_DELETE, 0);
    }

    @Test
    void shouldDeleteTransactions() {
        // Given
        update(stateStore).initialise(new PartitionsBuilder(tableProperties)
                .rootFirst("root")
                .splitToNewChildren("root", "L", "R", "split")
                .buildList());
        update(stateStore).addFile(FileReferenceFactory.from(stateStore).rootFile("test.parquet", 10));
        createSnapshots();

        // When
        SQSBatchResponse response = invokeForSingleTable();

        // Then
        assertThat(transactionLogs.getFilesLogStore().getTransactionEntries())
                .isEmpty();
        assertThat(transactionLogs.getPartitionsLogStore().getTransactionEntries())
                .isEmpty();
        assertThat(response.getBatchItemFailures()).isEmpty();
    }

    @Test
    void shouldFailWhenSnapshotMetadataIsUnreadable() {
        // Given
        update(stateStore).initialise(new PartitionsBuilder(tableProperties)
                .rootFirst("root")
                .splitToNewChildren("root", "L", "R", "split")
                .buildList());
        update(stateStore).addFile(FileReferenceFactory.from(stateStore).rootFile("test.parquet", 10));
        getLatestSnapshotsBefore = time -> {
            throw new RuntimeException("Snapshot metadata is unreadable");
        };

        // When
        SQSBatchResponse response = invokeForSingleTable();

        // Then
        assertThat(transactionLogs.getFilesLogStore().getTransactionEntries())
                .hasSize(1);
        assertThat(transactionLogs.getPartitionsLogStore().getTransactionEntries())
                .hasSize(1);
        assertThat(response.getBatchItemFailures()).singleElement()
                .extracting(BatchItemFailure::getItemIdentifier)
                .isEqualTo("test-message");
    }

    @Test
    void shouldIgnoreTableThatWasDeleted() {
        // When
        SQSBatchResponse response = invokeForSingleTable("missing-table");

        // Then
        assertThat(response.getBatchItemFailures()).isEmpty();
    }

    private void createSnapshots() {
        transactionLogs.createSnapshots(tableProperties);
        TransactionLogSnapshot filesSnapshot = transactionLogs.getFilesSnapshots().loadLatestSnapshotInRange(TransactionLogRange.fromMinimum(0)).orElseThrow();
        TransactionLogSnapshot partitionsSnapshot = transactionLogs.getPartitionsSnapshots().loadLatestSnapshotInRange(TransactionLogRange.fromMinimum(0)).orElseThrow();
        String basePath = TransactionLogSnapshotMetadata.getBasePath(instanceProperties, tableProperties);
        Instant creationTime = timeSupplier.get();
        getLatestSnapshotsBefore = time -> new LatestSnapshots(
                TransactionLogSnapshotMetadata.forFiles(basePath, filesSnapshot.getTransactionNumber(), creationTime),
                TransactionLogSnapshotMetadata.forFiles(basePath, partitionsSnapshot.getTransactionNumber()));
    }

    private SQSBatchResponse invokeForSingleTable() {
        return invokeForSingleTable(tableProperties.get(TABLE_ID));
    }

    private SQSBatchResponse invokeForSingleTable(String tableId) {
        return lambda().handleRequest(event(
                messageForTableWithId(tableId, "test-message")), null);
    }

    private TransactionLogTransactionDeletionLambda lambda() {
        return new TransactionLogTransactionDeletionLambda(
                instanceProperties, new FixedTablePropertiesProvider(tableProperties), PropertiesReloader.neverReload(),
                table -> transactionLogsPerTable.forTable(table).getFilesLogStore(),
                table -> transactionLogsPerTable.forTable(table).getPartitionsLogStore(),
                table -> getLatestSnapshotsBefore,
                timeSupplier);
    }

    private SQSEvent event(SQSMessage... messages) {
        SQSEvent event = new SQSEvent();
        event.setRecords(List.of(messages));
        return event;
    }

    private SQSMessage messageForTableWithId(String tableId, String messageId) {
        SQSMessage message = new SQSMessage();
        message.setBody(tableId);
        message.setMessageId(messageId);
        return message;
    }

}
