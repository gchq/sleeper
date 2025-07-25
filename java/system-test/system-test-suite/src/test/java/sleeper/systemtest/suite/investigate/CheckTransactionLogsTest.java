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
package sleeper.systemtest.suite.investigate;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import sleeper.compaction.core.job.CompactionJobFactory;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.statestore.ReplaceFileReferencesRequest;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.testutils.InMemoryTransactionLogStateStore;
import sleeper.core.statestore.testutils.InMemoryTransactionLogs;
import sleeper.core.statestore.transactionlog.log.TransactionLogEntry;
import sleeper.core.statestore.transactionlog.transaction.impl.ReplaceFileReferencesTransaction;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.schema.SchemaTestHelper.createSchemaWithKey;
import static sleeper.core.statestore.FileReferenceTestData.withJobId;
import static sleeper.core.statestore.ReplaceFileReferencesRequest.replaceJobFileReferences;
import static sleeper.core.statestore.testutils.StateStoreUpdatesWrapper.update;

public class CheckTransactionLogsTest {

    static final Instant UPDATE_TIME = Instant.parse("2025-02-27T12:43:00Z");
    InstanceProperties instanceProperties = createTestInstanceProperties();
    Schema schema = createSchemaWithKey("key");
    TableProperties tableProperties = createTestTableProperties(instanceProperties, createSchemaWithKey("key"));
    InMemoryTransactionLogs transactionLogs = new InMemoryTransactionLogs();
    StateStore stateStore = InMemoryTransactionLogStateStore.createAndInitialise(tableProperties, transactionLogs);

    @BeforeEach
    void setUp() {
        stateStore.fixFileUpdateTime(UPDATE_TIME);
    }

    @Test
    void shouldLoadFilesStateWithTransactionBody() {
        // Given
        FileReference file1 = fileFactory().rootFile("test1.parquet", 100);
        FileReference file2 = fileFactory().rootFile("test2.parquet", 100);
        transactionLogs.getTransactionBodyStore().setStoreTransactions(true);
        update(stateStore).addFile(file1);
        update(stateStore).addFile(file2);

        // When
        CheckTransactionLogs check = checkState();

        // Then
        assertThat(check.totalRowsAtTransaction(2)).isEqualTo(200);
    }

    @Test
    void shouldFindCompactionWhichChangedNumberOfRows() {
        // Given
        FileReference input = fileFactory().rootFile("input.parquet", 100);
        FileReference output = fileFactory().rootFile("output.parquet", 90);
        update(stateStore).addFile(input);
        update(stateStore).assignJobId("test-job", "root", List.of("input.parquet"));
        update(stateStore).atomicallyReplaceFileReferencesWithNewOnes("test-job", List.of("input.parquet"), output);

        // When
        CheckTransactionLogs check = checkState();

        // Then
        ReplaceFileReferencesRequest expectedRequest = replaceJobFileReferences("test-job", List.of("input.parquet"), output).withNoUpdateTime();
        ReplaceFileReferencesTransaction expectedTransaction = new ReplaceFileReferencesTransaction(List.of(expectedRequest));
        TransactionLogEntry expectedEntry = new TransactionLogEntry(3, UPDATE_TIME, expectedTransaction);
        assertThat(check.reportCompactionTransactionsChangedRowCount()).containsExactly(
                new CompactionChangedRowCountReport(expectedEntry, expectedTransaction,
                        List.of(new CompactionChangedRowCount(expectedRequest, List.of(withJobId("test-job", input)), output))));
    }

    @Test
    void shouldFindNoCompactionChangedNumberOfRows() {
        // Given
        FileReference input = fileFactory().rootFile("input.parquet", 100);
        FileReference output = fileFactory().rootFile("output.parquet", 100);
        update(stateStore).addFile(input);
        update(stateStore).assignJobId("test-job", "root", List.of("input.parquet"));
        update(stateStore).atomicallyReplaceFileReferencesWithNewOnes("test-job", List.of("input.parquet"), output);

        // When
        CheckTransactionLogs check = checkState();

        // Then
        assertThat(check.reportCompactionTransactionsChangedRowCount()).isEmpty();
    }

    @Test
    void shouldInferUnfinishedCompactionJob() {
        // Given
        FileReference file1 = fileFactory().rootFile("test1.parquet", 100);
        FileReference file2 = fileFactory().rootFile("test2.parquet", 100);
        update(stateStore).addFiles(List.of(file1, file2));
        update(stateStore).assignJobId("test-job", List.of(file1, file2));

        // When
        CheckTransactionLogs check = checkState();

        // Then
        assertThat(check.inferLastCompactionJobFromAssignJobIdsTransaction()).isEqualTo(
                new CompactionJobFactory(instanceProperties, tableProperties)
                        .createCompactionJobWithFilenames("test-job", List.of("test1.parquet", "test2.parquet"), "root"));
    }

    private FileReferenceFactory fileFactory() {
        return FileReferenceFactory.fromUpdatedAt(stateStore, UPDATE_TIME);
    }

    private CheckTransactionLogs checkState() {
        List<TransactionLogEntryHandle> filesLog = TransactionLogEntryHandle.load(
                tableProperties.get(TABLE_ID), transactionLogs.getFilesLogStore(), transactionLogs.getTransactionBodyStore());
        List<TransactionLogEntryHandle> partitionsLog = TransactionLogEntryHandle.load(
                tableProperties.get(TABLE_ID), transactionLogs.getPartitionsLogStore(), transactionLogs.getTransactionBodyStore());
        return new CheckTransactionLogs(instanceProperties, tableProperties, filesLog, partitionsLog);
    }

}
