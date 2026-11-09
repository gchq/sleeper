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
package sleeper.compaction.job.creation.lambda;

import com.amazonaws.services.lambda.runtime.events.SQSBatchResponse;
import com.amazonaws.services.lambda.runtime.events.SQSBatchResponse.BatchItemFailure;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.amazonaws.services.lambda.runtime.events.SQSEvent.SQSMessage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import sleeper.compaction.core.job.CompactionJob;
import sleeper.compaction.core.job.CompactionJobFactory;
import sleeper.compaction.core.job.creation.CreateCompactionJobBatches;
import sleeper.compaction.core.job.creation.CreateCompactionJobBatches.BatchJobsWriter;
import sleeper.compaction.core.job.creation.CreateCompactionJobBatches.BatchMessageSender;
import sleeper.compaction.core.job.creation.CreateCompactionJobs;
import sleeper.compaction.core.job.creation.strategy.impl.BasicCompactionStrategy;
import sleeper.core.properties.PropertiesReloader;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.testutils.FixedTablePropertiesProvider;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreProvider;
import sleeper.core.statestore.commit.StateStoreCommitRequestSender;
import sleeper.core.statestore.testutils.InMemoryTransactionLogStateStore;
import sleeper.core.statestore.testutils.InMemoryTransactionLogsPerTable;
import sleeper.core.util.ObjectFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.table.TableProperty.COMPACTION_FILES_BATCH_SIZE;
import static sleeper.core.properties.table.TableProperty.COMPACTION_STRATEGY_CLASS;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.schema.SchemaTestHelper.createSchemaWithKey;
import static sleeper.core.statestore.testutils.StateStoreUpdatesWrapper.update;
import static sleeper.core.testutils.SupplierTestHelper.supplyNumberedIdsWithPrefix;
import static sleeper.core.testutils.SupplierTestHelper.timePassesAMinuteAtATime;

public class CreateCompactionJobsLambdaTest {

    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final TableProperties tableProperties = createTestTableProperties(instanceProperties, createSchemaWithKey("key"));
    private final InMemoryTransactionLogsPerTable transactionLogs = new InMemoryTransactionLogsPerTable();
    private final StateStoreProvider stateStoreProvider = InMemoryTransactionLogStateStore.createProvider(instanceProperties, transactionLogs);
    private final List<CompactionJob> jobsCreated = new ArrayList<>();

    @BeforeEach
    void setUp() {
        tableProperties.set(COMPACTION_STRATEGY_CLASS, BasicCompactionStrategy.class.getName());
        tableProperties.setNumber(COMPACTION_FILES_BATCH_SIZE, 2);
    }

    @Test
    void shouldCreateJob() {
        // Given
        update(stateStore()).initialise(tableProperties);
        FileReference file1 = fileFactory().rootFile("file1", 10L);
        FileReference file2 = fileFactory().rootFile("file2", 20L);
        update(stateStore()).addFiles(List.of(file1, file2));

        // When
        SQSBatchResponse response = invokeForSingleTable();

        // Then
        assertThat(jobsCreated).containsExactly(
                compactionFactory().createCompactionJob("job1", List.of(file1, file2), "root"));
        assertThat(response.getBatchItemFailures()).isEmpty();
    }

    @Test
    void shouldFailWhenTableIsUnreadable() {
        // Given
        transactionLogs.forTable(tableProperties).getFilesLogStore().atStartOfReadTransactions(() -> {
            throw new RuntimeException("The file transaction log is unreadable");
        });

        // When
        SQSBatchResponse response = invokeForSingleTable();

        // Then
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

    private SQSBatchResponse invokeForSingleTable() {
        return invokeForSingleTable(tableProperties.get(TABLE_ID));
    }

    private SQSBatchResponse invokeForSingleTable(String tableId) {
        return lambda().handleRequest(event(
                messageForTableWithId(tableId, "test-message")), null);
    }

    private CreateCompactionJobsLambda lambda() {
        return new CreateCompactionJobsLambda(new FixedTablePropertiesProvider(tableProperties), PropertiesReloader.neverReload(), creator());
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

    private CreateCompactionJobs creator() {
        return new CreateCompactionJobs(instanceProperties, stateStoreProvider, batcher(), ObjectFactory.noUserJars(), supplyNumberedIdsWithPrefix("job")::get, new Random(0));
    }

    private CreateCompactionJobBatches batcher() {
        return new CreateCompactionJobBatches(instanceProperties, jobsWriter(), batchSender(), stateStoreCommitSender(), supplyNumberedIdsWithPrefix("batch")::get, timePassesAMinuteAtATime());
    }

    private StateStore stateStore() {
        return stateStoreProvider.getStateStore(tableProperties);
    }

    private FileReferenceFactory fileFactory() {
        return FileReferenceFactory.from(instanceProperties, tableProperties, stateStore());
    }

    private CompactionJobFactory compactionFactory() {
        return new CompactionJobFactory(instanceProperties, tableProperties);
    }

    private BatchJobsWriter jobsWriter() {
        return (bucket, key, jobs) -> jobsCreated.addAll(jobs);
    }

    private BatchMessageSender batchSender() {
        return request -> {
        };
    }

    private StateStoreCommitRequestSender stateStoreCommitSender() {
        return request -> {
        };
    }

}
