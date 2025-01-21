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
package sleeper.compaction.core.job.commit;

import org.junit.jupiter.api.Test;

import sleeper.compaction.core.job.CompactionJob;
import sleeper.compaction.core.job.CompactionJobFactory;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertiesStore;
import sleeper.core.properties.testutils.InMemoryTableProperties;
import sleeper.core.statestore.ReplaceFileReferencesRequest;
import sleeper.core.statestore.commit.StateStoreCommitRequest;
import sleeper.core.statestore.transactionlog.transactions.ReplaceFileReferencesTransaction;

import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;

public class CompactionCommitBatcherTest {
    // Tests
    // - Commit one compaction
    // - Compactions for multiple tables
    // - Multiple compactions for one table
    // - What happens when one table does not exist?
    // - One table failed to send to queue, other table sent successfully (retry just the failed table, callback to lambda/SQS code)

    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final TablePropertiesStore tables = InMemoryTableProperties.getStore();
    private final Queue<StateStoreCommitRequest> queue = new LinkedList<>();

    @Test
    void shouldSendOneCompactionCommit() {
        // Given
        TableProperties table = createTable();
        CompactionJob job = jobFactory(table).createCompactionJobWithFilenames(
                "test-job", List.of("test.parquet"), "root");
        ReplaceFileReferencesRequest request = defaultReplaceFileReferencesRequest(job);

        // When
        batcher().sendBatch(List.of(new CompactionCommitRequest(table.get(TABLE_ID), request)));

        // Then
        assertThat(queue).containsExactly(StateStoreCommitRequest.create(
                table.get(TABLE_ID), new ReplaceFileReferencesTransaction(List.of(request))));
    }

    @Test
    void shouldSendOneCompactionCommitForEachOfSeveralTables() {
        // Given
        TableProperties table1 = createTable();
        TableProperties table2 = createTable();
        CompactionJob job1 = jobFactory(table1).createCompactionJobWithFilenames(
                "job1", List.of("test.parquet"), "root");
        CompactionJob job2 = jobFactory(table2).createCompactionJobWithFilenames(
                "job1", List.of("test.parquet"), "root");
        ReplaceFileReferencesRequest request1 = defaultReplaceFileReferencesRequest(job1);
        ReplaceFileReferencesRequest request2 = defaultReplaceFileReferencesRequest(job2);

        // When
        batcher().sendBatch(List.of(
                new CompactionCommitRequest(table1.get(TABLE_ID), request1),
                new CompactionCommitRequest(table2.get(TABLE_ID), request2)));

        // Then
        assertThat(queue).containsExactlyInAnyOrder(
                StateStoreCommitRequest.create(table1.get(TABLE_ID),
                        new ReplaceFileReferencesTransaction(List.of(request1))),
                StateStoreCommitRequest.create(table2.get(TABLE_ID),
                        new ReplaceFileReferencesTransaction(List.of(request2))));
    }

    @Test
    void shouldSendMultipleCompactionCommitsForSameTableAsOneTransaction() {
        // Given
        TableProperties table = createTable();
        CompactionJob job1 = jobFactory(table).createCompactionJobWithFilenames(
                "job1", List.of("test.parquet"), "root");
        CompactionJob job2 = jobFactory(table).createCompactionJobWithFilenames(
                "job1", List.of("test.parquet"), "root");
        ReplaceFileReferencesRequest request1 = defaultReplaceFileReferencesRequest(job1);
        ReplaceFileReferencesRequest request2 = defaultReplaceFileReferencesRequest(job2);

        // When
        batcher().sendBatch(List.of(
                new CompactionCommitRequest(table.get(TABLE_ID), request1),
                new CompactionCommitRequest(table.get(TABLE_ID), request2)));

        // Then
        assertThat(queue).containsExactly(
                StateStoreCommitRequest.create(table.get(TABLE_ID),
                        new ReplaceFileReferencesTransaction(List.of(request1, request2))));
    }

    private TableProperties createTable() {
        TableProperties tableProperties = createTestTableProperties(instanceProperties, schemaWithKey("key"));
        tables.createTable(tableProperties);
        return tableProperties;
    }

    private CompactionJobFactory jobFactory(TableProperties tableProperties) {
        return new CompactionJobFactory(instanceProperties, tableProperties);
    }

    private ReplaceFileReferencesRequest defaultReplaceFileReferencesRequest(CompactionJob job) {
        return job.replaceFileReferencesRequestBuilder(100)
                .taskId("test-task")
                .jobRunId("test-run")
                .build();
    }

    private CompactionCommitBatcher batcher() {
        return new CompactionCommitBatcher(queue::add);
    }

}
