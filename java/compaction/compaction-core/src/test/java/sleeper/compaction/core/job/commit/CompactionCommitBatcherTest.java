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
import sleeper.core.statestore.ReplaceFileReferencesRequest;
import sleeper.core.statestore.commit.StateStoreCommitRequest;
import sleeper.core.statestore.commit.StateStoreCommitRequestSender;
import sleeper.core.statestore.transactionlog.transaction.impl.ReplaceFileReferencesTransaction;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;

public class CompactionCommitBatcherTest {

    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final Queue<StateStoreCommitRequest> queue = new LinkedList<>();

    @Test
    void shouldSendOneCompactionCommit() {
        // Given
        TableProperties table = createTable("test-table");
        CompactionJob job = jobFactory(table).createCompactionJobWithFilenames(
                "test-job", List.of("test.parquet"), "root");
        ReplaceFileReferencesRequest request = defaultReplaceFileReferencesRequest(job);

        // When
        batcher().sendBatch(List.of(commitRequest("test-table", request)));

        // Then
        assertThat(queue).containsExactly(StateStoreCommitRequest.create(
                "test-table", new ReplaceFileReferencesTransaction(List.of(request))));
    }

    @Test
    void shouldSendOneCompactionCommitForEachOfSeveralTables() {
        // Given
        TableProperties table1 = createTable("table1");
        TableProperties table2 = createTable("table2");
        CompactionJob job1 = jobFactory(table1).createCompactionJobWithFilenames(
                "job1", List.of("test.parquet"), "root");
        CompactionJob job2 = jobFactory(table2).createCompactionJobWithFilenames(
                "job1", List.of("test.parquet"), "root");
        ReplaceFileReferencesRequest request1 = defaultReplaceFileReferencesRequest(job1);
        ReplaceFileReferencesRequest request2 = defaultReplaceFileReferencesRequest(job2);

        // When
        batcher().sendBatch(List.of(
                commitRequest("table1", request1),
                commitRequest("table2", request2)));

        // Then
        assertThat(queue).containsExactlyInAnyOrder(
                StateStoreCommitRequest.create("table1",
                        new ReplaceFileReferencesTransaction(List.of(request1))),
                StateStoreCommitRequest.create("table2",
                        new ReplaceFileReferencesTransaction(List.of(request2))));
    }

    @Test
    void shouldSendMultipleCompactionCommitsForSameTableAsOneTransaction() {
        // Given
        TableProperties table = createTable("test-table");
        CompactionJob job1 = jobFactory(table).createCompactionJobWithFilenames(
                "job1", List.of("test.parquet"), "root");
        CompactionJob job2 = jobFactory(table).createCompactionJobWithFilenames(
                "job1", List.of("test.parquet"), "root");
        ReplaceFileReferencesRequest request1 = defaultReplaceFileReferencesRequest(job1);
        ReplaceFileReferencesRequest request2 = defaultReplaceFileReferencesRequest(job2);

        // When
        batcher().sendBatch(List.of(
                commitRequest("test-table", request1),
                commitRequest("test-table", request2)));

        // Then
        assertThat(queue).containsExactly(
                StateStoreCommitRequest.create("test-table",
                        new ReplaceFileReferencesTransaction(List.of(request1, request2))));
    }

    @Test
    void shouldReportFailureOfOneRequestAlongsideSuccessfulRequest() {
        // Given
        TableProperties table1 = createTable("table1");
        TableProperties table2 = createTable("table2");
        CompactionJob job1 = jobFactory(table1).createCompactionJobWithFilenames(
                "job1", List.of("test.parquet"), "root");
        CompactionJob job2 = jobFactory(table2).createCompactionJobWithFilenames(
                "job1", List.of("test.parquet"), "root");
        ReplaceFileReferencesRequest request1 = defaultReplaceFileReferencesRequest(job1);
        ReplaceFileReferencesRequest request2 = defaultReplaceFileReferencesRequest(job2);
        StateStoreCommitRequestSender sendCommits = SendStateStoreCommitDummy.sendToQueueExceptForTable(queue, "table1");
        List<String> failures = new ArrayList<>();

        // When
        new CompactionCommitBatcher(sendCommits).sendBatch(List.of(
                new CompactionCommitMessageHandle("table1", request1, () -> failures.add("first")),
                new CompactionCommitMessageHandle("table2", request2, () -> failures.add("second"))));

        // Then
        assertThat(queue).containsExactly(
                StateStoreCommitRequest.create("table2",
                        new ReplaceFileReferencesTransaction(List.of(request2))));
        assertThat(failures).containsExactly("first");
    }

    @Test
    void shouldReportFailureSendingMultipleCompactionsForOneTable() {
        // Given
        TableProperties table = createTable("some-table");
        CompactionJob job1 = jobFactory(table).createCompactionJobWithFilenames(
                "job1", List.of("test.parquet"), "root");
        CompactionJob job2 = jobFactory(table).createCompactionJobWithFilenames(
                "job1", List.of("test.parquet"), "root");
        ReplaceFileReferencesRequest request1 = defaultReplaceFileReferencesRequest(job1);
        ReplaceFileReferencesRequest request2 = defaultReplaceFileReferencesRequest(job2);
        StateStoreCommitRequestSender sendCommits = SendStateStoreCommitDummy.sendToQueueExceptForTable(queue, "some-table");
        List<String> failures = new ArrayList<>();

        // When
        new CompactionCommitBatcher(sendCommits).sendBatch(List.of(
                new CompactionCommitMessageHandle("some-table", request1, () -> failures.add("first")),
                new CompactionCommitMessageHandle("some-table", request2, () -> failures.add("second"))));

        // Then
        assertThat(queue).isEmpty();
        assertThat(failures).containsExactlyInAnyOrder("first", "second");
    }

    private CompactionCommitMessageHandle commitRequest(String tableId, ReplaceFileReferencesRequest request) {
        return new CompactionCommitMessageHandle(tableId, request, () -> {
        });
    }

    private TableProperties createTable(String tableId) {
        TableProperties tableProperties = createTestTableProperties(instanceProperties, schemaWithKey("key"));
        tableProperties.set(TABLE_ID, tableId);
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
