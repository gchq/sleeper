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

import org.junit.jupiter.api.Disabled;
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

    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final TablePropertiesStore tables = InMemoryTableProperties.getStore();
    private final Queue<StateStoreCommitRequest> queue = new LinkedList<>();

    @Test
    @Disabled
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
        return new CompactionCommitBatcher();
    }

}
