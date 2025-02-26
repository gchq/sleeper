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
package sleeper.ingest.runner.impl.commit;

import org.junit.jupiter.api.Test;

import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.commit.StateStoreCommitRequest;
import sleeper.core.statestore.testutils.InMemoryTransactionLogStateStore;
import sleeper.core.statestore.testutils.InMemoryTransactionLogs;
import sleeper.core.statestore.transactionlog.transaction.impl.AddFilesTransaction;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;
import static sleeper.core.statestore.FileReferenceTestData.withLastUpdate;

public class AddFilesToStateStoreTest {
    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final Schema schema = schemaWithKey("key");
    private final PartitionTree partitionTree = new PartitionsBuilder(schema).singlePartition("root").buildTree();
    private final FileReferenceFactory factory = FileReferenceFactory.from(partitionTree);
    private final TableProperties tableProperties = createTestTableProperties(instanceProperties, schemaWithKey("key"));
    private final InMemoryTransactionLogs transactionLogs = new InMemoryTransactionLogs();
    private final StateStore stateStore = InMemoryTransactionLogStateStore.createAndInitialise(tableProperties, transactionLogs);
    private final List<StateStoreCommitRequest> stateStoreCommitQueue = new ArrayList<>();

    @Test
    void shouldSendCommitRequestToQueue() throws Exception {
        // Given
        FileReference file = factory.rootFile("test.parquet", 123L);

        // When
        bySqs().addFiles(List.of(file));

        // Then
        assertThat(stateStoreCommitQueue)
                .containsExactly(StateStoreCommitRequest.create(tableProperties.get(TABLE_ID),
                        AddFilesTransaction.fromReferences(List.of(file))));
    }

    @Test
    void shouldCommitSynchronouslyWithNoJob() {
        // Given
        FileReference file = factory.rootFile("test.parquet", 123L);
        Instant updateTime = Instant.parse("2025-02-26T11:31:00Z");
        stateStore.fixFileUpdateTime(updateTime);

        // When
        synchronousNoJob().addFiles(List.of(file));

        // Then
        assertThat(stateStore.getFileReferences()).containsExactly(withLastUpdate(updateTime, file));
        assertThat(transactionLogs.getLastFilesTransaction(tableProperties))
                .isEqualTo(AddFilesTransaction.fromReferences(List.of(file)));
    }

    @Test
    void shouldCommitSynchronouslyWithJob() {
        // Given
        FileReference file = factory.rootFile("test.parquet", 123L);
        Instant updateTime = Instant.parse("2025-02-26T11:31:00Z");
        stateStore.fixFileUpdateTime(updateTime);

        // When
        synchronousNoJob().addFiles(List.of(file));

        // Then
        assertThat(stateStore.getFileReferences()).containsExactly(withLastUpdate(updateTime, file));
        assertThat(transactionLogs.getLastFilesTransaction(tableProperties))
                .isEqualTo(AddFilesTransaction.fromReferences(List.of(file)));
    }

    private AddFilesToStateStore bySqs() {
        return AddFilesToStateStore.bySqs(tableProperties, stateStoreCommitQueue::add,
                request -> {
                });
    }

    private AddFilesToStateStore synchronousNoJob() {
        return AddFilesToStateStore.synchronous(stateStore);
    }
}
