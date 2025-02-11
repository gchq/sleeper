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
import sleeper.core.statestore.AllReferencesToAFile;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.statestore.commit.StateStoreCommitRequest;
import sleeper.core.statestore.transactionlog.transaction.impl.AddFilesTransaction;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;

public class AddFilesToStateStoreTest {
    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final Schema schema = schemaWithKey("key");
    private final PartitionTree partitionTree = new PartitionsBuilder(schema).singlePartition("root").buildTree();
    private final FileReferenceFactory factory = FileReferenceFactory.from(partitionTree);
    private final TableProperties tableProperties = createTestTableProperties(instanceProperties, schemaWithKey("key"));
    private final List<StateStoreCommitRequest> stateStoreCommitQueue = new ArrayList<>();

    @Test
    void shouldSendCommitRequestToQueue() throws Exception {
        // Given
        FileReference file1 = factory.rootFile("test.parquet", 123L);

        // When
        bySqs().addFiles(List.of(file1));

        // Then
        assertThat(stateStoreCommitQueue)
                .containsExactly(StateStoreCommitRequest.create(tableProperties.get(TABLE_ID),
                        new AddFilesTransaction(AllReferencesToAFile.newFilesWithReferences(List.of(file1)))));
    }

    private AddFilesToStateStore bySqs() {
        return AddFilesToStateStore.bySqs(tableProperties, stateStoreCommitQueue::add,
                request -> {
                });
    }
}
