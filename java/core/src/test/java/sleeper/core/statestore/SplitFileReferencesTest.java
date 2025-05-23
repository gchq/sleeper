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

package sleeper.core.statestore;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.LongType;
import sleeper.core.statestore.testutils.InMemoryTransactionLogStateStore;
import sleeper.core.statestore.testutils.InMemoryTransactionLogs;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.schema.SchemaTestHelper.createSchemaWithKey;
import static sleeper.core.statestore.AssignJobIdRequest.assignJobOnPartitionToFiles;
import static sleeper.core.statestore.SplitFileReference.referenceForChildPartition;
import static sleeper.core.statestore.testutils.StateStoreUpdatesWrapper.update;

public class SplitFileReferencesTest {
    private static final Instant DEFAULT_UPDATE_TIME = Instant.parse("2023-10-04T14:08:00Z");
    private final Schema schema = createSchemaWithKey("key", new LongType());
    private final PartitionsBuilder partitions = new PartitionsBuilder(schema).rootFirst("root")
            .splitToNewChildren("root", "L", "R", 5L);
    private final FileReferenceFactory factory = FileReferenceFactory.fromUpdatedAt(partitions.buildTree(), DEFAULT_UPDATE_TIME);
    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final TableProperties tableProperties = createTestTableProperties(instanceProperties, schema);
    private final StateStore store = InMemoryTransactionLogStateStore
            .createAndInitialiseWithPartitions(partitions.buildList(), tableProperties, new InMemoryTransactionLogs());

    @BeforeEach
    void setUp() {
        store.fixFileUpdateTime(DEFAULT_UPDATE_TIME);
    }

    @Test
    void shouldFindFileInNonLeafPartitionToSplit() throws Exception {
        // Given
        FileReference file = factory.rootFile("file1", 100L);
        update(store).addFile(file);

        // When
        SplitFileReferences.from(store).split();

        // Then
        assertThat(store.getFileReferences()).containsExactly(
                splitFile(file, "L"),
                splitFile(file, "R"));
    }

    @Test
    void shouldIgnoreFileInLeafPartition() throws Exception {
        // Given
        FileReference file = factory.partitionFile("L", "file1", 100L);
        update(store).addFile(file);

        // When
        SplitFileReferences.from(store).split();

        // Then
        assertThat(store.getFileReferences()).containsExactly(file);
    }

    @Test
    void shouldIgnoreFileWithJobIdAssigned() throws Exception {
        // Given
        FileReference file = factory.rootFile("file1", 100L);
        update(store).addFile(file);
        update(store).assignJobIds(List.of(
                assignJobOnPartitionToFiles("job1", "root", List.of("file1"))));

        // When
        SplitFileReferences.from(store).split();

        // Then
        assertThat(store.getFileReferences()).containsExactly(withJobId("job1", file));
    }

    private FileReference splitFile(FileReference file, String partitionId) {
        return referenceForChildPartition(file, partitionId).toBuilder()
                .lastStateStoreUpdateTime(DEFAULT_UPDATE_TIME).build();
    }

    private static FileReference withJobId(String jobId, FileReference file) {
        return file.toBuilder().jobId(jobId).build();
    }
}
