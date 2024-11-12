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

package sleeper.statestore.dynamodb;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;

import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.LongType;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.statestore.StateStore;
import sleeper.statestore.StateStoreFactory;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.table.TableProperty.STATESTORE_CLASSNAME;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;

public class DynamoDBStateStoreMultipleTablesIT extends DynamoDBStateStoreTestBase {
    private final Schema schema = schemaWithKey("key", new LongType());
    private final StateStoreFactory stateStoreFactory = new StateStoreFactory(instanceProperties, null, dynamoDBClient, new Configuration());
    private final FileReferenceFactory fileReferenceFactory = FileReferenceFactory.from(new PartitionsBuilder(schema).singlePartition("root").buildTree());

    private StateStore initialiseTableStateStore() throws Exception {
        StateStore stateStore = getTableStateStore();
        stateStore.initialise();
        return stateStore;
    }

    private StateStore getTableStateStore() {
        TableProperties tableProperties = createTestTableProperties(instanceProperties, schema);
        tableProperties.set(STATESTORE_CLASSNAME, DynamoDBStateStore.class.getName());
        return stateStoreFactory.getStateStore(tableProperties);
    }

    @Test
    void shouldCreateFilesForTwoTables() throws Exception {
        // Given
        StateStore stateStore1 = initialiseTableStateStore();
        StateStore stateStore2 = initialiseTableStateStore();
        FileReference file1 = fileReferenceFactory.rootFile("file1.parquet", 12);
        FileReference file2 = fileReferenceFactory.rootFile("file2.parquet", 34);

        // When
        stateStore1.addFile(file1);
        stateStore2.addFile(file2);

        // Then
        assertThat(stateStore1.getFileReferences())
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("lastStateStoreUpdateTime")
                .containsExactly(file1);
        assertThat(stateStore2.getFileReferences())
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("lastStateStoreUpdateTime")
                .containsExactly(file2);
    }

    @Test
    void shouldCreatePartitionsForTwoTables() throws Exception {
        // Given
        StateStore stateStore1 = getTableStateStore();
        StateStore stateStore2 = getTableStateStore();
        PartitionTree tree1 = new PartitionsBuilder(schema).singlePartition("partition1").buildTree();
        PartitionTree tree2 = new PartitionsBuilder(schema).singlePartition("partition2").buildTree();

        // When
        stateStore1.initialise(tree1.getAllPartitions());
        stateStore2.initialise(tree2.getAllPartitions());

        // Then
        assertThat(stateStore1.getAllPartitions()).containsExactly(tree1.getRootPartition());
        assertThat(stateStore2.getAllPartitions()).containsExactly(tree2.getRootPartition());
    }

    @Test
    void shouldClearFilesForOneTableWhenTwoTablesArePresent() throws Exception {
        // Given
        StateStore stateStore1 = initialiseTableStateStore();
        StateStore stateStore2 = initialiseTableStateStore();
        FileReference file1 = fileReferenceFactory.rootFile("file1.parquet", 12);
        FileReference file2 = fileReferenceFactory.rootFile("file2.parquet", 34);
        stateStore1.addFile(file1);
        stateStore2.addFile(file2);

        // When
        stateStore1.clearFileData();

        // Then
        assertThat(stateStore1.getFileReferences()).isEmpty();
        assertThat(stateStore2.getFileReferences())
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("lastStateStoreUpdateTime")
                .containsExactly(file2);
    }

    @Test
    void shouldClearPartitionsAndFilesForOneTableWhenTwoTablesArePresent() throws Exception {
        // Given
        StateStore stateStore1 = getTableStateStore();
        StateStore stateStore2 = getTableStateStore();
        PartitionTree tree1 = new PartitionsBuilder(schema).singlePartition("partition1").buildTree();
        PartitionTree tree2 = new PartitionsBuilder(schema).singlePartition("partition2").buildTree();
        stateStore1.initialise(tree1.getAllPartitions());
        stateStore2.initialise(tree2.getAllPartitions());
        FileReference file1 = FileReferenceFactory.from(tree1).rootFile("file1.parquet", 12);
        FileReference file2 = FileReferenceFactory.from(tree2).rootFile("file2.parquet", 34);
        stateStore1.addFile(file1);
        stateStore2.addFile(file2);

        // When
        stateStore1.clearSleeperTable();

        // Then
        assertThat(stateStore1.getAllPartitions()).isEmpty();
        assertThat(stateStore2.getAllPartitions()).containsExactly(tree2.getRootPartition());
        assertThat(stateStore1.getFileReferences()).isEmpty();
        assertThat(stateStore2.getFileReferences())
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("lastStateStoreUpdateTime")
                .containsExactly(file2);
    }
}
