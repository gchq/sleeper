/*
 * Copyright 2022-2023 Crown Copyright
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

import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.LongType;
import sleeper.core.statestore.FileInfo;
import sleeper.core.statestore.FileInfoFactory;
import sleeper.core.statestore.StateStore;
import sleeper.statestore.StateStoreFactory;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.configuration.properties.table.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.configuration.properties.table.TableProperty.STATESTORE_CLASSNAME;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;

public class DynamoDBStateStoreMultipleTablesIT extends DynamoDBStateStoreTestBase {
    private final Schema schema = schemaWithKey("key", new LongType());
    private final StateStoreFactory stateStoreFactory = new StateStoreFactory(dynamoDBClient, instanceProperties, new Configuration());
    private final FileInfoFactory fileInfoFactory = FileInfoFactory.from(new PartitionsBuilder(schema).singlePartition("root").buildTree());

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
        FileInfo file1 = fileInfoFactory.rootFile("file1.parquet", 12);
        FileInfo file2 = fileInfoFactory.rootFile("file2.parquet", 34);

        // When
        stateStore1.addFile(file1);
        stateStore2.addFile(file2);

        // Then
        assertThat(stateStore1.getActiveFiles())
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("lastStateStoreUpdateTime")
                .containsExactly(file1);
        assertThat(stateStore2.getActiveFiles())
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
        FileInfo file1 = fileInfoFactory.rootFile("file1.parquet", 12);
        FileInfo file2 = fileInfoFactory.rootFile("file2.parquet", 34);
        stateStore1.addFile(file1);
        stateStore2.addFile(file2);

        // When
        stateStore1.clearFiles();

        // Then
        assertThat(stateStore1.getActiveFiles()).isEmpty();
        assertThat(stateStore2.getActiveFiles())
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
        FileInfo file1 = FileInfoFactory.from(tree1).rootFile("file1.parquet", 12);
        FileInfo file2 = FileInfoFactory.from(tree2).rootFile("file2.parquet", 34);
        stateStore1.addFile(file1);
        stateStore2.addFile(file2);

        // When
        stateStore1.clearTable();

        // Then
        assertThat(stateStore1.getAllPartitions()).isEmpty();
        assertThat(stateStore2.getAllPartitions()).containsExactly(tree2.getRootPartition());
        assertThat(stateStore1.getActiveFiles()).isEmpty();
        assertThat(stateStore2.getActiveFiles())
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("lastStateStoreUpdateTime")
                .containsExactly(file2);
    }
}
