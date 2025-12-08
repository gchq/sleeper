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
package sleeper.core.partition;

import org.junit.jupiter.api.Test;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.schema.type.IntType;

import java.util.Collection;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.schema.SchemaTestHelper.createSchemaWithKey;

public class PartitionTreeValidationTest {

    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final TableProperties tableProperties = createTestTableProperties(instanceProperties, createSchemaWithKey("key", new IntType()));

    @Test
    void shouldRefuseNoPartitions() {
        // When
        List<Partition> partitions = List.of();

        // Then
        assertThatThrownBy(() -> new PartitionTree(partitions))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("There should be exactly one root partition, found 0");
    }

    @Test
    void shouldRefuseExtraRootPartition() {
        // Given
        PartitionTree tree1 = new PartitionsBuilder(tableProperties).rootFirst("root1").buildTree();
        PartitionTree tree2 = new PartitionsBuilder(tableProperties).rootFirst("root2").buildTree();
        List<Partition> partitions = List.of(
                tree1.getPartition("root1"),
                tree2.getPartition("root2"));

        // When / Then
        assertThatThrownBy(() -> new PartitionTree(partitions))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("There should be exactly one root partition, found 2");
    }

    @Test
    void shouldRefuseChildPartitionsNotJoinedFromRoot() {
        // Given
        PartitionTree tree1 = new PartitionsBuilder(tableProperties).rootFirst("root").buildTree();
        PartitionTree tree2 = new PartitionsBuilder(tableProperties)
                .rootFirst("root")
                .splitToNewChildren("root", "L", "R", 50)
                .buildTree();
        List<Partition> partitions = List.of(
                tree1.getPartition("root"),
                tree2.getPartition("L"),
                tree2.getPartition("R"));

        // When / Then
        assertThatThrownBy(() -> validate(partitions))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Found partitions unlinked to the rest of the tree: [L, R]");
    }

    @Test
    void shouldRefuseChildPartitionsWithIncorrectParentIdSet() {
        // Given
        PartitionTree tree = new PartitionsBuilder(tableProperties)
                .rootFirst("root")
                .splitToNewChildren("root", "L", "R", 50)
                .splitToNewChildren("R", "RL", "RR", 75)
                .buildTree();
        List<Partition> partitions = List.of(
                tree.getRootPartition(),
                tree.getPartition("L").toBuilder().parentPartitionId("wrong1").build(),
                tree.getPartition("R"),
                tree.getPartition("RL"),
                tree.getPartition("RR").toBuilder().parentPartitionId("wrong2").build());

        // When / Then
        assertThatThrownBy(() -> validate(partitions))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Found partitions unlinked to the rest of the tree: [L, RR]");
    }

    @Test
    void shouldFindMissingChildPartitions() {
        // Given
        PartitionTree tree = new PartitionsBuilder(tableProperties)
                .rootFirst("root")
                .splitToNewChildren("root", "L", "R", 50)
                .splitToNewChildren("R", "RL", "RR", 75)
                .buildTree();
        List<Partition> partitions = List.of(
                tree.getRootPartition(),
                tree.getPartition("R"),
                tree.getPartition("RL"));

        // When / Then
        assertThatThrownBy(() -> validate(partitions))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Found missing child partitions: [L, RR]");
    }

    private void validate(Collection<Partition> partitions) {
        new PartitionTree(partitions).validate(tableProperties.getSchema());
    }

}
