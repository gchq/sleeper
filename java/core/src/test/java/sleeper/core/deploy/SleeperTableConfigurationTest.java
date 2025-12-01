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
package sleeper.core.deploy;

import org.junit.jupiter.api.Test;

import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.properties.SleeperPropertiesInvalidException;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.schema.type.StringType;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.core.properties.table.TableProperty.DATA_ENGINE;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.schema.SchemaTestHelper.createSchemaWithKey;

public class SleeperTableConfigurationTest {

    InstanceProperties instanceProperties = createTestInstanceProperties();
    TableProperties tableProperties = createTestTableProperties(instanceProperties, createSchemaWithKey("key", new StringType()));

    @Test
    void shouldAcceptValidPropertiesWithOnePartition() {
        // When
        SleeperTableConfiguration configuration = new SleeperTableConfiguration(tableProperties,
                new PartitionsBuilder(tableProperties).rootFirst("root").buildList());

        // Then
        assertThatCode(configuration::validate).doesNotThrowAnyException();
    }

    @Test
    void shouldRefuseInvalidProperties() {
        // Given
        tableProperties.set(DATA_ENGINE, "not-a-data-engine");

        // When
        SleeperTableConfiguration configuration = new SleeperTableConfiguration(tableProperties,
                new PartitionsBuilder(tableProperties).rootFirst("root").buildList());

        // Then
        assertThatThrownBy(configuration::validate)
                .isInstanceOf(SleeperPropertiesInvalidException.class);
    }

    @Test
    void shouldRefuseNoPartitions() {
        // When
        SleeperTableConfiguration configuration = new SleeperTableConfiguration(tableProperties, List.of());

        // Then
        assertThatThrownBy(configuration::validate)
                .isInstanceOf(InitialPartitionsInvalidException.class)
                .hasMessage("There should be exactly one root partition, found 0");
    }

    @Test
    void shouldRefuseExtraRootPartition() {
        // Given
        PartitionTree tree1 = new PartitionsBuilder(tableProperties).rootFirst("root1").buildTree();
        PartitionTree tree2 = new PartitionsBuilder(tableProperties).rootFirst("root2").buildTree();

        // When
        SleeperTableConfiguration configuration = new SleeperTableConfiguration(tableProperties,
                List.of(tree1.getRootPartition(), tree2.getRootPartition()));

        // Then
        assertThatThrownBy(configuration::validate)
                .isInstanceOf(InitialPartitionsInvalidException.class)
                .hasMessage("There should be exactly one root partition, found 2");
    }

    @Test
    void shouldRefuseChildPartitionsNotJoinedFromRoot() {
        // Given
        PartitionTree tree1 = new PartitionsBuilder(tableProperties).rootFirst("root").buildTree();
        PartitionTree tree2 = new PartitionsBuilder(tableProperties)
                .rootFirst("root")
                .splitToNewChildren("root", "L", "R", "aaa")
                .buildTree();

        // When
        SleeperTableConfiguration configuration = new SleeperTableConfiguration(tableProperties,
                List.of(tree1.getRootPartition(), tree2.getPartition("L"), tree2.getPartition("R")));

        // Then
        assertThatThrownBy(configuration::validate)
                .isInstanceOf(InitialPartitionsInvalidException.class)
                .hasMessage("Found partitions unlinked to the rest of the tree: [L, R]");
    }

    @Test
    void shouldRefuseChildPartitionsWithIncorrectParentIdSet() {
        // Given
        PartitionTree tree = new PartitionsBuilder(tableProperties)
                .rootFirst("root")
                .splitToNewChildren("root", "L", "R", "aaa")
                .splitToNewChildren("R", "RL", "RR", "bbb")
                .buildTree();

        // When
        SleeperTableConfiguration configuration = new SleeperTableConfiguration(tableProperties,
                List.of(
                        tree.getRootPartition(),
                        tree.getPartition("L").toBuilder().parentPartitionId("wrong1").build(),
                        tree.getPartition("R"),
                        tree.getPartition("RL"),
                        tree.getPartition("RR").toBuilder().parentPartitionId("wrong2").build()));

        // Then
        assertThatThrownBy(configuration::validate)
                .isInstanceOf(InitialPartitionsInvalidException.class)
                .hasMessage("Found partitions unlinked to the rest of the tree: [L, RR]");
    }

    @Test
    void shouldFindMissingChildPartitions() {
        // Given
        PartitionTree tree = new PartitionsBuilder(tableProperties)
                .rootFirst("root")
                .splitToNewChildren("root", "L", "R", "aaa")
                .splitToNewChildren("R", "RL", "RR", "bbb")
                .buildTree();

        // When
        SleeperTableConfiguration configuration = new SleeperTableConfiguration(tableProperties,
                List.of(
                        tree.getRootPartition(),
                        tree.getPartition("R"),
                        tree.getPartition("RL")));

        // Then
        assertThatThrownBy(configuration::validate)
                .isInstanceOf(InitialPartitionsInvalidException.class)
                .hasMessage("Found missing child partitions: [L, RR]");
    }

}
