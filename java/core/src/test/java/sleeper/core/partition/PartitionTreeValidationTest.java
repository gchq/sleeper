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

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.range.Range.RangeFactory;
import sleeper.core.range.Region;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.LongType;

import java.util.Collection;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.schema.SchemaTestHelper.createSchemaWithKey;
import static sleeper.core.schema.SchemaTestHelper.createSchemaWithMultipleKeys;

public class PartitionTreeValidationTest {

    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final TableProperties tableProperties = createTestTableProperties(instanceProperties, createSchemaWithKey("key", new IntType()));

    @Nested
    @DisplayName("Accept valid trees")
    class AcceptValidTrees {

        @Test
        void shouldAcceptSinglePartition() {
            // Given
            List<Partition> partitions = new PartitionsBuilder(tableProperties).singlePartition("root").buildList();

            // When / Then
            assertThatCode(() -> validate(partitions))
                    .doesNotThrowAnyException();
        }

        @Test
        void shouldAcceptOneSplitAtRoot() {
            // Given
            List<Partition> partitions = new PartitionsBuilder(tableProperties)
                    .rootFirst("root")
                    .splitToNewChildren("root", "L", "R", 50)
                    .buildList();

            // When / Then
            assertThatCode(() -> validate(partitions))
                    .doesNotThrowAnyException();
        }

        @Test
        void shouldAcceptMultipleLevelsOfSplits() {
            // Given
            List<Partition> partitions = new PartitionsBuilder(tableProperties)
                    .rootFirst("root")
                    .splitToNewChildren("root", "L", "R", 50)
                    .splitToNewChildren("L", "LL", "LR", 25)
                    .splitToNewChildren("LR", "LRL", "LRR", 30)
                    .splitToNewChildren("R", "RL", "RR", 75)
                    .buildList();

            // When / Then
            assertThatCode(() -> validate(partitions))
                    .doesNotThrowAnyException();
        }

        @Test
        void shouldAcceptSplitsOnMultipleDimensions() {
            // Given
            tableProperties.setSchema(createSchemaWithMultipleKeys("key1", new IntType(), "key2", new LongType()));
            List<Partition> partitions = new PartitionsBuilder(tableProperties)
                    .rootFirst("root")
                    .splitToNewChildrenOnDimension("root", "L", "R", 0, 50)
                    .splitToNewChildrenOnDimension("R", "RL", "RR", 1, 500L)
                    .buildList();

            // When / Then
            assertThatCode(() -> validate(partitions))
                    .doesNotThrowAnyException();
        }
    }

    @Nested
    @DisplayName("Validate number of roots")
    class NumberOfRoots {

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
    }

    @Nested
    @DisplayName("Validate leaf partition flag")
    class LeafPartitionFlag {

        @Test
        void shouldRefuseRootSetAsLeafWithChildPartitions() {
            // Given
            PartitionTree tree = new PartitionsBuilder(tableProperties)
                    .rootFirst("root")
                    .splitToNewChildren("root", "L", "R", 50)
                    .buildTree();
            List<Partition> partitions = List.of(
                    tree.getPartition("root").toBuilder().leafPartition(true).build(),
                    tree.getPartition("L"),
                    tree.getPartition("R"));

            // When / Then
            assertThatThrownBy(() -> validate(partitions))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("Partition has 2 child partitions but is marked as a leaf partition: root");
        }

        @Test
        void shouldRefuseRootSetAsNonLeafWithNoChildPartitions() {
            // Given
            PartitionTree tree = new PartitionsBuilder(tableProperties).singlePartition("root").buildTree();
            List<Partition> partitions = List.of(
                    tree.getPartition("root").toBuilder().leafPartition(false).build());

            // When / Then
            assertThatThrownBy(() -> validate(partitions))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("Expected 2 child partitions under root, left and right of split point, found 0");
        }

        @Test
        void shouldRefuseMiddleSetAsLeafWithChildPartitions() {
            // Given
            PartitionTree tree = new PartitionsBuilder(tableProperties)
                    .rootFirst("root")
                    .splitToNewChildren("root", "L", "R", 50)
                    .splitToNewChildren("R", "RL", "RR", 75)
                    .buildTree();
            List<Partition> partitions = List.of(
                    tree.getPartition("root"),
                    tree.getPartition("L"),
                    tree.getPartition("R").toBuilder().leafPartition(true).build(),
                    tree.getPartition("RL"),
                    tree.getPartition("RR"));

            // When / Then
            assertThatThrownBy(() -> validate(partitions))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("Partition has 2 child partitions but is marked as a leaf partition: R");
        }

        @Test
        void shouldRefuseNonRootSetAsNonLeafWithNoChildPartitions() {
            // Given
            PartitionTree tree = new PartitionsBuilder(tableProperties)
                    .rootFirst("root")
                    .splitToNewChildren("root", "L", "R", 50)
                    .buildTree();
            List<Partition> partitions = List.of(
                    tree.getPartition("root"),
                    tree.getPartition("L"),
                    tree.getPartition("R").toBuilder().leafPartition(false).build());

            // When / Then
            assertThatThrownBy(() -> validate(partitions))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("Expected 2 child partitions under R, left and right of split point, found 0");
        }
    }

    @Nested
    @DisplayName("Validate unjoined child partitions")
    class UnjoinedChildPartitions {

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
        void shouldRefuseWhenMissingChildPartitions() {
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
    }

    @Nested
    @DisplayName("Validate split points")
    class SplitPoints {

        @Test
        void shouldRefuseWhenChildPartitionsDoNotMeetAtSplitPoint() {
            // Given
            PartitionTree tree = new PartitionsBuilder(tableProperties)
                    .rootFirst("root")
                    .splitToNewChildren("root", "L", "R", 0)
                    .buildTree();
            List<Partition> partitions = List.of(
                    tree.getPartition("root"),
                    tree.getPartition("L"),
                    tree.getPartition("R")
                            .toBuilder().region(new Region(rangeFactory().createRange("key", 1, null))).build());

            // When / Then
            assertThatThrownBy(() -> validate(partitions))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("Child partitions do not meet at a split point on dimension 0 set in parent partition root");
        }

        @Test
        void shouldRefuseWhenWrongDimensionIsSetOnParent() {
            // Given
            tableProperties.setSchema(createSchemaWithMultipleKeys("key1", new IntType(), "key2", new LongType()));
            PartitionTree tree = new PartitionsBuilder(tableProperties)
                    .rootFirst("root")
                    .splitToNewChildrenOnDimension("root", "L", "R", 0, 50)
                    .buildTree();
            List<Partition> partitions = List.of(
                    tree.getPartition("root").toBuilder().dimension(1).build(),
                    tree.getPartition("L"), tree.getPartition("R"));

            // When / Then
            assertThatThrownBy(() -> validate(partitions))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("Child partitions do not meet at a split point on dimension 1 set in parent partition root");
        }

        @Test
        void shouldRefuseWhenLeftChildPartitionDoesNotHaveSameBoundaryAsParent() {
            // Given
            PartitionTree tree = new PartitionsBuilder(tableProperties)
                    .rootFirst("root")
                    .splitToNewChildren("root", "L", "R", 0)
                    .splitToNewChildren("R", "RL", "RR", 50)
                    .buildTree();
            List<Partition> partitions = List.of(
                    tree.getPartition("root"),
                    tree.getPartition("L"),
                    tree.getPartition("R"),
                    tree.getPartition("RL")
                            .toBuilder().region(new Region(rangeFactory().createRange("key", 25, 50))).build(),
                    tree.getPartition("RR"));

            // When / Then
            assertThatThrownBy(() -> validate(partitions))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("Left child partition RL does not match boundary of parent R");
        }

        @Test
        void shouldRefuseWhenRightChildPartitionDoesNotHaveSameBoundaryAsParent() {
            // Given
            PartitionTree tree = new PartitionsBuilder(tableProperties)
                    .rootFirst("root")
                    .splitToNewChildren("root", "L", "R", 100)
                    .splitToNewChildren("L", "LL", "LR", 50)
                    .buildTree();
            List<Partition> partitions = List.of(
                    tree.getPartition("root"),
                    tree.getPartition("L"),
                    tree.getPartition("R"),
                    tree.getPartition("LL"),
                    tree.getPartition("LR")
                            .toBuilder().region(new Region(rangeFactory().createRange("key", 50, 75))).build());

            // When / Then
            assertThatThrownBy(() -> validate(partitions))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("Right child partition LR does not match boundary of parent L");
        }
    }

    private void validate(Collection<Partition> partitions) {
        new PartitionTree(partitions).validate(tableProperties.getSchema());
    }

    private RangeFactory rangeFactory() {
        return new RangeFactory(tableProperties.getSchema());
    }

}
