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
package sleeper.core.partition;

import org.junit.jupiter.api.Test;

import sleeper.core.range.Range.RangeFactory;
import sleeper.core.range.Region;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.StringType;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;

class PartitionsBuilderTest {

    @Test
    void shouldBuildPartitionsSpecifyingSplitPointsLeavesFirst() {
        // Given
        Field field = new Field("key1", new StringType());
        Schema schema = Schema.builder().rowKeyFields(field).build();
        RangeFactory rangeFactory = new RangeFactory(schema);

        // When
        PartitionsBuilder builder = PartitionsBuilderSplitsFirst.leavesWithSplits(schema,
                List.of("A", "B", "C"),
                List.of("aaa", "bbb"))
                .parentJoining("D", "A", "B")
                .parentJoining("E", "D", "C");

        // Then
        List<Partition> expectedPartitions = List.of(
                Partition.builder()
                        .region(new Region(rangeFactory.createRange(field, "", "aaa")))
                        .id("A")
                        .leafPartition(true)
                        .parentPartitionId("D")
                        .childPartitionIds(List.of())
                        .dimension(-1)
                        .build(),
                Partition.builder()
                        .region(new Region(rangeFactory.createRange(field, "aaa", "bbb")))
                        .id("B")
                        .leafPartition(true)
                        .parentPartitionId("D")
                        .childPartitionIds(List.of())
                        .dimension(-1)
                        .build(),
                Partition.builder()
                        .region(new Region(rangeFactory.createRange(field, "bbb", null)))
                        .id("C")
                        .leafPartition(true)
                        .parentPartitionId("E")
                        .childPartitionIds(List.of())
                        .dimension(-1)
                        .build(),
                Partition.builder()
                        .region(new Region(rangeFactory.createRange(field, "", "bbb")))
                        .id("D")
                        .leafPartition(false)
                        .parentPartitionId("E")
                        .childPartitionIds(List.of("A", "B"))
                        .dimension(0)
                        .build(),
                Partition.builder()
                        .region(new Region(rangeFactory.createRange(field, "", null)))
                        .id("E")
                        .leafPartition(false)
                        .parentPartitionId(null)
                        .childPartitionIds(List.of("D", "C"))
                        .dimension(0)
                        .build());
        assertThat(builder.buildList()).isEqualTo(expectedPartitions);
        assertThat(builder.buildTree()).isEqualTo(new PartitionTree(expectedPartitions));
    }

    @Test
    void shouldBuildPartitionsSpecifyingSplitPointsLeavesFirstWhenOnlyCareAboutLeaves() {
        // Given
        Field field = new Field("key1", new StringType());
        Schema schema = Schema.builder().rowKeyFields(field).build();

        // When I only care about leaf partitions, so I want any tree without caring about
        // the structure or the non-leaf IDs
        PartitionTree tree = PartitionsBuilderSplitsFirst.leavesWithSplits(schema,
                List.of("A", "B", "C"),
                List.of("aaa", "bbb"))
                .anyTreeJoiningAllLeaves()
                .buildTree();

        // Then all leaves have a path to the root partition
        assertThat(tree.getAllAncestors("A")).endsWith(tree.getRootPartition());
        assertThat(tree.getAllAncestors("B")).endsWith(tree.getRootPartition());
        assertThat(tree.getAllAncestors("C")).endsWith(tree.getRootPartition());
    }

    @Test
    void failJoiningAllLeavesIfNonLeafSpecified() {
        // Given
        Field field = new Field("key1", new StringType());
        Schema schema = Schema.builder().rowKeyFields(field).build();
        PartitionsBuilderSplitsFirst builder = PartitionsBuilderSplitsFirst.leavesWithSplits(schema,
                List.of("A", "B", "C"),
                List.of("aaa", "bbb"))
                .parentJoining("D", "A", "B");

        // When / Then
        assertThatThrownBy(builder::anyTreeJoiningAllLeaves)
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void shouldBuildPartitionsSpecifyingSplitOnSecondDimension() {
        // Given
        Field field1 = new Field("key1", new StringType());
        Field field2 = new Field("key2", new StringType());
        Schema schema = Schema.builder().rowKeyFields(field1, field2).build();

        // When
        PartitionTree tree = PartitionsBuilderSplitsFirst
                .leavesWithSplitsOnDimension(schema, 1, List.of("A", "B"), List.of("aaa"))
                .anyTreeJoiningAllLeaves()
                .buildTree();

        // Then
        RangeFactory rangeFactory = new RangeFactory(schema);
        assertThat(tree.getPartition("A").getRegion()).isEqualTo(new Region(List.of(
                rangeFactory.createRange(field1, "", null),
                rangeFactory.createRange(field2, "", "aaa"))));
        assertThat(tree.getPartition("B").getRegion()).isEqualTo(new Region(List.of(
                rangeFactory.createRange(field1, "", null),
                rangeFactory.createRange(field2, "aaa", null))));
    }

    @Test
    void shouldBuildPartitionsSpecifyingSplitOnTwoDifferentDimensions() {
        // Given
        Field field1 = new Field("key1", new StringType());
        Field field2 = new Field("key2", new StringType());
        Schema schema = Schema.builder().rowKeyFields(field1, field2).build();

        // When
        PartitionTree tree = new PartitionsBuilder(schema)
                .rootFirst("parent")
                .splitToNewChildrenOnDimension("parent", "A", "B", 0, "aaa")
                .splitToNewChildrenOnDimension("B", "C", "D", 1, "bbb")
                .buildTree();

        // Then
        RangeFactory rangeFactory = new RangeFactory(schema);
        assertThat(tree.getPartition("parent").getRegion()).isEqualTo(new Region(List.of(
                rangeFactory.createRange(field1, "", null),
                rangeFactory.createRange(field2, "", null))));
        assertThat(tree.getPartition("A").getRegion()).isEqualTo(new Region(List.of(
                rangeFactory.createRange(field1, "", "aaa"),
                rangeFactory.createRange(field2, "", null))));
        assertThat(tree.getPartition("B").getRegion()).isEqualTo(new Region(List.of(
                rangeFactory.createRange(field1, "aaa", null),
                rangeFactory.createRange(field2, "", null))));
        assertThat(tree.getPartition("C").getRegion()).isEqualTo(new Region(List.of(
                rangeFactory.createRange(field1, "aaa", null),
                rangeFactory.createRange(field2, "", "bbb"))));
        assertThat(tree.getPartition("D").getRegion()).isEqualTo(new Region(List.of(
                rangeFactory.createRange(field1, "aaa", null),
                rangeFactory.createRange(field2, "bbb", null))));
    }

    @Test
    void shouldBuildSinglePartitionTree() {
        Field field = new Field("key1", new StringType());
        Schema schema = Schema.builder().rowKeyFields(field).build();
        PartitionTree tree = new PartitionsBuilder(schema)
                .singlePartition("A")
                .buildTree();

        assertThat(tree.getAllPartitions())
                .containsExactly(tree.getRootPartition());
        assertThat(tree.getPartition("A"))
                .isEqualTo(tree.getRootPartition());
    }

    @Test
    void shouldFailWhenStartingFromSplitsThenFromRoot() {
        PartitionsBuilder builder = PartitionsBuilderSplitsFirst.leavesWithSplits(
                schemaWithKey("key", new StringType()),
                List.of("A", "B"), List.of("aaa"));

        assertThatThrownBy(() -> builder.rootFirst("root"))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
