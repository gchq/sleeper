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
    void shouldBuildPartitionsSpecifyingSplitPointsRootFirst() {
        // Given
        Field field = new Field("key1", new StringType());
        Schema schema = Schema.builder().rowKeyFields(field).build();

        // When
        PartitionsBuilder builder = new PartitionsBuilder(schema)
                .rootFirst("root")
                .splitToNewChildren("root", "L", "R", "aaa")
                .splitToNewChildren("R", "RL", "RR", "bbb");

        // Then
        RangeFactory rangeFactory = new RangeFactory(schema);
        List<Partition> expectedPartitions = List.of(
                Partition.builder()
                        .region(new Region(rangeFactory.createRange(field, "", null)))
                        .id("root")
                        .leafPartition(false)
                        .parentPartitionId(null)
                        .childPartitionIds(List.of("L", "R"))
                        .dimension(0)
                        .build(),
                Partition.builder()
                        .region(new Region(rangeFactory.createRange(field, "", "aaa")))
                        .id("L")
                        .leafPartition(true)
                        .parentPartitionId("root")
                        .childPartitionIds(List.of())
                        .dimension(-1)
                        .build(),
                Partition.builder()
                        .region(new Region(rangeFactory.createRange(field, "aaa", null)))
                        .id("R")
                        .leafPartition(false)
                        .parentPartitionId("root")
                        .childPartitionIds(List.of("RL", "RR"))
                        .dimension(0)
                        .build(),
                Partition.builder()
                        .region(new Region(rangeFactory.createRange(field, "aaa", "bbb")))
                        .id("RL")
                        .leafPartition(true)
                        .parentPartitionId("R")
                        .childPartitionIds(List.of())
                        .dimension(-1)
                        .build(),
                Partition.builder()
                        .region(new Region(rangeFactory.createRange(field, "bbb", null)))
                        .id("RR")
                        .leafPartition(true)
                        .parentPartitionId("R")
                        .childPartitionIds(List.of())
                        .dimension(-1)
                        .build());
        assertThat(builder.buildList()).isEqualTo(expectedPartitions);
        assertThat(builder.buildTree()).isEqualTo(new PartitionTree(expectedPartitions));
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
        Schema schema = schemaWithKey("key");
        PartitionTree tree = new PartitionsBuilder(schema)
                .singlePartition("A")
                .buildTree();

        assertThat(tree.getAllPartitions())
                .containsExactly(tree.getRootPartition());
        assertThat(tree.getPartition("A"))
                .isEqualTo(tree.getRootPartition());
    }

    @Test
    void shouldFailSpecifyingRootTwice() {
        Schema schema = schemaWithKey("key");
        PartitionsBuilder builder = new PartitionsBuilder(schema)
                .rootFirst("root");

        assertThatThrownBy(() -> builder.singlePartition("root"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void shouldFailSpecifyingSameChildTwice() {
        Schema schema = schemaWithKey("key", new StringType());
        PartitionsBuilder builder = new PartitionsBuilder(schema)
                .rootFirst("root")
                .splitToNewChildren("root", "A", "B", "aaa");

        assertThatThrownBy(() -> builder.splitToNewChildren("root", "B", "C", "bbb"))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
