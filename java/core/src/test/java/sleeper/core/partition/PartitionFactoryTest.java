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
package sleeper.core.partition;

import org.junit.jupiter.api.Test;

import sleeper.core.range.Range.RangeFactory;
import sleeper.core.range.Region;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.PrimitiveType;
import sleeper.core.schema.type.StringType;

import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class PartitionFactoryTest {

    private PartitionSplitResult toPartition(Partition parent, String leftId, String rightId, int dimension, Object splitPoint, PartitionFactory partitionFactory) {
        return partitionFactory.split(parent, leftId, rightId, dimension, splitPoint);
    }

    @Test
    void shouldSpecifyParentThenChildPartition() {
        Field key = new Field("key", new StringType());
        Schema schema = Schema.builder().rowKeyFields(key).build();
        RangeFactory rangeFactory = new RangeFactory(schema);
        PartitionFactory partitionFactory = new PartitionFactory(schema);
        Partition parent = partitionFactory.rootFirst("parent").build();
        PartitionSplitResult splitResult = toPartition(parent, "left", "right", 0, "aaa", partitionFactory);
        List<Partition> children = splitResult.buildChildren();
        parent = splitResult.buildParent();

        List<PrimitiveType> rowKeyTypes = schema.getRowKeyTypes();
        assertThat(parent).isEqualTo(
                Partition.builder()
                        .rowKeyTypes(rowKeyTypes)
                        .region(new Region(rangeFactory.createRange(key, "", null)))
                        .id("parent")
                        .leafPartition(false)
                        .parentPartitionId(null)
                        .childPartitionIds(List.of("left", "right"))
                        .dimension(0)
                        .build());
        assertThat(children).containsExactly(
                Partition.builder()
                        .rowKeyTypes(rowKeyTypes)
                        .region(new Region(rangeFactory.createRange(key, "", "aaa")))
                        .id("left")
                        .leafPartition(true)
                        .parentPartitionId("parent")
                        .childPartitionIds(Collections.emptyList())
                        .dimension(-1)
                        .build(),
                Partition.builder()
                        .rowKeyTypes(rowKeyTypes)
                        .region(new Region(rangeFactory.createRange(key, "aaa", null)))
                        .id("right")
                        .leafPartition(true)
                        .parentPartitionId("parent")
                        .childPartitionIds(Collections.emptyList())
                        .dimension(-1)
                        .build());
    }

    @Test
    void shouldSpecifyParentThenChildPartitionsWithTwoDimensions() {
        Field key1 = new Field("key1", new StringType());
        Field key2 = new Field("key2", new StringType());
        Schema schema = Schema.builder().rowKeyFields(key1, key2).build();
        RangeFactory rangeFactory = new RangeFactory(schema);
        PartitionFactory partitionFactory = new PartitionFactory(schema);
        Partition parent = partitionFactory.rootFirst("parent").build();
        PartitionSplitResult splitResult = toPartition(parent, "left", "right", 0, "aaa", partitionFactory);
        List<Partition> children = splitResult.buildChildren();
        parent = splitResult.buildParent();
        PartitionSplitResult nestedSplitResult = toPartition(children.get(1), "nestedLeft", "nestedRight", 1, "bbb", partitionFactory);
        List<Partition> nestedChildren = nestedSplitResult.buildChildren();
        children.set(1, nestedSplitResult.buildParent());


        List<PrimitiveType> rowKeyTypes = schema.getRowKeyTypes();
        assertThat(parent).isEqualTo(
                Partition.builder()
                        .rowKeyTypes(rowKeyTypes)
                        .region(new Region(List.of(
                                rangeFactory.createRange(key1, "", null),
                                rangeFactory.createRange(key2, "", null))))
                        .id("parent")
                        .leafPartition(false)
                        .parentPartitionId(null)
                        .childPartitionIds(List.of("left", "right"))
                        .dimension(0)
                        .build());
        assertThat(children).containsExactly(
                Partition.builder()
                        .rowKeyTypes(rowKeyTypes)
                        .region(new Region(List.of(
                                rangeFactory.createRange(key1, "", "aaa"),
                                rangeFactory.createRange(key2, "", null))))
                        .id("left")
                        .leafPartition(true)
                        .parentPartitionId("parent")
                        .childPartitionIds(Collections.emptyList())
                        .dimension(-1)
                        .build(),
                Partition.builder()
                        .rowKeyTypes(rowKeyTypes)
                        .region(new Region(List.of(
                                rangeFactory.createRange(key1, "aaa", null),
                                rangeFactory.createRange(key2, "", null))))
                        .id("right")
                        .leafPartition(false)
                        .parentPartitionId("parent")
                        .childPartitionIds(List.of("nestedLeft", "nestedRight"))
                        .dimension(1)
                        .build());
        assertThat(nestedChildren).containsExactly(
                Partition.builder()
                        .rowKeyTypes(rowKeyTypes)
                        .region(new Region(List.of(
                                rangeFactory.createRange(key1, "aaa", null),
                                rangeFactory.createRange(key2, "", "bbb"))))
                        .id("nestedLeft")
                        .leafPartition(true)
                        .parentPartitionId("right")
                        .childPartitionIds(Collections.emptyList())
                        .dimension(-1)
                        .build(),
                Partition.builder()
                        .rowKeyTypes(rowKeyTypes)
                        .region(new Region(List.of(
                                rangeFactory.createRange(key1, "aaa", null),
                                rangeFactory.createRange(key2, "bbb", null))))
                        .id("nestedRight")
                        .leafPartition(true)
                        .parentPartitionId("right")
                        .childPartitionIds(Collections.emptyList())
                        .dimension(-1)
                        .build());
    }

    @Test
    void shouldSpecifyChildrenThenParentPartition() {
        Field key = new Field("key", new StringType());
        Schema schema = Schema.builder().rowKeyFields(key).build();
        RangeFactory rangeFactory = new RangeFactory(schema);
        PartitionFactory partitionFactory = new PartitionFactory(schema);
        Partition.Builder a = partitionFactory.partition("A", "", "aaa");
        Partition.Builder b = partitionFactory.partition("B", "aaa", null);
        Partition parent = partitionFactory.parentJoining("parent", a, b).build();

        List<PrimitiveType> rowKeyTypes = schema.getRowKeyTypes();
        assertThat(a.build()).isEqualTo(
                Partition.builder()
                        .rowKeyTypes(rowKeyTypes)
                        .region(new Region(rangeFactory.createRange(key, "", "aaa")))
                        .id("A")
                        .leafPartition(true)
                        .parentPartitionId("parent")
                        .childPartitionIds(Collections.emptyList())
                        .dimension(-1)
                        .build());
        assertThat(b.build()).isEqualTo(
                Partition.builder()
                        .rowKeyTypes(rowKeyTypes)
                        .region(new Region(rangeFactory.createRange(key, "aaa", null)))
                        .id("B")
                        .leafPartition(true)
                        .parentPartitionId("parent")
                        .childPartitionIds(Collections.emptyList())
                        .dimension(-1)
                        .build());
        assertThat(parent).isEqualTo(
                Partition.builder()
                        .rowKeyTypes(rowKeyTypes)
                        .region(new Region(rangeFactory.createRange(key, "", null)))
                        .id("parent")
                        .leafPartition(false)
                        .parentPartitionId(null)
                        .childPartitionIds("A", "B")
                        .dimension(0)
                        .build());
    }
}
