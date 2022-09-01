/*
 * Copyright 2022 Crown Copyright
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

import org.junit.Test;
import sleeper.core.range.Range;
import sleeper.core.range.Region;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.PrimitiveType;
import sleeper.core.schema.type.StringType;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class PartitionsBuilderTest {

    @Test
    public void canBuildPartitionsRootFirst() {
        // Given
        Field field = new Field("key1", new StringType());
        Schema schema = Schema.builder().rowKeyFields(field).build();

        // When
        PartitionsBuilder builder = new PartitionsBuilder(schema);
        Partition root = builder.partition("A", "", null);
        Partition mid = builder.child(root, "B", "", "def");
        builder.child(mid, "C", "", "abc");
        builder.child(mid, "D", "abc", "def");
        builder.child(root, "E", "def", null);

        // Then
        List<PrimitiveType> rowKeyTypes = schema.getRowKeyTypes();
        List<Partition> expectedPartitions = Arrays.asList(
                new Partition(rowKeyTypes, new Region(new Range(field, "", null)),
                        "A", false, null, Arrays.asList("B", "E"), 0),
                new Partition(rowKeyTypes, new Region(new Range(field, "", "def")),
                        "B", false, "A", Arrays.asList("C", "D"), 0),
                new Partition(rowKeyTypes, new Region(new Range(field, "", "abc")),
                        "C", true, "B", Collections.emptyList(), -1),
                new Partition(rowKeyTypes, new Region(new Range(field, "abc", "def")),
                        "D", true, "B", Collections.emptyList(), -1),
                new Partition(rowKeyTypes, new Region(new Range(field, "def", null)),
                        "E", true, "A", Collections.emptyList(), -1));
        assertThat(builder.buildList()).isEqualTo(expectedPartitions);
        assertThat(builder.buildTree()).isEqualTo(new PartitionTree(schema, expectedPartitions));
    }

    @Test
    public void canBuildPartitionsLeafFirst() {
        // Given
        Field field = new Field("key1", new StringType());
        Schema schema = Schema.builder().rowKeyFields(field).build();

        // When
        PartitionsBuilder builder = new PartitionsBuilder(schema);
        Partition a = builder.partition("A", "", "abc");
        Partition b = builder.partition("B", "abc", "def");
        Partition c = builder.partition("C", "def", null);
        Partition mid = builder.parent(Arrays.asList(a, b), "D", "", "def");
        Partition root = builder.parent(Arrays.asList(mid, c), "E", "", null);

        // Then
        List<PrimitiveType> rowKeyTypes = schema.getRowKeyTypes();
        List<Partition> expectedPartitions = Arrays.asList(
                new Partition(rowKeyTypes, new Region(new Range(field, "", "abc")),
                        "A", true, "D", Collections.emptyList(), -1),
                new Partition(rowKeyTypes, new Region(new Range(field, "abc", "def")),
                        "B", true, "D", Collections.emptyList(), -1),
                new Partition(rowKeyTypes, new Region(new Range(field, "def", null)),
                        "C", true, "E", Collections.emptyList(), -1),
                new Partition(rowKeyTypes, new Region(new Range(field, "", "def")),
                        "D", false, "E", Arrays.asList("A", "B"), 0),
                new Partition(rowKeyTypes, new Region(new Range(field, "", null)),
                        "E", false, null, Arrays.asList("D", "C"), 0));
        assertThat(builder.buildList()).isEqualTo(expectedPartitions);
        assertThat(builder.buildTree()).isEqualTo(new PartitionTree(schema, expectedPartitions));
    }

    @Test
    public void canBuildPartitionsSpecifyingSplitPoints() {
        // Given
        Field field = new Field("key1", new StringType());
        Schema schema = Schema.builder().rowKeyFields(field).build();

        // When
        PartitionsBuilder builder = new PartitionsBuilder(schema)
                .root("A", "", null)
                .split("A", "B", "C", "abc")
                .split("C", "D", "E", "def");

        // Then
        List<PrimitiveType> rowKeyTypes = schema.getRowKeyTypes();
        List<Partition> expectedPartitions = Arrays.asList(
                new Partition(rowKeyTypes, new Region(new Range(field, "", null)),
                        "A", false, null, Arrays.asList("B", "C"), 0),
                new Partition(rowKeyTypes, new Region(new Range(field, "", "abc")),
                        "B", true, "A", Collections.emptyList(), -1),
                new Partition(rowKeyTypes, new Region(new Range(field, "abc", null)),
                        "C", false, "A", Arrays.asList("D", "E"), 0),
                new Partition(rowKeyTypes, new Region(new Range(field, "abc", "def")),
                        "D", true, "C", Collections.emptyList(), -1),
                new Partition(rowKeyTypes, new Region(new Range(field, "def", null)),
                        "E", true, "C", Collections.emptyList(), -1));
        assertThat(builder.buildList()).isEqualTo(expectedPartitions);
        assertThat(builder.buildTree()).isEqualTo(new PartitionTree(schema, expectedPartitions));
    }

    @Test
    public void canBuildPartitionsSpecifyingSplitPointsLeavesFirst() {
        // Given
        Field field = new Field("key1", new StringType());
        Schema schema = Schema.builder().rowKeyFields(field).build();

        // When
        PartitionsBuilder builder = new PartitionsBuilder(schema)
                .leavesWithSplits(
                        Arrays.asList("A", "B", "C"),
                        Arrays.asList("aaa", "bbb"))
                .join("D", "A", "B")
                .join("E", "D", "C");

        // Then
        List<PrimitiveType> rowKeyTypes = schema.getRowKeyTypes();
        List<Partition> expectedPartitions = Arrays.asList(
                new Partition(rowKeyTypes, new Region(new Range(field, "", "aaa")),
                        "A", true, "D", Collections.emptyList(), -1),
                new Partition(rowKeyTypes, new Region(new Range(field, "aaa", "bbb")),
                        "B", true, "D", Collections.emptyList(), -1),
                new Partition(rowKeyTypes, new Region(new Range(field, "bbb", null)),
                        "C", true, "E", Collections.emptyList(), -1),
                new Partition(rowKeyTypes, new Region(new Range(field, "", "bbb")),
                        "D", false, "E", Arrays.asList("A", "B"), 0),
                new Partition(rowKeyTypes, new Region(new Range(field, "", null)),
                        "E", false, null, Arrays.asList("D", "C"), 0));
        assertThat(builder.buildList()).isEqualTo(expectedPartitions);
        assertThat(builder.buildTree()).isEqualTo(new PartitionTree(schema, expectedPartitions));
    }

    @Test
    public void canBuildPartitionsSpecifyingSplitPointsLeavesFirstJoinAllLeftFirst() {
        // Given
        Field field = new Field("key1", new StringType());
        Schema schema = Schema.builder().rowKeyFields(field).build();

        // When
        PartitionsBuilder builder = new PartitionsBuilder(schema)
                .leavesWithSplits(
                        Arrays.asList("A", "B", "C"),
                        Arrays.asList("aaa", "bbb"))
                .joinAllLeftFirst("D", "E");

        // Then
        assertThat(builder.buildList()).isEqualTo(new PartitionsBuilder(schema)
                .leavesWithSplits(
                        Arrays.asList("A", "B", "C"),
                        Arrays.asList("aaa", "bbb"))
                .join("D", "A", "B")
                .join("E", "D", "C")
                .buildList());
    }
}
