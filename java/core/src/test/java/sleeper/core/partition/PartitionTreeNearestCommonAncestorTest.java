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

import sleeper.core.key.Key;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.StringType;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class PartitionTreeNearestCommonAncestorTest {

    @Test
    public void shouldTellWhenBothPartitionsAreTheSame() {
        Schema schema = Schema.builder().rowKeyFields(new Field("key1", new StringType())).build();
        PartitionTree tree = PartitionsFromSplitPoints.treeFrom(schema, Collections.emptyList());

        assertThat(tree.getNearestCommonAncestor(schema, Key.create("a"), Key.create("b")))
                .isEqualTo(tree.getRootPartition());
    }

    @Test
    public void shouldGetRootPartitionForTwoImmediatelyBeneathIt() {
        Schema schema = Schema.builder().rowKeyFields(new Field("key1", new StringType())).build();
        PartitionTree tree = PartitionsFromSplitPoints.treeFrom(schema, Collections.singletonList("abc"));

        assertThat(tree.getNearestCommonAncestor(schema, Key.create("a"), Key.create("b")))
                .isEqualTo(tree.getRootPartition());
    }

    @Test
    public void shouldGetRootPartitionForTwoOutsideMaxAndMinSplitPoints() {
        Schema schema = Schema.builder().rowKeyFields(new Field("key1", new StringType())).build();
        PartitionTree tree = PartitionsFromSplitPoints.treeFrom(schema, Arrays.asList("abc", "def"));

        assertThat(tree.getNearestCommonAncestor(schema, Key.create("a"), Key.create("z")))
                .isEqualTo(tree.getRootPartition());
    }

    @Test
    public void shouldGetMidPartitionForPartitionsWithSameMidParent() {
        Schema schema = Schema.builder().rowKeyFields(new Field("key1", new StringType())).build();
        PartitionTree tree = PartitionsBuilderSplitsFirst.leavesWithSplits(schema,
                List.of("A", "B", "C"),
                List.of("abc", "def"))
                .parentJoining("D", "A", "B")
                .parentJoining("E", "D", "C")
                .buildTree();

        assertThat(tree.getNearestCommonAncestor(schema, Key.create("a"), Key.create("d")))
                .isEqualTo(tree.getPartition("D"));
    }

    @Test
    public void shouldGetMidPartitionForPartitionsWithSeparatedMidAncestor() {
        Schema schema = Schema.builder().rowKeyFields(new Field("key1", new StringType())).build();
        PartitionTree tree = PartitionsBuilderSplitsFirst.leavesWithSplits(schema,
                List.of("A", "B", "C", "D"),
                List.of("abc", "def", "ghi"))
                .parentJoining("E", "A", "B")
                .parentJoining("F", "E", "C")
                .parentJoining("G", "F", "D")
                .buildTree();

        assertThat(tree.getNearestCommonAncestor(schema, Key.create("a"), Key.create("f")))
                .isEqualTo(tree.getPartition("F"));
    }
}
