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

import sleeper.core.key.Key;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.StringType;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.schema.SchemaTestHelper.createSchemaWithKey;

public class PartitionTreeNearestCommonAncestorTest {
    Schema schema = createSchemaWithKey("key1", new StringType());

    @Test
    public void shouldTellWhenKeysAreOnSamePartition() {
        // Given
        PartitionTree tree = PartitionsFromSplitPoints.treeFrom(schema, Collections.emptyList());

        // When / Then
        assertThat(tree.getNearestCommonAncestor(schema, Key.create("a"), Key.create("b")))
                .isEqualTo(tree.getRootPartition());
    }

    @Test
    public void shouldGetRootPartitionWhenKeysAreOnDifferentChildPartitions() {
        // Given
        PartitionTree tree = PartitionsFromSplitPoints.treeFrom(schema, Collections.singletonList("abc"));

        // When / Then
        assertThat(tree.getNearestCommonAncestor(schema, Key.create("a"), Key.create("b")))
                .isEqualTo(tree.getRootPartition());
    }

    @Test
    public void shouldGetRootPartitionWhenKeysAreOnNestedPartitionsOutsideMaxAndMinSplitPoints() {
        // Given
        PartitionTree tree = PartitionsFromSplitPoints.treeFrom(schema, Arrays.asList("abc", "def"));

        // When / Then
        assertThat(tree.getNearestCommonAncestor(schema, Key.create("a"), Key.create("z")))
                .isEqualTo(tree.getRootPartition());
    }

    @Test
    public void shouldGetMidPartitionForPartitionsWithSameMidParent() {
        // Given
        PartitionTree tree = PartitionsBuilderSplitsFirst.leavesWithSplits(schema,
                List.of("A", "B", "C"),
                List.of("abc", "def"))
                .parentJoining("D", "A", "B")
                .parentJoining("E", "D", "C")
                .buildTree();

        // When / Then
        assertThat(tree.getNearestCommonAncestor(schema, Key.create("a"), Key.create("d")))
                .isEqualTo(tree.getPartition("D"));
    }

    @Test
    public void shouldGetMidPartitionForPartitionsWithSeparatedMidAncestor() {
        // Given
        PartitionTree tree = PartitionsBuilderSplitsFirst.leavesWithSplits(schema,
                List.of("A", "B", "C", "D"),
                List.of("abc", "def", "ghi"))
                .parentJoining("E", "A", "B")
                .parentJoining("F", "E", "C")
                .parentJoining("G", "F", "D")
                .buildTree();

        // When / Then
        assertThat(tree.getNearestCommonAncestor(schema, Key.create("a"), Key.create("f")))
                .isEqualTo(tree.getPartition("F"));
    }
}
