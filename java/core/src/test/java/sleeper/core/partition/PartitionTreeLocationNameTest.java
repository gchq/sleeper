/*
 * Copyright 2022-2026 Crown Copyright
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

import sleeper.core.schema.Schema;
import sleeper.core.schema.type.StringType;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.core.schema.SchemaTestHelper.createSchemaWithKey;

public class PartitionTreeLocationNameTest {
    Schema schema = createSchemaWithKey("key", new StringType());

    @Test
    void shouldNameRootPartition() {
        // Given
        PartitionTree tree = new PartitionsBuilder(schema)
                .singlePartition("A")
                .buildTree();

        // When / Then
        assertThat(tree.buildLocationName(tree.getPartition("A")))
                .isEqualTo("root");
    }

    @Test
    void shouldNameChildrenOfRootPartition() {
        // Given
        PartitionTree tree = new PartitionsBuilder(schema)
                .rootFirst("A")
                .splitToNewChildren("A", "B", "C", "mmm")
                .buildTree();

        // When / Then
        assertThat(tree.buildLocationName(tree.getPartition("B")))
                .isEqualTo("L");
        assertThat(tree.buildLocationName(tree.getPartition("C")))
                .isEqualTo("R");
    }

    @Test
    void shouldNameNestedPartitionsWithPathFromRoot() {
        // Given
        PartitionTree tree = new PartitionsBuilder(schema)
                .rootFirst("A")
                .splitToNewChildren("A", "B", "C", "mmm")
                .splitToNewChildren("B", "D", "E", "ggg")
                .splitToNewChildren("C", "F", "G", "ttt")
                .buildTree();

        // When / Then
        assertThat(tree.buildLocationName(tree.getPartition("D")))
                .isEqualTo("LL");
        assertThat(tree.buildLocationName(tree.getPartition("E")))
                .isEqualTo("LR");
        assertThat(tree.buildLocationName(tree.getPartition("F")))
                .isEqualTo("RL");
        assertThat(tree.buildLocationName(tree.getPartition("G")))
                .isEqualTo("RR");
    }

    @Test
    void shouldNameDeeplyNestedPartitionInOrderFromRoot() {
        // Given
        PartitionTree tree = new PartitionsBuilder(schema)
                .rootFirst("A")
                .splitToNewChildren("A", "B", "C", "mmm")
                .splitToNewChildren("B", "D", "E", "ggg")
                .splitToNewChildren("E", "F", "G", "jjj")
                .splitToNewChildren("G", "H", "I", "kkk")
                .buildTree();

        // When / Then
        assertThat(tree.buildLocationName(tree.getPartition("H")))
                .isEqualTo("LRRL");
    }

    @Test
    void shouldFailWhenPartitionIsNotAChildOfItsParent() {
        // Given
        PartitionTree validTree = new PartitionsBuilder(schema)
                .rootFirst("A")
                .splitToNewChildren("A", "B", "C", "mmm")
                .buildTree();
        Partition root = validTree.getPartition("A");
        PartitionTree tree = new PartitionTree(List.of(
                root.toBuilder().childPartitionIds(List.of("B")).build(),
                validTree.getPartition("B"),
                validTree.getPartition("C")));
        Partition partition = tree.getPartition("C");

        // When / Then
        assertThatThrownBy(() -> tree.buildLocationName(partition))
                .isInstanceOf(IllegalStateException.class);
    }
}
