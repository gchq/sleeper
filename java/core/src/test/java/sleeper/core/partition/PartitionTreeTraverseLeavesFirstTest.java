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

import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.StringType;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class PartitionTreeTraverseLeavesFirstTest {

    private static final Schema SCHEMA = Schema.builder()
            .rowKeyFields(new Field("key", new StringType()))
            .build();

    public static PartitionsBuilderSplitsFirst leavesWithSplits(List<String> ids, List<Object> splits) {
        return PartitionsBuilderSplitsFirst.leavesWithSplits(SCHEMA, ids, splits);
    }

    @Test
    public void shouldTraversePartitionsByTreeLeavesFirst() {
        // Given
        PartitionTree partitions = leavesWithSplits(
                List.of("A", "B", "C"), List.of("aaa", "bbb"))
                .parentJoining("D", "A", "B")
                .parentJoining("root", "D", "C")
                .buildTree();

        // When / Then
        assertThat(partitions.traverseLeavesFirst()).extracting("id")
                .containsExactly("A", "B", "C", "D", "root");
    }

    @Test
    public void shouldPutMinAndMaxSideOfPartitionSplitInOrderWhenIdsAreNotInAlphabeticalOrder() {
        // Given
        PartitionTree partitions = leavesWithSplits(
                List.of("some-leaf", "other-leaf"), List.of("aaa"))
                .parentJoining("root", "some-leaf", "other-leaf")
                .buildTree();

        // When / Then
        assertThat(partitions.traverseLeavesFirst()).extracting("id")
                .containsExactly("some-leaf", "other-leaf", "root");
    }

    @Test
    public void shouldPutNestedLeavesInOrderOfPartitionSplitSideWhenParentIdsAreNotInAlphabeticalOrder() {
        // Given
        PartitionTree partitions = leavesWithSplits(
                List.of("some-leaf", "other-leaf", "third-leaf", "fourth-leaf"),
                List.of("aaa", "bbb", "ccc"))
                .parentJoining("some-middle", "some-leaf", "other-leaf")
                .parentJoining("other-middle", "third-leaf", "fourth-leaf")
                .parentJoining("root", "some-middle", "other-middle")
                .buildTree();

        // When / Then
        assertThat(partitions.traverseLeavesFirst()).extracting("id").containsExactly(
                "some-leaf", "other-leaf", "third-leaf", "fourth-leaf",
                "some-middle", "other-middle", "root");
    }
}
