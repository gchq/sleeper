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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class PartitionSubtreeFactoryTest extends PartitionTreeTestBase {
    public static final Logger LOGGER = LoggerFactory.getLogger(PartitionSubtreeFactoryTest.class);

    @Test
    void shouldCreateBalancedSubtreeWithExactLeafPartitionCount() {
        // Given / When
        int leafPartitionCount = 2;

        PartitionTree subtree = PartitionSubtreeFactory.createSubtree(
                new PartitionTree(List.of(rootWithChildren,
                        l1Left, l1Right,
                        l2LeftOfL1L, l2RightOfL1L, l2LeftOfL1R, l2RightOfL1R)),
                leafPartitionCount);

        // Then
        Partition l1LeftAsLeaf = adjustToLeafStatus(l1Left);
        Partition l1RightAsLeaf = adjustToLeafStatus(l1Right);
        assertThat(subtree.getLeafPartitions()).contains(l1LeftAsLeaf, l1RightAsLeaf);
        assertThat(subtree.getLeafPartitions().size()).isEqualTo(leafPartitionCount);
    }

    @Test
    void shouldFindSubtreeDownTwoLevelsOfTreeWithThreeLevelsOfSplits() {
        // Given
        PartitionTree tree = new PartitionsBuilder(schema)
                .rootFirst("root")
                .splitToNewChildren("root", "L", "R", 0L)
                .splitToNewChildren("L", "LL", "LR", -100L)
                .splitToNewChildren("R", "RL", "RR", 100L)
                .splitToNewChildren("LL", "LLL", "LLR", -150L)
                .splitToNewChildren("LR", "LRL", "LRR", -50L)
                .splitToNewChildren("RL", "RLL", "RLR", 50L)
                .splitToNewChildren("RR", "RRL", "RRR", 150L)
                .buildTree();

        // When
        PartitionTree result = PartitionSubtreeFactory.createSubtree(tree, 5);

        // Then
        assertThat(result).isEqualTo(new PartitionsBuilder(schema)
                .rootFirst("root")
                .splitToNewChildren("root", "L", "R", 0L)
                .splitToNewChildren("L", "LL", "LR", -100L)
                .splitToNewChildren("R", "RL", "RR", 100L)
                .splitToNewChildren("LL", "LLL", "LLR", -150L)
                .buildTree());
    }

    @Test
    void shouldGetSubtreeForManyLeafPartitions() {
        // Given
        Instant start = Instant.now();
        List<Object> splitPoints = LongStream.range(0, 100000).mapToObj(i -> (Object) i).toList();
        PartitionTree tree = PartitionsFromSplitPoints.treeFrom(schema, splitPoints);
        Instant generated = Instant.now();

        // When
        PartitionTree result = PartitionSubtreeFactory.createSubtree(tree, 50000);
        Instant end = Instant.now();

        // Then
        assertThat(result.getLeafPartitions()).hasSize(50000);
        LOGGER.info("Generated in {}, ran in {}", Duration.between(start, generated), Duration.between(generated, end));
    }

    @Test
    void shouldCreateBalancedSubtreeWithLeafCountCausingUnbalancedTreeLeftBias() {
        // Given / When
        int leafPartitionCount = 3;
        PartitionTree subtree = PartitionSubtreeFactory.createSubtree(
                new PartitionTree(List.of(rootWithChildren,
                        l1Left, l1Right,
                        l2LeftOfL1L, l2RightOfL1L, l2LeftOfL1R, l2RightOfL1R)),
                leafPartitionCount);

        // Then
        assertThat(subtree.getLeafPartitions().size()).isEqualTo(leafPartitionCount);
        assertThat(subtree).isEqualTo(generateTreeTo2LevelsUnbalanced());
    }

    @Test
    void shouldCreateRootOnlySubtreeWhenGivenLeafPartitions() {
        // Given / When
        int leafPartitionCount = 0;
        PartitionTree subtree = PartitionSubtreeFactory.createSubtree(generateTreeTo2LevelsBalanced(), leafPartitionCount);

        // Then
        assertThat(subtree).isEqualTo(generateTreeToRootLevel());

        //Incremented to account for root now being the only leaf
        assertThat(subtree.getLeafPartitions().size()).isEqualTo(leafPartitionCount + 1);
    }

    @Test
    void shouldCreateSeperateSubtreesFrom3LevelTree() {
        // Given
        int midPartitionCount = 4;
        int smallPartitionCount = 2;

        PartitionTree originalTree = new PartitionTree(List.of(rootWithChildren,
                l1Left, l1Right,
                l2LeftOfL1L, l2RightOfL1L, l2LeftOfL1R, l2RightOfL1R,
                l3LeftOfL2LL, l3RightOfL2LL, l3LeftOfL2LR, l3RightOfL2LR, l3LeftOfL2RL, l3RightOfL2RL, l3LeftOfL2RR, l3RightOfL2RR));

        // When 1
        PartitionTree midSubtree = PartitionSubtreeFactory.createSubtree(originalTree, midPartitionCount);

        // Then 1
        assertThat(midSubtree.getLeafPartitions().size()).isEqualTo(midPartitionCount);
        assertThat(midSubtree).isEqualTo(generateTreeTo2LevelsBalanced());

        // When 2
        PartitionTree smallSubtree = PartitionSubtreeFactory.createSubtree(originalTree, smallPartitionCount);

        // Then 2
        assertThat(smallSubtree.getLeafPartitions().size()).isEqualTo(smallPartitionCount);
        assertThat(smallSubtree).isEqualTo(generateTreeTo1Level());

    }

    @Test
    void shouldThrowExceptionIfRequestLeafCountGreaterThanOriginalTree() {
        assertThatThrownBy(() -> PartitionSubtreeFactory.createSubtree(generateTreeTo1Level(), 7))
                .isInstanceOf(PartitionTreeException.class)
                .hasMessageContaining("Requested size of 7 is greater than");
    }
}
