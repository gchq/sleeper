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

import sleeper.core.testutils.printers.PartitionsPrinter;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;

public class PartitionSubtreeFactoryTest extends PartitionTreeTestBase {
    public static final Logger LOGGER = LoggerFactory.getLogger(PartitionSubtreeFactoryTest.class);

    @Test
    void shouldCreateBalancedSubtreeWithExactLeafPartitionCount() throws PartitionTreeException {
        // Given / When
        int leafPartitionCount = 2;

        PartitionTree originalTree = new PartitionsBuilder(schema)
                .rootFirst(ROOT)
                .splitToNewChildren(ROOT, L1_LEFT, L1_RIGHT, 0L)
                .splitToNewChildren(L1_LEFT, L2_LEFT_OF_L1L, L2_RIGHT_OF_L1L, -1000000L)
                .splitToNewChildren(L1_RIGHT, L2_LEFT_OF_L1R, L2_RIGHT_OF_L1R, 123456789L)
                .buildTree();
        PartitionTree subtree = PartitionSubtreeFactory.createSubtree(originalTree, leafPartitionCount);

        // Then
        assertThat(subtree)
                .withRepresentation(tree -> PartitionsPrinter.printPartitions(schema, (PartitionTree) tree))
                .isEqualTo(new PartitionsBuilder(schema)
                        .rootFirst(ROOT)
                        .splitToNewChildren(ROOT, L1_LEFT, L1_RIGHT, 0L).buildTree());
        assertThat(subtree.getLeafPartitionIds()).containsExactly(L1_LEFT, L1_RIGHT);
    }

    @Test
    void shouldFindSubtreeDownTwoLevelsOfTreeWithThreeLevelsOfSplits() throws PartitionTreeException {
        // Given
        PartitionTree originalTree = new PartitionsBuilder(schema)
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
        PartitionTree subtree = PartitionSubtreeFactory.createSubtree(originalTree, 5);

        // Then
        assertThat(subtree)
                .withRepresentation(tree -> PartitionsPrinter.printPartitions(schema, (PartitionTree) tree))
                .isEqualTo(new PartitionsBuilder(schema)
                        .rootFirst("root")
                        .splitToNewChildren("root", "L", "R", 0L)
                        .splitToNewChildren("L", "LL", "LR", -100L)
                        .splitToNewChildren("R", "RL", "RR", 100L)
                        .splitToNewChildren("LL", "LLL", "LLR", -150L)
                        .buildTree());
        assertThat(subtree.getLeafPartitionIds()).containsExactly("LLL", "LLR", "LR", "RL", "RR");
    }

    @Test
    void shouldCreateBalancedSubtreeWithLeafCountCausingUnbalancedTreeLeftBias() throws PartitionTreeException {
        // Given / When
        int leafPartitionCount = 3;
        PartitionTree subtree = PartitionSubtreeFactory.createSubtree(
                new PartitionsBuilder(schema)
                        .rootFirst(ROOT)
                        .splitToNewChildren(ROOT, L1_LEFT, L1_RIGHT, 0L)
                        .splitToNewChildren(L1_LEFT, L2_LEFT_OF_L1L, L2_RIGHT_OF_L1L, -1000000L)
                        .splitToNewChildren(L1_RIGHT, L2_LEFT_OF_L1R, L2_RIGHT_OF_L1R, 123456789L)
                        .buildTree(),
                leafPartitionCount);

        // Then
        assertThat(subtree)
                .withRepresentation(tree -> PartitionsPrinter.printPartitions(schema, (PartitionTree) tree))
                .isEqualTo(new PartitionsBuilder(schema)
                        .rootFirst(ROOT)
                        .splitToNewChildren(ROOT, L1_LEFT, L1_RIGHT, 0L)
                        .splitToNewChildren(L1_LEFT, L2_LEFT_OF_L1L, L2_RIGHT_OF_L1L, -1000000L)
                        .buildTree());
        assertThat(subtree.getLeafPartitionIds())
                .containsExactly(L1_RIGHT,
                        L2_LEFT_OF_L1L, L2_RIGHT_OF_L1L);
    }

    @Test
    void shouldCreateRootOnlySubtreeWhenGivenLeafPartitions() throws PartitionTreeException {
        // Given / When
        int leafPartitionCount = 0;
        PartitionTree subtree = PartitionSubtreeFactory.createSubtree(new PartitionsBuilder(schema)
                .rootFirst(ROOT)
                .splitToNewChildren(ROOT, L1_LEFT, L1_RIGHT, 0L)
                .splitToNewChildren(L1_LEFT, L2_LEFT_OF_L1L, L2_RIGHT_OF_L1L, -1000000L)
                .splitToNewChildren(L1_RIGHT, L2_LEFT_OF_L1R, L2_RIGHT_OF_L1R, 123456789L)
                .buildTree(),
                leafPartitionCount);

        // Then
        assertThat(subtree)
                .withRepresentation(tree -> PartitionsPrinter.printPartitions(schema, (PartitionTree) tree))
                .isEqualTo(new PartitionsBuilder(schema)
                        .rootFirst(ROOT)
                        .buildTree());

        //Incremented to account for root now being the only leaf
        assertThat(subtree.getLeafPartitions().size()).isEqualTo(leafPartitionCount + 1);
        assertThat(subtree.getLeafPartitionIds()).containsExactly(ROOT);
    }

    @Test
    void shouldCreateSeperateSubtreesFrom3LevelTree() throws PartitionTreeException {
        // Given
        int largePartitionCount = 6;
        int midPartitionCount = 4;
        int smallPartitionCount = 2;

        PartitionTree level3TreeOriginal = new PartitionsBuilder(schema)
                .rootFirst(ROOT)
                .splitToNewChildren(ROOT, L1_LEFT, L1_RIGHT, 0L)
                .splitToNewChildren(L1_LEFT, L2_LEFT_OF_L1L, L2_RIGHT_OF_L1L, -1000000L)
                .splitToNewChildren(L1_RIGHT, L2_LEFT_OF_L1R, L2_RIGHT_OF_L1R, 123456789L)
                .splitToNewChildren(L2_LEFT_OF_L1L, L3_LEFT_OF_L2LL, L3_RIGHT_OF_L2LL, -2000000L)
                .splitToNewChildren(L2_RIGHT_OF_L1L, L3_LEFT_OF_L2LR, L3_RIGHT_OF_L2LR, -500000L)
                .splitToNewChildren(L2_LEFT_OF_L1R, L3_LEFT_OF_L2RL, L3_RIGHT_OF_L2RL, 12345678L)
                .splitToNewChildren(L2_RIGHT_OF_L1R, L3_LEFT_OF_L2RR, L3_RIGHT_OF_L2RR, 234567890L)
                .buildTree();

        // When 1
        PartitionTree largeSubtree = PartitionSubtreeFactory.createSubtree(level3TreeOriginal, largePartitionCount);

        // Then 1
        assertThat(largeSubtree)
                .withRepresentation(tree -> PartitionsPrinter.printPartitions(schema, (PartitionTree) tree))
                .isEqualTo(new PartitionsBuilder(schema)
                        .rootFirst(ROOT)
                        .splitToNewChildren(ROOT, L1_LEFT, L1_RIGHT, 0L)
                        .splitToNewChildren(L1_LEFT, L2_LEFT_OF_L1L, L2_RIGHT_OF_L1L, -1000000L)
                        .splitToNewChildren(L1_RIGHT, L2_LEFT_OF_L1R, L2_RIGHT_OF_L1R, 123456789L)
                        .splitToNewChildren(L2_LEFT_OF_L1L, L3_LEFT_OF_L2LL, L3_RIGHT_OF_L2LL, -2000000L)
                        .splitToNewChildren(L2_RIGHT_OF_L1L, L3_LEFT_OF_L2LR, L3_RIGHT_OF_L2LR, -500000L)
                        .buildTree());
        assertThat(largeSubtree.getLeafPartitionIds())
                .containsExactly(L2_LEFT_OF_L1R, L2_RIGHT_OF_L1R,
                        L3_LEFT_OF_L2LL, L3_LEFT_OF_L2LR, L3_RIGHT_OF_L2LL, L3_RIGHT_OF_L2LR);

        // When 2
        PartitionTree midSubtree = PartitionSubtreeFactory.createSubtree(level3TreeOriginal, midPartitionCount);

        // Then 2
        assertThat(midSubtree.getLeafPartitions().size()).isEqualTo(midPartitionCount);
        assertThat(midSubtree)
                .withRepresentation(tree -> PartitionsPrinter.printPartitions(schema, (PartitionTree) tree))
                .isEqualTo(new PartitionsBuilder(schema)
                        .rootFirst(ROOT)
                        .splitToNewChildren(ROOT, L1_LEFT, L1_RIGHT, 0L)
                        .splitToNewChildren(L1_LEFT, L2_LEFT_OF_L1L, L2_RIGHT_OF_L1L, -1000000L)
                        .splitToNewChildren(L1_RIGHT, L2_LEFT_OF_L1R, L2_RIGHT_OF_L1R, 123456789L)
                        .buildTree());
        assertThat(midSubtree.getLeafPartitionIds()).containsExactly(L2_LEFT_OF_L1L, L2_LEFT_OF_L1R, L2_RIGHT_OF_L1L, L2_RIGHT_OF_L1R);

        // When 3
        PartitionTree smallSubtree = PartitionSubtreeFactory.createSubtree(level3TreeOriginal, smallPartitionCount);

        // Then 3
        assertThat(smallSubtree)
                .withRepresentation(tree -> PartitionsPrinter.printPartitions(schema, (PartitionTree) tree))
                .isEqualTo(new PartitionsBuilder(schema)
                        .rootFirst(ROOT)
                        .splitToNewChildren(ROOT, L1_LEFT, L1_RIGHT, 0L)
                        .buildTree());
        assertThat(smallSubtree.getLeafPartitionIds()).containsExactly(L1_LEFT, L1_RIGHT);

    }

    @Test
    void shouldThrowExceptionIfRequestLeafCountGreaterThanOriginalTree() {
        // Given
        PartitionTree originalTree = new PartitionsBuilder(schema)
                .rootFirst(ROOT)
                .splitToNewChildren(ROOT, L1_LEFT, L1_RIGHT, 0L)
                .buildTree();

        // When
        try {
            PartitionSubtreeFactory.createSubtree(originalTree, 7);
        } catch (Exception e) {
            //Then
            assertThat(e).isInstanceOf(PartitionTreeException.class);
            assertThat(e).hasMessageContaining("Requested size of 7 is greater than");
        }
    }

    @Test
    void shouldGetSubtreeForManyLeafPartitions() throws PartitionTreeException {
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
}
