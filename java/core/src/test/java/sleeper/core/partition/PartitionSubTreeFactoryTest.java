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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class PartitionSubTreeFactoryTest extends PartitionTreeTestBase {

    @Test
    void shouldCreateSubTreeWithExactLeafPartitionCount() {
        // Given / When
        int leafPartitionCount = 2;
        PartitionTree subTree = PartitionSubTreeFactory.createSubTree(generateTreeTo2Levels(), leafPartitionCount);

        // Then
        Partition l1LeftAsLeaf = adjustToLeafStatus(l1Left);
        Partition l1RightAsLeaf = adjustToLeafStatus(l1Right);
        assertThat(subTree.getLeafPartitions()).contains(l1LeftAsLeaf, l1RightAsLeaf);
        assertThat(subTree.getLeafPartitions().size()).isEqualTo(leafPartitionCount);
    }

    @Test
    void shouldCreateSubTreeWithLeafCountGreaterThanRequested() {
        // Given / When
        int leafPartitionCount = 3;
        PartitionTree subTree = PartitionSubTreeFactory.createSubTree(generateTreeTo2Levels(), 3);

        // Then
        assertThat(subTree.getAllPartitions()).contains(l1Left, l1Right);
        assertThat(subTree.getLeafPartitions()).doesNotContain(l1Left, l1Right);

        assertThat(subTree.getLeafPartitions()).contains(l2LeftOfL1L, l2RightOfL1L, l2LeftOfL1R, l2RightOfL1R);
        assertThat(subTree.getLeafPartitions().size()).isGreaterThanOrEqualTo(leafPartitionCount);
    }

    @Test
    void shouldCreateRootOnlySubTreeWhenGivenLeafPartitions() {
        // Given / When
        int leafPartitionCount = 0;
        PartitionTree subTree = PartitionSubTreeFactory.createSubTree(generateTreeTo2Levels(), leafPartitionCount);

        // Then
        assertThat(subTree.getRootPartition()).isEqualTo(rootOnly);

        //Incremented to account for root now being the only leaf
        assertThat(subTree.getLeafPartitions().size()).isEqualTo(leafPartitionCount + 1);
    }

    @Test
    void shouldCreateSeperateSubTreesFrom3LevelTree() {
        // Given
        int midPartitionCount = 4;
        int smallPartitionCount = 2;

        Partition l1LeftAsLeaf = adjustToLeafStatus(l1Left);
        Partition l1RightAsLeaf = adjustToLeafStatus(l1Right);

        PartitionTree originalTree = generateTreeTo3Levels();

        // When 1
        PartitionTree midSubTree = PartitionSubTreeFactory.createSubTree(originalTree, midPartitionCount);

        // Then 1
        assertThat(midSubTree.getLeafPartitions().size()).isEqualTo(midPartitionCount);
        assertThat(midSubTree.getAllPartitions()).contains(rootWithChildren, l1Left, l1Right);
        assertThat(midSubTree.getAllPartitions()).doesNotContain(l1LeftAsLeaf, l1RightAsLeaf);
        assertThat(midSubTree.getLeafPartitions()).contains(l2LeftOfL1L, l2RightOfL1L, l2LeftOfL1R, l2RightOfL1R);

        // When 2
        PartitionTree smallSubTree = PartitionSubTreeFactory.createSubTree(originalTree, smallPartitionCount);

        // Then 2
        assertThat(smallSubTree.getLeafPartitions().size()).isEqualTo(smallPartitionCount);
        assertThat(smallSubTree.getLeafPartitions()).contains(l1LeftAsLeaf, l1RightAsLeaf);

    }

    @Test
    void shouldThrowExceptionIfRequestLeafCountGreaterThanOriginalTree() {
        assertThatThrownBy(() -> PartitionSubTreeFactory.createSubTree(generateTreeTo1LevelsEvenSplit(), 7))
                .isInstanceOf(PartitionTreeException.class)
                .hasMessageContaining("Requested size of 7 is greater than");
    }
}
