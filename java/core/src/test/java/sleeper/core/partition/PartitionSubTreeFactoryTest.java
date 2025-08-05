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
    void shouldCreateBalancedSubTreeWithExactLeafPartitionCount() {
        // Given / When
        int leafPartitionCount = 2;
        PartitionTree subTree = PartitionSubTreeFactory.createBalancedSubTree(generateTreeTo2Levels(), leafPartitionCount);

        // Then
        Partition l1LeftAsLeaf = adjustToLeafStatus(l1Left);
        Partition l1RightAsLeaf = adjustToLeafStatus(l1Right);
        assertThat(subTree.getLeafPartitions()).contains(l1LeftAsLeaf, l1RightAsLeaf);
        assertThat(subTree.getLeafPartitions().size()).isEqualTo(leafPartitionCount);
    }

    @Test
    void shouldCreateBalancedSubTreeWithLeafCountGreaterThanRequested() {
        // Given / When
        int leafPartitionCount = 3;
        PartitionTree subTree = PartitionSubTreeFactory.createBalancedSubTree(generateTreeTo3Levels(), 3);

        // Then
        assertThat(subTree.getLeafPartitions().size()).isGreaterThanOrEqualTo(leafPartitionCount);
        assertThat(subTree.getLeafPartitions().size() % 2).isEqualTo(0);
        assertThat(subTree).isEqualTo(generateTreeTo2Levels());
    }

    @Test
    void shouldCreateRootOnlySubTreeWhenGivenLeafPartitions() {
        // Given / When
        int leafPartitionCount = 0;
        PartitionTree subTree = PartitionSubTreeFactory.createBalancedSubTree(generateTreeTo2Levels(), leafPartitionCount);

        // Then
        assertThat(subTree.getRootPartition()).isEqualTo(generateTreeToRootLevel());

        //Incremented to account for root now being the only leaf
        assertThat(subTree.getLeafPartitions().size()).isEqualTo(leafPartitionCount + 1);
    }

    @Test
    void shouldCreateSeperateSubTreesFrom3LevelTree() {
        // Given
        int midPartitionCount = 4;
        int smallPartitionCount = 2;

        PartitionTree originalTree = generateTreeTo3Levels();

        // When 1
        PartitionTree midSubTree = PartitionSubTreeFactory.createBalancedSubTree(originalTree, midPartitionCount);

        // Then 1
        assertThat(midSubTree.getLeafPartitions().size()).isEqualTo(midPartitionCount);
        assertThat(midSubTree).isEqualTo(generateTreeTo2Levels());

        // When 2
        PartitionTree smallSubTree = PartitionSubTreeFactory.createBalancedSubTree(originalTree, smallPartitionCount);

        // Then 2
        assertThat(smallSubTree.getLeafPartitions().size()).isEqualTo(smallPartitionCount);
        assertThat(smallSubTree).isEqualTo(generateTreeTo1Level());

    }

    @Test
    void shouldThrowExceptionIfRequestLeafCountGreaterThanOriginalTree() {
        assertThatThrownBy(() -> PartitionSubTreeFactory.createBalancedSubTree(generateTreeTo1Level(), 7))
                .isInstanceOf(PartitionTreeException.class)
                .hasMessageContaining("Requested size of 7 is greater than");
    }
}
