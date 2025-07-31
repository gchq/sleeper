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

public class PartitionSubTreeTest extends PartitionTreeTestBase {

    @Test
    void shouldCreateSubTreeWithExactLeafPartitionCount() {
        // Given / When
        int leafPartitionCount = 2;
        PartitionSubTree subTree = new PartitionSubTree(generateTreeTo2Levels(), leafPartitionCount);

        // Then
        Partition l1LeftAsLeaf = adjustLeafStatus(l1Left, true);
        Partition l1RightAsLeaf = adjustLeafStatus(l1Right, true);
        assertThat(subTree.getLeafPartitions()).contains(l1LeftAsLeaf, l1RightAsLeaf);
        assertThat(subTree.getLeafPartitions().size()).isEqualTo(leafPartitionCount);
    }

    @Test
    void shouldCreateSubTreeWithLeafCountGreaterThanRequested() {
        // Given / When
        int leafPartitionCount = 3;
        PartitionSubTree subTree = new PartitionSubTree(generateTreeTo2Levels(), 3);

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
        PartitionSubTree subTree = new PartitionSubTree(generateTreeTo2Levels(), leafPartitionCount);

        // Then
        assertThat(subTree.getRootPartition()).isEqualTo(root);
        assertThat(subTree.getLeafPartitions().size()).isEqualTo(leafPartitionCount);
    }
}
