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

import sleeper.core.range.Range;
import sleeper.core.range.Region;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.StringType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class PartitionComparatorTest extends PartitionTreeTestBase {

    List<Partition> leftBiasLevel3 = List.of(
            l3LeftOfL2LL, l3RightOfL2LL, l3LeftOfL2LR, l3RightOfL2LR,
            l3LeftOfL2RL, l3RightOfL2RL, l3LeftOfL2RR, l3RightOfL2RR);

    List<Partition> rightBiasLevel3 = List.of(
            l3RightOfL2RR, l3LeftOfL2RR, l3RightOfL2RL, l3LeftOfL2RL,
            l3RightOfL2LR, l3LeftOfL2LR, l3RightOfL2LL, l3LeftOfL2LL);

    @Test
    void shouldVerifyLeftBiasedOrderingForLongs() {
        // Given
        List<Partition> randomList = new ArrayList<Partition>(rightBiasLevel3);
        Collections.shuffle(randomList);

        // When
        assertThat(randomList).isNotEqualTo(leftBiasLevel3);
        randomList.sort(new PartitionComparator());

        // Then
        assertThat(randomList).isEqualTo(leftBiasLevel3);
    }

    @Test
    void shouldVerifyRightBiasedOrderingForLongs() {
        // Given
        List<Partition> randomList = new ArrayList<Partition>(leftBiasLevel3);
        Collections.shuffle(randomList);

        // When
        assertThat(randomList).isNotEqualTo(rightBiasLevel3);
        randomList.sort(new PartitionComparator().reversed());

        // Then
        assertThat(randomList).isEqualTo(rightBiasLevel3);
    }

    @Test
    void shouldVerifyPartitionSortingForIntegers() {
        //Given
        Schema schemaInteger = Schema.builder().rowKeyFields(new Field("id", new IntType())).build();
        Range.RangeFactory rangeFactoryInteger = new Range.RangeFactory(schemaInteger);
        Partition l2LeftOfL1L_integer = Partition.builder()
                .region(new Region(rangeFactoryInteger.createRange("id", Integer.MIN_VALUE, true, -1000, false)))
                .id(L2_LEFT_OF_L1L)
                .leafPartition(true)
                .parentPartitionId(L1_LEFT)
                .childPartitionIds(Collections.emptyList())
                .dimension(-1)
                .build();
        Partition l2RightOfL1L_integer = Partition.builder()
                .region(new Region(rangeFactoryInteger.createRange("id", -1000, true, 0, false)))
                .id(L2_RIGHT_OF_L1L)
                .leafPartition(true)
                .parentPartitionId(L1_LEFT)
                .childPartitionIds(Collections.emptyList())
                .dimension(-1)
                .build();
        Partition l2LeftOfL1R_integer = Partition.builder()
                .region(new Region(rangeFactoryInteger.createRange("id", 0, true, 1234, false)))
                .id(L2_LEFT_OF_L1R)
                .leafPartition(true)
                .parentPartitionId(L1_RIGHT)
                .childPartitionIds(Collections.emptyList())
                .dimension(-1)
                .build();
        Partition l2RightOfL1R_integer = Partition.builder()
                .region(new Region(rangeFactoryInteger.createRange("id", 1234, true, null, false)))
                .id(L2_RIGHT_OF_L1R)
                .leafPartition(true)
                .parentPartitionId(L1_RIGHT)
                .childPartitionIds(Collections.emptyList())
                .dimension(-1)
                .build();

        PartitionTree integerTree = new PartitionsBuilder(schemaInteger)
                .rootFirst(ROOT)
                .splitToNewChildren(ROOT, L1_LEFT, L1_RIGHT, 0)
                .splitToNewChildren(L1_LEFT, L2_LEFT_OF_L1L, L2_RIGHT_OF_L1L, -1000)
                .splitToNewChildren(L1_RIGHT, L2_LEFT_OF_L1R, L2_RIGHT_OF_L1R, 1234)
                .buildTree();

        List<Partition> randomList = new ArrayList<Partition>(integerTree.getLeafPartitions());
        Collections.shuffle(randomList);

        // When
        randomList.sort(new PartitionComparator());

        // Then
        assertThat(randomList).isEqualTo(List.of(l2LeftOfL1L_integer,
                l2RightOfL1L_integer,
                l2LeftOfL1R_integer,
                l2RightOfL1R_integer));
    }

    @Test
    void shouldVerifyPartitionSortingForString() {
        // Given
        Schema schemaString = Schema.builder().rowKeyFields(new Field("id", new StringType())).build();
        Range.RangeFactory rangeFactoryString = new Range.RangeFactory(schemaString);
        Partition l2LeftOfL1L_string = Partition.builder()
                .region(new Region(rangeFactoryString.createRange("id", "", true, "aaa", false)))
                .id(L2_LEFT_OF_L1L)
                .leafPartition(true)
                .parentPartitionId(L1_LEFT)
                .childPartitionIds(Collections.emptyList())
                .dimension(-1)
                .build();
        Partition l2RightOfL1L_string = Partition.builder()
                .region(new Region(rangeFactoryString.createRange("id", "aaa", true, "bbb", false)))
                .id(L2_RIGHT_OF_L1L)
                .leafPartition(true)
                .parentPartitionId(L1_LEFT)
                .childPartitionIds(Collections.emptyList())
                .dimension(-1)
                .build();
        Partition l2LeftOfL1R_string = Partition.builder()
                .region(new Region(rangeFactoryString.createRange("id", "bbb", true, "ccc", false)))
                .id(L2_LEFT_OF_L1R)
                .leafPartition(true)
                .parentPartitionId(L1_RIGHT)
                .childPartitionIds(Collections.emptyList())
                .dimension(-1)
                .build();
        Partition l2RightOfL1R_string = Partition.builder()
                .region(new Region(rangeFactoryString.createRange("id", "ccc", true, null, false)))
                .id(L2_RIGHT_OF_L1R)
                .leafPartition(true)
                .parentPartitionId(L1_RIGHT)
                .childPartitionIds(Collections.emptyList())
                .dimension(-1)
                .build();

        PartitionTree stringTree = new PartitionsBuilder(schemaString)
                .rootFirst(ROOT)
                .splitToNewChildren(ROOT, L1_LEFT, L1_RIGHT, "bbb")
                .splitToNewChildren(L1_LEFT, L2_LEFT_OF_L1L, L2_RIGHT_OF_L1L, "aaa")
                .splitToNewChildren(L1_RIGHT, L2_LEFT_OF_L1R, L2_RIGHT_OF_L1R, "ccc")
                .buildTree();

        List<Partition> randomList = new ArrayList<Partition>(stringTree.getLeafPartitions());
        Collections.shuffle(randomList);

        // When
        randomList.sort(new PartitionComparator());

        // Then
        assertThat(randomList).isEqualTo(List.of(l2LeftOfL1L_string,
                l2RightOfL1L_string,
                l2LeftOfL1R_string,
                l2RightOfL1R_string));
    }

}
