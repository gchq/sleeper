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

import sleeper.core.range.Range;
import sleeper.core.range.Region;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.LongType;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public abstract class PartitionTreeTestBase {

    protected static final String ROOT = "root";
    protected static final String L1_LEFT = "l1_left";
    protected static final String L1_RIGHT = "l1_right";
    protected static final String L2_LEFT_OF_L1L = "l2_left_of_l1l";
    protected static final String L2_RIGHT_OF_L1L = "l2_right_of_l1l";
    protected static final String L2_LEFT_OF_L1R = "l2_left_of_l1r";
    protected static final String L2_RIGHT_OF_L1R = "l2_right_of_l1r";

    protected Schema schema = Schema.builder().rowKeyFields(new Field("id", new LongType())).build();
    protected Range.RangeFactory rangeFactory = new Range.RangeFactory(schema);
    protected Region rootRegion = new Region(rangeFactory.createRange("id", Long.MIN_VALUE, true, null, false));
    protected Partition root = Partition.builder()
            .region(rootRegion)
            .id(ROOT)
            .leafPartition(false)
            .parentPartitionId(null)
            .childPartitionIds(Arrays.asList(L1_LEFT, L1_RIGHT))
            .dimension(-1)
            .build();
    protected Region l1LeftRegion = new Region(rangeFactory.createRange("id", Long.MIN_VALUE, true, 0L, false));
    protected Partition l1Left = Partition.builder()
            .region(l1LeftRegion)
            .id(L1_LEFT)
            .leafPartition(false)
            .parentPartitionId(ROOT)
            .childPartitionIds(Arrays.asList(L2_LEFT_OF_L1L, L2_RIGHT_OF_L1L))
            .dimension(-1)
            .build();
    protected Region l1RightRegion = new Region(rangeFactory.createRange("id", 0L, true, null, false));
    protected Partition l1Right = Partition.builder()
            .region(l1RightRegion)
            .id(L1_RIGHT)
            .leafPartition(false)
            .parentPartitionId(ROOT)
            .childPartitionIds(Arrays.asList(L2_LEFT_OF_L1R, L2_RIGHT_OF_L1R))
            .dimension(-1)
            .build();
    protected Region l2LeftOfL1LRegion = new Region(rangeFactory.createRange("id", Long.MIN_VALUE, true, -1000000L, false));
    protected Partition l2LeftOfL1L = Partition.builder()
            .region(l2LeftOfL1LRegion)
            .id(L2_LEFT_OF_L1L)
            .leafPartition(true)
            .parentPartitionId(L1_LEFT)
            .childPartitionIds(Collections.emptyList())
            .dimension(-1)
            .build();
    protected Region l2RightOfL1LRegion = new Region(rangeFactory.createRange("id", -1000000L, true, 0L, false));
    protected Partition l2RightOfL1L = Partition.builder()
            .region(l2RightOfL1LRegion)
            .id(L2_RIGHT_OF_L1L)
            .leafPartition(true)
            .parentPartitionId(L1_LEFT)
            .childPartitionIds(Collections.emptyList())
            .dimension(-1)
            .build();
    protected Region l2LeftOfL1RRegion = new Region(rangeFactory.createRange("id", 0L, true, 123456789L, false));
    protected Partition l2LeftOfL1R = Partition.builder()
            .region(l2LeftOfL1RRegion)
            .id(L2_LEFT_OF_L1R)
            .leafPartition(true)
            .parentPartitionId(L1_RIGHT)
            .childPartitionIds(Collections.emptyList())
            .dimension(-1)
            .build();
    protected Region l2RightOfL1RRegion = new Region(rangeFactory.createRange("id", 123456789L, true, null, false));
    protected Partition l2RightOfL1R = Partition.builder()
            .region(l2RightOfL1RRegion)
            .id(L2_RIGHT_OF_L1R)
            .leafPartition(true)
            .parentPartitionId(L1_RIGHT)
            .childPartitionIds(Collections.emptyList())
            .dimension(-1)
            .build();

    protected PartitionTree generateTreeToRootLevel() {
        return new PartitionTree(List.of(adjustLeafStatus(root, true)));
    }

    protected PartitionTree generateTreeTo1Levels() {
        return new PartitionTree(List.of(root, adjustLeafStatus(l1Left, true), adjustLeafStatus(l1Right, true)));
    }

    protected PartitionTree generateTreeTo2Levels() {
        return new PartitionTree(List.of(root, l1Left, l1Right, l2LeftOfL1L, l2RightOfL1L, l2LeftOfL1R, l2RightOfL1R));
    }

    protected Partition adjustLeafStatus(Partition partitionIn, boolean leafStatus) {
        return partitionIn.toBuilder().leafPartition(leafStatus).build();
    }
}
