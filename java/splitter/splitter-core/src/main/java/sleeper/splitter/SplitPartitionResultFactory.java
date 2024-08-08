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
package sleeper.splitter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.core.partition.Partition;
import sleeper.core.range.Range;
import sleeper.core.range.Range.RangeFactory;
import sleeper.core.range.Region;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.StateStoreException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Identifies the median value of the first dimension. If that leads to a valid
 * split (i.e. one where it is not equal to the minimum value and not equal to
 * the maximum value) then that is used to split the partition. If it doesn't
 * lead to a valid split then the above is repeated for the second dimension.
 * This continues until either a valid split is found or no split is possible.
 * <p>
 * Note that there are two situations in which a partition cannot be split:
 * - If the partition consists of a single point (i.e. the minimum
 * equals the maximum).
 * - If the median equals the minimum then the partition cannot be split.
 * This is because it would have to be split into [min, median) and [median, max),
 * but if the min equals the median then the left one can't have any data in it
 * as a key x in it would have to have min <= x < median = min which is a
 * contradiction.
 * <p>
 */
public class SplitPartitionResultFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(SplitPartitionResultFactory.class);

    private final Schema schema;
    private final RangeFactory rangeFactory;
    private final Supplier<String> idSupplier;

    public SplitPartitionResultFactory(Schema schema, Supplier<String> idSupplier) {
        this.schema = schema;
        this.rangeFactory = new RangeFactory(schema);
        this.idSupplier = idSupplier;
    }

    public SplitPartitionResult splitPartition(Partition partition, Object splitPoint, int dimension) throws StateStoreException {
        Field fieldToSplitOn = schema.getRowKeyFields().get(dimension);
        LOGGER.info("Splitting partition {} on split point {} in dimension {}", partition.getId(), splitPoint, dimension);

        // New partitions
        List<Range> leftChildRanges = removeRange(partition.getRegion().getRanges(), fieldToSplitOn.getName());
        Range rangeForSplitDimensionLeftChild = rangeFactory.createRange(fieldToSplitOn, partition.getRegion().getRange(fieldToSplitOn.getName()).getMin(), splitPoint);
        leftChildRanges.add(rangeForSplitDimensionLeftChild);
        Region leftChildRegion = new Region(leftChildRanges);
        Partition leftChild = Partition.builder()
                .region(leftChildRegion)
                .id(idSupplier.get())
                .leafPartition(true)
                .parentPartitionId(partition.getId())
                .childPartitionIds(new ArrayList<>())
                .dimension(-1)
                .build();

        List<Range> rightChildRanges = removeRange(partition.getRegion().getRanges(), fieldToSplitOn.getName());
        Range rangeForSplitDimensionRightChild = rangeFactory.createRange(fieldToSplitOn, splitPoint, partition.getRegion().getRange(fieldToSplitOn.getName()).getMax());
        rightChildRanges.add(rangeForSplitDimensionRightChild);
        Region rightChildRegion = new Region(rightChildRanges);
        Partition rightChild = Partition.builder()
                .region(rightChildRegion)
                .id(idSupplier.get())
                .leafPartition(true)
                .parentPartitionId(partition.getId())
                .childPartitionIds(new ArrayList<>())
                .dimension(-1)
                .build();

        // Updated split partition
        partition = partition.toBuilder()
                .leafPartition(false)
                .childPartitionIds(Arrays.asList(leftChild.getId(), rightChild.getId()))
                .dimension(dimension).build();

        LOGGER.info("Updating StateStore:");
        LOGGER.info("Split partition ({}) is marked as not a leaf partition, split on field {}",
                partition.getId(), fieldToSplitOn.getName());
        LOGGER.info("New partition: {}", leftChild);
        LOGGER.info("New partition: {}", rightChild);

        return new SplitPartitionResult(partition, leftChild, rightChild);
    }

    private List<Range> removeRange(List<Range> inputRanges, String rangeToRemove) {
        return inputRanges.stream()
                .filter(r -> !r.getFieldName().equals(rangeToRemove))
                .collect(Collectors.toList());
    }
}
