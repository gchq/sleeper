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
package sleeper.splitter.core.split;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.core.partition.Partition;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.ByteArray;
import sleeper.core.schema.type.ByteArrayType;
import sleeper.sketches.Sketches;
import sleeper.splitter.core.sketches.SketchForSplitting;
import sleeper.splitter.core.sketches.SketchesForSplitting;

import java.util.Comparator;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.IntStream;

/**
 * Finds a split point for a partition based on sketches of the data in the partition.
 */
public class FindPartitionSplitPoint {

    private static final Logger LOGGER = LoggerFactory.getLogger(FindPartitionSplitPoint.class);

    private FindPartitionSplitPoint() {
    }

    public static Optional<SplitPartitionResult> getResultIfSplittable(Schema schema, Partition partition, SketchesForSplitting sketches, Supplier<String> idSupplier) {
        return getResultIfSplittable(schema, 0, partition, sketches, idSupplier);
    }

    public static Optional<SplitPartitionResult> getResultIfSplittable(Schema schema, long minRowsInSketch, Partition partition, SketchesForSplitting sketches, Supplier<String> idSupplier) {
        SplitPartitionResultFactory resultFactory = new SplitPartitionResultFactory(schema, idSupplier);
        LOGGER.info("Looking for split point for partition {}", partition.getId());
        return IntStream.range(0, schema.getRowKeyFields().size())
                .mapToObj(dimension -> splitPointForDimension(schema, minRowsInSketch, sketches, dimension)
                        .map(splitPoint -> resultFactory.splitPartition(partition, splitPoint, dimension)))
                .flatMap(Optional::stream)
                .findFirst();
    }

    private static Optional<Object> splitPointForDimension(Schema schema, long minRowsInSketch, SketchesForSplitting sketches, int dimension) {
        Field field = schema.getRowKeyFields().get(dimension);
        SketchForSplitting sketch = sketches.getSketch(field);
        LOGGER.info("Testing field {} of type {} (dimension {}) to see if it can be split",
                field.getName(), field.getType(), dimension);
        if (sketch.getNumberOfRows() < minRowsInSketch) {
            LOGGER.info("Sketch is not based on enough rows for a split, found {}, required {}", sketch.getNumberOfRows(), minRowsInSketch);
            return Optional.empty();
        }
        Optional<Object> splitPoint = splitPointForField(field, dimension, sketch);
        if (field.getType() instanceof ByteArrayType) {
            return splitPoint.map(object -> (ByteArray) object).map(ByteArray::getArray);
        } else {
            return splitPoint;
        }
    }

    private static Optional<Object> splitPointForField(Field field, int dimension, SketchForSplitting sketch) {
        Comparator<Object> comparator = Sketches.createComparator(field.getType());
        Object min = sketch.getMin();
        Object median = sketch.getMedian();
        Object max = sketch.getMax();
        LOGGER.debug("Min = {}, median = {}, max = {}", min, median, max);
        if (comparator.compare(min, max) > 0) {
            throw new IllegalStateException("Min > max");
        }
        if (comparator.compare(min, median) < 0 && comparator.compare(median, max) < 0) {
            LOGGER.debug("For dimension {} min < median && median < max", dimension);
            return Optional.of(median);
        } else {
            LOGGER.info("For dimension {} it is not true that min < median && median < max, so NOT splitting", dimension);
            return Optional.empty();
        }
    }

}
