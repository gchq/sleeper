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
package sleeper.configuration;

import com.facebook.collections.ByteArray;
import org.apache.datasketches.quantiles.ItemsSketch;

import sleeper.core.record.Record;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.ByteArrayType;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;

public class EstimateSplitPoints {
    private final Field rowKey1;
    private final Iterable<Record> records;
    private final int numPartitions;
    private final int sketchSize;

    public EstimateSplitPoints(Schema schema, Iterable<Record> records, int numPartitions, int sketchSize) {
        if (numPartitions < 2) {
            throw new IllegalArgumentException("Number of partitions must be >= 2");
        }
        this.rowKey1 = schema.getRowKeyFields().get(0);
        this.records = records;
        this.numPartitions = numPartitions;
        this.sketchSize = sketchSize;
    }

    public EstimateSplitPoints(Schema schema, Iterable<Record> records, int numPartitions) {
        this(schema, records, numPartitions, 32768);
    }

    public List<Object> estimate() {
        if (1 == numPartitions) {
            return Collections.emptyList();
        }

        // Add all the values to the sketch
        ItemsSketch sketch = ItemsSketch.getInstance(sketchSize, Comparator.naturalOrder());
        for (Record record : records) {
            Object firstRowKey = record.get(rowKey1.getName());
            if (rowKey1.getType() instanceof ByteArrayType) {
                sketch.update(ByteArray.wrap((byte[]) firstRowKey));
            } else {
                sketch.update(firstRowKey);
            }
        }

        // The getQuantiles method returns the min and median and max given a value of 3; hence need to add one to get
        // the correct number of split points, and need to remove the first and last entries.
        Object[] splitPointsWithMinAndMax = sketch.getQuantiles(numPartitions + 1);
        Object[] splitPoints = Arrays.copyOfRange(splitPointsWithMinAndMax, 1, splitPointsWithMinAndMax.length - 1);
        if (splitPoints.length != numPartitions - 1) {
            throw new RuntimeException("There should have been " + (numPartitions - 1) + "partitions; got " + splitPoints.length);
        }

        // Remove any duplicate values (which means the number of split points returned may be less than that requested.
        List<Object> deduplicatedSplitPoints = Arrays.asList(splitPoints);
        deduplicatedSplitPoints = deduplicatedSplitPoints.stream().filter(Objects::nonNull).collect(Collectors.toList());
        SortedSet<Object> sortedSet = new TreeSet<>(deduplicatedSplitPoints);

        if (rowKey1.getType() instanceof ByteArrayType) {
            return sortedSet.stream().map(b -> (ByteArray) b).map(ByteArray::getArray).collect(Collectors.toList());
        }
        return Arrays.asList(sortedSet.toArray());
    }
}
