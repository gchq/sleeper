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
package sleeper.splitterv2.core.split;

import com.facebook.collections.ByteArray;
import org.apache.datasketches.quantiles.ItemsSketch;
import org.apache.datasketches.quantiles.ItemsUnion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.ByteArrayType;
import sleeper.sketchesv2.Sketches;
import sleeper.sketchesv2.store.SketchesStore;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

/**
 * Finds a split point for a partition by examining the sketches for each file.
 */
public class FindPartitionSplitPoint {

    public static final Logger LOGGER = LoggerFactory.getLogger(FindPartitionSplitPoint.class);

    private final Schema schema;
    private final List<String> fileNames;
    private final SketchesStore sketchesStore;
    private static List<Sketches> sketches;

    public FindPartitionSplitPoint(Schema schema, List<String> fileNames, SketchesStore sketchesStore) {
        this.schema = schema;
        this.fileNames = fileNames;
        this.sketchesStore = sketchesStore;
        sketches = loadSketches();
    }

    public Optional<Object> splitPointForDimension(int dimension) {
        Field field = schema.getRowKeyFields().get(dimension);
        LOGGER.info("Testing field {} of type {} (dimension {}) to see if it can be split",
                field.getName(), field.getType(), dimension);
        Optional<Object> splitPoint = splitPointForField(field, dimension);
        if (field.getType() instanceof ByteArrayType) {
            return splitPoint.map(object -> (ByteArray) object).map(ByteArray::getArray);
        } else {
            return splitPoint;
        }
    }

    private Optional<Object> splitPointForField(Field field, int dimension) {
        ItemsSketch sketch = unionSketches(field);
        Comparator comparator = Sketches.createComparator(field.getType());
        Object min = Sketches.readValueFromSketchWithWrappedBytes(sketch.getMinValue(), field);
        Object median = Sketches.readValueFromSketchWithWrappedBytes(sketch.getQuantile(0.5D), field);
        Object max = Sketches.readValueFromSketchWithWrappedBytes(sketch.getMaxValue(), field);
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

    private <T> ItemsSketch<T> unionSketches(Field field) {
        ItemsUnion<T> union = Sketches.createUnion(field.getType(), 16384);
        for (Sketches sketch : sketches) {
            union.update(sketch.getQuantilesSketch(field.getName()));
        }
        return union.getResult();
    }

    private List<Sketches> loadSketches() {
        List<Sketches> sketches = new ArrayList<>();
        for (String fileName : fileNames) {
            LOGGER.info("Loading sketches for file {}", fileName);
            sketches.add(sketchesStore.loadFileSketches(fileName, schema));
        }
        return sketches;
    }

}
