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

import com.facebook.collections.ByteArray;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.datasketches.quantiles.ItemsSketch;
import org.apache.datasketches.quantiles.ItemsUnion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.core.schema.Schema;
import sleeper.core.schema.type.ByteArrayType;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.PrimitiveType;
import sleeper.core.schema.type.StringType;
import sleeper.sketches.Sketches;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

public class FindPartitionSplitPoint {

    public static final Logger LOGGER = LoggerFactory.getLogger(FindPartitionSplitPoint.class);

    private final Schema schema;
    private final List<PrimitiveType> rowKeyTypes;
    private final List<String> fileNames;
    private final SketchesLoader sketchesLoader;

    public FindPartitionSplitPoint(Schema schema, List<String> fileNames, SketchesLoader sketchesLoader) {
        this.schema = schema;
        this.rowKeyTypes = schema.getRowKeyTypes();
        this.fileNames = fileNames;
        this.sketchesLoader = sketchesLoader;
    }

    public Optional<Object> splitPointForDimension(int dimension) throws IOException {
        PrimitiveType rowKeyType = rowKeyTypes.get(dimension);
        LOGGER.info("Testing field {} of type {} (dimension {}) to see if it can be split",
                schema.getRowKeyFieldNames().get(dimension), rowKeyType, dimension);
        if (rowKeyType instanceof IntType) {
            return splitPointForDimension(getMinMedianMaxIntKey(dimension), dimension);
        } else if (rowKeyType instanceof LongType) {
            return splitPointForDimension(getMinMedianMaxLongKey(dimension), dimension);
        } else if (rowKeyType instanceof StringType) {
            return splitPointForDimension(getMinMedianMaxStringKey(dimension), dimension);
        } else if (rowKeyType instanceof ByteArrayType) {
            return splitPointForDimension(getMinMedianMaxByteArrayKey(dimension), dimension, ByteArray::getArray);
        } else {
            throw new IllegalArgumentException("Unknown type " + rowKeyType);
        }
    }

    private <T extends Comparable<T>> Optional<Object> splitPointForDimension(
            Triple<T, T, T> minMedianMax, int dimension) {
        return splitPointForDimension(minMedianMax, dimension, median -> median);
    }

    private <T extends Comparable<T>> Optional<Object> splitPointForDimension(
            Triple<T, T, T> minMedianMax, int dimension, Function<T, Object> getValue) {
        T min = minMedianMax.getLeft();
        T median = minMedianMax.getMiddle();
        T max = minMedianMax.getRight();
        LOGGER.debug("Min = {}, median = {}, max = {}", min, median, max);
        if (min.compareTo(max) > 0) {
            throw new IllegalStateException("Min > max");
        }
        if (min.compareTo(median) < 0 && median.compareTo(max) < 0) {
            LOGGER.debug("For dimension {} min < median && median < max", dimension);
            return Optional.of(getValue.apply(median));
        } else {
            LOGGER.info("For dimension {} it is not true that min < median && median < max, so NOT splitting", dimension);
            return Optional.empty();
        }
    }

    private Triple<Integer, Integer, Integer> getMinMedianMaxIntKey(int dimension) throws IOException {
        String keyField = schema.getRowKeyFields().get(dimension).getName();

        // Read all sketches
        List<ItemsSketch<Integer>> sketchList = new ArrayList<>();
        for (String fileName : fileNames) {
            String sketchesFile = fileName.replace(".parquet", ".sketches");
            LOGGER.info("Loading Sketches from {}", sketchesFile);
            Sketches sketches = sketchesLoader.load(sketchesFile);
            sketchList.add(sketches.getQuantilesSketch(keyField));
        }

        // Union all the sketches
        ItemsUnion<Integer> union = ItemsUnion.getInstance(16384, Comparator.naturalOrder());
        for (ItemsSketch<Integer> s : sketchList) {
            union.update(s);
        }
        ItemsSketch<Integer> sketch = union.getResult();

        Integer min = sketch.getMinValue();
        Integer median = sketch.getQuantile(0.5D);
        Integer max = sketch.getMaxValue();
        return new ImmutableTriple<>(min, median, max);
    }

    private Triple<Long, Long, Long> getMinMedianMaxLongKey(int dimension) throws IOException {
        String keyField = schema.getRowKeyFields().get(dimension).getName();

        // Read all sketches
        List<ItemsSketch<Long>> sketchList = new ArrayList<>();
        for (String fileName : fileNames) {
            String sketchesFile = fileName.replace(".parquet", ".sketches");
            LOGGER.info("Loading Sketches from {}", sketchesFile);
            Sketches sketches = sketchesLoader.load(sketchesFile);
            sketchList.add(sketches.getQuantilesSketch(keyField));
        }

        // Union all the sketches
        ItemsUnion<Long> union = ItemsUnion.getInstance(16384, Comparator.naturalOrder());
        for (ItemsSketch<Long> s : sketchList) {
            union.update(s);
        }
        ItemsSketch<Long> sketch = union.getResult();

        Long min = sketch.getMinValue();
        Long median = sketch.getQuantile(0.5D);
        Long max = sketch.getMaxValue();
        return new ImmutableTriple<>(min, median, max);
    }

    private Triple<String, String, String> getMinMedianMaxStringKey(int dimension) throws IOException {
        String keyField = schema.getRowKeyFields().get(dimension).getName();

        // Read all sketches
        List<ItemsSketch<String>> sketchList = new ArrayList<>();
        for (String fileName : fileNames) {
            String sketchesFile = fileName.replace(".parquet", ".sketches");
            LOGGER.info("Loading Sketches from {}", sketchesFile);
            Sketches sketches = sketchesLoader.load(sketchesFile);
            sketchList.add(sketches.getQuantilesSketch(keyField));
        }

        // Union all the sketches
        ItemsUnion<String> union = ItemsUnion.getInstance(16384, Comparator.naturalOrder());
        for (ItemsSketch<String> s : sketchList) {
            union.update(s);
        }
        ItemsSketch<String> sketch = union.getResult();

        String min = sketch.getMinValue();
        String median = sketch.getQuantile(0.5D);
        String max = sketch.getMaxValue();
        return new ImmutableTriple<>(min, median, max);
    }

    private Triple<ByteArray, ByteArray, ByteArray> getMinMedianMaxByteArrayKey(int dimension) throws IOException {
        String keyField = schema.getRowKeyFields().get(dimension).getName();

        // Read all sketches
        List<ItemsSketch<ByteArray>> sketchList = new ArrayList<>();
        for (String fileName : fileNames) {
            String sketchesFile = fileName.replace(".parquet", ".sketches");
            LOGGER.info("Loading Sketches from {}", sketchesFile);
            Sketches sketches = sketchesLoader.load(sketchesFile);
            sketchList.add(sketches.getQuantilesSketch(keyField));
        }

        // Union all the sketches
        ItemsUnion<ByteArray> union = ItemsUnion.getInstance(16384, Comparator.naturalOrder());
        for (ItemsSketch<ByteArray> s : sketchList) {
            union.update(s);
        }
        ItemsSketch<ByteArray> sketch = union.getResult();

        ByteArray min = sketch.getMinValue();
        ByteArray median = sketch.getQuantile(0.5D);
        ByteArray max = sketch.getMaxValue();
        return new ImmutableTriple<>(min, median, max);
    }

    public interface SketchesLoader {
        Sketches load(String filename) throws IOException;
    }

}
