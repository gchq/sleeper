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
package sleeper.splitter.split;

import com.facebook.collections.ByteArray;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.datasketches.quantiles.ItemsSketch;
import org.apache.datasketches.quantiles.ItemsUnion;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.core.properties.table.TableProperties;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.ByteArrayType;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.PrimitiveType;
import sleeper.core.schema.type.StringType;
import sleeper.sketches.Sketches;
import sleeper.sketches.s3.SketchesSerDeToS3;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

/**
 * Finds a split point for a partition by examining the sketches for each file.
 */
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

    public Optional<Object> splitPointForDimension(int dimension) {
        PrimitiveType rowKeyType = rowKeyTypes.get(dimension);
        LOGGER.info("Testing field {} of type {} (dimension {}) to see if it can be split",
                schema.getRowKeyFieldNames().get(dimension), rowKeyType, dimension);
        if (rowKeyType instanceof IntType) {
            return splitPointForDimension(getMinMedianMaxKey(dimension), dimension);
        } else if (rowKeyType instanceof LongType) {
            return splitPointForDimension(getMinMedianMaxKey(dimension), dimension);
        } else if (rowKeyType instanceof StringType) {
            return splitPointForDimension(getMinMedianMaxKey(dimension), dimension);
        } else if (rowKeyType instanceof ByteArrayType) {
            return splitPointForDimension(getMinMedianMaxKey(dimension), dimension, ByteArray::getArray);
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

    private <T> Triple<T, T, T> getMinMedianMaxKey(int dimension) {
        return getMinMedianMaxKey(schema.getRowKeyFields().get(dimension));
    }

    private <T> Triple<T, T, T> getMinMedianMaxKey(Field field) {

        // Union all sketches
        ItemsUnion<T> union = Sketches.createUnion(field.getType(), 16384);
        for (String fileName : fileNames) {
            String sketchesFile = fileName.replace(".parquet", ".sketches");
            LOGGER.info("Loading Sketches from {}", sketchesFile);
            Sketches sketches = loadSketches(sketchesFile);
            union.union(sketches.getQuantilesSketch(field.getName()));
        }
        ItemsSketch<T> sketch = union.getResult();

        T min = sketch.getMinItem();
        T median = sketch.getQuantile(0.5D);
        T max = sketch.getMaxItem();
        return new ImmutableTriple<>(min, median, max);
    }

    private Sketches loadSketches(String filename) {
        try {
            return sketchesLoader.load(filename);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public interface SketchesLoader {
        Sketches load(String filename) throws IOException;
    }

    public static SketchesLoader loadSketchesFromFile(Schema schema, Configuration conf) {
        return (filename) -> new SketchesSerDeToS3(schema).loadFromHadoopFS(new Path(filename), conf);
    }

    public static SketchesLoader loadSketchesFromFile(TableProperties tableProperties, Configuration conf) {
        SketchesSerDeToS3 serDe = new SketchesSerDeToS3(tableProperties.getSchema());
        return (filename) -> serDe.loadFromHadoopFS(new Path(filename), conf);
    }

}
