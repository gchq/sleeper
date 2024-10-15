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
import sleeper.core.schema.type.PrimitiveType;
import sleeper.sketches.Sketches;
import sleeper.sketches.s3.SketchesSerDeToS3;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

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

    private <T> Optional<T> splitPointForField(Field field, int dimension) {
        ItemsSketch<T> sketch = unionSketches(field);
        Comparator<T> comparator = Sketches.createComparator(field.getType());
        T min = sketch.getMinItem();
        T median = sketch.getQuantile(0.5D);
        T max = sketch.getMaxItem();
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
        for (String fileName : fileNames) {
            String sketchesFile = fileName.replace(".parquet", ".sketches");
            LOGGER.info("Loading Sketches from {}", sketchesFile);
            Sketches sketches = loadSketches(sketchesFile);
            union.union(sketches.getQuantilesSketch(field.getName()));
        }
        return union.getResult();
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
