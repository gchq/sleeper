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
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.transfer.s3.S3TransferManager;

import sleeper.core.properties.table.TableProperties;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.ByteArrayType;
import sleeper.sketchesv2.Sketches;
import sleeper.sketchesv2.store.S3SketchesStore;
import sleeper.sketchesv2.store.SketchesStore;

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
    private final List<String> fileNames;
    private final SketchesLoader sketchesLoader;

    public FindPartitionSplitPoint(Schema schema, List<String> fileNames, SketchesStore sketchesStore) {
        this.schema = schema;
        this.fileNames = fileNames;
        this.sketchesLoader = loadSketchesFromFile(schema, sketchesStore);
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
        for (String fileName : fileNames) {
            LOGGER.info("Loading sketches for file {}", fileName);
            Sketches sketches = loadSketches(fileName);
            union.update(sketches.getQuantilesSketch(field.getName()));
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

    public static SketchesLoader loadSketchesFromFile(S3Client s3Client, S3TransferManager s3TransferManager, TableProperties tableProperties) {
        return (filename) -> new S3SketchesStore(s3Client, s3TransferManager).loadFileSketches(filename, tableProperties.getSchema());
    }

    public static SketchesLoader loadSketchesFromFile(Schema schema, SketchesStore sketchesStore) {
        return (filename) -> sketchesStore.loadFileSketches(filename, schema);
    }

}
