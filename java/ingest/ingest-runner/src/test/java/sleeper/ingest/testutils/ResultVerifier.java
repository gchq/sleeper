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
package sleeper.ingest.testutils;

import com.facebook.collections.ByteArray;
import org.apache.datasketches.quantiles.ItemsSketch;
import org.apache.datasketches.quantiles.ItemsUnion;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetReader;

import sleeper.core.iterator.CloseableIterator;
import sleeper.core.key.Key;
import sleeper.core.record.KeyComparator;
import sleeper.core.record.Record;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.ByteArrayType;
import sleeper.core.schema.type.PrimitiveType;
import sleeper.core.statestore.FileReference;
import sleeper.io.parquet.record.ParquetReaderIterator;
import sleeper.io.parquet.record.ParquetRecordReader;
import sleeper.sketches.Sketches;
import sleeper.sketches.s3.SketchesSerDeToS3;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

public class ResultVerifier {

    private ResultVerifier() {
    }

    public static Map<Field, ItemsSketch> readFieldToItemSketchMap(Schema sleeperSchema,
            List<FileReference> partitionFileReferenceList,
            Configuration hadoopConfiguration) {
        List<Sketches> readSketchesList = partitionFileReferenceList.stream()
                .map(fileReference -> {
                    try {
                        String sketchFileName = fileReference.getFilename().replace(".parquet", ".sketches");
                        return new SketchesSerDeToS3(sleeperSchema).loadFromHadoopFS(new Path(sketchFileName), hadoopConfiguration);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }).collect(Collectors.toList());
        Set<String> fieldNameSet = readSketchesList.stream()
                .flatMap(sketches -> sketches.getQuantilesSketches().keySet().stream())
                .collect(Collectors.toSet());
        return fieldNameSet.stream()
                .map(fieldName -> {
                    List<ItemsSketch> itemsSketchList = readSketchesList.stream().map(sketches -> sketches.getQuantilesSketch(fieldName)).collect(Collectors.toList());
                    Field field = sleeperSchema.getField(fieldName).orElseThrow();
                    return Map.entry(field, mergeSketches(field, itemsSketchList));
                }).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    private static ItemsSketch mergeSketches(Field field, List<ItemsSketch> itemsSketchList) {
        ItemsUnion union = Sketches.createUnion(field.getType());
        itemsSketchList.forEach(union::union);
        return union.getResult();
    }

    private static ItemsSketch createItemSketch(Field field, List<Record> recordList) {
        ItemsSketch itemsSketch = Sketches.createSketch(field.getType());
        if (field.getType() instanceof ByteArrayType) {
            recordList.forEach(record -> itemsSketch.update(ByteArray.wrap((byte[]) record.get(field.getName()))));
        } else {
            recordList.forEach(record -> itemsSketch.update(record.get(field.getName())));
        }
        return itemsSketch;
    }

    public static List<Record> readMergedRecordsFromPartitionDataFiles(Schema sleeperSchema,
            List<FileReference> fileReferenceList,
            Configuration hadoopConfiguration) {
        List<Record> recordsRead = new ArrayList<>();
        Set<String> filenames = new HashSet<>();
        for (FileReference fileReference : fileReferenceList) {
            if (filenames.contains(fileReference.getFilename())) {
                continue;
            }
            filenames.add(fileReference.getFilename());
            try (CloseableIterator<Record> iterator = createParquetReaderIterator(
                    sleeperSchema, new Path(fileReference.getFilename()), hadoopConfiguration)) {
                iterator.forEachRemaining(recordsRead::add);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        return recordsRead;
    }

    public static List<Record> readRecordsFromPartitionDataFile(Schema sleeperSchema,
            FileReference fileReference,
            Configuration hadoopConfiguration) {

        try (CloseableIterator<Record> iterator = createParquetReaderIterator(
                sleeperSchema, new Path(fileReference.getFilename()), hadoopConfiguration)) {
            List<Record> recordsRead = new ArrayList<>();
            iterator.forEachRemaining(recordsRead::add);
            return recordsRead;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static ParquetReaderIterator createParquetReaderIterator(Schema sleeperSchema,
            Path filePath,
            Configuration hadoopConfiguration) {
        try {
            ParquetReader<Record> recordParquetReader = new ParquetRecordReader.Builder(filePath, sleeperSchema)
                    .withConf(hadoopConfiguration)
                    .build();
            return new ParquetReaderIterator(recordParquetReader);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static void assertOnSketch(Field field, RecordGenerator.RecordListAndSchema recordListAndSchema,
            List<FileReference> actualFiles, Configuration hadoopConfiguration) {
        ItemsSketch expectedSketch = createItemSketch(field, recordListAndSchema.recordList);
        ItemsSketch savedSketch = readFieldToItemSketchMap(recordListAndSchema.sleeperSchema, actualFiles, hadoopConfiguration).get(field);
        assertOnSketch(field, expectedSketch, savedSketch);
    }

    public static void assertOnSketch(Field field, ItemsSketch expectedSketch, ItemsSketch savedSketch) {
        KeyComparator keyComparator = new KeyComparator((PrimitiveType) field.getType());
        Function<Object, Key> readKey = field.getType() instanceof ByteArrayType
                ? object -> Key.create(((ByteArray) object).getArray())
                : Key::create;
        Object[] actual = savedSketch.getQuantiles(ACTUAL_QUANTILES_QUERY);
        Object[] expected = expectedSketch.getQuantiles(EXPECTED_QUANTILES_QUERY);
        for (TestQuantile quantile : TEST_QUANTILES) {
            assertThat(List.of(
                    readKey.apply(quantile.expectedLowerValue(expected)),
                    readKey.apply(quantile.actualValue(actual)),
                    readKey.apply(quantile.expectedUpperValue(expected))))
                    .isSortedAccordingTo(keyComparator);
        }
    }

    private static final double QUANTILE_SKETCH_TOLERANCE = 0.01;
    private static final List<TestQuantile> TEST_QUANTILES = IntStream.rangeClosed(0, 10)
            .mapToObj(index -> new TestQuantile(index, index * 0.1, QUANTILE_SKETCH_TOLERANCE))
            .collect(Collectors.toUnmodifiableList());
    private static final double[] ACTUAL_QUANTILES_QUERY = TEST_QUANTILES.stream()
            .mapToDouble(TestQuantile::actualQuantile).toArray();
    private static final double[] EXPECTED_QUANTILES_QUERY = TEST_QUANTILES.stream()
            .flatMapToDouble(TestQuantile::expectedQuantiles).toArray();

    private static class TestQuantile {
        private final double quantile;
        private final double quantileWithToleranceLower;
        private final double quantileWithToleranceUpper;
        private final int actualOffset;
        private final int expectedLowerOffset;
        private final int expectedUpperOffset;

        TestQuantile(int index, double quantile, double tolerance) {
            this.quantile = quantile;
            quantileWithToleranceLower = Math.max(quantile - tolerance, 0);
            quantileWithToleranceUpper = Math.min(quantile + tolerance, 1);
            actualOffset = index;
            expectedLowerOffset = index * 2;
            expectedUpperOffset = index * 2 + 1;
        }

        public Object expectedLowerValue(Object[] expected) {
            return expected[expectedLowerOffset];
        }

        public Object expectedUpperValue(Object[] expected) {
            return expected[expectedUpperOffset];
        }

        public Object actualValue(Object[] actual) {
            return actual[actualOffset];
        }

        public DoubleStream expectedQuantiles() {
            return DoubleStream.of(quantileWithToleranceLower, quantileWithToleranceUpper);
        }

        public double actualQuantile() {
            return quantile;
        }
    }
}
