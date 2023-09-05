/*
 * Copyright 2022-2023 Crown Copyright
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
import sleeper.core.iterator.MergingIterator;
import sleeper.core.key.Key;
import sleeper.core.record.KeyComparator;
import sleeper.core.record.Record;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.ByteArrayType;
import sleeper.core.schema.type.PrimitiveType;
import sleeper.core.statestore.FileInfo;
import sleeper.io.parquet.record.ParquetReaderIterator;
import sleeper.io.parquet.record.ParquetRecordReader;
import sleeper.sketches.Sketches;
import sleeper.sketches.s3.SketchesSerDeToS3;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

public class ResultVerifier {
    private ResultVerifier() {
    }

    public static Map<Field, ItemsSketch> readFieldToItemSketchMap(Schema sleeperSchema,
                                                                   List<FileInfo> partitionFileInfoList,
                                                                   Configuration hadoopConfiguration) {
        List<Sketches> readSketchesList = partitionFileInfoList.stream()
                .map(fileInfo -> {
                    try {
                        String sketchFileName = fileInfo.getFilename().replace(".parquet", ".sketches");
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
                    Field field = sleeperSchema.getField(fieldName).get();
                    return new AbstractMap.SimpleEntry<>(field, mergeSketches(itemsSketchList));
                }).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    private static ItemsSketch mergeSketches(List<ItemsSketch> itemsSketchList) {
        ItemsUnion union = ItemsUnion.getInstance(1024, Comparator.naturalOrder());
        itemsSketchList.forEach(union::update);
        return union.getResult();
    }

    private static ItemsSketch createItemSketch(Field field, List<Record> recordList) {
        ItemsSketch itemsSketch = ItemsSketch.getInstance(1024, Comparator.naturalOrder());
        if (field.getType() instanceof ByteArrayType) {
            recordList.forEach(record -> itemsSketch.update(ByteArray.wrap((byte[]) record.get(field.getName()))));
        } else {
            recordList.forEach(record -> itemsSketch.update(record.get(field.getName())));
        }
        return itemsSketch;
    }

    public static List<Record> readMergedRecordsFromPartitionDataFiles(Schema sleeperSchema,
                                                                       List<FileInfo> fileInfoList,
                                                                       Configuration hadoopConfiguration) {
        List<CloseableIterator<Record>> inputIterators = fileInfoList.stream()
                .map(fileInfo -> createParquetReaderIterator(
                        sleeperSchema, new Path(fileInfo.getFilename()), hadoopConfiguration))
                .collect(Collectors.toList());

        MergingIterator mergingIterator = new MergingIterator(sleeperSchema, inputIterators);
        List<Record> recordsRead = new ArrayList<>();
        while (mergingIterator.hasNext()) {
            recordsRead.add(mergingIterator.next());
        }

        return recordsRead;
    }

    public static List<Record> readRecordsFromPartitionDataFile(Schema sleeperSchema,
                                                                FileInfo fileInfo,
                                                                Configuration hadoopConfiguration) throws IOException {

        try (CloseableIterator<Record> inputIterator = createParquetReaderIterator(sleeperSchema, new Path(fileInfo.getFilename()), hadoopConfiguration)) {
            List<Record> recordsRead = new ArrayList<>();
            while (inputIterator.hasNext()) {
                recordsRead.add(inputIterator.next());
            }
            return recordsRead;
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
                                      List<FileInfo> actualFiles, Configuration hadoopConfiguration) {
        final double QUANTILE_SKETCH_TOLERANCE = 0.01;
        ItemsSketch expectedSketch = createItemSketch(field, recordListAndSchema.recordList);
        ItemsSketch savedSketch = readFieldToItemSketchMap(recordListAndSchema.sleeperSchema, actualFiles, hadoopConfiguration).get(field);
        IntStream.rangeClosed(0, 10).forEach(quantileNo -> {
            KeyComparator keyComparator = new KeyComparator((PrimitiveType) field.getType());
            double quantile = 0.1 * quantileNo;
            double quantileWithToleranceLower = (quantile - QUANTILE_SKETCH_TOLERANCE) > 0 ? quantile - QUANTILE_SKETCH_TOLERANCE : 0;
            double quantileWithToleranceUpper = (quantile + QUANTILE_SKETCH_TOLERANCE) < 1 ? quantile + QUANTILE_SKETCH_TOLERANCE : 1;
            Key expectedUpperQuantileKey =
                    field.getType() instanceof ByteArrayType
                            ? Key.create(((ByteArray) expectedSketch.getQuantile(quantileWithToleranceUpper)).getArray())
                            : Key.create(expectedSketch.getQuantile(quantileWithToleranceUpper));
            Key expectedLowerQuantileKey =
                    field.getType() instanceof ByteArrayType
                            ? Key.create(((ByteArray) expectedSketch.getQuantile(quantileWithToleranceLower)).getArray())
                            : Key.create(expectedSketch.getQuantile(quantileWithToleranceLower));
            Key savedQuantileKey =
                    field.getType() instanceof ByteArrayType
                            ? Key.create(((ByteArray) savedSketch.getQuantile(quantile)).getArray())
                            : Key.create(savedSketch.getQuantile(quantile));
            assertThat(keyComparator.compare(
                    savedQuantileKey,
                    expectedLowerQuantileKey))
                    .isGreaterThanOrEqualTo(0);
            assertThat(keyComparator.compare(
                    savedQuantileKey,
                    expectedUpperQuantileKey))
                    .isLessThanOrEqualTo(0);
        });
    }
}
