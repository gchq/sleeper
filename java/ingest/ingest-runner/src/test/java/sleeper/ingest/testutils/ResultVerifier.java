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
import sleeper.core.partition.Partition;
import sleeper.core.partition.PartitionTree;
import sleeper.core.record.KeyComparator;
import sleeper.core.record.Record;
import sleeper.core.record.RecordComparator;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.ByteArrayType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.PrimitiveType;
import sleeper.io.parquet.record.ParquetReaderIterator;
import sleeper.io.parquet.record.ParquetRecordReader;
import sleeper.sketches.Sketches;
import sleeper.sketches.s3.SketchesSerDeToS3;
import sleeper.core.statestore.FileInfo;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

public class ResultVerifier {
    private static final double QUANTILE_SKETCH_TOLERANCE = 0.01;

    private ResultVerifier() {
    }

    public static void verify(
            StateStore stateStore,
            Schema sleeperSchema,
            Function<Key, Integer> keyToPartitionNoMappingFn,
            List<Record> expectedRecords,
            Map<Integer, Integer> partitionNoToExpectedNoOfFilesMap,
            Configuration hadoopConfiguration,
            String localWorkingDirectory) throws StateStoreException, IOException {
        java.nio.file.Path localWorkingDirectoryPath = Paths.get(localWorkingDirectory);
        List<String> filesLeftInWorkingDirectory = (Files.exists(localWorkingDirectoryPath)) ?
                Files.walk(localWorkingDirectoryPath)
                        .filter(Files::isRegularFile)
                        .map(java.nio.file.Path::toString)
                        .collect(Collectors.toList()) :
                Collections.emptyList();
        assertThat(filesLeftInWorkingDirectory).isEmpty();

        PartitionTree partitionTree = new PartitionTree(sleeperSchema, stateStore.getAllPartitions());

        Map<Integer, List<Record>> partitionNoToExpectedRecordsMap = expectedRecords.stream()
                .collect(Collectors.groupingBy(
                        record -> keyToPartitionNoMappingFn.apply(Key.create(record.getValues(sleeperSchema.getRowKeyFieldNames())))));
        Map<String, List<FileInfo>> partitionIdToFileInfosMap = stateStore.getActiveFiles().stream()
                .collect(Collectors.groupingBy(FileInfo::getPartitionId));
        Map<String, Integer> partitionIdToPartitionNoMap = partitionNoToExpectedRecordsMap.entrySet().stream()
                .map(entry -> {
                    int partitionNo = entry.getKey();
                    Key keyOfFirstRecord = Key.create(entry.getValue().get(0).getValues(sleeperSchema.getRowKeyFieldNames()));
                    Partition partitionOfFirstRecord = partitionTree.getLeafPartition(keyOfFirstRecord);
                    return new AbstractMap.SimpleEntry<>(partitionOfFirstRecord.getId(), partitionNo);
                }).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        Map<Integer, List<FileInfo>> partitionNoToFileInfosMap = partitionIdToFileInfosMap.entrySet().stream()
                .collect(Collectors.toMap(
                        entry -> partitionIdToPartitionNoMap.get(entry.getKey()),
                        Map.Entry::getValue));
        int expectedTotalNoOfFiles = partitionNoToExpectedNoOfFilesMap.values().stream()
                .mapToInt(Integer::valueOf)
                .sum();

        Set<Integer> allPartitionNoSet = Stream.of(
                        partitionNoToFileInfosMap.keySet().stream(),
                        partitionNoToExpectedNoOfFilesMap.keySet().stream(),
                        partitionNoToExpectedRecordsMap.keySet().stream())
                .flatMap(Function.identity())
                .collect(Collectors.toSet());

        assertThat(stateStore.getActiveFiles()).hasSize(expectedTotalNoOfFiles);
        assertThat(allPartitionNoSet).allMatch(partitionNoToExpectedNoOfFilesMap::containsKey);

        allPartitionNoSet.forEach(partitionNo -> verifyPartition(
                sleeperSchema,
                partitionNoToFileInfosMap.getOrDefault(partitionNo, Collections.emptyList()),
                partitionNoToExpectedNoOfFilesMap.get(partitionNo),
                partitionNoToExpectedRecordsMap.getOrDefault(partitionNo, Collections.emptyList()),
                hadoopConfiguration));
    }

    private static void verifyPartition(Schema sleeperSchema,
                                        List<FileInfo> partitionFileInfoList,
                                        int expectedNoOfFiles,
                                        List<Record> expectedRecordList,
                                        Configuration hadoopConfiguration) {
        Comparator<Record> recordComparator = new RecordComparator(sleeperSchema);
        List<Record> expectedSortedRecordList = expectedRecordList.stream()
                .sorted(recordComparator)
                .collect(Collectors.toList());
        List<Record> savedRecordList = readMergedRecordsFromPartitionDataFiles(sleeperSchema, partitionFileInfoList, hadoopConfiguration);

        assertThat(partitionFileInfoList).hasSize(expectedNoOfFiles);
        assertListsIdentical(expectedSortedRecordList, savedRecordList);

        // In some situations, check that the file min and max match the min and max of dimension 0
        if (expectedNoOfFiles == 1 &&
                sleeperSchema.getRowKeyFields().get(0).getType() instanceof LongType) {
            String rowKeyFieldNameDimension0 = sleeperSchema.getRowKeyFieldNames().get(0);
            Key minRowKeyDimension0 = expectedRecordList.stream()
                    .map(record -> (Long) record.get(rowKeyFieldNameDimension0))
                    .min(Comparator.naturalOrder())
                    .map(Key::create)
                    .get();
            Key maxRowKeyDimension0 = expectedRecordList.stream()
                    .map(record -> (Long) record.get(rowKeyFieldNameDimension0))
                    .max(Comparator.naturalOrder())
                    .map(Key::create)
                    .get();
            partitionFileInfoList.forEach(fileInfo -> {
                assertThat(fileInfo.getMinRowKey()).isEqualTo(minRowKeyDimension0);
                assertThat(fileInfo.getMaxRowKey()).isEqualTo(maxRowKeyDimension0);
            });
        }

        if (expectedNoOfFiles > 0) {
            Map<Field, ItemsSketch> expectedFieldToItemsSketchMap = createFieldToItemSketchMap(sleeperSchema, expectedRecordList);
            Map<Field, ItemsSketch> savedFieldToItemsSketchMap = readFieldToItemSketchMap(sleeperSchema, partitionFileInfoList, hadoopConfiguration);
            sleeperSchema.getRowKeyFields().forEach(field -> {
                ItemsSketch expectedSketch = expectedFieldToItemsSketchMap.get(field);
                ItemsSketch savedSketch = savedFieldToItemsSketchMap.get(field);
                assertThat(savedSketch.getMinValue()).isEqualTo(expectedSketch.getMinValue());
                assertThat(savedSketch.getMaxValue()).isEqualTo(expectedSketch.getMaxValue());
                IntStream.rangeClosed(0, 10).forEach(quantileNo -> {
                    double quantile = 0.1 * quantileNo;
                    double quantileWithToleranceLower = (quantile - QUANTILE_SKETCH_TOLERANCE) > 0 ? quantile - QUANTILE_SKETCH_TOLERANCE : 0;
                    double quantileWithToleranceUpper = (quantile + QUANTILE_SKETCH_TOLERANCE) < 1 ? quantile + QUANTILE_SKETCH_TOLERANCE : 1;
                    KeyComparator keyComparator = new KeyComparator((PrimitiveType) field.getType());
                    if (field.getType() instanceof ByteArrayType) {
                        assertThat(keyComparator.compare(
                                Key.create(((ByteArray) savedSketch.getQuantile(quantile)).getArray()),
                                Key.create(((ByteArray) expectedSketch.getQuantile(quantileWithToleranceLower)).getArray())))
                                .isGreaterThanOrEqualTo(0);
                        assertThat(keyComparator.compare(
                                Key.create(((ByteArray) savedSketch.getQuantile(quantile)).getArray()),
                                Key.create(((ByteArray) expectedSketch.getQuantile(quantileWithToleranceUpper)).getArray())))
                                .isLessThanOrEqualTo(0);
                    } else {
                        assertThat(keyComparator.compare(
                                Key.create(savedSketch.getQuantile(quantile)),
                                Key.create(expectedSketch.getQuantile(quantileWithToleranceLower))))
                                .isGreaterThanOrEqualTo(0);
                        assertThat(keyComparator.compare(
                                Key.create(savedSketch.getQuantile(quantile)),
                                Key.create(expectedSketch.getQuantile(quantileWithToleranceUpper))))
                                .isLessThanOrEqualTo(0);
                    }
                });
            });
        }
    }

    private static void assertListsIdentical(List<?> list1, List<?> list2) {
        assertThat(list2).hasSameSizeAs(list1);
        IntStream.range(0, list1.size()).forEach(i ->
                assertThat(list2.get(i)).as(String.format("First difference found at element %d (of %d)", i, list1.size())).isEqualTo(list1.get(i)));
    }

    private static Map<Field, ItemsSketch> readFieldToItemSketchMap(Schema sleeperSchema,
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

    private static Map<Field, ItemsSketch> createFieldToItemSketchMap(Schema sleeperSchema, List<Record> recordList) {
        return sleeperSchema.getRowKeyFields().stream()
                .map(field -> new AbstractMap.SimpleEntry<>(field, createItemSketch(field, recordList)))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
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

    private static List<Record> readMergedRecordsFromPartitionDataFiles(Schema sleeperSchema,
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
}
