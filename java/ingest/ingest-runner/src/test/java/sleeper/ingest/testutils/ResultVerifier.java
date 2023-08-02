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
import sleeper.core.range.Range;
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
import sleeper.statestore.FileInfo;
import sleeper.statestore.StateStore;
import sleeper.statestore.StateStoreException;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
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
    private static final Integer MAXIMUM_RECORDS_IN_FILE = 100000;

    private ResultVerifier() {
    }

    public static void verify(
            StateStore stateStore,
            Schema sleeperSchema,
            Configuration hadoopConfiguration,
            List<Record> expectedRecords,
            String localWorkingDirectory) throws StateStoreException, IOException {

        java.nio.file.Path localWorkingDirectoryPath = Paths.get(localWorkingDirectory);  //Gets the path of the local directory of the file

        List<String> filesLeftInWorkingDirectory = (Files.exists(localWorkingDirectoryPath)) ?
                Files.walk(localWorkingDirectoryPath)
                        .filter(Files::isRegularFile)
                        .map(java.nio.file.Path::toString)
                        .collect(Collectors.toList()) :
                Collections.emptyList();
        //If the directory exists, get all the files regular files in the directory, if it doesn't return an empty list

        assertThat(filesLeftInWorkingDirectory).isEmpty();
        //Assert that the list of files in the working directory is empty

        PartitionTree partitionTree = new PartitionTree(sleeperSchema, stateStore.getAllPartitions());
        //Create a new partition tree with all the partitions passed in from the stateStore

        Function<Key, Integer> keyToPartitionNoMappingFn = key -> {
            for (int i = 0; i < partitionTree.getAllPartitions().size(); i++) {
                if (partitionTree.getAllPartitions().get(i).getRegion().isKeyInRegion(sleeperSchema, key)) {
                    return i;
                }
            }
            return 0;
        };
        Map<Integer, List<Record>> partitionNoToExpectedRecordsMap = expectedRecords.stream()
                .collect(Collectors.groupingBy(
                        record -> keyToPartitionNoMappingFn.apply(Key.create(record.getValues(sleeperSchema.getRowKeyFieldNames())))));


        //Map the partition number to the record that starts the partition

        Map<String, List<FileInfo>> partitionIdToFileInfosMap = stateStore.getActiveFiles().stream()
                .collect(Collectors.groupingBy(FileInfo::getPartitionId));
        // Creates a map of with of the partitionIDs as keys, and an array of FileInfos'

        Map<String, Integer> partitionIdToPartitionNoMap = partitionNoToExpectedRecordsMap.entrySet().stream()
                .map(entry -> {
                    int partitionNo = entry.getKey();
                    Key keyOfFirstRecord = Key.create(entry.getValue().get(0).getValues(sleeperSchema.getRowKeyFieldNames()));
                    Partition partitionOfFirstRecord = partitionTree.getLeafPartition(keyOfFirstRecord);
                    return new AbstractMap.SimpleEntry<>(partitionOfFirstRecord.getId(), partitionNo);
                }).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        // Gets the key of the partition of the first record, and creates, and creates a map with the partitionID and the number that said partition is

        Map<Integer, List<FileInfo>> partitionNoToFileInfosMap = partitionIdToFileInfosMap.entrySet().stream()
                .collect(Collectors.toMap(
                        entry -> partitionIdToPartitionNoMap.get(entry.getKey()),
                        Map.Entry::getValue));
        // Creates a map of the array position of the partition, and the FileInfo


        Map<Integer, Integer> partitionNoToExpectedNoOfFilesMap = new HashMap<>();
        int i = 0;
        for (Partition partition : partitionTree.getAllPartitions().stream().filter(Partition::isLeafPartition).collect(Collectors.toList())) {
            List<Record> recordsInRange = new ArrayList<>();
            for (Record record : expectedRecords) {
                Boolean thisPartition = false;
                for (Range range : partition.getRegion().getRanges()) {
                    thisPartition = range.doesRangeContainObject(record.get(range.getFieldName()));
                    if (!thisPartition) {
                        break;
                    }
                }
                if (thisPartition) {
                    recordsInRange.add(record);
                }
            }

            Integer numberOfFiles =
                    recordsInRange.size() == 0
                            ? 0
                            : 1 + ((recordsInRange.size() - (recordsInRange.size() % MAXIMUM_RECORDS_IN_FILE)) / MAXIMUM_RECORDS_IN_FILE);

            partitionNoToExpectedNoOfFilesMap.put(i, numberOfFiles);
            i += 1;
        }
        int expectedTotalNoOfFiles = partitionNoToExpectedNoOfFilesMap.values().stream()
                .mapToInt(Integer::valueOf)
                .sum();
        //Gets the expected number of files

        Set<Integer> allPartitionNoSet = Stream.of(
                        partitionNoToFileInfosMap.keySet().stream(),
                        partitionNoToExpectedNoOfFilesMap.keySet().stream(),
                        partitionNoToExpectedRecordsMap.keySet().stream())
                .flatMap(Function.identity())
                .collect(Collectors.toSet());
        //Creates a set of the number of all the partition numbers

        assertThat(stateStore.getActiveFiles()).hasSize(expectedTotalNoOfFiles);
        //Asserts the active files in stateStore has the same length as the expected number of files is
        assertThat(allPartitionNoSet).allMatch(partitionNoToExpectedNoOfFilesMap::containsKey);
        //Asserts that the set of all partition numbers match exactly to the expected partition numbers (the key of the partition no to expected no of files map)

        allPartitionNoSet.forEach(partitionNo -> verifyPartition(
                sleeperSchema,
                partitionNoToFileInfosMap.getOrDefault(partitionNo, Collections.emptyList()), //Passes in the value in partitionNoToFileInfosMap or if it doesn't exist give empty list
                partitionNoToExpectedNoOfFilesMap.get(partitionNo), //Passes in the value in partitionNoToExpectedNoOfFilesMap
                partitionNoToExpectedRecordsMap.getOrDefault(partitionNo, Collections.emptyList()), //Passes in the value in partitionNoToExpectedRecordsMap or if it doesn't exist give empty list
                hadoopConfiguration));
        //Iterates through the set of all partition numbers, calling verify partition for each partition number
    }

    private static void verifyPartition(Schema sleeperSchema,
                                        List<FileInfo> partitionFileInfoList,
                                        int expectedNoOfFiles,
                                        List<Record> expectedRecordList,
                                        Configuration hadoopConfiguration) {

        Comparator<Record> recordComparator = new RecordComparator(sleeperSchema);
        //Creates a new record compactor with the schema passed in

        List<Record> expectedSortedRecordList = expectedRecordList.stream()
                .sorted(recordComparator)
                .collect(Collectors.toList());
        //Gets sorted list of all records

        List<Record> savedRecordList = readMergedRecordsFromPartitionDataFiles(sleeperSchema, partitionFileInfoList, hadoopConfiguration);
        //Reads the records from hadoop

        assertThat(partitionFileInfoList).hasSize(expectedNoOfFiles);

        assertListsIdentical(expectedSortedRecordList, savedRecordList);
        //Asserts that the expecetedSortedRecordList is indentical to the savedRecordList

        // In some situations, check that the file min and max match the min and max of dimension 0
        if (expectedNoOfFiles == 1 &&
                sleeperSchema.getRowKeyFields().get(0).getType() instanceof LongType) {
            //If there's 1 expectedNoOfFiles is 1, and the schema type is LongType

            String rowKeyFieldNameDimension0 = sleeperSchema.getRowKeyFieldNames().get(0);
            //Get the row key field name for the 0th dimension

            Key minRowKeyDimension0 = expectedRecordList.stream()
                    .map(record -> (Long) record.get(rowKeyFieldNameDimension0))
                    .min(Comparator.naturalOrder())
                    .map(Key::create)
                    .get();
            //Get the minimum row key for the 0th dimension

            Key maxRowKeyDimension0 = expectedRecordList.stream()
                    .map(record -> (Long) record.get(rowKeyFieldNameDimension0))
                    .max(Comparator.naturalOrder())
                    .map(Key::create)
                    .get();
            //Get the maximum row key for the 0th dimension

            partitionFileInfoList.forEach(fileInfo -> {
                assertThat(fileInfo.getMinRowKey()).isEqualTo(minRowKeyDimension0);
                //For each FileInfo in partitionFileInfoList Assert that it is equal to the minRowDimensionKey0
                assertThat(fileInfo.getMaxRowKey()).isEqualTo(maxRowKeyDimension0);
                //For each FileInfo in partitionFileInfoList Assert that it is equal to the maxRowDimensionKey0
            });
        }

        if (expectedNoOfFiles > 0) {
            Map<Field, ItemsSketch> expectedFieldToItemsSketchMap = createFieldToItemSketchMap(sleeperSchema, expectedRecordList);
            //Creates sketch map from the expected record list

            Map<Field, ItemsSketch> savedFieldToItemsSketchMap = readFieldToItemSketchMap(sleeperSchema, partitionFileInfoList, hadoopConfiguration);
            //Creates sketch map from real record list

            sleeperSchema.getRowKeyFields().forEach(field -> {
                ItemsSketch expectedSketch = expectedFieldToItemsSketchMap.get(field);
                //Gets the expected sketch

                ItemsSketch savedSketch = savedFieldToItemsSketchMap.get(field);
                //Gets the real sketch

                assertThat(savedSketch.getMinValue()).isEqualTo(expectedSketch.getMinValue());
                //Asserts that the minimum value in the actual sketch is the same as the expected sketch

                assertThat(savedSketch.getMaxValue()).isEqualTo(expectedSketch.getMaxValue());
                //Asserts that the maximum value in the actual sketch is the same as the expected sketch

                IntStream.rangeClosed(0, 10).forEach(quantileNo -> {
                    double quantile = 0.1 * quantileNo;

                    double quantileWithToleranceLower = (quantile - QUANTILE_SKETCH_TOLERANCE) > 0 ? quantile - QUANTILE_SKETCH_TOLERANCE : 0;

                    double quantileWithToleranceUpper = (quantile + QUANTILE_SKETCH_TOLERANCE) < 1 ? quantile + QUANTILE_SKETCH_TOLERANCE : 1;

                    KeyComparator keyComparator = new KeyComparator((PrimitiveType) field.getType());

                    if (field.getType() instanceof ByteArrayType) {
                        //If the field is of type ByteArrayType

                        assertThat(keyComparator.compare(
                                Key.create(((ByteArray) savedSketch.getQuantile(quantile)).getArray()),
                                Key.create(((ByteArray) expectedSketch.getQuantile(quantileWithToleranceLower)).getArray())))
                                .isGreaterThanOrEqualTo(0);
                        //Asserts that the key from the saved sketch is the same or greater than the key from the expected sketch

                        assertThat(keyComparator.compare(
                                Key.create(((ByteArray) savedSketch.getQuantile(quantile)).getArray()),
                                Key.create(((ByteArray) expectedSketch.getQuantile(quantileWithToleranceUpper)).getArray())))
                                .isLessThanOrEqualTo(0);
                        //Asserts that the key from the saved sketch is the same or less than the key from the expected sketch

                    } else {
                        //If the field is not of type ByteArrayType

                        assertThat(keyComparator.compare(
                                Key.create(savedSketch.getQuantile(quantile)),
                                Key.create(expectedSketch.getQuantile(quantileWithToleranceLower))))
                                .isGreaterThanOrEqualTo(0);
                        //Asserts that the key from the saved sketch is the same or greater than the key from the expected sketch

                        assertThat(keyComparator.compare(
                                Key.create(savedSketch.getQuantile(quantile)),
                                Key.create(expectedSketch.getQuantile(quantileWithToleranceUpper))))
                                .isLessThanOrEqualTo(0);
                        //Asserts that the key from the saved sketch is the same or less than the key from the expected sketch

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
