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

package sleeper.ingest.runner;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import sleeper.core.partition.Partition;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.row.Row;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.ByteArrayType;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.LongType;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.testutils.InMemoryTransactionLogStateStore;
import sleeper.core.statestore.testutils.InMemoryTransactionLogs;
import sleeper.example.iterator.AdditionIterator;
import sleeper.sketches.testutils.SketchesDeciles;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.instance.ArrayListIngestProperty.MAX_IN_MEMORY_BATCH_SIZE;
import static sleeper.core.properties.instance.ArrayListIngestProperty.MAX_RECORDS_TO_WRITE_LOCALLY;
import static sleeper.core.properties.model.IngestFileWritingStrategy.ONE_FILE_PER_LEAF;
import static sleeper.core.properties.table.TableProperty.INGEST_FILE_WRITING_STRATEGY;
import static sleeper.core.properties.table.TableProperty.ITERATOR_CLASS_NAME;
import static sleeper.ingest.runner.testutils.IngestRecordsTestDataHelper.getLotsOfRows;
import static sleeper.ingest.runner.testutils.IngestRecordsTestDataHelper.getRows;
import static sleeper.ingest.runner.testutils.IngestRecordsTestDataHelper.getRows2DimByteArrayKey;
import static sleeper.ingest.runner.testutils.IngestRecordsTestDataHelper.getRowsByteArrayKey;
import static sleeper.ingest.runner.testutils.IngestRecordsTestDataHelper.getRowsForAggregationIteratorTest;
import static sleeper.ingest.runner.testutils.IngestRecordsTestDataHelper.getRowsInFirstPartitionOnly;
import static sleeper.ingest.runner.testutils.IngestRecordsTestDataHelper.getRowsOscillatingBetween2Partitions;
import static sleeper.ingest.runner.testutils.IngestRecordsTestDataHelper.getUnsortedRows;
import static sleeper.ingest.runner.testutils.IngestRecordsTestDataHelper.readRecordsFromParquetFile;
import static sleeper.ingest.runner.testutils.IngestRecordsTestDataHelper.schemaWithRowKeys;

class IngestRecordsIT extends IngestRecordsTestBase {
    @BeforeEach
    void setUp() {
        tableProperties.setEnum(INGEST_FILE_WRITING_STRATEGY, ONE_FILE_PER_LEAF);
    }

    @Test
    void shouldWriteRecordsSplitByPartitionLongKey() throws Exception {
        // Given
        StateStore stateStore = initialiseStateStore(new PartitionsBuilder(schema)
                .rootFirst("root")
                .splitToNewChildren("root", "L", "R", 2L)
                .buildList());

        // When
        long numWritten = ingestRecords(stateStore, getRows()).getRecordsWritten();

        // Then:
        //  - Check the correct number of records were written
        assertThat(numWritten).isEqualTo(2);
        //  - Check StateStore has correct information
        FileReferenceFactory fileReferenceFactory = FileReferenceFactory.from(stateStore);
        List<FileReference> fileReferences = stateStore.getFileReferences().stream()
                .sorted(Comparator.comparing(FileReference::getPartitionId))
                .collect(Collectors.toList());
        FileReference leftFile = fileReferences.get(0);
        FileReference rightFile = fileReferences.get(1);
        assertThat(fileReferences)
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("filename", "lastStateStoreUpdateTime")
                .containsExactly(
                        fileReferenceFactory.partitionFile("L", 1L),
                        fileReferenceFactory.partitionFile("R", 1L));
        //  - Read files and check they have the correct records
        assertThat(readRecords(leftFile))
                .containsExactly(getRows().get(0));
        assertThat(readRecords(rightFile))
                .containsExactly(getRows().get(1));
        //  - Check quantiles sketches have been written and are correct
        assertThat(SketchesDeciles.fromFile(schema, leftFile, sketchesStore))
                .isEqualTo(SketchesDeciles.builder()
                        .field("key", deciles -> deciles
                                .min(1L).max(1L)
                                .rank(0.1, 1L).rank(0.2, 1L).rank(0.3, 1L)
                                .rank(0.4, 1L).rank(0.5, 1L).rank(0.6, 1L)
                                .rank(0.7, 1L).rank(0.8, 1L).rank(0.9, 1L))
                        .build());
        assertThat(SketchesDeciles.fromFile(schema, rightFile, sketchesStore))
                .isEqualTo(SketchesDeciles.builder()
                        .field("key", deciles -> deciles
                                .min(3L).max(3L)
                                .rank(0.1, 3L).rank(0.2, 3L).rank(0.3, 3L)
                                .rank(0.4, 3L).rank(0.5, 3L).rank(0.6, 3L)
                                .rank(0.7, 3L).rank(0.8, 3L).rank(0.9, 3L))
                        .build());
    }

    @Test
    void shouldWriteRecordsSplitByPartitionByteArrayKey() throws Exception {
        // Given
        Field field = new Field("key", new ByteArrayType());
        setSchema(schemaWithRowKeys(field));
        StateStore stateStore = initialiseStateStore(new PartitionsBuilder(schema)
                .rootFirst("root")
                .splitToNewChildren("root", "L", "R", new byte[]{64, 64})
                .buildList());

        // When
        long numWritten = ingestRecords(stateStore, getRowsByteArrayKey()).getRecordsWritten();

        // Then:
        //  - Check the correct number of records were written
        assertThat(numWritten).isEqualTo(3);
        //  - Check StateStore has correct information
        FileReferenceFactory fileReferenceFactory = FileReferenceFactory.from(stateStore);
        List<FileReference> fileReferences = stateStore.getFileReferences().stream()
                .sorted(Comparator.comparing(FileReference::getPartitionId))
                .collect(Collectors.toList());
        FileReference leftFile = fileReferences.get(0);
        FileReference rightFile = fileReferences.get(1);
        assertThat(fileReferences)
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("filename", "lastStateStoreUpdateTime")
                .containsExactly(
                        fileReferenceFactory.partitionFile("L", 2L),
                        fileReferenceFactory.partitionFile("R", 1L));
        //  - Read files and check they have the correct records
        assertThat(readRecordsFromParquetFile(leftFile.getFilename(), schema))
                .containsExactly(
                        getRowsByteArrayKey().get(0),
                        getRowsByteArrayKey().get(1));
        assertThat(readRecordsFromParquetFile(rightFile.getFilename(), schema))
                .containsExactly(
                        getRowsByteArrayKey().get(2));
        //  - Check quantiles sketches have been written and are correct
        assertThat(SketchesDeciles.fromFile(schema, leftFile, sketchesStore))
                .isEqualTo(SketchesDeciles.builder()
                        .field("key", deciles -> deciles
                                .minBytes(1, 1).maxBytes(2, 2)
                                .rankBytes(0.1, 1, 1).rankBytes(0.2, 1, 1).rankBytes(0.3, 1, 1)
                                .rankBytes(0.4, 1, 1).rankBytes(0.5, 2, 2).rankBytes(0.6, 2, 2)
                                .rankBytes(0.7, 2, 2).rankBytes(0.8, 2, 2).rankBytes(0.9, 2, 2))
                        .build());
        assertThat(SketchesDeciles.fromFile(schema, rightFile, sketchesStore))
                .isEqualTo(SketchesDeciles.builder()
                        .field("key", deciles -> deciles
                                .minBytes(64, 65).maxBytes(64, 65)
                                .rankBytes(0.1, 64, 65).rankBytes(0.2, 64, 65).rankBytes(0.3, 64, 65)
                                .rankBytes(0.4, 64, 65).rankBytes(0.5, 64, 65).rankBytes(0.6, 64, 65)
                                .rankBytes(0.7, 64, 65).rankBytes(0.8, 64, 65).rankBytes(0.9, 64, 65))
                        .build());
    }

    @Test
    void shouldWriteRecordsSplitByPartition2DimensionalByteArrayKey() throws Exception {
        // Given
        Field field1 = new Field("key1", new ByteArrayType());
        Field field2 = new Field("key2", new ByteArrayType());
        setSchema(schemaWithRowKeys(field1, field2));
        StateStore stateStore = initialiseStateStore(new PartitionsBuilder(schema)
                .rootFirst("root")
                .splitToNewChildrenOnDimension("root", "L", "R", 0, new byte[]{10})
                .buildList());

        // When
        long numWritten = ingestRecords(stateStore, getRows2DimByteArrayKey()).getRecordsWritten();

        // Then:
        //  - Check the correct number of records were written
        assertThat(numWritten).isEqualTo(getRows2DimByteArrayKey().size());
        //  - Check StateStore has correct information
        FileReferenceFactory fileReferenceFactory = FileReferenceFactory.from(stateStore);
        List<FileReference> fileReferences = stateStore.getFileReferences().stream()
                .sorted(Comparator.comparing(FileReference::getPartitionId))
                .collect(Collectors.toList());
        FileReference leftFile = fileReferences.get(0);
        FileReference rightFile = fileReferences.get(1);
        assertThat(fileReferences)
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("filename", "lastStateStoreUpdateTime")
                .containsExactly(
                        fileReferenceFactory.partitionFile("L", 2L),
                        fileReferenceFactory.partitionFile("R", 3L));
        //  - Read files and check they have the correct records
        assertThat(readRecords(leftFile, schema))
                .containsExactly(
                        getRows2DimByteArrayKey().get(0),
                        getRows2DimByteArrayKey().get(4));
        assertThat(readRecords(rightFile, schema))
                .containsExactly(
                        getRows2DimByteArrayKey().get(1),
                        getRows2DimByteArrayKey().get(2),
                        getRows2DimByteArrayKey().get(3));
        //  - Check quantiles sketches have been written and are correct
        assertThat(SketchesDeciles.fromFile(schema, leftFile, sketchesStore))
                .isEqualTo(SketchesDeciles.builder()
                        .field("key1", deciles -> deciles
                                .minBytes(1, 1).maxBytes(5)
                                .rankBytes(0.1, 1, 1).rankBytes(0.2, 1, 1).rankBytes(0.3, 1, 1)
                                .rankBytes(0.4, 1, 1).rankBytes(0.5, 5).rankBytes(0.6, 5)
                                .rankBytes(0.7, 5).rankBytes(0.8, 5).rankBytes(0.9, 5))
                        .field("key2", deciles -> deciles
                                .minBytes(2, 3).maxBytes(99)
                                .rankBytes(0.1, 2, 3).rankBytes(0.2, 2, 3).rankBytes(0.3, 2, 3)
                                .rankBytes(0.4, 2, 3).rankBytes(0.5, 99).rankBytes(0.6, 99)
                                .rankBytes(0.7, 99).rankBytes(0.8, 99).rankBytes(0.9, 99))
                        .build());
        assertThat(SketchesDeciles.fromFile(schema, rightFile, sketchesStore))
                .isEqualTo(SketchesDeciles.builder()
                        .field("key1", deciles -> deciles
                                .minBytes(11, 2).maxBytes(64, 65)
                                .rankBytes(0.1, 11, 2).rankBytes(0.2, 11, 2).rankBytes(0.3, 11, 2)
                                .rankBytes(0.4, 64, 65).rankBytes(0.5, 64, 65).rankBytes(0.6, 64, 65)
                                .rankBytes(0.7, 64, 65).rankBytes(0.8, 64, 65).rankBytes(0.9, 64, 65))
                        .field("key2", deciles -> deciles
                                .minBytes(2, 2).maxBytes(67, 68)
                                .rankBytes(0.1, 2, 2).rankBytes(0.2, 2, 2).rankBytes(0.3, 2, 2)
                                .rankBytes(0.4, 67, 68).rankBytes(0.5, 67, 68).rankBytes(0.6, 67, 68)
                                .rankBytes(0.7, 67, 68).rankBytes(0.8, 67, 68).rankBytes(0.9, 67, 68))
                        .build());
    }

    @Test
    void shouldWriteRecordsSplitByPartition2DimensionalDifferentTypeKeysWhenSplitOnDim1() throws Exception {
        // Given
        Field field1 = new Field("key1", new IntType());
        Field field2 = new Field("key2", new LongType());
        setSchema(schemaWithRowKeys(field1, field2));
        // The original root partition was split on the second dimension.
        // Ordering (sorted using the first dimension with the second dimension
        // used to break ties):
        //
        // Key        (0,1) < (0,20) < (100,1) < (100,50)
        // Partition    1        2        1         2
        // (Note in practice it's unlikely that the root partition would be
        // split into two on dimension 2 given data that looks like the points
        // below, but it's not impossible as when the partition was split the
        // data could have consisted purely of points with the same first dimension.)
        //
        //   Dimension 2  |         partition 2
        //           null |
        //                |
        //                |    p2: (0,20)   p4: (100,50)
        //             10 |-----------------------------
        //                |
        //                |
        //                |    p1: (0,1)    p3: (100,1)
        //                |
        //                |
        //                |      partition 1
        //                |
        // Long.MIN_VALUE |----------------------------
        //               Long.MIN_VALUE            null   Dimension 1
        StateStore stateStore = initialiseStateStore(new PartitionsBuilder(schema)
                .rootFirst("root")
                .splitToNewChildrenOnDimension("root", "L", "R", 1, 10L)
                .buildList());

        // When
        //  - When sorted the records in getRecordsOscillateBetweenTwoPartitions
        //  appear in partition 1 then partition 2 then partition 1, then 2, etc
        long numWritten = ingestRecords(stateStore, getRowsOscillatingBetween2Partitions()).getRecordsWritten();

        // Then:
        //  - Check the correct number of records were written
        assertThat(numWritten).isEqualTo(getRowsOscillatingBetween2Partitions().size());
        //  - Check StateStore has correct information
        FileReferenceFactory fileReferenceFactory = FileReferenceFactory.from(stateStore);
        List<FileReference> fileReferences = stateStore.getFileReferences().stream()
                .sorted(Comparator.comparing(FileReference::getPartitionId))
                .collect(Collectors.toList());
        FileReference leftFile = fileReferences.get(0);
        FileReference rightFile = fileReferences.get(1);
        assertThat(fileReferences)
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("filename", "lastStateStoreUpdateTime")
                .containsExactly(
                        fileReferenceFactory.partitionFile("L", 2L),
                        fileReferenceFactory.partitionFile("R", 2L));
        //  - Read files and check they have the correct records
        assertThat(readRecords(leftFile, schema))
                .containsExactly(
                        getRowsOscillatingBetween2Partitions().get(0),
                        getRowsOscillatingBetween2Partitions().get(2));
        assertThat(readRecords(rightFile, schema))
                .containsExactly(
                        getRowsOscillatingBetween2Partitions().get(1),
                        getRowsOscillatingBetween2Partitions().get(3));
        //  - Check quantiles sketches have been written and are correct
        assertThat(SketchesDeciles.fromFile(schema, leftFile, sketchesStore))
                .isEqualTo(SketchesDeciles.builder()
                        .field("key1", deciles -> deciles
                                .min(0).max(100)
                                .rank(0.1, 0).rank(0.2, 0).rank(0.3, 0)
                                .rank(0.4, 0).rank(0.5, 100).rank(0.6, 100)
                                .rank(0.7, 100).rank(0.8, 100).rank(0.9, 100))
                        .field("key2", deciles -> deciles
                                .min(1L).max(1L)
                                .rank(0.1, 1L).rank(0.2, 1L).rank(0.3, 1L)
                                .rank(0.4, 1L).rank(0.5, 1L).rank(0.6, 1L)
                                .rank(0.7, 1L).rank(0.8, 1L).rank(0.9, 1L))
                        .build());
        assertThat(SketchesDeciles.fromFile(schema, rightFile, sketchesStore))
                .isEqualTo(SketchesDeciles.builder()
                        .field("key1", deciles -> deciles
                                .min(0).max(100)
                                .rank(0.1, 0).rank(0.2, 0).rank(0.3, 0)
                                .rank(0.4, 0).rank(0.5, 100).rank(0.6, 100)
                                .rank(0.7, 100).rank(0.8, 100).rank(0.9, 100))
                        .field("key2", deciles -> deciles
                                .min(20L).max(50L)
                                .rank(0.1, 20L).rank(0.2, 20L).rank(0.3, 20L)
                                .rank(0.4, 20L).rank(0.5, 50L).rank(0.6, 50L)
                                .rank(0.7, 50L).rank(0.8, 50L).rank(0.9, 50L))
                        .build());
    }

    @Test
    void shouldWriteRecordsSplitByPartitionWhenThereIsOnlyDataInOnePartition() throws Exception {
        // Given
        StateStore stateStore = initialiseStateStore(new PartitionsBuilder(schema)
                .rootFirst("root")
                .splitToNewChildren("root", "L", "R", 2L).buildList());

        // When
        long numWritten = ingestRecords(stateStore, getRowsInFirstPartitionOnly()).getRecordsWritten();

        // Then:
        //  - Check the correct number of records were written
        assertThat(numWritten).isEqualTo(getRowsInFirstPartitionOnly().size());
        //  - Check StateStore has correct information
        FileReferenceFactory fileReferenceFactory = FileReferenceFactory.from(stateStore);
        List<FileReference> fileReferences = stateStore.getFileReferences().stream()
                .sorted(Comparator.comparing(FileReference::getPartitionId))
                .collect(Collectors.toList());
        assertThat(fileReferences)
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("filename", "lastStateStoreUpdateTime")
                .containsExactly(fileReferenceFactory.partitionFile("L", 2L));
        //  - Read files and check they have the correct records
        FileReference leftFile = fileReferences.get(0);
        assertThat(readRecords(leftFile, schema))
                .containsExactly(
                        getRowsInFirstPartitionOnly().get(1),
                        getRowsInFirstPartitionOnly().get(0));
        //  - Check quantiles sketches have been written and are correct
        assertThat(SketchesDeciles.fromFile(schema, leftFile, sketchesStore))
                .isEqualTo(SketchesDeciles.builder()
                        .field("key", deciles -> deciles
                                .min(0L).max(1L)
                                .rank(0.1, 0L).rank(0.2, 0L).rank(0.3, 0L)
                                .rank(0.4, 0L).rank(0.5, 1L).rank(0.6, 1L)
                                .rank(0.7, 1L).rank(0.8, 1L).rank(0.9, 1L))
                        .build());
    }

    @Test
    void shouldWriteDuplicateRecords() throws Exception {
        // Given
        StateStore stateStore = initialiseStateStore(new PartitionsBuilder(schema).singlePartition("root").buildList());

        // When
        List<Row> records = new ArrayList<>(getRows());
        records.addAll(getRows());
        long numWritten = ingestRecords(stateStore, records).getRecordsWritten();

        // Then:
        //  - Check the correct number of records were written
        assertThat(numWritten).isEqualTo(2L * getRows().size());
        //  - Check StateStore has correct information
        FileReferenceFactory fileReferenceFactory = FileReferenceFactory.from(stateStore);
        List<FileReference> fileReferences = stateStore.getFileReferences().stream()
                .sorted(Comparator.comparing(FileReference::getPartitionId))
                .collect(Collectors.toList());
        assertThat(fileReferences)
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("filename", "lastStateStoreUpdateTime")
                .containsExactly(fileReferenceFactory.rootFile(4L));
        FileReference fileReference = fileReferences.get(0);
        //  - Read file and check it has correct records
        assertThat(readRecords(fileReference))
                .containsExactly(
                        getRows().get(0),
                        getRows().get(0),
                        getRows().get(1),
                        getRows().get(1));
        //  - Check quantiles sketches have been written and are correct
        assertThat(SketchesDeciles.fromFile(schema, fileReference, sketchesStore))
                .isEqualTo(SketchesDeciles.builder()
                        .field("key", deciles -> deciles
                                .min(1L).max(3L)
                                .rank(0.1, 1L).rank(0.2, 1L).rank(0.3, 1L)
                                .rank(0.4, 1L).rank(0.5, 3L).rank(0.6, 3L)
                                .rank(0.7, 3L).rank(0.8, 3L).rank(0.9, 3L))
                        .build());
    }

    @Test
    void shouldWriteRecordsWhenThereAreMoreRecordsInAPartitionThanCanFitInMemory() throws Exception {
        // Given
        instanceProperties.setNumber(MAX_RECORDS_TO_WRITE_LOCALLY, 1000L);
        instanceProperties.setNumber(MAX_IN_MEMORY_BATCH_SIZE, 5);
        StateStore stateStore = initialiseStateStore(new PartitionsBuilder(schema)
                .rootFirst("root")
                .splitToNewChildren("root", "L", "R", 2L).buildList());
        List<Row> records = getLotsOfRows();

        // When
        long numWritten = ingestRecords(stateStore, records).getRecordsWritten();

        // Then:
        //  - Check the correct number of records were written
        assertThat(numWritten).isEqualTo(records.size());
        //  - Check StateStore has correct information
        FileReferenceFactory fileReferenceFactory = FileReferenceFactory.from(stateStore);
        List<FileReference> fileReferences = stateStore.getFileReferences().stream()
                .sorted(Comparator.comparing(FileReference::getPartitionId))
                .collect(Collectors.toList());
        assertThat(fileReferences)
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("filename", "lastStateStoreUpdateTime")
                .containsExactly(
                        fileReferenceFactory.partitionFile("L", 200L),
                        fileReferenceFactory.partitionFile("R", 200L));

        FileReference leftFile = fileReferences.get(0);
        FileReference rightFile = fileReferences.get(1);
        List<Row> leftRecords = records.stream()
                .filter(r -> ((long) r.get("key")) < 2L)
                .collect(Collectors.toList());
        assertThat(readRecords(leftFile, schema))
                .containsExactlyInAnyOrderElementsOf(leftRecords);
        List<Row> rightRecords = records.stream()
                .filter(r -> ((long) r.get("key")) >= 2L)
                .collect(Collectors.toList());
        assertThat(readRecords(rightFile, schema))
                .containsExactlyInAnyOrderElementsOf(rightRecords);

        //  - Check quantiles sketches have been written and are correct
        assertThat(SketchesDeciles.fromFile(schema, leftFile, sketchesStore))
                .isEqualTo(SketchesDeciles.builder()
                        .field("key", deciles -> deciles
                                .min(-198L).max(1L)
                                .rank(0.1, -178L).rank(0.2, -158L).rank(0.3, -138L)
                                .rank(0.4, -118L).rank(0.5, -98L).rank(0.6, -78L)
                                .rank(0.7, -58L).rank(0.8, -38L).rank(0.9, -18L))
                        .build());
        assertThat(SketchesDeciles.fromFile(schema, rightFile, sketchesStore))
                .isEqualTo(SketchesDeciles.builder()
                        .field("key", deciles -> deciles
                                .min(2L).max(201L)
                                .rank(0.1, 22L).rank(0.2, 42L).rank(0.3, 62L)
                                .rank(0.4, 82L).rank(0.5, 102L).rank(0.6, 122L)
                                .rank(0.7, 142L).rank(0.8, 162L).rank(0.9, 182L))
                        .build());
    }

    @Test
    void shouldWriteRecordsWhenThereAreMoreRecordsThanCanFitInLocalFile() throws Exception {
        // Given
        instanceProperties.setNumber(MAX_RECORDS_TO_WRITE_LOCALLY, 10L);
        instanceProperties.setNumber(MAX_IN_MEMORY_BATCH_SIZE, 5);
        StateStore stateStore = initialiseStateStore(new PartitionsBuilder(schema)
                .rootFirst("root")
                .splitToNewChildren("root", "L", "R", 2L).buildList());
        List<Row> rows = getLotsOfRows();

        // When
        long numWritten = ingestRecords(stateStore, rows).getRecordsWritten();

        // Then:
        //  - Check the correct number of records were written
        assertThat(numWritten).isEqualTo(rows.size());
        //  - Check that the correct number of files have been written
        Map<String, List<String>> partitionToFileMapping = stateStore.getPartitionToReferencedFilesMap();
        assertThat(partitionToFileMapping.get("L")).hasSize(40);
        assertThat(partitionToFileMapping.get("R")).hasSize(40);
        //  - Check that the files in each partition contain the correct data
        List<Row> expectedLeftRecords = rows.stream()
                .filter(r -> ((long) r.get("key")) < 2L)
                .collect(Collectors.toList());
        assertThat(readRecords(partitionToFileMapping.get("L").stream()))
                .containsExactlyInAnyOrderElementsOf(expectedLeftRecords);
        //  - Merge the sketch files for the partition and check it has the right properties
        assertThat(SketchesDeciles.fromFiles(schema, partitionToFileMapping.get("L"), sketchesStore))
                .isEqualTo(SketchesDeciles.from(schema, expectedLeftRecords));
        List<Row> expectedRightRecords = rows.stream()
                .filter(r -> ((long) r.get("key")) >= 2L)
                .collect(Collectors.toList());
        assertThat(readRecords(partitionToFileMapping.get("R").stream()))
                .containsExactlyInAnyOrderElementsOf(expectedRightRecords);
        //  - Merge the sketch files for the partition and check it has the right properties
        assertThat(SketchesDeciles.fromFiles(schema, partitionToFileMapping.get("R"), sketchesStore))
                .isEqualTo(SketchesDeciles.from(schema, expectedRightRecords));
    }

    @Test
    void shouldSortRecords() throws Exception {
        // Given
        StateStore stateStore = initialiseStateStore(new PartitionsBuilder(schema).singlePartition("root").buildList());

        // When
        long numWritten = ingestRecords(stateStore, getUnsortedRows()).getRecordsWritten();

        // Then:
        //  - Check the correct number of records were written
        assertThat(numWritten).isEqualTo(getUnsortedRows().size());
        //  - Check StateStore has correct information
        FileReferenceFactory fileReferenceFactory = FileReferenceFactory.from(stateStore);
        List<FileReference> fileReferences = stateStore.getFileReferences().stream()
                .sorted(Comparator.comparing(FileReference::getPartitionId))
                .collect(Collectors.toList());
        assertThat(fileReferences)
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("filename", "lastStateStoreUpdateTime")
                .containsExactly(fileReferenceFactory.rootFile(20L));
        //  - Read file and check it has correct records
        assertThat(readRecords(fileReferences.get(0)))
                .containsExactlyElementsOf(getUnsortedRows().stream()
                        .sorted(Comparator.comparing(o -> (Long) o.get("key")))
                        .collect(Collectors.toList()));
        //  - Check quantiles sketches have been written and are correct
        assertThat(SketchesDeciles.fromFile(schema, fileReferences.get(0), sketchesStore))
                .isEqualTo(SketchesDeciles.builder()
                        .field("key", deciles -> deciles
                                .min(1L).max(10L)
                                .rank(0.1, 3L).rank(0.2, 5L).rank(0.3, 5L)
                                .rank(0.4, 5L).rank(0.5, 5L).rank(0.6, 5L)
                                .rank(0.7, 5L).rank(0.8, 7L).rank(0.9, 9L))
                        .build());
    }

    @Test
    void shouldApplyIterator() throws Exception {
        // Given
        setSchema(Schema.builder()
                .rowKeyFields(new Field("key", new ByteArrayType()))
                .sortKeyFields(new Field("sort", new LongType()))
                .valueFields(new Field("value", new LongType()))
                .build());
        tableProperties.set(ITERATOR_CLASS_NAME, AdditionIterator.class.getName());
        StateStore stateStore = initialiseStateStore(new PartitionsBuilder(schema).singlePartition("root").buildList());

        // When
        long numWritten = ingestRecords(stateStore, getRowsForAggregationIteratorTest()).getRecordsWritten();

        // Then:
        //  - Check the correct number of records were written
        assertThat(numWritten).isEqualTo(2L);
        //  - Check StateStore has correct information
        FileReferenceFactory fileReferenceFactory = FileReferenceFactory.from(stateStore);
        List<FileReference> fileReferences = stateStore.getFileReferences().stream()
                .sorted(Comparator.comparing(FileReference::getPartitionId))
                .collect(Collectors.toList());
        assertThat(fileReferences)
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("filename", "lastStateStoreUpdateTime")
                .containsExactly(fileReferenceFactory.rootFile(2L));
        //  - Read file and check it has correct records
        assertThat(readRecords(fileReferences.get(0), schema))
                .containsExactly(
                        new Row(Map.of(
                                "key", new byte[]{1, 1},
                                "sort", 2L,
                                "value", 7L)),
                        new Row(Map.of(
                                "key", new byte[]{11, 2},
                                "sort", 1L,
                                "value", 4L)));

        //  - Check quantiles sketches have been written and are correct
        assertThat(SketchesDeciles.fromFile(schema, fileReferences.get(0), sketchesStore))
                .isEqualTo(SketchesDeciles.builder()
                        .field("key", deciles -> deciles
                                .minBytes(1, 1).maxBytes(11, 2)
                                .rankBytes(0.1, 1, 1).rankBytes(0.2, 1, 1).rankBytes(0.3, 1, 1)
                                .rankBytes(0.4, 1, 1).rankBytes(0.5, 11, 2).rankBytes(0.6, 11, 2)
                                .rankBytes(0.7, 11, 2).rankBytes(0.8, 11, 2).rankBytes(0.9, 11, 2))
                        .build());
    }

    private StateStore initialiseStateStore(List<Partition> partitions) {
        return InMemoryTransactionLogStateStore
                .createAndInitialiseWithPartitions(partitions, tableProperties, new InMemoryTransactionLogs());
    }
}
