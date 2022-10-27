/*
 * Copyright 2022 Crown Copyright
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

package sleeper.ingest;

import com.facebook.collections.ByteArray;
import org.apache.datasketches.quantiles.ItemsSketch;
import org.apache.datasketches.quantiles.ItemsUnion;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetReader;
import org.assertj.core.util.Streams;
import org.junit.Test;
import sleeper.core.iterator.CloseableIterator;
import sleeper.core.iterator.MergingIterator;
import sleeper.core.iterator.WrappedIterator;
import sleeper.core.iterator.impl.AdditionIterator;
import sleeper.core.partition.Partition;
import sleeper.core.range.Range;
import sleeper.core.range.Region;
import sleeper.core.record.Record;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.ByteArrayType;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.LongType;
import sleeper.ingest.testutils.AssertQuantiles;
import sleeper.io.parquet.record.ParquetReaderIterator;
import sleeper.io.parquet.record.ParquetRecordReader;
import sleeper.sketches.Sketches;
import sleeper.sketches.s3.SketchesSerDeToS3;
import sleeper.statestore.FileInfo;
import sleeper.statestore.StateStore;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.facebook.collections.ByteArray.wrap;
import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.ingest.testutils.IngestRecordsTestDataHelper.assertSketchUsingDirectValuesAllQuantiles;
import static sleeper.ingest.testutils.IngestRecordsTestDataHelper.calculateQuantiles;
import static sleeper.ingest.testutils.IngestRecordsTestDataHelper.createLeafPartition;
import static sleeper.ingest.testutils.IngestRecordsTestDataHelper.createRootPartition;
import static sleeper.ingest.testutils.IngestRecordsTestDataHelper.getLotsOfRecords;
import static sleeper.ingest.testutils.IngestRecordsTestDataHelper.getRecords;
import static sleeper.ingest.testutils.IngestRecordsTestDataHelper.getRecords2DimByteArrayKey;
import static sleeper.ingest.testutils.IngestRecordsTestDataHelper.getRecordsByteArrayKey;
import static sleeper.ingest.testutils.IngestRecordsTestDataHelper.getRecordsForAggregationIteratorTest;
import static sleeper.ingest.testutils.IngestRecordsTestDataHelper.getRecordsInFirstPartitionOnly;
import static sleeper.ingest.testutils.IngestRecordsTestDataHelper.getRecordsOscillatingBetween2Partitions;
import static sleeper.ingest.testutils.IngestRecordsTestDataHelper.getSketches;
import static sleeper.ingest.testutils.IngestRecordsTestDataHelper.getUnsortedRecords;
import static sleeper.ingest.testutils.IngestRecordsTestDataHelper.readRecordsFromParquetFile;
import static sleeper.ingest.testutils.IngestRecordsTestDataHelper.schemaWithRowKeys;

public class IngestRecordsTest extends IngestRecordsTestBase {
    @Test
    public void shouldWriteRecordsSplitByPartitionLongKey() throws Exception {
        // Given
        Range rootRange = new Range.RangeFactory(schema).createRange(field, Long.MIN_VALUE, null);
        Region rootRegion = new Region(rootRange);
        Partition rootPartition = createRootPartition(rootRegion, new LongType());
        Range range1 = new Range.RangeFactory(schema).createRange(field, Long.MIN_VALUE, 2L);
        Region region1 = new Region(range1);
        Partition partition1 = createLeafPartition("partition1", region1, new LongType());
        Range range2 = new Range.RangeFactory(schema).createRange(field, 2L, null);
        Region region2 = new Region(range2);
        Partition partition2 = createLeafPartition("partition2", region2, new LongType());
        rootPartition.setChildPartitionIds(Arrays.asList(partition1.getId(), partition2.getId()));
        StateStore stateStore = getStateStore(schema,
                Arrays.asList(rootPartition, partition1, partition2));

        // When
        IngestProperties properties = defaultPropertiesBuilder(stateStore, schema).build();
        IngestRecords ingestRecords = new IngestRecords(properties);
        ingestRecords.init();
        for (Record record : getRecords()) {
            ingestRecords.write(record);
        }
        long numWritten = ingestRecords.close();

        // Then:
        //  - Check the correct number of records were written
        assertThat(numWritten).isEqualTo(getRecords().size());
        //  - Check StateStore has correct information
        List<FileInfo> activeFiles = stateStore.getActiveFiles()
                .stream()
                .sorted((f1, f2) -> (int) (((long) f1.getMinRowKey().get(0)) - ((long) f2.getMinRowKey().get(0))))
                .collect(Collectors.toList());
        assertThat(activeFiles).hasSize(2);
        FileInfo fileInfo = activeFiles.get(0);
        assertThat((long) fileInfo.getMinRowKey().get(0)).isOne();
        assertThat((long) fileInfo.getMaxRowKey().get(0)).isOne();
        assertThat(fileInfo.getNumberOfRecords().longValue()).isOne();
        assertThat(fileInfo.getPartitionId()).isEqualTo(partition1.getId());
        fileInfo = activeFiles.get(1);
        assertThat((long) fileInfo.getMinRowKey().get(0)).isEqualTo(3L);
        assertThat((long) fileInfo.getMaxRowKey().get(0)).isEqualTo(3L);
        assertThat(fileInfo.getNumberOfRecords().longValue()).isOne();
        assertThat(fileInfo.getPartitionId()).isEqualTo(partition2.getId());
        //  - Read files and check they have the correct records
        List<Record> readRecords1 = readRecordsFromParquetFile(activeFiles.get(0).getFilename(), schema);
        assertThat(readRecords1).hasSize(1);
        assertThat(readRecords1.get(0)).isEqualTo(getRecords().get(0));
        List<Record> readRecords2 = readRecordsFromParquetFile(activeFiles.get(1).getFilename(), schema);
        assertThat(readRecords2).hasSize(1);
        assertThat(readRecords2.get(0)).isEqualTo(getRecords().get(1));
        //  - Check quantiles sketches have been written and are correct (NB the sketches are stochastic so may not be identical)
        AssertQuantiles.forSketch(getSketches(schema, activeFiles.get(0).getFilename()), "key")
                .min(1L).max(1L).quantile(0.4, 1L).quantile(0.6, 1L).verify();
        AssertQuantiles.forSketch(getSketches(schema, activeFiles.get(1).getFilename()), "key")
                .min(3L).max(3L).quantile(0.4, 3L).quantile(0.6, 3L).verify();
    }

    @Test
    public void shouldWriteRecordsSplitByPartitionByteArrayKey() throws Exception {
        // Given
        Field field = new Field("key", new ByteArrayType());
        Schema schema = schemaWithRowKeys(field);
        Range rootRange = new Range.RangeFactory(schema).createRange(field, new byte[]{}, null);
        Region rootRegion = new Region(rootRange);
        Partition rootPartition = createRootPartition(rootRegion, new ByteArrayType());
        Range range1 = new Range.RangeFactory(schema).createRange(field, new byte[]{}, new byte[]{64, 64});
        Region region1 = new Region(range1);
        Partition partition1 = createLeafPartition("partition1", region1, new ByteArrayType());
        Range range2 = new Range.RangeFactory(schema).createRange(field, new byte[]{64, 64}, null);
        Region region2 = new Region(range2);
        Partition partition2 = createLeafPartition("partition2", region2, new ByteArrayType());
        rootPartition.setChildPartitionIds(Arrays.asList(partition1.getId(), partition2.getId()));
        StateStore stateStore = getStateStore(schema,
                Arrays.asList(rootPartition, partition1, partition2));

        // When
        IngestProperties properties = defaultPropertiesBuilder(stateStore, schema).build();
        IngestRecords ingestRecords = new IngestRecords(properties);
        ingestRecords.init();
        for (Record record : getRecordsByteArrayKey()) {
            ingestRecords.write(record);
        }
        long numWritten = ingestRecords.close();

        // Then:
        //  - Check the correct number of records were written
        assertThat(numWritten).isEqualTo(getRecordsByteArrayKey().size());
        //  - Check StateStore has correct information
        List<FileInfo> activeFiles = stateStore.getActiveFiles();
        assertThat(activeFiles).hasSize(2);
        //  - Sort by number of lines so that we know which file corresponds to
        //      which partition
        List<FileInfo> activeFilesSortedByNumberOfLines = activeFiles.stream()
                .sorted((f1, f2) -> (int) (f1.getNumberOfRecords() - f2.getNumberOfRecords()))
                .collect(Collectors.toList());
        FileInfo fileInfo = activeFilesSortedByNumberOfLines.get(1);
        assertThat((byte[]) fileInfo.getMinRowKey().get(0)).containsExactly(new byte[]{1, 1});
        assertThat((byte[]) fileInfo.getMaxRowKey().get(0)).containsExactly(new byte[]{2, 2});
        assertThat(fileInfo.getNumberOfRecords().longValue()).isEqualTo(2L);
        assertThat(fileInfo.getPartitionId()).isEqualTo(partition1.getId());
        fileInfo = activeFilesSortedByNumberOfLines.get(0);
        assertThat((byte[]) fileInfo.getMinRowKey().get(0)).containsExactly(new byte[]{64, 65});
        assertThat((byte[]) fileInfo.getMaxRowKey().get(0)).containsExactly(new byte[]{64, 65});
        assertThat(fileInfo.getNumberOfRecords().longValue()).isOne();
        assertThat(fileInfo.getPartitionId()).isEqualTo(partition2.getId());
        //  - Read files and check they have the correct records
        List<Record> readRecords1 = readRecordsFromParquetFile(activeFilesSortedByNumberOfLines.get(1).getFilename(), schema);
        assertThat(readRecords1).hasSize(2);
        assertThat(readRecords1.get(0)).isEqualTo(getRecordsByteArrayKey().get(0));
        assertThat(readRecords1.get(1)).isEqualTo(getRecordsByteArrayKey().get(1));
        List<Record> readRecords2 = readRecordsFromParquetFile(activeFilesSortedByNumberOfLines.get(0).getFilename(), schema);
        assertThat(readRecords2).hasSize(1);
        assertThat(readRecords2.get(0)).isEqualTo(getRecordsByteArrayKey().get(2));
        //  - Check quantiles sketches have been written and are correct (NB the sketches are stochastic so may not be identical)
        assertSketchUsingDirectValuesAllQuantiles(schema, "key", activeFilesSortedByNumberOfLines.get(1).getFilename(),
                wrap(new byte[]{1, 1}), wrap(new byte[]{2, 2}),
                calculateQuantiles(Arrays.asList(wrap(new byte[]{1, 1}), wrap(new byte[]{2, 2}))));
        assertSketchUsingDirectValuesAllQuantiles(schema, "key", activeFilesSortedByNumberOfLines.get(0).getFilename(),
                wrap(new byte[]{64, 65}), wrap(new byte[]{64, 65}),
                calculateQuantiles(wrap(new byte[]{64, 65})));
    }

    @Test
    public void shouldWriteRecordsSplitByPartition2DimensionalByteArrayKey() throws Exception {
        // Given
        Field field1 = new Field("key1", new ByteArrayType());
        Field field2 = new Field("key2", new ByteArrayType());
        Schema schema = schemaWithRowKeys(field1, field2);
        Range rootRange1 = new Range.RangeFactory(schema).createRange(field1, new byte[]{}, null);
        Range rootRange2 = new Range.RangeFactory(schema).createRange(field2, new byte[]{}, null);
        Region rootRegion = new Region(Arrays.asList(rootRange1, rootRange2));
        Partition rootPartition = createRootPartition(rootRegion, new ByteArrayType(), new ByteArrayType());
        Range range11 = new Range.RangeFactory(schema).createRange(field1, new byte[]{}, new byte[]{10});
        Range range12 = new Range.RangeFactory(schema).createRange(field2, new byte[]{}, null);
        Region region1 = new Region(Arrays.asList(range11, range12));
        Partition partition1 = createLeafPartition("partition1", region1, new ByteArrayType(), new ByteArrayType());
        Range range21 = new Range.RangeFactory(schema).createRange(field1, new byte[]{10}, null);
        Range range22 = new Range.RangeFactory(schema).createRange(field2, new byte[]{}, null);
        Region region2 = new Region(Arrays.asList(range21, range22));
        Partition partition2 = createLeafPartition("partition2", region2, new ByteArrayType(), new ByteArrayType());
        rootPartition.setChildPartitionIds(Arrays.asList(partition1.getId(), partition2.getId()));
        StateStore stateStore = getStateStore(schema,
                Arrays.asList(rootPartition, partition1, partition2));

        // When
        IngestProperties properties = defaultPropertiesBuilder(stateStore, schema).build();
        IngestRecords ingestRecords = new IngestRecords(properties);
        ingestRecords.init();
        for (Record record : getRecords2DimByteArrayKey()) {
            ingestRecords.write(record);
        }
        long numWritten = ingestRecords.close();

        // Then:
        //  - Check the correct number of records were written
        assertThat(numWritten).isEqualTo(getRecords2DimByteArrayKey().size());
        //  - Check StateStore has correct information
        List<FileInfo> activeFiles = stateStore.getActiveFiles();
        assertThat(activeFiles).hasSize(2);
        //  - Sort by number of lines so that we know which file corresponds to
        //      which partition
        List<FileInfo> activeFilesSortedByNumberOfLines = activeFiles.stream()
                .sorted((f1, f2) -> (int) (f1.getNumberOfRecords() - f2.getNumberOfRecords()))
                .collect(Collectors.toList());
        FileInfo fileInfo = activeFilesSortedByNumberOfLines.get(0);
        assertThat((byte[]) fileInfo.getMinRowKey().get(0)).containsExactly(new byte[]{1, 1});
        assertThat((byte[]) fileInfo.getMaxRowKey().get(0)).containsExactly(new byte[]{5});
        assertThat(fileInfo.getNumberOfRecords().longValue()).isEqualTo(2L);
        assertThat(fileInfo.getPartitionId()).isEqualTo(partition1.getId());
        fileInfo = activeFilesSortedByNumberOfLines.get(1);
        assertThat((byte[]) fileInfo.getMinRowKey().get(0)).containsExactly(new byte[]{11, 2});
        assertThat((byte[]) fileInfo.getMaxRowKey().get(0)).containsExactly(new byte[]{64, 65});
        assertThat(fileInfo.getNumberOfRecords().longValue()).isEqualTo(3L);
        assertThat(fileInfo.getPartitionId()).isEqualTo(partition2.getId());
        //  - Read files and check they have the correct records
        List<Record> readRecords1 = readRecordsFromParquetFile(activeFilesSortedByNumberOfLines.get(0).getFilename(), schema);
        assertThat(readRecords1).hasSize(2);
        assertThat(readRecords1.get(0)).isEqualTo(getRecords2DimByteArrayKey().get(0));
        assertThat(readRecords1.get(1)).isEqualTo(getRecords2DimByteArrayKey().get(4));
        List<Record> readRecords2 = readRecordsFromParquetFile(activeFilesSortedByNumberOfLines.get(1).getFilename(), schema);
        assertThat(readRecords2).hasSize(3);
        assertThat(readRecords2.get(0)).isEqualTo(getRecords2DimByteArrayKey().get(1));
        assertThat(readRecords2.get(1)).isEqualTo(getRecords2DimByteArrayKey().get(2));
        assertThat(readRecords2.get(2)).isEqualTo(getRecords2DimByteArrayKey().get(3));
        //  - Check quantiles sketches have been written and are correct (NB the sketches are stochastic so may not be identical)
        assertSketchUsingDirectValuesAllQuantiles(schema, "key1", activeFilesSortedByNumberOfLines.get(0).getFilename(),
                wrap(new byte[]{1, 1}), wrap(new byte[]{5}),
                calculateQuantiles(Arrays.asList(wrap(new byte[]{1, 1}), wrap(new byte[]{5}))));
        assertSketchUsingDirectValuesAllQuantiles(schema, "key2", activeFilesSortedByNumberOfLines.get(0).getFilename(),
                wrap(new byte[]{2, 3}), wrap(new byte[]{99}),
                calculateQuantiles(Arrays.asList(wrap(new byte[]{2, 3}), wrap(new byte[]{99}))));
        assertSketchUsingDirectValuesAllQuantiles(schema, "key1", activeFilesSortedByNumberOfLines.get(1).getFilename(),
                wrap(new byte[]{11, 2}), wrap(new byte[]{64, 65}),
                calculateQuantiles(Arrays.asList(wrap(new byte[]{11, 2}), wrap(new byte[]{64, 65}), wrap(new byte[]{64, 65}))));
        assertSketchUsingDirectValuesAllQuantiles(schema, "key2", activeFilesSortedByNumberOfLines.get(1).getFilename(),
                wrap(new byte[]{2, 2}), wrap(new byte[]{67, 68}),
                calculateQuantiles(Arrays.asList(wrap(new byte[]{2, 2}), wrap(new byte[]{67, 68}), wrap(new byte[]{67, 68}))));
    }

    @Test
    public void shouldWriteRecordsSplitByPartition2DimensionalDifferentTypeKeysWhenSplitOnDim1() throws Exception {
        // Given
        Field field1 = new Field("key1", new IntType());
        Field field2 = new Field("key2", new LongType());
        Schema schema = schemaWithRowKeys(field1, field2);
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
        Range rootRange1 = new Range.RangeFactory(schema).createRange(field1, Integer.MIN_VALUE, null);
        Range rootRange2 = new Range.RangeFactory(schema).createRange(field2, Long.MIN_VALUE, null);
        Region rootRegion = new Region(Arrays.asList(rootRange1, rootRange2));
        Partition rootPartition = createRootPartition(rootRegion, new IntType(), new LongType());
        rootPartition.setDimension(1);
        Range partition1Range1 = new Range.RangeFactory(schema).createRange(field1, Integer.MIN_VALUE, null);
        Range partition1Range2 = new Range.RangeFactory(schema).createRange(field2, Long.MIN_VALUE, 10L);
        Region region1 = new Region(Arrays.asList(partition1Range1, partition1Range2));
        Partition partition1 = createLeafPartition("partition1", region1, new IntType(), new LongType());
        partition1.setDimension(-1);
        Range partition2Range1 = new Range.RangeFactory(schema).createRange(field1, Integer.MIN_VALUE, null);
        Range partition2Range2 = new Range.RangeFactory(schema).createRange(field2, 10L, null);
        Region region2 = new Region(Arrays.asList(partition2Range1, partition2Range2));
        Partition partition2 = createLeafPartition("partition2", region2, new IntType(), new LongType());
        partition2.setDimension(-1);
        rootPartition.setChildPartitionIds(Arrays.asList(partition1.getId(), partition2.getId()));
        StateStore stateStore = getStateStore(schema,
                Arrays.asList(rootPartition, partition1, partition2));

        // When
        IngestProperties properties = defaultPropertiesBuilder(stateStore, schema)
                .compressionCodec("snappy")
                .build();
        IngestRecords ingestRecords = new IngestRecords(properties);
        ingestRecords.init();
        //  - When sorted the records in getRecordsOscillateBetweenTwoPartitions
        //  appear in partition 1 then partition 2 then partition 1, then 2, etc
        for (Record record : getRecordsOscillatingBetween2Partitions()) {
            ingestRecords.write(record);
        }
        long numWritten = ingestRecords.close();

        // Then:
        //  - Check the correct number of records were written
        assertThat(numWritten).isEqualTo(getRecordsOscillatingBetween2Partitions().size());
        //  - Check StateStore has correct information
        List<FileInfo> activeFiles = stateStore.getActiveFiles();
        assertThat(activeFiles).hasSize(2);
        // Find file that corresponds to partition 1
        FileInfo fileInfo1 = activeFiles.stream().filter(f -> f.getPartitionId().equals(partition1.getId())).findFirst().get();
        assertThat(fileInfo1.getMinRowKey().get(0)).isEqualTo(0);
        assertThat(fileInfo1.getMaxRowKey().get(0)).isEqualTo(100);
        assertThat(fileInfo1.getNumberOfRecords().longValue()).isEqualTo(2L);
        // Find file that corresponds to partition 2
        FileInfo fileInfo2 = activeFiles.stream().filter(f -> f.getPartitionId().equals(partition2.getId())).findFirst().get();
        assertThat(fileInfo2.getMinRowKey().get(0)).isEqualTo(0);
        assertThat(fileInfo2.getMaxRowKey().get(0)).isEqualTo(100);
        assertThat(fileInfo2.getNumberOfRecords().longValue()).isEqualTo(2L);
        //  - Read files and check they have the correct records
        List<Record> readRecords1 = readRecordsFromParquetFile(fileInfo1.getFilename(), schema);
        assertThat(readRecords1).hasSize(2);
        assertThat(readRecords1.get(0)).isEqualTo(getRecordsOscillatingBetween2Partitions().get(0));
        assertThat(readRecords1.get(1)).isEqualTo(getRecordsOscillatingBetween2Partitions().get(2));
        List<Record> readRecords2 = readRecordsFromParquetFile(fileInfo2.getFilename(), schema);
        assertThat(readRecords2).hasSize(2);
        assertThat(readRecords2.get(0)).isEqualTo(getRecordsOscillatingBetween2Partitions().get(1));
        assertThat(readRecords2.get(1)).isEqualTo(getRecordsOscillatingBetween2Partitions().get(3));
        //  - Check quantiles sketches have been written and are correct (NB the sketches are stochastic so may not be identical)
        assertSketchUsingDirectValuesAllQuantiles(schema, "key1", fileInfo1.getFilename(),
                0, 100, calculateQuantiles(Arrays.asList(0, 100)));
        assertSketchUsingDirectValuesAllQuantiles(schema, "key2", fileInfo1.getFilename(),
                1L, 1L, calculateQuantiles(1L));
        assertSketchUsingDirectValuesAllQuantiles(schema, "key1", fileInfo2.getFilename(),
                0, 100, calculateQuantiles(Arrays.asList(0, 100)));
        assertSketchUsingDirectValuesAllQuantiles(schema, "key2", fileInfo2.getFilename(),
                20L, 50L, calculateQuantiles(Arrays.asList(20L, 50L)));
    }

    @Test
    public void shouldWriteRecordsSplitByPartitionWhenThereIsOnlyDataInOnePartition() throws Exception {
        // Given
        Range rootRange = new Range.RangeFactory(schema).createRange(field, Long.MIN_VALUE, null);
        Region rootRegion = new Region(rootRange);
        Partition rootPartition = createRootPartition(rootRegion, new LongType());
        Range range1 = new Range.RangeFactory(schema).createRange(field, Long.MIN_VALUE, 2L);
        Region region1 = new Region(range1);
        Partition partition1 = createLeafPartition("partition1", region1, new LongType());
        Range range2 = new Range.RangeFactory(schema).createRange(field, 2L, null);
        Region region2 = new Region(range2);
        Partition partition2 = createLeafPartition("partition2", region2, new LongType());
        rootPartition.setChildPartitionIds(Arrays.asList(partition1.getId(), partition2.getId()));
        StateStore stateStore = getStateStore(schema,
                Arrays.asList(rootPartition, partition1, partition2));

        // When
        IngestProperties properties = defaultPropertiesBuilder(stateStore, schema).build();
        IngestRecords ingestRecords = new IngestRecords(properties);
        ingestRecords.init();
        for (Record record : getRecordsInFirstPartitionOnly()) {
            ingestRecords.write(record);
        }
        long numWritten = ingestRecords.close();

        // Then:
        //  - Check the correct number of records were written
        assertThat(numWritten).isEqualTo(getRecordsInFirstPartitionOnly().size());
        //  - Check StateStore has correct information
        List<FileInfo> activeFiles = stateStore.getActiveFiles();
        assertThat(activeFiles).hasSize(1);
        FileInfo fileInfo = activeFiles.get(0);
        assertThat((long) fileInfo.getMinRowKey().get(0)).isZero();
        assertThat((long) fileInfo.getMaxRowKey().get(0)).isOne();
        assertThat(fileInfo.getNumberOfRecords().longValue()).isEqualTo(2L);
        assertThat(fileInfo.getPartitionId()).isEqualTo(partition1.getId());
        //  - Read files and check they have the correct records
        List<Record> readRecords1 = readRecordsFromParquetFile(activeFiles.get(0).getFilename(), schema);
        assertThat(readRecords1).hasSize(2);
        assertThat(readRecords1.get(0)).isEqualTo(getRecordsInFirstPartitionOnly().get(1));
        assertThat(readRecords1.get(1)).isEqualTo(getRecordsInFirstPartitionOnly().get(0));
        //  - Check quantiles sketches have been written and are correct (NB the sketches are stochastic so may not be identical)
        assertSketchUsingDirectValuesAllQuantiles(schema, "key", fileInfo.getFilename(),
                0L, 1L, calculateQuantiles(Arrays.asList(0L, 1L)));
    }

    @Test
    public void shouldWriteDuplicateRecords() throws Exception {
        // Given
        StateStore stateStore = getStateStore(schema);

        // When
        List<Record> records = new ArrayList<>(getRecords());
        records.addAll(getRecords());
        IngestProperties properties = defaultPropertiesBuilder(stateStore, schema).build();
        IngestRecords ingestRecords = new IngestRecords(properties);
        ingestRecords.init();
        for (Record record : records) {
            ingestRecords.write(record);
        }
        long numWritten = ingestRecords.close();

        // Then:
        //  - Check the correct number of records were written
        assertThat(numWritten).isEqualTo(2 * getRecords().size());
        //  - Check StateStore has correct information
        List<FileInfo> activeFiles = stateStore.getActiveFiles();
        assertThat(activeFiles).hasSize(1);
        FileInfo fileInfo = activeFiles.get(0);
        assertThat((long) fileInfo.getMinRowKey().get(0)).isOne();
        assertThat((long) fileInfo.getMaxRowKey().get(0)).isEqualTo(3L);
        assertThat(fileInfo.getNumberOfRecords().longValue()).isEqualTo(4L);
        assertThat(fileInfo.getPartitionId()).isEqualTo(stateStore.getAllPartitions().get(0).getId());
        //  - Read file and check it has correct records
        List<Record> readRecords1 = readRecordsFromParquetFile(fileInfo.getFilename(), schema);
        assertThat(readRecords1).hasSize(4);
        assertThat(readRecords1.get(0)).isEqualTo(getRecords().get(0));
        assertThat(readRecords1.get(1)).isEqualTo(getRecords().get(0));
        assertThat(readRecords1.get(2)).isEqualTo(getRecords().get(1));
        assertThat(readRecords1.get(3)).isEqualTo(getRecords().get(1));
        //  - Check quantiles sketches have been written and are correct (NB the sketches are stochastic so may not be identical)
        assertSketchUsingDirectValuesAllQuantiles(schema, "key", fileInfo.getFilename(),
                1L, 3L, calculateQuantiles(Arrays.asList(1L, 3L)));
    }

    @Test
    public void shouldWriteRecordsWhenThereAreMoreRecordsInAPartitionThanCanFitInMemory() throws Exception {
        // Given
        Range rootRange = new Range.RangeFactory(schema).createRange(field, Long.MIN_VALUE, null);
        Region rootRegion = new Region(rootRange);
        Partition rootPartition = createRootPartition(rootRegion, new LongType());
        Range range1 = new Range.RangeFactory(schema).createRange(field, Long.MIN_VALUE, 2L);
        Region region1 = new Region(range1);
        Partition partition1 = createLeafPartition("partition1", region1, new LongType());
        Range range2 = new Range.RangeFactory(schema).createRange(field, 2L, null);
        Region region2 = new Region(range2);
        Partition partition2 = createLeafPartition("partition2", region2, new LongType());
        rootPartition.setChildPartitionIds(Arrays.asList(partition1.getId(), partition2.getId()));
        StateStore stateStore = getStateStore(schema,
                Arrays.asList(rootPartition, partition1, partition2));
        List<Record> records = getLotsOfRecords();

        // When
        IngestProperties properties = defaultPropertiesBuilder(stateStore, schema)
                .maxRecordsToWriteLocally(1000L)
                .maxInMemoryBatchSize(5L).build();
        IngestRecords ingestRecords = new IngestRecords(properties);
        ingestRecords.init();
        for (Record record : records) {
            ingestRecords.write(record);
        }
        long numWritten = ingestRecords.close();

        // Then:
        //  - Check the correct number of records were written
        assertThat(numWritten).isEqualTo(records.size());
        //  - Check StateStore has correct information
        List<FileInfo> activeFiles = new ArrayList<>(stateStore.getActiveFiles());
        assertThat(activeFiles).hasSize(2);

        //  - Make sure the first file in the list is the one that belongs to the
        //      smallest partition
        if ((long) activeFiles.get(0).getMinRowKey().get(0) > (long) activeFiles.get(1).getMinRowKey().get(0)) {
            FileInfo leftFileInfo = activeFiles.get(1);
            FileInfo rightFileInfo = activeFiles.get(0);
            activeFiles.clear();
            activeFiles.add(leftFileInfo);
            activeFiles.add(rightFileInfo);
        }

        FileInfo fileInfo = activeFiles.get(0);
        long minLeftFile = (long) records.stream()
                .filter(r -> ((long) r.get("key")) < 2L)
                .min(Comparator.comparing(r -> ((Long) r.get("key"))))
                .get()
                .get("key");
        assertThat((long) fileInfo.getMinRowKey().get(0)).isEqualTo(minLeftFile);

        long maxLeftFile = (long) records.stream()
                .filter(r -> ((long) r.get("key")) < 2L)
                .max(Comparator.comparing(r -> ((Long) r.get("key"))))
                .get()
                .get("key");
        assertThat((long) fileInfo.getMaxRowKey().get(0)).isEqualTo(maxLeftFile);

        long recordsInLeftFile = records.stream()
                .filter(r -> ((long) r.get("key")) < 2L)
                .count();

        assertThat(fileInfo.getNumberOfRecords().longValue()).isEqualTo(recordsInLeftFile);

        assertThat(fileInfo.getPartitionId()).isEqualTo(partition1.getId());
        fileInfo = activeFiles.get(1);

        long minRightFile = (long) records.stream()
                .filter(r -> ((long) r.get("key")) >= 2L)
                .min(Comparator.comparing(r -> ((Long) r.get("key"))))
                .get()
                .get("key");
        assertThat((long) fileInfo.getMinRowKey().get(0)).isEqualTo(minRightFile);

        long maxRightFile = (long) records.stream()
                .filter(r -> ((long) r.get("key")) >= 2L)
                .max(Comparator.comparing(r -> ((Long) r.get("key"))))
                .get()
                .get("key");
        assertThat((long) fileInfo.getMaxRowKey().get(0)).isEqualTo(maxRightFile);

        long recordsInRightFile = records.stream()
                .filter(r -> ((long) r.get("key")) >= 2L)
                .count();

        assertThat(fileInfo.getNumberOfRecords().longValue()).isEqualTo(recordsInRightFile);
        assertThat(fileInfo.getPartitionId()).isEqualTo(partition2.getId());

        //  - Read files and check they have the correct records
        List<Record> readRecords1 = readRecordsFromParquetFile(activeFiles.get(0).getFilename(), schema);
        assertThat(readRecords1.size()).isEqualTo(recordsInLeftFile);
        List<Record> expectedRecords1 = records.stream()
                .filter(r -> ((long) r.get("key")) < 2L)
                .sorted(Comparator.comparing(r -> ((Long) r.get("key"))))
                .collect(Collectors.toList());
        assertThat(readRecords1).isEqualTo(expectedRecords1);
        List<Record> readRecords2 = readRecordsFromParquetFile(activeFiles.get(1).getFilename(), schema);
        assertThat(readRecords2.size()).isEqualTo(recordsInRightFile);
        List<Record> expectedRecords2 = records.stream()
                .filter(r -> ((long) r.get("key")) >= 2L)
                .sorted(Comparator.comparing(r -> ((Long) r.get("key"))))
                .collect(Collectors.toList());
        assertThat(readRecords2).isEqualTo(expectedRecords2);
        //  - Check quantiles sketches have been written and are correct (NB the sketches are stochastic so may not be identical)
        List<Long> keysInLeftFile = records.stream().map(r -> ((Long) r.get("key")))
                .filter(r -> r < 2L).sorted().collect(Collectors.toList());
        List<Long> keysInRightFile = records.stream().map(r -> ((Long) r.get("key")))
                .filter(r -> r >= 2L).sorted().collect(Collectors.toList());
        assertSketchUsingDirectValuesAllQuantiles(schema, "key", activeFiles.get(0).getFilename(),
                minLeftFile, maxLeftFile, calculateQuantiles(keysInLeftFile));
        assertSketchUsingDirectValuesAllQuantiles(schema, "key", activeFiles.get(1).getFilename(),
                minRightFile, maxRightFile, calculateQuantiles(keysInRightFile));
    }

    @Test
    public void shouldWriteRecordsWhenThereAreMoreRecordsThanCanFitInLocalFile() throws Exception {
        // Given
        Range rootRange = new Range.RangeFactory(schema).createRange(field, Long.MIN_VALUE, null);
        Region rootRegion = new Region(rootRange);
        Partition rootPartition = createRootPartition(rootRegion, new LongType());
        Range range1 = new Range.RangeFactory(schema).createRange(field, Long.MIN_VALUE, 2L);
        Region region1 = new Region(range1);
        Partition partition1 = createLeafPartition("partition1", region1, new LongType());
        Range range2 = new Range.RangeFactory(schema).createRange(field, 2L, null);
        Region region2 = new Region(range2);
        Partition partition2 = createLeafPartition("partition2", region2, new LongType());
        rootPartition.setChildPartitionIds(Arrays.asList(partition1.getId(), partition2.getId()));
        StateStore stateStore = getStateStore(schema,
                Arrays.asList(rootPartition, partition1, partition2));
        List<Record> records = getLotsOfRecords();

        // When
        IngestProperties properties = defaultPropertiesBuilder(stateStore, schema)
                .maxRecordsToWriteLocally(10L)
                .maxInMemoryBatchSize(5L).build();
        IngestRecords ingestRecords = new IngestRecords(properties);
        ingestRecords.init();
        for (Record record : records) {
            ingestRecords.write(record);
        }
        long numWritten = ingestRecords.close();

        // Then:
        //  - Check the correct number of records were written
        assertThat(numWritten).isEqualTo(records.size());
        //  - Check that the correct number of files have been written
        Map<String, List<String>> partitionToFileMapping = stateStore.getPartitionToActiveFilesMap();
        assertThat(partitionToFileMapping.get(partition1.getId())).hasSize(40);
        assertThat(partitionToFileMapping.get(partition2.getId())).hasSize(40);
        //  - Check that the files in each partition contain the correct data
        List<CloseableIterator<Record>> inputIterators = new ArrayList<>();
        for (String file : partitionToFileMapping.get(partition1.getId())) {
            ParquetReader<Record> reader = new ParquetRecordReader.Builder(new Path(file), schema)
                    .build();
            ParquetReaderIterator recordIterator = new ParquetReaderIterator(reader);
            inputIterators.add(recordIterator);
        }
        MergingIterator mergingIterator = new MergingIterator(schema, inputIterators);
        List<Record> recordsInPartition1 = new ArrayList<>();
        while (mergingIterator.hasNext()) {
            recordsInPartition1.add(mergingIterator.next());
        }
        List<Record> expectedRecords = records.stream()
                .filter(r -> ((long) r.get("key")) < 2L)
                .sorted(Comparator.comparing(r -> ((Long) r.get("key"))))
                .collect(Collectors.toList());
        assertThat(recordsInPartition1).isEqualTo(expectedRecords);
        //  - Merge the sketch files for the partition and check it has the right properties
        ItemsUnion<Long> union = ItemsUnion.getInstance(1024, Comparator.naturalOrder());
        for (String file : partitionToFileMapping.get(partition1.getId())) {
            String sketchFile = file.replace(".parquet", ".sketches");
            assertThat(Files.exists(new File(sketchFile).toPath(), LinkOption.NOFOLLOW_LINKS)).isTrue();
            Sketches readSketches = new SketchesSerDeToS3(schema).loadFromHadoopFS("", sketchFile, new Configuration());
            union.update(readSketches.getQuantilesSketch("key"));
        }
        ItemsSketch<Long> readSketch0 = union.getResult();
        ItemsSketch<Long> expectedSketch0 = ItemsSketch.getInstance(1024, Comparator.naturalOrder());
        expectedRecords.forEach(r -> expectedSketch0.update((Long) r.get("key")));
        assertThat(readSketch0.getMinValue()).isEqualTo(expectedSketch0.getMinValue());
        assertThat(readSketch0.getMaxValue()).isEqualTo(expectedSketch0.getMaxValue());
        for (double d = 0.0D; d < 1.0D; d += 0.1D) {
            assertThat(readSketch0.getQuantile(d)).isEqualTo(expectedSketch0.getQuantile(d));
        }

        // Repeat for the second partition
        inputIterators.clear();
        for (String file : partitionToFileMapping.get(partition2.getId())) {
            ParquetReader<Record> reader = new ParquetRecordReader.Builder(new Path(file), schema)
                    .build();
            ParquetReaderIterator recordIterator = new ParquetReaderIterator(reader);
            inputIterators.add(recordIterator);
        }
        mergingIterator = new MergingIterator(schema, inputIterators);
        List<Record> recordsInPartition2 = new ArrayList<>();
        while (mergingIterator.hasNext()) {
            recordsInPartition2.add(mergingIterator.next());
        }
        List<Record> expectedRecords2 = records.stream()
                .filter(r -> ((long) r.get("key")) >= 2L)
                .sorted(Comparator.comparing(r -> ((Long) r.get("key"))))
                .collect(Collectors.toList());
        assertThat(recordsInPartition2).isEqualTo(expectedRecords2);
        //  - Merge the sketch files for the partition and check it has the right properties
        ItemsUnion<Long> union2 = ItemsUnion.getInstance(1024, Comparator.naturalOrder());
        for (String file : partitionToFileMapping.get(partition2.getId())) {
            String sketchFile = file.replace(".parquet", ".sketches");
            assertThat(Files.exists(new File(sketchFile).toPath(), LinkOption.NOFOLLOW_LINKS)).isTrue();
            Sketches readSketches = new SketchesSerDeToS3(schema).loadFromHadoopFS("", sketchFile, new Configuration());
            union2.update(readSketches.getQuantilesSketch("key"));
        }
        ItemsSketch<Long> readSketch1 = union2.getResult();
        ItemsSketch<Long> expectedSketch1 = ItemsSketch.getInstance(1024, Comparator.naturalOrder());
        expectedRecords2.forEach(r -> expectedSketch1.update((Long) r.get("key")));
        assertThat(readSketch1.getMinValue()).isEqualTo(expectedSketch1.getMinValue());
        assertThat(readSketch1.getMaxValue()).isEqualTo(expectedSketch1.getMaxValue());
        for (double d = 0.0D; d < 1.0D; d += 0.1D) {
            assertThat(readSketch1.getQuantile(d)).isEqualTo(expectedSketch1.getQuantile(d));
        }
    }

    @Test
    public void shouldSortRecords() throws Exception {
        // Given
        StateStore stateStore = getStateStore(schema);

        // When
        IngestProperties properties = defaultPropertiesBuilder(stateStore, schema).build();
        IngestRecords ingestRecords = new IngestRecords(properties);
        ingestRecords.init();
        for (Record record : getUnsortedRecords()) {
            ingestRecords.write(record);
        }
        long numWritten = ingestRecords.close();

        // Then:
        //  - Check the correct number of records were written
        assertThat(numWritten).isEqualTo(getUnsortedRecords().size());
        //  - Check StateStore has correct information
        List<FileInfo> activeFiles = stateStore.getActiveFiles();
        assertThat(activeFiles).hasSize(1);
        FileInfo fileInfo = activeFiles.get(0);
        assertThat((long) fileInfo.getMinRowKey().get(0)).isOne();
        assertThat((long) fileInfo.getMaxRowKey().get(0)).isEqualTo(10L);
        assertThat(fileInfo.getNumberOfRecords().longValue()).isEqualTo(20L);
        assertThat(fileInfo.getPartitionId()).isEqualTo(stateStore.getAllPartitions().get(0).getId());
        //  - Read file and check it has correct records
        List<Record> readRecords = readRecordsFromParquetFile(fileInfo.getFilename(), schema);
        assertThat(readRecords.size()).isEqualTo(20L);
        List<Record> sortedRecords = new ArrayList<>(getUnsortedRecords());
        sortedRecords.sort(Comparator.comparing(o -> ((Long) o.get("key"))));
        int i = 0;
        for (Record record1 : sortedRecords) {
            assertThat(readRecords.get(i)).isEqualTo(record1);
            i++;
        }
        //  - Check quantiles sketches have been written and are correct (NB the sketches are stochastic so may not be identical)
        List<Long> sortedKeyList = sortedRecords.stream().map(r -> (Long) r.get("key")).collect(Collectors.toList());
        assertSketchUsingDirectValuesAllQuantiles(schema, "key", fileInfo.getFilename(),
                1L, 10L, calculateQuantiles(sortedKeyList));
    }

    @Test
    public void shouldApplyIterator() throws Exception {
        // Given
        Schema schema = Schema.builder()
                .rowKeyFields(new Field("key", new ByteArrayType()))
                .sortKeyFields(new Field("sort", new LongType()))
                .valueFields(new Field("value", new LongType()))
                .build();
        StateStore stateStore = getStateStore(schema);

        // When
        IngestProperties properties = defaultPropertiesBuilder(stateStore, schema)
                .iteratorClassName(AdditionIterator.class.getName()).build();
        IngestRecords ingestRecords = new IngestRecords(properties);
        ingestRecords.init();
        for (Record record : getRecordsForAggregationIteratorTest()) {
            ingestRecords.write(record);
        }
        long numWritten = ingestRecords.close();

        // Then:
        //  - Check the correct number of records were written
        assertThat(numWritten).isEqualTo(2L);
        //  - Check StateStore has correct information
        List<FileInfo> activeFiles = stateStore.getActiveFiles();
        assertThat(activeFiles).hasSize(1);
        FileInfo fileInfo = activeFiles.get(0);
        assertThat((byte[]) fileInfo.getMinRowKey().get(0)).containsExactly(new byte[]{1, 1});
        assertThat((byte[]) fileInfo.getMaxRowKey().get(0)).containsExactly(new byte[]{11, 2});
        assertThat(fileInfo.getNumberOfRecords().longValue()).isEqualTo(2L);
        assertThat(fileInfo.getPartitionId()).isEqualTo(stateStore.getAllPartitions().get(0).getId());
        //  - Read file and check it has correct records
        List<Record> readRecords = readRecordsFromParquetFile(fileInfo.getFilename(), schema);
        assertThat(readRecords.size()).isEqualTo(2L);

        Record expectedRecord1 = new Record();
        expectedRecord1.put("key", new byte[]{1, 1});
        expectedRecord1.put("sort", 2L);
        expectedRecord1.put("value", 7L);
        assertThat(readRecords.get(0)).isEqualTo(expectedRecord1);
        Record expectedRecord2 = new Record();
        expectedRecord2.put("key", new byte[]{11, 2});
        expectedRecord2.put("sort", 1L);
        expectedRecord2.put("value", 4L);
        assertThat(readRecords.get(1)).isEqualTo(expectedRecord2);

        //  - Check quantiles sketches have been written and are correct (NB the sketches are stochastic so may not be identical)
        AdditionIterator additionIterator = new AdditionIterator();
        additionIterator.init("", schema);
        List<Record> sortedRecords = new ArrayList<>(getRecordsForAggregationIteratorTest());
        sortedRecords.sort(Comparator.comparing(o -> wrap(((byte[]) o.get("key")))));
        CloseableIterator<Record> aggregatedRecords = additionIterator.apply(new WrappedIterator<>(sortedRecords.iterator()));
        List<ByteArray> sortedKeyList = Streams.stream(aggregatedRecords).map(r -> wrap((byte[]) r.get("key"))).collect(Collectors.toList());
        assertSketchUsingDirectValuesAllQuantiles(schema, "key", activeFiles.get(0).getFilename(),
                wrap(new byte[]{1, 1}), wrap(new byte[]{11, 2}),
                calculateQuantiles(sortedKeyList));
    }
}
