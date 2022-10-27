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

import org.junit.Test;
import sleeper.core.iterator.impl.AdditionIterator;
import sleeper.core.partition.Partition;
import sleeper.core.range.Range;
import sleeper.core.range.Region;
import sleeper.core.record.Record;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.ByteArrayType;
import sleeper.core.schema.type.LongType;
import sleeper.ingest.testutils.AssertQuantiles;
import sleeper.statestore.FileInfo;
import sleeper.statestore.StateStore;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import static com.facebook.collections.ByteArray.wrap;
import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.ingest.testutils.IngestRecordsTestDataHelper.calculateAllQuantiles;
import static sleeper.ingest.testutils.IngestRecordsTestDataHelper.createLeafPartition;
import static sleeper.ingest.testutils.IngestRecordsTestDataHelper.createRootPartition;
import static sleeper.ingest.testutils.IngestRecordsTestDataHelper.getRecords;
import static sleeper.ingest.testutils.IngestRecordsTestDataHelper.getRecordsForAggregationIteratorTest;
import static sleeper.ingest.testutils.IngestRecordsTestDataHelper.getSketches;
import static sleeper.ingest.testutils.IngestRecordsTestDataHelper.getUnsortedRecords;
import static sleeper.ingest.testutils.IngestRecordsTestDataHelper.readRecordsFromParquetFile;

public class IngestRecordsFromIteratorTest extends IngestRecordsTestBase {

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
        long numWritten = new IngestRecordsFromIterator(properties, getRecords().iterator()).write();

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
        List<Record> readRecords = readRecordsFromParquetFile(activeFiles.get(0).getFilename(), schema);
        assertThat(readRecords).containsExactly(getRecords().get(0));
        readRecords = readRecordsFromParquetFile(activeFiles.get(1).getFilename(), schema);
        assertThat(readRecords).containsExactly(getRecords().get(1));
        //  - Check quantiles sketches have been written and are correct (NB the sketches are stochastic so may not be identical)
        AssertQuantiles.forSketch(getSketches(schema, activeFiles.get(0).getFilename()).getQuantilesSketch("key"))
                .min(1L).max(1L)
                .quantile(0.4, 1L).quantile(0.6, 1L).verify();
        AssertQuantiles.forSketch(getSketches(schema, activeFiles.get(1).getFilename()).getQuantilesSketch("key"))
                .min(3L).max(3L)
                .quantile(0.4, 3L).quantile(0.6, 3L).verify();
    }

    @Test
    public void shouldWriteDuplicateRecords() throws Exception {
        // Given
        StateStore stateStore = getStateStore(schema);

        // When
        List<Record> records = new ArrayList<>(getRecords());
        records.addAll(getRecords());
        IngestProperties properties = defaultPropertiesBuilder(stateStore, schema).build();
        long numWritten = new IngestRecordsFromIterator(properties, records.iterator()).write();

        // Then:
        //  - Check the correct number of records were written
        assertThat(numWritten).isEqualTo(2L * getRecords().size());
        //  - Check StateStore has correct information
        List<FileInfo> activeFiles = stateStore.getActiveFiles();
        assertThat(activeFiles).hasSize(1);
        FileInfo fileInfo = activeFiles.get(0);
        assertThat((long) fileInfo.getMinRowKey().get(0)).isOne();
        assertThat((long) fileInfo.getMaxRowKey().get(0)).isEqualTo(3L);
        assertThat(fileInfo.getNumberOfRecords().longValue()).isEqualTo(4L);
        assertThat(fileInfo.getPartitionId()).isEqualTo(stateStore.getAllPartitions().get(0).getId());
        //  - Read file and check it has correct records
        List<Record> readRecords = readRecordsFromParquetFile(fileInfo.getFilename(), schema);
        assertThat(readRecords).containsExactly(
                getRecords().get(0), getRecords().get(0),
                getRecords().get(1), getRecords().get(1));
        //  - Check quantiles sketches have been written and are correct (NB the sketches are stochastic so may not be identical)
        AssertQuantiles.forSketch(getSketches(schema, activeFiles.get(0).getFilename()).getQuantilesSketch("key"))
                .min(1L).max(3L)
                .quantile(0.4, 1L).quantile(0.6, 3L).verify();
    }

    @Test
    public void shouldSortRecords() throws Exception {
        // Given
        StateStore stateStore = getStateStore(schema);

        // When
        IngestProperties properties = defaultPropertiesBuilder(stateStore, schema).build();
        long numWritten = new IngestRecordsFromIterator(properties, getUnsortedRecords().iterator()).write();

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
        AssertQuantiles.forSketch(getSketches(schema, activeFiles.get(0).getFilename()).getQuantilesSketch("key"))
                .min(1L).max(10L)
                .quantiles(calculateAllQuantiles(sortedKeyList)).verify();
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
                .iteratorClassName(AdditionIterator.class.getName())
                .build();
        long numWritten = new IngestRecordsFromIterator(properties, getRecordsForAggregationIteratorTest().iterator()).write();

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
        Record expectedRecord1 = new Record();
        expectedRecord1.put("key", new byte[]{1, 1});
        expectedRecord1.put("sort", 2L);
        expectedRecord1.put("value", 7L);
        Record expectedRecord2 = new Record();
        expectedRecord2.put("key", new byte[]{11, 2});
        expectedRecord2.put("sort", 1L);
        expectedRecord2.put("value", 4L);
        assertThat(readRecords).containsExactly(expectedRecord1, expectedRecord2);

        //  - Check quantiles sketches have been written and are correct (NB the sketches are stochastic so may not be identical)
        AssertQuantiles.forSketch(getSketches(schema, activeFiles.get(0).getFilename()).getQuantilesSketch("key"))
                .min(wrap(new byte[]{1, 1})).max(wrap(new byte[]{11, 2}))
                .quantile(0.4, wrap(new byte[]{1, 1})).quantile(0.6, wrap(new byte[]{11, 2})).verify();
    }

    @Test
    public void shouldWriteNoRecordsSuccessfully() throws Exception {
        // Given
        StateStore stateStore = getStateStore(schema);

        // When
        IngestProperties properties = defaultPropertiesBuilder(stateStore, schema).build();
        long numWritten = new IngestRecordsFromIterator(properties, Collections.emptyIterator()).write();

        // Then:
        //  - Check the correct number of records were written
        assertThat(numWritten).isZero();
        //  - Check StateStore has correct information
        List<FileInfo> activeFiles = stateStore.getActiveFiles();
        assertThat(activeFiles).isEmpty();
    }
}
