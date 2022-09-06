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

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.facebook.collections.ByteArray;
import org.apache.datasketches.quantiles.ItemsSketch;
import org.apache.datasketches.quantiles.ItemsUnion;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.testcontainers.containers.GenericContainer;
import sleeper.configuration.jars.ObjectFactory;
import sleeper.configuration.jars.ObjectFactoryException;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.core.CommonTestConstants;
import sleeper.core.iterator.CloseableIterator;
import sleeper.core.iterator.IteratorException;
import sleeper.core.iterator.MergingIterator;
import sleeper.core.iterator.WrappedIterator;
import sleeper.core.iterator.impl.AdditionIterator;
import sleeper.core.partition.Partition;
import sleeper.core.partition.PartitionsFromSplitPoints;
import sleeper.core.range.Range;
import sleeper.core.range.Range.RangeFactory;
import sleeper.core.range.Region;
import sleeper.core.record.CloneRecord;
import sleeper.core.record.Record;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.ByteArrayType;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.LongType;
import sleeper.io.parquet.record.ParquetReaderIterator;
import sleeper.io.parquet.record.ParquetRecordReader;
import sleeper.sketches.Sketches;
import sleeper.sketches.s3.SketchesSerDeToS3;
import sleeper.statestore.FileInfo;
import sleeper.statestore.StateStoreException;
import sleeper.statestore.dynamodb.DynamoDBStateStore;
import sleeper.statestore.dynamodb.DynamoDBStateStoreCreator;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

public class IngestRecordsIT {
    private static final int DYNAMO_PORT = 8000;

    @ClassRule
    public static GenericContainer dynamoDb = new GenericContainer(CommonTestConstants.DYNAMODB_LOCAL_CONTAINER)
            .withExposedPorts(DYNAMO_PORT);

    @Rule
    public TemporaryFolder folder = new TemporaryFolder(CommonTestConstants.TMP_DIRECTORY);

    private final Field field = new Field("key", new LongType());
    private final Schema schema = schemaWithRowKeys(field);

    private Schema schemaWithRowKeys(Field... fields) {
        return Schema.builder()
                .rowKeyFields(fields)
                .valueFields(new Field("value1", new LongType()), new Field("value2", new LongType()))
                .build();
    }

    public static List<Record> getRecords() {
        List<Record> records = new ArrayList<>();
        Record record1 = new Record();
        record1.put("key", 1L);
        record1.put("value1", 2L);
        record1.put("value2", 3L);
        Record record2 = new Record();
        record2.put("key", 3L);
        record2.put("value1", 4L);
        record2.put("value2", 6L);
        records.add(record1);
        records.add(record2);
        return records;
    }

    private static List<Record> getLotsOfRecords() {
        List<Record> records = new ArrayList<>();
        for (int i = 0; i < 200; i++) {
            Record record1 = new Record();
            record1.put("key", 1L - i);
            record1.put("value1", 2L * i);
            record1.put("value2", 3L * i);
            Record record2 = new Record();
            record2.put("key", 2L + i);
            record2.put("value1", 4L * i);
            record2.put("value2", 6L * i);
            records.add(record1);
            records.add(record2);
        }
        return records;
    }

    private static List<Record> getRecordsInFirstPartitionOnly() {
        List<Record> records = new ArrayList<>();
        Record record1 = new Record();
        record1.put("key", 1L);
        record1.put("value1", 2L);
        record1.put("value2", 3L);
        Record record2 = new Record();
        record2.put("key", 0L);
        record2.put("value1", 4L);
        record2.put("value2", 6L);
        records.add(record1);
        records.add(record2);
        return records;
    }

    private static List<Record> getRecordsByteArrayKey() {
        List<Record> records = new ArrayList<>();
        Record record1 = new Record();
        record1.put("key", new byte[]{1, 1});
        record1.put("value1", 2L);
        record1.put("value2", 3L);
        Record record2 = new Record();
        record2.put("key", new byte[]{2, 2});
        record2.put("value1", 2L);
        record2.put("value2", 3L);
        Record record3 = new Record();
        record3.put("key", new byte[]{64, 65});
        record3.put("value1", 4L);
        record3.put("value2", 6L);
        records.add(record1);
        records.add(record2);
        records.add(record3);
        return records;
    }

    private static List<Record> getRecords2DimByteArrayKey() {
        List<Record> records = new ArrayList<>();
        Record record1 = new Record();
        record1.put("key1", new byte[]{1, 1});
        record1.put("key2", new byte[]{2, 3});
        record1.put("value1", 2L);
        record1.put("value2", 3L);
        Record record2 = new Record();
        record2.put("key1", new byte[]{11, 2});
        record2.put("key2", new byte[]{2, 2});
        record2.put("value1", 2L);
        record2.put("value2", 3L);
        Record record3 = new Record();
        record3.put("key1", new byte[]{64, 65});
        record3.put("key2", new byte[]{67, 68});
        record3.put("value1", 4L);
        record3.put("value2", 6L);
        Record record4 = new Record();
        record4.put("key1", new byte[]{5});
        record4.put("key2", new byte[]{99});
        record4.put("value1", 2L);
        record4.put("value2", 3L);
        records.add(record1);
        records.add(record2);
        records.add(record3);
        records.add(record3); // Add twice so that one file has more entries so we can tell them apart
        records.add(record4);
        return records;
    }

    private static List<Record> getUnsortedRecords() {
        List<Record> records = new ArrayList<>();
        for (int i = 10; i > 0; i--) {
            Record record1 = new Record();
            record1.put("key", (long) i);
            record1.put("value1", 2L);
            record1.put("value2", 3L);
            records.add(record1);
            Record record2 = new Record();
            record2.put("key", 5L);
            record2.put("value1", 4L);
            record2.put("value2", 6L);
            records.add(record2);
        }
        return records;
    }

    private static List<Record> getRecordsForAggregationIteratorTest() {
        List<Record> records = new ArrayList<>();
        Record record1 = new Record();
        record1.put("key", new byte[]{1, 1});
        record1.put("sort", 2L);
        record1.put("value", 1L);
        Record record2 = new Record();
        record2.put("key", new byte[]{11, 2});
        record2.put("sort", 1L);
        record2.put("value", 1L);
        Record record3 = new Record();
        record3.put("key", new byte[]{1, 1});
        record3.put("sort", 2L);
        record3.put("value", 6L);
        Record record4 = new Record();
        record4.put("key", new byte[]{11, 2});
        record4.put("sort", 1L);
        record4.put("value", 3L);
        records.add(record1);
        records.add(record2);
        records.add(record3);
        records.add(record4);
        return records;
    }

    private static List<Record> getRecordsOscillatingBetween2Partitions() {
        List<Record> records = new ArrayList<>();
        Record record1 = new Record();
        record1.put("key1", 0);
        record1.put("key2", 1L);
        record1.put("value1", 2L);
        record1.put("value2", 1L);
        Record record2 = new Record();
        record2.put("key1", 0);
        record2.put("key2", 20L);
        record2.put("value1", 200L);
        record2.put("value2", 100L);
        Record record3 = new Record();
        record3.put("key1", 100);
        record3.put("key2", 1L);
        record3.put("value1", 20000L);
        record3.put("value2", 10000L);
        Record record4 = new Record();
        record4.put("key1", 100);
        record4.put("key2", 50L);
        record4.put("value1", 2000000L);
        record4.put("value2", 1000000L);
        records.add(record1);
        records.add(record2);
        records.add(record3);
        records.add(record4);
        return records;
    }

    private static DynamoDBStateStore getStateStore(Schema schema) throws StateStoreException {
        return getStateStore(schema, new PartitionsFromSplitPoints(schema, Collections.emptyList()).construct());
    }

    private static DynamoDBStateStore getStateStore(Schema schema, List<Partition> initialPartitions)
            throws StateStoreException {
        AwsClientBuilder.EndpointConfiguration endpointConfiguration =
                new AwsClientBuilder.EndpointConfiguration("http://" + dynamoDb.getContainerIpAddress() + ":"
                        + dynamoDb.getMappedPort(DYNAMO_PORT), "us-west-2");
        AmazonDynamoDB dynamoDBClient = AmazonDynamoDBClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials("12345", "6789")))
                .withEndpointConfiguration(endpointConfiguration)
                .build();
        String tableNameStub = UUID.randomUUID().toString();
        DynamoDBStateStoreCreator dynamoDBStateStoreCreator = new DynamoDBStateStoreCreator(tableNameStub, schema, dynamoDBClient);
        DynamoDBStateStore stateStore = dynamoDBStateStoreCreator.create();
        stateStore.initialise(initialPartitions);
        return stateStore;
    }

    @Test
    public void shouldWriteRecordsCorrectly() throws StateStoreException, IOException, InterruptedException, IteratorException, ObjectFactoryException {
        // Given
        DynamoDBStateStore stateStore = getStateStore(schema);
        String localDir = folder.newFolder().getAbsolutePath();

        // When
        IngestRecords ingestRecords = new IngestRecords(
                new ObjectFactory(new InstanceProperties(), null, ""),
                localDir,
                10L,
                1000L,
                ParquetWriter.DEFAULT_BLOCK_SIZE,
                ParquetWriter.DEFAULT_PAGE_SIZE,
                "zstd",
                stateStore,
                schema,
                "",
                folder.newFolder().getAbsolutePath(),
                null,
                null,
                120);
        ingestRecords.init();
        for (Record record : getRecords()) {
            ingestRecords.write(record);
        }
        long numWritten = ingestRecords.close();

        // Then:
        //  - Check the correct number of records were written
        assertThat(numWritten).isEqualTo(getRecords().size());
        //  - Check StateStore has correct information
        List<FileInfo> activeFiles = stateStore.getActiveFiles();
        assertThat(activeFiles).hasSize(1);
        FileInfo fileInfo = activeFiles.get(0);
        assertThat((long) fileInfo.getMinRowKey().get(0)).isOne();
        assertThat((long) fileInfo.getMaxRowKey().get(0)).isEqualTo(3L);
        assertThat(fileInfo.getNumberOfRecords().longValue()).isEqualTo(2L);
        assertThat(fileInfo.getPartitionId()).isEqualTo(stateStore.getAllPartitions().get(0).getId());
        //  - Read file and check it has correct records
        ParquetReader<Record> reader = new ParquetRecordReader.Builder(new Path(fileInfo.getFilename()), schema).build();
        List<Record> readRecords = new ArrayList<>();
        CloneRecord cloneRecord = new CloneRecord(schema);
        Record record = reader.read();
        while (null != record) {
            readRecords.add(cloneRecord.clone(record));
            record = reader.read();
        }
        reader.close();
        assertThat(readRecords).hasSize(2);
        assertThat(readRecords.get(0)).isEqualTo(getRecords().get(0));
        assertThat(readRecords.get(1)).isEqualTo(getRecords().get(1));
        //  - Local files should have been deleted
        assertThat(Files.walk(Paths.get(localDir)).filter(Files::isRegularFile).count()).isZero();
        //  - Check quantiles sketches have been written and are correct (NB the sketches are stochastic so may not be identical)
        String sketchFile = fileInfo.getFilename().replace(".parquet", ".sketches");
        assertThat(Files.exists(new File(sketchFile).toPath(), LinkOption.NOFOLLOW_LINKS)).isTrue();
        Sketches readSketches = new SketchesSerDeToS3(schema).loadFromHadoopFS("", sketchFile, new Configuration());
        ItemsSketch<Long> expectedSketch = ItemsSketch.getInstance(1024, Comparator.naturalOrder());
        getRecords().forEach(r -> expectedSketch.update((Long) r.get("key")));
        assertThat(readSketches.getQuantilesSketch("key").getMinValue()).isEqualTo(expectedSketch.getMinValue());
        assertThat(readSketches.getQuantilesSketch("key").getMaxValue()).isEqualTo(expectedSketch.getMaxValue());
        for (double d = 0.0D; d < 1.0D; d += 0.1D) {
            assertThat(readSketches.getQuantilesSketch("key").getQuantile(d)).isEqualTo(expectedSketch.getQuantile(d));
        }
    }

    @Test
    public void shouldWriteRecordsSplitByPartitionLongKey() throws StateStoreException, IOException, InterruptedException, IteratorException, ObjectFactoryException {
        // Given
        Partition rootPartition = new Partition();
        rootPartition.setRowKeyTypes(new LongType());
        rootPartition.setId("root");
        Range rootRange = new RangeFactory(schema).createRange(field, Long.MIN_VALUE, null);
        Region rootRegion = new Region(rootRange);
        rootPartition.setRegion(rootRegion);
        rootPartition.setLeafPartition(false);
        rootPartition.setParentPartitionId(null);
        Partition partition1 = new Partition();
        partition1.setRowKeyTypes(new LongType());
        partition1.setId("partition1");
        Range range1 = new RangeFactory(schema).createRange(field, Long.MIN_VALUE, 2L);
        Region region1 = new Region(range1);
        partition1.setRegion(region1);
        partition1.setLeafPartition(true);
        partition1.setParentPartitionId(rootPartition.getId());
        partition1.setChildPartitionIds(new ArrayList<>());
        Partition partition2 = new Partition();
        partition2.setRowKeyTypes(new LongType());
        partition2.setId("partition2");
        Range range2 = new RangeFactory(schema).createRange(field, 2L, null);
        Region region2 = new Region(range2);
        partition2.setRegion(region2);
        partition2.setLeafPartition(true);
        partition2.setParentPartitionId(rootPartition.getId());
        partition2.setChildPartitionIds(new ArrayList<>());
        rootPartition.setChildPartitionIds(Arrays.asList(partition1.getId(), partition2.getId()));
        DynamoDBStateStore stateStore = getStateStore(schema,
                Arrays.asList(rootPartition, partition1, partition2));

        // When
        IngestRecords ingestRecords = new IngestRecords(
                new ObjectFactory(new InstanceProperties(), null, ""),
                folder.newFolder().getAbsolutePath(),
                10L,
                1000L,
                ParquetWriter.DEFAULT_BLOCK_SIZE,
                ParquetWriter.DEFAULT_PAGE_SIZE,
                "zstd",
                stateStore,
                schema,
                "",
                folder.newFolder().getAbsolutePath(),
                null,
                null,
                120);
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
        ParquetReader<Record> reader = new ParquetRecordReader.Builder(new Path(activeFiles.get(0).getFilename()), schema).build();
        List<Record> readRecords = new ArrayList<>();
        CloneRecord cloneRecord = new CloneRecord(schema);
        Record record = reader.read();
        while (null != record) {
            readRecords.add(cloneRecord.clone(record));
            record = reader.read();
        }
        reader.close();
        assertThat(readRecords).hasSize(1);
        assertThat(readRecords.get(0)).isEqualTo(getRecords().get(0));
        reader = new ParquetRecordReader.Builder(new Path(activeFiles.get(1).getFilename()), schema).build();
        readRecords.clear();
        record = reader.read();
        while (null != record) {
            readRecords.add(cloneRecord.clone(record));
            record = reader.read();
        }
        reader.close();
        assertThat(readRecords).hasSize(1);
        assertThat(readRecords.get(0)).isEqualTo(getRecords().get(1));
        //  - Check quantiles sketches have been written and are correct (NB the sketches are stochastic so may not be identical)
        String sketchFile = activeFiles.get(0).getFilename().replace(".parquet", ".sketches");
        assertThat(Files.exists(new File(sketchFile).toPath(), LinkOption.NOFOLLOW_LINKS)).isTrue();
        Sketches readSketches = new SketchesSerDeToS3(schema).loadFromHadoopFS("", sketchFile, new Configuration());
        ItemsSketch<Long> expectedSketch0 = ItemsSketch.getInstance(1024, Comparator.naturalOrder());
        getRecords().stream()
                .filter(r -> ((long) r.get("key")) < 2L)
                .forEach(r -> expectedSketch0.update((Long) r.get("key")));
        assertThat(readSketches.getQuantilesSketch("key").getMinValue()).isEqualTo(expectedSketch0.getMinValue());
        assertThat(readSketches.getQuantilesSketch("key").getMaxValue()).isEqualTo(expectedSketch0.getMaxValue());
        for (double d = 0.0D; d < 1.0D; d += 0.1D) {
            assertThat(readSketches.getQuantilesSketch("key").getQuantile(d)).isEqualTo(expectedSketch0.getQuantile(d));
        }
        sketchFile = activeFiles.get(1).getFilename().replace(".parquet", ".sketches");
        assertThat(Files.exists(new File(sketchFile).toPath(), LinkOption.NOFOLLOW_LINKS)).isTrue();
        readSketches = new SketchesSerDeToS3(schema).loadFromHadoopFS("", sketchFile, new Configuration());
        ItemsSketch<Long> expectedSketch1 = ItemsSketch.getInstance(1024, Comparator.naturalOrder());
        getRecords().stream()
                .filter(r -> ((long) r.get("key")) >= 2L)
                .forEach(r -> expectedSketch1.update((Long) r.get("key")));
        assertThat(readSketches.getQuantilesSketch("key").getMinValue()).isEqualTo(expectedSketch1.getMinValue());
        assertThat(readSketches.getQuantilesSketch("key").getMaxValue()).isEqualTo(expectedSketch1.getMaxValue());
        for (double d = 0.0D; d < 1.0D; d += 0.1D) {
            assertThat(readSketches.getQuantilesSketch("key").getQuantile(d)).isEqualTo(expectedSketch1.getQuantile(d));
        }
    }

    @Test
    public void shouldWriteRecordsSplitByPartitionByteArrayKey() throws StateStoreException, IOException, InterruptedException, IteratorException, ObjectFactoryException {
        // Given
        Field field = new Field("key", new ByteArrayType());
        Schema schema = schemaWithRowKeys(field);
        Partition rootPartition = new Partition();
        rootPartition.setRowKeyTypes(new ByteArrayType());
        rootPartition.setId("root");
        Range rootRange = new RangeFactory(schema).createRange(field, new byte[]{}, null);
        Region rootRegion = new Region(rootRange);
        rootPartition.setRegion(rootRegion);
        rootPartition.setLeafPartition(false);
        rootPartition.setParentPartitionId(null);
        Partition partition1 = new Partition();
        partition1.setRowKeyTypes(new ByteArrayType());
        partition1.setId("partition1");
        Range range1 = new RangeFactory(schema).createRange(field, new byte[]{}, new byte[]{64, 64});
        Region region1 = new Region(range1);
        partition1.setRegion(region1);
        partition1.setLeafPartition(true);
        partition1.setParentPartitionId(rootPartition.getId());
        partition1.setChildPartitionIds(new ArrayList<>());
        Partition partition2 = new Partition();
        partition2.setRowKeyTypes(new ByteArrayType());
        partition2.setId("partition2");
        Range range2 = new RangeFactory(schema).createRange(field, new byte[]{64, 64}, null);
        Region region2 = new Region(range2);
        partition2.setRegion(region2);
        partition2.setLeafPartition(true);
        partition2.setParentPartitionId(rootPartition.getId());
        partition2.setChildPartitionIds(new ArrayList<>());
        rootPartition.setChildPartitionIds(Arrays.asList(partition1.getId(), partition2.getId()));
        DynamoDBStateStore stateStore = getStateStore(schema,
                Arrays.asList(rootPartition, partition1, partition2));

        // When
        IngestRecords ingestRecords = new IngestRecords(
                new ObjectFactory(new InstanceProperties(), null, ""),
                folder.newFolder().getAbsolutePath(),
                10L,
                1000L,
                ParquetWriter.DEFAULT_BLOCK_SIZE,
                ParquetWriter.DEFAULT_PAGE_SIZE,
                "zstd",
                stateStore,
                schema,
                "",
                folder.newFolder().getAbsolutePath(),
                null,
                null,
                120);
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
        ParquetReader<Record> reader = new ParquetRecordReader.Builder(
                new Path(activeFilesSortedByNumberOfLines.get(1).getFilename()), schema).build();
        List<Record> readRecords = new ArrayList<>();
        CloneRecord cloneRecord = new CloneRecord(schema);
        Record record = reader.read();
        while (null != record) {
            readRecords.add(cloneRecord.clone(record));
            record = reader.read();
        }
        reader.close();
        assertThat(readRecords).hasSize(2);
        assertThat(readRecords.get(0)).isEqualTo(getRecordsByteArrayKey().get(0));
        assertThat(readRecords.get(1)).isEqualTo(getRecordsByteArrayKey().get(1));
        reader = new ParquetRecordReader.Builder(
                new Path(activeFilesSortedByNumberOfLines.get(0).getFilename()), schema).build();
        readRecords.clear();
        record = reader.read();
        while (null != record) {
            readRecords.add(cloneRecord.clone(record));
            record = reader.read();
        }
        reader.close();
        assertThat(readRecords).hasSize(1);
        assertThat(readRecords.get(0)).isEqualTo(getRecordsByteArrayKey().get(2));
        //  - Check quantiles sketches have been written and are correct (NB the sketches are stochastic so may not be identical)
        String sketchFile = activeFilesSortedByNumberOfLines.get(1).getFilename().replace(".parquet", ".sketches");
        assertThat(Files.exists(new File(sketchFile).toPath(), LinkOption.NOFOLLOW_LINKS)).isTrue();
        Sketches readSketches = new SketchesSerDeToS3(schema).loadFromHadoopFS("", sketchFile, new Configuration());
        ItemsSketch<ByteArray> expectedSketch0 = ItemsSketch.getInstance(1024, Comparator.naturalOrder());
        getRecordsByteArrayKey().stream()
                .map(r -> ByteArray.wrap((byte[]) r.get("key")))
                .filter(ba -> ba.compareTo(ByteArray.wrap(new byte[]{64, 64})) < 0)
                .forEach(expectedSketch0::update);
        assertThat(readSketches.getQuantilesSketch("key").getMinValue()).isEqualTo(expectedSketch0.getMinValue());
        assertThat(readSketches.getQuantilesSketch("key").getMaxValue()).isEqualTo(expectedSketch0.getMaxValue());
        for (double d = 0.0D; d < 1.0D; d += 0.1D) {
            assertThat(readSketches.getQuantilesSketch("key").getQuantile(d)).isEqualTo(expectedSketch0.getQuantile(d));
        }
        sketchFile = activeFilesSortedByNumberOfLines.get(0).getFilename().replace(".parquet", ".sketches");
        assertThat(Files.exists(new File(sketchFile).toPath(), LinkOption.NOFOLLOW_LINKS)).isTrue();
        readSketches = new SketchesSerDeToS3(schema).loadFromHadoopFS("", sketchFile, new Configuration());
        ItemsSketch<ByteArray> expectedSketch1 = ItemsSketch.getInstance(1024, Comparator.naturalOrder());
        getRecordsByteArrayKey().stream()
                .map(r -> ByteArray.wrap((byte[]) r.get("key")))
                .filter(ba -> ba.compareTo(ByteArray.wrap(new byte[]{64, 64})) >= 0)
                .forEach(expectedSketch1::update);
        assertThat(readSketches.getQuantilesSketch("key").getMinValue()).isEqualTo(expectedSketch1.getMinValue());
        assertThat(readSketches.getQuantilesSketch("key").getMaxValue()).isEqualTo(expectedSketch1.getMaxValue());
        for (double d = 0.0D; d < 1.0D; d += 0.1D) {
            assertThat(readSketches.getQuantilesSketch("key").getQuantile(d)).isEqualTo(expectedSketch1.getQuantile(d));
        }
    }

    @Test
    public void shouldWriteRecordsSplitByPartition2DimensionalByteArrayKey() throws StateStoreException, IOException, InterruptedException, IteratorException, ObjectFactoryException {
        // Given
        Field field1 = new Field("key1", new ByteArrayType());
        Field field2 = new Field("key2", new ByteArrayType());
        Schema schema = schemaWithRowKeys(field1, field2);
        Partition rootPartition = new Partition();
        rootPartition.setRowKeyTypes(new ByteArrayType(), new ByteArrayType());
        rootPartition.setId("root");
        Range rootRange1 = new RangeFactory(schema).createRange(field1, new byte[]{}, null);
        Range rootRange2 = new RangeFactory(schema).createRange(field2, new byte[]{}, null);
        Region rootRegion = new Region(Arrays.asList(rootRange1, rootRange2));
        rootPartition.setRegion(rootRegion);
        rootPartition.setLeafPartition(false);
        rootPartition.setParentPartitionId(null);
        rootPartition.setDimension(1);
        Partition partition1 = new Partition();
        partition1.setRowKeyTypes(new ByteArrayType(), new ByteArrayType());
        partition1.setId("id1");
        Range range11 = new RangeFactory(schema).createRange(field1, new byte[]{}, new byte[]{10});
        Range range12 = new RangeFactory(schema).createRange(field2, new byte[]{}, null);
        Region region1 = new Region(Arrays.asList(range11, range12));
        partition1.setRegion(region1);
        partition1.setLeafPartition(true);
        partition1.setParentPartitionId(rootPartition.getId());
        partition1.setChildPartitionIds(new ArrayList<>());
        Partition partition2 = new Partition();
        partition2.setRowKeyTypes(new ByteArrayType(), new ByteArrayType());
        partition2.setId("id2");
        Range range21 = new RangeFactory(schema).createRange(field1, new byte[]{10}, null);
        Range range22 = new RangeFactory(schema).createRange(field2, new byte[]{}, null);
        Region region2 = new Region(Arrays.asList(range21, range22));
        partition2.setRegion(region2);
        partition2.setLeafPartition(true);
        partition2.setParentPartitionId(rootPartition.getId());
        partition2.setChildPartitionIds(new ArrayList<>());
        rootPartition.setChildPartitionIds(Arrays.asList(partition1.getId(), partition2.getId()));
        DynamoDBStateStore stateStore = getStateStore(schema,
                Arrays.asList(rootPartition, partition1, partition2));

        // When
        IngestRecords ingestRecords = new IngestRecords(
                new ObjectFactory(new InstanceProperties(), null, ""),
                folder.newFolder().getAbsolutePath(),
                10L,
                1000L,
                ParquetWriter.DEFAULT_BLOCK_SIZE,
                ParquetWriter.DEFAULT_PAGE_SIZE,
                "zstd",
                stateStore,
                schema,
                "",
                folder.newFolder().getAbsolutePath(),
                null,
                null,
                120);
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
        assertThat(fileInfo.getPartitionId()).isEqualTo(stateStore.getAllPartitions().get(0).getId());
        fileInfo = activeFilesSortedByNumberOfLines.get(1);
        assertThat((byte[]) fileInfo.getMinRowKey().get(0)).containsExactly(new byte[]{11, 2});
        assertThat((byte[]) fileInfo.getMaxRowKey().get(0)).containsExactly(new byte[]{64, 65});
        assertThat(fileInfo.getNumberOfRecords().longValue()).isEqualTo(3L);
        assertThat(fileInfo.getPartitionId()).isEqualTo(stateStore.getAllPartitions().get(1).getId());
        //  - Read files and check they have the correct records
        ParquetReader<Record> reader = new ParquetRecordReader.Builder(
                new Path(activeFilesSortedByNumberOfLines.get(0).getFilename()), schema).build();
        List<Record> readRecords = new ArrayList<>();
        CloneRecord cloneRecord = new CloneRecord(schema);
        Record record = reader.read();
        while (null != record) {
            readRecords.add(cloneRecord.clone(record));
            record = reader.read();
        }
        reader.close();
        assertThat(readRecords).hasSize(2);
        assertThat(readRecords.get(0)).isEqualTo(getRecords2DimByteArrayKey().get(0));
        assertThat(readRecords.get(1)).isEqualTo(getRecords2DimByteArrayKey().get(4));
        reader = new ParquetRecordReader.Builder(
                new Path(activeFilesSortedByNumberOfLines.get(1).getFilename()), schema).build();
        readRecords.clear();
        record = reader.read();
        while (null != record) {
            readRecords.add(cloneRecord.clone(record));
            record = reader.read();
        }
        reader.close();
        assertThat(readRecords).hasSize(3);
        assertThat(readRecords.get(0)).isEqualTo(getRecords2DimByteArrayKey().get(1));
        assertThat(readRecords.get(1)).isEqualTo(getRecords2DimByteArrayKey().get(2));
        assertThat(readRecords.get(2)).isEqualTo(getRecords2DimByteArrayKey().get(3));
        //  - Check quantiles sketches have been written and are correct (NB the sketches are stochastic so may not be identical)
        String sketchFile = activeFilesSortedByNumberOfLines.get(0).getFilename().replace(".parquet", ".sketches");
        assertThat(Files.exists(new File(sketchFile).toPath(), LinkOption.NOFOLLOW_LINKS)).isTrue();
        Sketches readSketches = new SketchesSerDeToS3(schema).loadFromHadoopFS("", sketchFile, new Configuration());
        ItemsSketch<ByteArray> expectedSketch0 = ItemsSketch.getInstance(1024, Comparator.naturalOrder());
        getRecords2DimByteArrayKey().stream()
                .map(r -> ByteArray.wrap((byte[]) r.get("key1")))
                .filter(ba -> ba.compareTo(ByteArray.wrap(new byte[]{10})) < 0)
                .forEach(expectedSketch0::update);
        assertThat(readSketches.getQuantilesSketch("key1").getMinValue()).isEqualTo(expectedSketch0.getMinValue());
        assertThat(readSketches.getQuantilesSketch("key1").getMaxValue()).isEqualTo(expectedSketch0.getMaxValue());
        for (double d = 0.0D; d < 1.0D; d += 0.1D) {
            assertThat(readSketches.getQuantilesSketch("key1").getQuantile(d)).isEqualTo(expectedSketch0.getQuantile(d));
        }
        sketchFile = activeFilesSortedByNumberOfLines.get(1).getFilename().replace(".parquet", ".sketches");
        assertThat(Files.exists(new File(sketchFile).toPath(), LinkOption.NOFOLLOW_LINKS)).isTrue();
        readSketches = new SketchesSerDeToS3(schema).loadFromHadoopFS("", sketchFile, new Configuration());
        ItemsSketch<ByteArray> expectedSketch1 = ItemsSketch.getInstance(1024, Comparator.naturalOrder());
        getRecords2DimByteArrayKey().stream()
                .map(r -> ByteArray.wrap((byte[]) r.get("key1")))
                .filter(ba -> ba.compareTo(ByteArray.wrap(new byte[]{10})) >= 0)
                .forEach(expectedSketch1::update);
        assertThat(readSketches.getQuantilesSketch("key1").getMinValue()).isEqualTo(expectedSketch1.getMinValue());
        assertThat(readSketches.getQuantilesSketch("key1").getMaxValue()).isEqualTo(expectedSketch1.getMaxValue());
        for (double d = 0.0D; d < 1.0D; d += 0.1D) {
            assertThat(readSketches.getQuantilesSketch("key1").getQuantile(d)).isEqualTo(expectedSketch1.getQuantile(d));
        }
    }

    @Test
    public void shouldWriteRecordsSplitByPartition2DimensionalDifferentTypeKeysWhenSplitOnDim1() throws StateStoreException, IOException, InterruptedException, IteratorException, ObjectFactoryException {
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
        Partition rootPartition = new Partition();
        rootPartition.setRowKeyTypes(new IntType(), new LongType());
        rootPartition.setId("root");
        Range rootRange1 = new RangeFactory(schema).createRange(field1, Integer.MIN_VALUE, null);
        Range rootRange2 = new RangeFactory(schema).createRange(field2, Long.MIN_VALUE, null);
        Region rootRegion = new Region(Arrays.asList(rootRange1, rootRange2));
        rootPartition.setRegion(rootRegion);
        rootPartition.setLeafPartition(false);
        rootPartition.setParentPartitionId(null);
        rootPartition.setDimension(1);
        Partition partition1 = new Partition();
        partition1.setRowKeyTypes(new IntType(), new LongType());
        partition1.setId("partition1");
        Range partition1Range1 = new RangeFactory(schema).createRange(field1, Integer.MIN_VALUE, null);
        Range partition1Range2 = new RangeFactory(schema).createRange(field2, Long.MIN_VALUE, 10L);
        Region region1 = new Region(Arrays.asList(partition1Range1, partition1Range2));
        partition1.setRegion(region1);
        partition1.setLeafPartition(true);
        partition1.setParentPartitionId("root");
        partition1.setChildPartitionIds(new ArrayList<>());
        partition1.setDimension(-1);
        Partition partition2 = new Partition();
        partition2.setRowKeyTypes(new IntType(), new LongType());
        partition2.setId("partition2");
        Range partition2Range1 = new RangeFactory(schema).createRange(field1, Integer.MIN_VALUE, null);
        Range partition2Range2 = new RangeFactory(schema).createRange(field2, 10L, null);
        Region region2 = new Region(Arrays.asList(partition2Range1, partition2Range2));
        partition2.setRegion(region2);
        partition2.setLeafPartition(true);
        partition2.setParentPartitionId("root");
        partition2.setChildPartitionIds(new ArrayList<>());
        partition2.setDimension(-1);
        rootPartition.setChildPartitionIds(Arrays.asList(partition1.getId(), partition2.getId()));
        DynamoDBStateStore stateStore = getStateStore(schema,
                Arrays.asList(rootPartition, partition1, partition2));

        // When
        IngestRecords ingestRecords = new IngestRecords(
                new ObjectFactory(new InstanceProperties(), null, ""),
                folder.newFolder().getAbsolutePath(),
                10L,
                1000L,
                ParquetWriter.DEFAULT_BLOCK_SIZE,
                ParquetWriter.DEFAULT_PAGE_SIZE,
                "snappy",
                stateStore,
                schema,
                "",
                folder.newFolder().getAbsolutePath(),
                null,
                null,
                120);
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
        ParquetReader<Record> reader = new ParquetRecordReader.Builder(
                new Path(fileInfo1.getFilename()), schema).build();
        List<Record> readRecords = new ArrayList<>();
        CloneRecord cloneRecord = new CloneRecord(schema);
        Record record = reader.read();
        while (null != record) {
            readRecords.add(cloneRecord.clone(record));
            record = reader.read();
        }
        reader.close();
        assertThat(readRecords).hasSize(2);
        assertThat(readRecords.get(0)).isEqualTo(getRecordsOscillatingBetween2Partitions().get(0));
        assertThat(readRecords.get(1)).isEqualTo(getRecordsOscillatingBetween2Partitions().get(2));
        reader = new ParquetRecordReader.Builder(
                new Path(fileInfo2.getFilename()), schema).build();
        readRecords.clear();
        record = reader.read();
        while (null != record) {
            readRecords.add(cloneRecord.clone(record));
            record = reader.read();
        }
        reader.close();
        assertThat(readRecords).hasSize(2);
        assertThat(readRecords.get(0)).isEqualTo(getRecordsOscillatingBetween2Partitions().get(1));
        assertThat(readRecords.get(1)).isEqualTo(getRecordsOscillatingBetween2Partitions().get(3));
        //  - Check quantiles sketches have been written and are correct (NB the sketches are stochastic so may not be identical)
        String sketchFile = fileInfo1.getFilename().replace(".parquet", ".sketches");
        assertThat(Files.exists(new File(sketchFile).toPath(), LinkOption.NOFOLLOW_LINKS)).isTrue();
        Sketches readSketches = new SketchesSerDeToS3(schema).loadFromHadoopFS("", sketchFile, new Configuration());
        ItemsSketch<Integer> expectedSketch0 = ItemsSketch.getInstance(1024, Comparator.naturalOrder());
        getRecordsOscillatingBetween2Partitions().stream()
                .filter(r -> ((long) r.get("key2")) < 10L)
                .map(r -> (int) r.get("key1"))
                .forEach(expectedSketch0::update);
        assertThat(readSketches.getQuantilesSketch("key1").getMinValue()).isEqualTo(expectedSketch0.getMinValue());
        assertThat(readSketches.getQuantilesSketch("key1").getMaxValue()).isEqualTo(expectedSketch0.getMaxValue());
        for (double d = 0.0D; d < 1.0D; d += 0.1D) {
            assertThat(readSketches.getQuantilesSketch("key1").getQuantile(d)).isEqualTo(expectedSketch0.getQuantile(d));
        }
        sketchFile = fileInfo2.getFilename().replace(".parquet", ".sketches");
        assertThat(Files.exists(new File(sketchFile).toPath(), LinkOption.NOFOLLOW_LINKS)).isTrue();
        readSketches = new SketchesSerDeToS3(schema).loadFromHadoopFS("", sketchFile, new Configuration());
        ItemsSketch<Integer> expectedSketch1 = ItemsSketch.getInstance(1024, Comparator.naturalOrder());
        getRecordsOscillatingBetween2Partitions().stream()
                .filter(r -> ((long) r.get("key2")) >= 10L)
                .map(r -> (int) r.get("key1"))
                .forEach(expectedSketch1::update);
        assertThat(readSketches.getQuantilesSketch("key1").getMinValue()).isEqualTo(expectedSketch1.getMinValue());
        assertThat(readSketches.getQuantilesSketch("key1").getMaxValue()).isEqualTo(expectedSketch1.getMaxValue());
        for (double d = 0.0D; d < 1.0D; d += 0.1D) {
            assertThat(readSketches.getQuantilesSketch("key1").getQuantile(d)).isEqualTo(expectedSketch1.getQuantile(d));
        }
    }

    @Test
    public void shouldWriteRecordsSplitByPartitionWhenThereIsOnlyDataInOnePartition() throws StateStoreException, IOException, InterruptedException, IteratorException, ObjectFactoryException {
        // Given
        Partition rootPartition = new Partition();
        rootPartition.setRowKeyTypes(new LongType());
        rootPartition.setId("root");
        Range rootRange = new RangeFactory(schema).createRange(field, Long.MIN_VALUE, null);
        Region rootRegion = new Region(rootRange);
        rootPartition.setRegion(rootRegion);
        rootPartition.setLeafPartition(false);
        rootPartition.setParentPartitionId(null);
        Partition partition1 = new Partition();
        partition1.setRowKeyTypes(new LongType());
        partition1.setId("partition1");
        Range range1 = new RangeFactory(schema).createRange(field, Long.MIN_VALUE, 2L);
        Region region1 = new Region(range1);
        partition1.setRegion(region1);
        partition1.setLeafPartition(true);
        partition1.setParentPartitionId(rootPartition.getId());
        partition1.setChildPartitionIds(new ArrayList<>());
        Partition partition2 = new Partition();
        partition2.setRowKeyTypes(new LongType());
        partition2.setId("partition2");
        Range range2 = new RangeFactory(schema).createRange(field, 2L, null);
        Region region2 = new Region(range2);
        partition2.setRegion(region2);
        partition2.setLeafPartition(true);
        partition2.setParentPartitionId(rootPartition.getId());
        partition2.setChildPartitionIds(new ArrayList<>());
        rootPartition.setChildPartitionIds(Arrays.asList(partition1.getId(), partition2.getId()));
        DynamoDBStateStore stateStore = getStateStore(schema,
                Arrays.asList(rootPartition, partition1, partition2));

        // When
        IngestRecords ingestRecords = new IngestRecords(
                new ObjectFactory(new InstanceProperties(), null, ""),
                folder.newFolder().getAbsolutePath(),
                10L,
                1000L,
                ParquetWriter.DEFAULT_BLOCK_SIZE,
                ParquetWriter.DEFAULT_PAGE_SIZE,
                "zstd",
                stateStore,
                schema,
                "",
                folder.newFolder().getAbsolutePath(),
                null,
                null,
                120);
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
        ParquetReader<Record> reader = new ParquetRecordReader.Builder(new Path(activeFiles.get(0).getFilename()), schema).build();
        List<Record> readRecords = new ArrayList<>();
        CloneRecord cloneRecord = new CloneRecord(schema);
        Record record = reader.read();
        while (null != record) {
            readRecords.add(cloneRecord.clone(record));
            record = reader.read();
        }
        reader.close();
        assertThat(readRecords).hasSize(2);
        assertThat(readRecords.get(0)).isEqualTo(getRecordsInFirstPartitionOnly().get(1));
        assertThat(readRecords.get(1)).isEqualTo(getRecordsInFirstPartitionOnly().get(0));
        //  - Check quantiles sketches have been written and are correct (NB the sketches are stochastic so may not be identical)
        String sketchFile = fileInfo.getFilename().replace(".parquet", ".sketches");
        assertThat(Files.exists(new File(sketchFile).toPath(), LinkOption.NOFOLLOW_LINKS)).isTrue();
        Sketches readSketches = new SketchesSerDeToS3(schema).loadFromHadoopFS("", sketchFile, new Configuration());
        ItemsSketch<Long> expectedSketch0 = ItemsSketch.getInstance(1024, Comparator.naturalOrder());
        getRecordsInFirstPartitionOnly().stream()
                .filter(r -> ((long) r.get("key")) < 2L)
                .forEach(r -> expectedSketch0.update((Long) r.get("key")));
        assertThat(readSketches.getQuantilesSketch("key").getMinValue()).isEqualTo(expectedSketch0.getMinValue());
        assertThat(readSketches.getQuantilesSketch("key").getMaxValue()).isEqualTo(expectedSketch0.getMaxValue());
        for (double d = 0.0D; d < 1.0D; d += 0.1D) {
            assertThat(readSketches.getQuantilesSketch("key").getQuantile(d)).isEqualTo(expectedSketch0.getQuantile(d));
        }
    }

    @Test
    public void shouldWriteDuplicateRecords() throws StateStoreException, IOException, InterruptedException, IteratorException, ObjectFactoryException {
        // Given
        DynamoDBStateStore stateStore = getStateStore(schema);

        // When
        List<Record> records = new ArrayList<>(getRecords());
        records.addAll(getRecords());
        IngestRecords ingestRecords = new IngestRecords(
                new ObjectFactory(new InstanceProperties(), null, ""),
                folder.newFolder().getAbsolutePath(),
                10L,
                1000L,
                ParquetWriter.DEFAULT_BLOCK_SIZE,
                ParquetWriter.DEFAULT_PAGE_SIZE,
                "zstd",
                stateStore,
                schema,
                "",
                folder.newFolder().getAbsolutePath(),
                null,
                null,
                120);
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
        ParquetReader<Record> reader = new ParquetRecordReader.Builder(new Path(fileInfo.getFilename()), schema).build();
        List<Record> readRecords = new ArrayList<>();
        CloneRecord cloneRecord = new CloneRecord(schema);
        Record record = reader.read();
        while (null != record) {
            readRecords.add(cloneRecord.clone(record));
            record = reader.read();
        }
        reader.close();
        assertThat(readRecords).hasSize(4);
        assertThat(readRecords.get(0)).isEqualTo(getRecords().get(0));
        assertThat(readRecords.get(1)).isEqualTo(getRecords().get(0));
        assertThat(readRecords.get(2)).isEqualTo(getRecords().get(1));
        assertThat(readRecords.get(3)).isEqualTo(getRecords().get(1));
        //  - Check quantiles sketches have been written and are correct (NB the sketches are stochastic so may not be identical)
        String sketchFile = fileInfo.getFilename().replace(".parquet", ".sketches");
        assertThat(Files.exists(new File(sketchFile).toPath(), LinkOption.NOFOLLOW_LINKS)).isTrue();
        Sketches readSketches = new SketchesSerDeToS3(schema).loadFromHadoopFS("", sketchFile, new Configuration());
        ItemsSketch<Long> expectedSketch0 = ItemsSketch.getInstance(1024, Comparator.naturalOrder());
        records.forEach(r -> expectedSketch0.update((Long) r.get("key")));
        assertThat(readSketches.getQuantilesSketch("key").getMinValue()).isEqualTo(expectedSketch0.getMinValue());
        assertThat(readSketches.getQuantilesSketch("key").getMaxValue()).isEqualTo(expectedSketch0.getMaxValue());
        for (double d = 0.0D; d < 1.0D; d += 0.1D) {
            assertThat(readSketches.getQuantilesSketch("key").getQuantile(d)).isEqualTo(expectedSketch0.getQuantile(d));
        }
    }

    @Test
    public void shouldWriteRecordsWhenThereAreMoreRecordsInAPartitionThanCanFitInMemory() throws StateStoreException, IOException, InterruptedException, IteratorException, ObjectFactoryException {
        // Given
        Partition rootPartition = new Partition();
        rootPartition.setRowKeyTypes(new LongType());
        rootPartition.setId("root");
        Range rootRange = new RangeFactory(schema).createRange(field, Long.MIN_VALUE, null);
        Region rootRegion = new Region(rootRange);
        rootPartition.setRegion(rootRegion);
        rootPartition.setLeafPartition(false);
        rootPartition.setParentPartitionId(null);
        Partition partition1 = new Partition();
        partition1.setRowKeyTypes(new LongType());
        partition1.setId("partition1");
        Range range1 = new RangeFactory(schema).createRange(field, Long.MIN_VALUE, 2L);
        Region region1 = new Region(range1);
        partition1.setRegion(region1);
        partition1.setLeafPartition(true);
        partition1.setParentPartitionId(rootPartition.getId());
        partition1.setChildPartitionIds(new ArrayList<>());
        Partition partition2 = new Partition();
        partition2.setRowKeyTypes(new LongType());
        partition2.setId("partition2");
        Range range2 = new RangeFactory(schema).createRange(field, 2L, null);
        Region region2 = new Region(range2);
        partition2.setRegion(region2);
        partition2.setLeafPartition(true);
        partition2.setParentPartitionId(rootPartition.getId());
        partition2.setChildPartitionIds(new ArrayList<>());
        rootPartition.setChildPartitionIds(Arrays.asList(partition1.getId(), partition2.getId()));
        DynamoDBStateStore stateStore = getStateStore(schema,
                Arrays.asList(rootPartition, partition1, partition2));
        List<Record> records = getLotsOfRecords();

        // When
        IngestRecords ingestRecords = new IngestRecords(
                new ObjectFactory(new InstanceProperties(), null, ""),
                folder.newFolder().getAbsolutePath(),
                1000L,
                5L,
                ParquetWriter.DEFAULT_BLOCK_SIZE,
                ParquetWriter.DEFAULT_PAGE_SIZE,
                "zstd",
                stateStore,
                schema,
                "",
                folder.newFolder().getAbsolutePath(),
                null,
                null,
                120);
        ingestRecords.init();
        for (Record record : records) {
            ingestRecords.write(record);
        }
        long numWritten = ingestRecords.close();

        // Then:
        //  - Check the correct number of records were written
        assertThat(numWritten).isEqualTo(records.size());
        //  - Check StateStore has correct information
        List<FileInfo> activeFiles = stateStore.getActiveFiles();
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
        ParquetReader<Record> reader = new ParquetRecordReader.Builder(new Path(activeFiles.get(0).getFilename()), schema).build();
        List<Record> readRecords = new ArrayList<>();
        CloneRecord cloneRecord = new CloneRecord(schema);
        Record record = reader.read();
        while (null != record) {
            readRecords.add(cloneRecord.clone(record));
            record = reader.read();
        }
        reader.close();
        assertThat(readRecords.size()).isEqualTo(recordsInLeftFile);

        List<Record> expectedRecords = records.stream()
                .filter(r -> ((long) r.get("key")) < 2L)
                .sorted(Comparator.comparing(r -> ((Long) r.get("key"))))
                .collect(Collectors.toList());
        assertThat(readRecords).isEqualTo(expectedRecords);
        reader = new ParquetRecordReader.Builder(new Path(activeFiles.get(1).getFilename()), schema).build();
        readRecords.clear();
        record = reader.read();
        while (null != record) {
            readRecords.add(cloneRecord.clone(record));
            record = reader.read();
        }
        reader.close();
        assertThat(readRecords.size()).isEqualTo(recordsInRightFile);

        expectedRecords = records.stream()
                .filter(r -> ((long) r.get("key")) >= 2L)
                .sorted(Comparator.comparing(r -> ((Long) r.get("key"))))
                .collect(Collectors.toList());
        assertThat(readRecords).isEqualTo(expectedRecords);

        //  - Check quantiles sketches have been written and are correct (NB the sketches are stochastic so may not be identical)
        String sketchFile = activeFiles.get(0).getFilename().replace(".parquet", ".sketches");
        assertThat(Files.exists(new File(sketchFile).toPath(), LinkOption.NOFOLLOW_LINKS)).isTrue();
        Sketches readSketches = new SketchesSerDeToS3(schema).loadFromHadoopFS("", sketchFile, new Configuration());
        ItemsSketch<Long> expectedSketch0 = ItemsSketch.getInstance(1024, Comparator.naturalOrder());
        records.stream()
                .filter(r -> ((long) r.get("key")) < 2L)
                .forEach(r -> expectedSketch0.update((Long) r.get("key")));
        assertThat(readSketches.getQuantilesSketch("key").getMinValue()).isEqualTo(expectedSketch0.getMinValue());
        assertThat(readSketches.getQuantilesSketch("key").getMaxValue()).isEqualTo(expectedSketch0.getMaxValue());
        for (double d = 0.0D; d < 1.0D; d += 0.1D) {
            assertThat(readSketches.getQuantilesSketch("key").getQuantile(d)).isEqualTo(expectedSketch0.getQuantile(d));
        }
        sketchFile = activeFiles.get(1).getFilename().replace(".parquet", ".sketches");
        assertThat(Files.exists(new File(sketchFile).toPath(), LinkOption.NOFOLLOW_LINKS)).isTrue();
        readSketches = new SketchesSerDeToS3(schema).loadFromHadoopFS("", sketchFile, new Configuration());
        ItemsSketch<Long> expectedSketch1 = ItemsSketch.getInstance(1024, Comparator.naturalOrder());
        records.stream()
                .filter(r -> ((long) r.get("key")) >= 2L)
                .forEach(r -> expectedSketch1.update((Long) r.get("key")));
        assertThat(readSketches.getQuantilesSketch("key").getMinValue()).isEqualTo(expectedSketch1.getMinValue());
        assertThat(readSketches.getQuantilesSketch("key").getMaxValue()).isEqualTo(expectedSketch1.getMaxValue());
        for (double d = 0.0D; d < 1.0D; d += 0.1D) {
            assertThat(readSketches.getQuantilesSketch("key").getQuantile(d)).isEqualTo(expectedSketch1.getQuantile(d));
        }
    }

    @Test
    public void shouldWriteRecordsWhenThereAreMoreRecordsThanCanFitInLocalFile() throws StateStoreException, IOException, InterruptedException, IteratorException, ObjectFactoryException {
        // Given
        Partition rootPartition = new Partition();
        rootPartition.setRowKeyTypes(new LongType());
        rootPartition.setId("root");
        Range rootRange = new RangeFactory(schema).createRange(field, Long.MIN_VALUE, null);
        Region rootRegion = new Region(rootRange);
        rootPartition.setRegion(rootRegion);
        rootPartition.setLeafPartition(false);
        rootPartition.setParentPartitionId(null);
        Partition partition1 = new Partition();
        partition1.setRowKeyTypes(new LongType());
        partition1.setId("partition1");
        Range range1 = new RangeFactory(schema).createRange(field, Long.MIN_VALUE, 2L);
        Region region1 = new Region(range1);
        partition1.setRegion(region1);
        partition1.setLeafPartition(true);
        partition1.setParentPartitionId(rootPartition.getId());
        partition1.setChildPartitionIds(new ArrayList<>());
        Partition partition2 = new Partition();
        partition2.setRowKeyTypes(new LongType());
        partition2.setId("partition2");
        Range range2 = new RangeFactory(schema).createRange(field, 2L, null);
        Region region2 = new Region(range2);
        partition2.setRegion(region2);
        partition2.setLeafPartition(true);
        partition2.setParentPartitionId(rootPartition.getId());
        partition2.setChildPartitionIds(new ArrayList<>());
        rootPartition.setChildPartitionIds(Arrays.asList(partition1.getId(), partition2.getId()));
        DynamoDBStateStore stateStore = getStateStore(schema,
                Arrays.asList(rootPartition, partition1, partition2));
        List<Record> records = getLotsOfRecords();

        // When
        IngestRecords ingestRecords = new IngestRecords(
                new ObjectFactory(new InstanceProperties(), null, ""),
                folder.newFolder().getAbsolutePath(),
                10L,
                5L,
                ParquetWriter.DEFAULT_BLOCK_SIZE,
                ParquetWriter.DEFAULT_PAGE_SIZE,
                "zstd",
                stateStore,
                schema,
                "",
                folder.newFolder().getAbsolutePath(),
                null,
                null,
                120);
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
    public void shouldSortRecords() throws StateStoreException, IOException, InterruptedException, IteratorException, ObjectFactoryException {
        // Given
        DynamoDBStateStore stateStore = getStateStore(schema);

        // When
        IngestRecords ingestRecords = new IngestRecords(
                new ObjectFactory(new InstanceProperties(), null, ""),
                folder.newFolder().getAbsolutePath(),
                10L,
                1000L,
                ParquetWriter.DEFAULT_BLOCK_SIZE,
                ParquetWriter.DEFAULT_PAGE_SIZE,
                "zstd",
                stateStore,
                schema,
                "",
                folder.newFolder().getAbsolutePath(),
                null,
                null,
                120);
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
        ParquetReader<Record> reader = new ParquetRecordReader.Builder(new Path(fileInfo.getFilename()), schema).build();
        List<Record> readRecords = new ArrayList<>();
        CloneRecord cloneRecord = new CloneRecord(schema);
        Record record = reader.read();
        while (null != record) {
            readRecords.add(cloneRecord.clone(record));
            record = reader.read();
        }
        reader.close();
        assertThat(readRecords.size()).isEqualTo(20L);
        List<Record> sortedRecords = new ArrayList<>(getUnsortedRecords());
        sortedRecords.sort(Comparator.comparing(o -> ((Long) o.get("key"))));
        int i = 0;
        for (Record record1 : sortedRecords) {
            assertThat(readRecords.get(i)).isEqualTo(record1);
            i++;
        }
        //  - Check quantiles sketches have been written and are correct (NB the sketches are stochastic so may not be identical)
        String sketchFile = activeFiles.get(0).getFilename().replace(".parquet", ".sketches");
        assertThat(Files.exists(new File(sketchFile).toPath(), LinkOption.NOFOLLOW_LINKS)).isTrue();
        Sketches readSketches = new SketchesSerDeToS3(schema).loadFromHadoopFS("", sketchFile, new Configuration());
        ItemsSketch<Long> expectedSketch = ItemsSketch.getInstance(1024, Comparator.naturalOrder());
        getUnsortedRecords().forEach(r -> expectedSketch.update((Long) r.get("key")));
        assertThat(readSketches.getQuantilesSketch("key").getMinValue()).isEqualTo(expectedSketch.getMinValue());
        assertThat(readSketches.getQuantilesSketch("key").getMaxValue()).isEqualTo(expectedSketch.getMaxValue());
        for (double d = 0.0D; d < 1.0D; d += 0.1D) {
            assertThat(readSketches.getQuantilesSketch("key").getQuantile(d)).isEqualTo(expectedSketch.getQuantile(d));
        }
    }

    @Test
    public void shouldWriteNoRecordsSuccessfully() throws StateStoreException, IOException, InterruptedException, IteratorException, ObjectFactoryException {
        // Given
        DynamoDBStateStore stateStore = getStateStore(schema);

        // When
        IngestRecords ingestRecords = new IngestRecords(
                new ObjectFactory(new InstanceProperties(), null, ""),
                folder.newFolder().getAbsolutePath(),
                10L,
                1000L,
                ParquetWriter.DEFAULT_BLOCK_SIZE,
                ParquetWriter.DEFAULT_PAGE_SIZE,
                "zstd",
                stateStore,
                schema,
                "",
                folder.newFolder().getAbsolutePath(),
                null,
                null,
                120);
        ingestRecords.init();
        long numWritten = ingestRecords.close();

        // Then:
        //  - Check the correct number of records were written
        assertThat(numWritten).isZero();
        //  - Check StateStore has correct information
        List<FileInfo> activeFiles = stateStore.getActiveFiles();
        assertThat(activeFiles).isEmpty();
    }

    @Test
    public void shouldApplyIterator() throws StateStoreException, IOException, InterruptedException, IteratorException, ObjectFactoryException {
        // Given
        Schema schema = Schema.builder()
                .rowKeyFields(new Field("key", new ByteArrayType()))
                .sortKeyFields(new Field("sort", new LongType()))
                .valueFields(new Field("value", new LongType()))
                .build();
        DynamoDBStateStore stateStore = getStateStore(schema);

        // When
        IngestRecords ingestRecords = new IngestRecords(
                new ObjectFactory(new InstanceProperties(), null, ""),
                folder.newFolder().getAbsolutePath(),
                10L,
                1000L,
                ParquetWriter.DEFAULT_BLOCK_SIZE,
                ParquetWriter.DEFAULT_PAGE_SIZE,
                "zstd",
                stateStore,
                schema,
                "",
                folder.newFolder().getAbsolutePath(),
                AdditionIterator.class.getName(),
                "",
                120);
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
        ParquetReader<Record> reader = new ParquetRecordReader.Builder(new Path(fileInfo.getFilename()), schema).build();
        List<Record> readRecords = new ArrayList<>();
        CloneRecord cloneRecord = new CloneRecord(schema);
        Record record = reader.read();
        while (null != record) {
            readRecords.add(cloneRecord.clone(record));
            record = reader.read();
        }
        reader.close();
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
        String sketchFile = activeFiles.get(0).getFilename().replace(".parquet", ".sketches");
        assertThat(Files.exists(new File(sketchFile).toPath(), LinkOption.NOFOLLOW_LINKS)).isTrue();
        Sketches readSketches = new SketchesSerDeToS3(schema).loadFromHadoopFS("", sketchFile, new Configuration());
        ItemsSketch<ByteArray> expectedSketch = ItemsSketch.getInstance(1024, Comparator.naturalOrder());
        AdditionIterator additionIterator = new AdditionIterator();
        additionIterator.init("", schema);
        List<Record> sortedRecords = new ArrayList<>(getRecordsForAggregationIteratorTest());
        sortedRecords.sort(Comparator.comparing(o -> ByteArray.wrap(((byte[]) o.get("key")))));
        CloseableIterator<Record> aggregatedRecords = additionIterator.apply(new WrappedIterator<>(sortedRecords.iterator()));
        while (aggregatedRecords.hasNext()) {
            expectedSketch.update(ByteArray.wrap((byte[]) aggregatedRecords.next().get("key")));
        }
        assertThat(readSketches.getQuantilesSketch("key").getMinValue()).isEqualTo(expectedSketch.getMinValue());
        assertThat(readSketches.getQuantilesSketch("key").getMaxValue()).isEqualTo(expectedSketch.getMaxValue());
        for (double d = 0.0D; d < 1.0D; d += 0.1D) {
            assertThat(readSketches.getQuantilesSketch("key").getQuantile(d)).isEqualTo(expectedSketch.getQuantile(d));
        }
    }
}
