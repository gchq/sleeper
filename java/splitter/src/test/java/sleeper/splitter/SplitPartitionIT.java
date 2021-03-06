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
package sleeper.splitter;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.facebook.collections.ByteArray;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.ParquetWriter;
import org.junit.*;
import org.junit.rules.TemporaryFolder;
import org.testcontainers.containers.GenericContainer;
import sleeper.configuration.jars.ObjectFactory;
import sleeper.configuration.jars.ObjectFactoryException;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.core.CommonTestConstants;
import sleeper.core.iterator.IteratorException;
import sleeper.core.partition.Partition;
import sleeper.core.partition.PartitionsFromSplitPoints;
import sleeper.core.range.Range;
import sleeper.core.range.Range.RangeFactory;
import sleeper.core.range.Region;
import sleeper.core.record.Record;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.ByteArrayType;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.StringType;
import sleeper.ingest.IngestRecordsFromIterator;
import sleeper.statestore.FileInfo;
import sleeper.statestore.StateStore;
import sleeper.statestore.StateStoreException;
import sleeper.statestore.dynamodb.DynamoDBStateStoreCreator;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

public class SplitPartitionIT {
    private static final int DYNAMO_PORT = 8000;
    @ClassRule
    public static GenericContainer dynamoDb = new GenericContainer(CommonTestConstants.DYNAMODB_LOCAL_CONTAINER)
            .withExposedPorts(DYNAMO_PORT);
    private static AmazonDynamoDB dynamoDBClient;
    @Rule
    public TemporaryFolder folder = new TemporaryFolder(CommonTestConstants.TMP_DIRECTORY);

    @BeforeClass
    public static void initDynamoClient() {
        AwsClientBuilder.EndpointConfiguration endpointConfiguration =
                new AwsClientBuilder.EndpointConfiguration("http://" + dynamoDb.getContainerIpAddress() + ":"
                        + dynamoDb.getMappedPort(DYNAMO_PORT), "us-west-2");
        dynamoDBClient = AmazonDynamoDBClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials("12345", "6789")))
                .withEndpointConfiguration(endpointConfiguration)
                .build();
    }

    @AfterClass
    public static void shutdownDynamoClient() {
        dynamoDBClient.shutdown();
    }

    private static StateStore getStateStore(Schema schema) throws StateStoreException {
        return getStateStore(schema, new PartitionsFromSplitPoints(schema, Collections.emptyList()).construct());
    }

    private static StateStore getStateStore(Schema schema, List<Partition> partitions) throws StateStoreException {
        String id = UUID.randomUUID().toString();
        DynamoDBStateStoreCreator dynamoDBStateStoreCreator = new DynamoDBStateStoreCreator(id, schema, dynamoDBClient);
        StateStore dynamoStateStore = dynamoDBStateStoreCreator.create();
        dynamoStateStore.initialise(partitions);
        return dynamoStateStore;
    }

    @Test
    public void shouldSplitPartitionForIntKeyCorrectly()
            throws StateStoreException, IOException, IteratorException, InterruptedException, ObjectFactoryException {
        // Given
        Schema schema = new Schema();
        schema.setRowKeyFields(new Field("key", new IntType()));
        StateStore stateStore = getStateStore(schema);
        String path = folder.newFolder().getAbsolutePath();
        String path2 = folder.newFolder().getAbsolutePath();
        Partition rootPartition = stateStore.getAllPartitions().get(0);
        for (int i = 0; i < 10; i++) {
            List<Record> records = new ArrayList<>();
            for (int r = 100 * i; r < 100 * (i + 1); r++) {
                Record record = new Record();
                record.put("key", r);
                records.add(record);
            }
            IngestRecordsFromIterator ingestRecordsFromIterator = new IngestRecordsFromIterator(new ObjectFactory(new InstanceProperties(), null, ""),
                    records.iterator(),
                    path,
                    1_000_000L,
                    1_000_000L,
                    ParquetWriter.DEFAULT_BLOCK_SIZE,
                    ParquetWriter.DEFAULT_PAGE_SIZE,
                    "zstd",
                    stateStore,
                    schema,
                    "",
                    path2,
                    null,
                    null,
                    1_000_000
            );
            ingestRecordsFromIterator.write();
        }
        SplitPartition partitionSplitter = new SplitPartition(stateStore, schema, new Configuration());

        // When
        partitionSplitter.splitPartition(rootPartition, stateStore.getActiveFiles().stream().map(FileInfo::getFilename).collect(Collectors.toList()));

        // Then
        List<Partition> partitions = stateStore.getAllPartitions();
        assertEquals(3, partitions.size());
        List<Partition> nonLeafPartitions = partitions.stream()
                .filter(p -> !p.isLeafPartition())
                .collect(Collectors.toList());
        assertEquals(1, nonLeafPartitions.size());
        Set<Partition> leafPartitions = partitions.stream()
                .filter(Partition::isLeafPartition)
                .collect(Collectors.toSet());
        Iterator<Partition> it = leafPartitions.iterator();
        Partition leafPartition1 = it.next();
        Partition leafPartition2 = it.next();
        int splitPoint;
        int minRowkey1 = (int) leafPartition1.getRegion().getRange("key").getMin();
        int minRowkey2 = (int) leafPartition2.getRegion().getRange("key").getMin();
        Integer maxRowKey1 = (Integer) leafPartition1.getRegion().getRange("key").getMax();
        Integer maxRowKey2 = (Integer) leafPartition2.getRegion().getRange("key").getMax();
        if (minRowkey1 < minRowkey2) {
            splitPoint = maxRowKey1;
            assertEquals(minRowkey1, -2147483648);
            assertEquals((int) maxRowKey1, minRowkey2);
            assertNull(maxRowKey2);
        } else {
            splitPoint = maxRowKey2;
            assertEquals(minRowkey2, -2147483648);
            assertEquals((int) maxRowKey2, minRowkey1);
            assertNull(maxRowKey1);
        }
        assertTrue(400 < splitPoint && splitPoint < 600);
        leafPartitions.forEach(p -> assertEquals(p.getParentPartitionId(), rootPartition.getId()));
        leafPartitions.forEach(p -> assertEquals(p.getChildPartitionIds(), new ArrayList<>()));
        assertEquals(rootPartition, nonLeafPartitions.get(0));
    }

    @Test
    public void shouldNotSplitPartitionForIntKeyIfItCannotBeSplitBecausePartitionIsOnePoint()
            throws StateStoreException, IOException, IteratorException, InterruptedException, ObjectFactoryException {
        // Given
        Schema schema = new Schema();
        Field field = new Field("key", new IntType());
        schema.setRowKeyFields(field);
        // Non-leaf partitions
        Range rootRange = new RangeFactory(schema).createRange(field, Integer.MIN_VALUE, null);
        Partition rootPartition = new Partition(
                schema.getRowKeyTypes(),
                new Region(rootRange),
                "root",
                false,
                null,
                null,
                0);
        Range range12 = new RangeFactory(schema).createRange(field, Integer.MIN_VALUE, 1);
        Partition partition12 = new Partition(
                schema.getRowKeyTypes(),
                new Region(range12),
                "id12",
                false,
                rootPartition.getId(),
                null,
                0);
        // Leaf partitions
        Range range1 = new RangeFactory(schema).createRange(field, Integer.MIN_VALUE, 0);
        Partition partition1 = new Partition(schema.getRowKeyTypes(),
                new Region(range1),
                "id1",
                true,
                partition12.getId(),
                null,
                -1);
        //  - Partition 2 only includes the key 0 (partitions do not include
        //      the maximum key), and so cannot be split.
        Range range2 = new RangeFactory(schema).createRange(field, 0, 1);
        Partition partition2 = new Partition(schema.getRowKeyTypes(),
                new Region(range2),
                "id2",
                true,
                partition12.getId(),
                null,
                -1);
        Range range3 = new RangeFactory(schema).createRange(field, 1, null);
        Partition partition3 = new Partition(schema.getRowKeyTypes(),
                new Region(range3),
                "id3",
                true,
                rootPartition.getId(),
                null,
                -1);
        // Wire up partitions
        rootPartition.setChildPartitionIds(Arrays.asList(partition12.getId(), partition3.getId()));
        partition12.setChildPartitionIds(Arrays.asList(partition1.getId(), partition2.getId()));
        //
        List<Partition> partitions = Arrays.asList(rootPartition, partition12, partition1, partition2, partition3);
        StateStore stateStore = getStateStore(schema, partitions);
        String path = folder.newFolder().getAbsolutePath();
        String path2 = folder.newFolder().getAbsolutePath();
        for (Partition partition : partitions) {
            for (int i = 0; i < 10; i++) {
                List<Record> records = new ArrayList<>();
                int j = 0;
                int minRange = (int) partition.getRegion().getRange("key").getMin();
                int maxRange = null == partition.getRegion().getRange("key").getMax() ? Integer.MAX_VALUE : (int) partition.getRegion().getRange("key").getMax();
                for (int r = minRange; r < maxRange && j < 10; r++, j++) {
                    Record record = new Record();
                    record.put("key", r);
                    records.add(record);
                }
                IngestRecordsFromIterator ingestRecordsFromIterator = new IngestRecordsFromIterator(new ObjectFactory(new InstanceProperties(), null, ""),
                        records.iterator(),
                        path,
                        1_000_000L,
                        1_000_000L,
                        ParquetWriter.DEFAULT_BLOCK_SIZE,
                        ParquetWriter.DEFAULT_PAGE_SIZE,
                        "zstd",
                        stateStore,
                        schema,
                        "",
                        path2,
                        null,
                        null,
                        1_000_000
                );
                ingestRecordsFromIterator.write();
            }
        }
        SplitPartition partitionSplitter = new SplitPartition(stateStore, schema, new Configuration());

        // When
        List<String> fileNames = stateStore.getActiveFiles().stream()
                .filter(fi -> fi.getPartitionId().equals(partition2.getId()))
                .map(FileInfo::getFilename)
                .collect(Collectors.toList());
        partitionSplitter.splitPartition(partition2, fileNames);

        // Then
        List<Partition> partitionsAfterSplit = stateStore.getAllPartitions();
        assertEquals(partitions.size(), partitionsAfterSplit.size());
        assertEquals(new HashSet<>(partitions), new HashSet<>(partitionsAfterSplit));
    }

    @Test
    public void shouldNotSplitPartitionForIntKeyIfItCannotBeSplitBecauseDataIsConstant()
            throws StateStoreException, IOException, IteratorException, InterruptedException, ObjectFactoryException {
        // Given
        Schema schema = new Schema();
        Field field = new Field("key", new IntType());
        schema.setRowKeyFields(field);
        // Non-leaf partitions
        Range rootRange = new RangeFactory(schema).createRange(field, Integer.MIN_VALUE, null);
        Partition rootPartition = new Partition(
                schema.getRowKeyTypes(),
                new Region(rootRange),
                "root",
                false,
                null,
                null,
                0);
        Range range12 = new RangeFactory(schema).createRange(field, Integer.MIN_VALUE, 10);
        Partition partition12 = new Partition(
                schema.getRowKeyTypes(),
                new Region(range12),
                "id12",
                false,
                rootPartition.getId(),
                null,
                0);
        // Leaf partitions
        Range range1 = new RangeFactory(schema).createRange(field, Integer.MIN_VALUE, 0);
        Partition partition1 = new Partition(schema.getRowKeyTypes(),
                new Region(range1),
                "id1",
                true,
                partition12.getId(),
                null,
                -1);
        //  - Partition 2 only includes the key 0 (partitions do not include
        //      the maximum key), and so cannot be split.
        Range range2 = new RangeFactory(schema).createRange(field, 0, 10);
        Partition partition2 = new Partition(schema.getRowKeyTypes(),
                new Region(range2),
                "id2",
                true,
                partition12.getId(),
                null,
                -1);
        Range range3 = new RangeFactory(schema).createRange(field, 10, null);
        Partition partition3 = new Partition(schema.getRowKeyTypes(),
                new Region(range3),
                "id3",
                true,
                rootPartition.getId(),
                null,
                -1);
        // Wire up partitions
        rootPartition.setChildPartitionIds(Arrays.asList(partition12.getId(), partition3.getId()));
        partition12.setChildPartitionIds(Arrays.asList(partition1.getId(), partition2.getId()));
        //
        List<Partition> partitions = Arrays.asList(rootPartition, partition12, partition1, partition2, partition3);
        StateStore stateStore = getStateStore(schema, partitions);
        String path = folder.newFolder().getAbsolutePath();
        String path2 = folder.newFolder().getAbsolutePath();
        for (Partition partition : partitions) {
            for (int i = 0; i < 10; i++) {
                List<Record> records = new ArrayList<>();
                int j = 0;
                if (!partition.equals(partition2)) {
                    int minRange = (int) partition.getRegion().getRange("key").getMin();
                    int maxRange = null == partition.getRegion().getRange("key").getMax() ? Integer.MAX_VALUE : (int) partition.getRegion().getRange("key").getMax();
                    for (int r = minRange; r < maxRange && j < 10; r++, j++) {
                        Record record = new Record();
                        record.put("key", r);
                        records.add(record);
                    }
                } else {
                    // Files in partition2 all have the same value for the key
                    for (int r = 0; r < 10; r++) {
                        Record record = new Record();
                        record.put("key", 1);
                        records.add(record);
                    }
                }
                IngestRecordsFromIterator ingestRecordsFromIterator = new IngestRecordsFromIterator(new ObjectFactory(new InstanceProperties(), null, ""),
                        records.iterator(),
                        path,
                        1_000_000L,
                        1_000_000L,
                        ParquetWriter.DEFAULT_BLOCK_SIZE,
                        ParquetWriter.DEFAULT_PAGE_SIZE,
                        "zstd",
                        stateStore,
                        schema,
                        "",
                        path2,
                        null,
                        null,
                        1_000_000
                );
                ingestRecordsFromIterator.write();
            }
        }
        SplitPartition partitionSplitter = new SplitPartition(stateStore, schema, new Configuration());

        // When
        List<String> fileNames = stateStore.getActiveFiles().stream()
                .filter(fi -> fi.getPartitionId().equals(partition2.getId()))
                .map(FileInfo::getFilename)
                .collect(Collectors.toList());
        partitionSplitter.splitPartition(partition2, fileNames);

        // Then
        List<Partition> partitionsAfterSplit = stateStore.getAllPartitions();
        assertEquals(partitions.size(), partitionsAfterSplit.size());
        assertEquals(new HashSet<>(partitions), new HashSet<>(partitionsAfterSplit));
    }

    @Test
    public void shouldSplitPartitionForIntMultidimensionalKeyOnFirstDimensionCorrectly()
            throws StateStoreException, IOException, IteratorException, InterruptedException, ObjectFactoryException {
        // Given
        Schema schema = new Schema();
        schema.setRowKeyFields(new Field("key1", new IntType()), new Field("key2", new IntType()));
        StateStore stateStore = getStateStore(schema);
        String path = folder.newFolder().getAbsolutePath();
        String path2 = folder.newFolder().getAbsolutePath();
        Partition rootPartition = stateStore.getAllPartitions().get(0);
        for (int i = 0; i < 10; i++) {
            List<Record> records = new ArrayList<>();
            for (int r = 0; r < 100; r++) {
                Record record = new Record();
                record.put("key1", r);
                record.put("key2", 10);
                records.add(record);
            }
            IngestRecordsFromIterator ingestRecordsFromIterator = new IngestRecordsFromIterator(new ObjectFactory(new InstanceProperties(), null, ""),
                    records.iterator(),
                    path,
                    1_000_000L,
                    1_000_000L,
                    ParquetWriter.DEFAULT_BLOCK_SIZE,
                    ParquetWriter.DEFAULT_PAGE_SIZE,
                    "zstd",
                    stateStore,
                    schema,
                    "",
                    path2,
                    null,
                    null,
                    1_000_000
            );
            ingestRecordsFromIterator.write();
        }
        SplitPartition partitionSplitter = new SplitPartition(stateStore, schema, new Configuration());

        // When
        List<String> fileNames = stateStore.getActiveFiles().stream()
                .filter(fi -> fi.getPartitionId().equals(rootPartition.getId()))
                .map(FileInfo::getFilename)
                .collect(Collectors.toList());
        partitionSplitter.splitPartition(rootPartition, fileNames);

        // Then
        //  - There should be 3 partitions
        List<Partition> partitions = stateStore.getAllPartitions();
        assertEquals(3, partitions.size());
        //  - There should be 1 non-leaf partition
        List<Partition> nonLeafPartitions = partitions.stream()
                .filter(p -> !p.isLeafPartition())
                .collect(Collectors.toList());
        assertEquals(1, nonLeafPartitions.size());
        //  - The root partition should have been split on the first dimension
        assertEquals(0, nonLeafPartitions.get(0).getDimension());
        //  - The leaf partitions should have been split on a value which is between
        //      0 and 100.
        Set<Partition> leafPartitions = partitions.stream()
                .filter(Partition::isLeafPartition)
                .collect(Collectors.toSet());
        Iterator<Partition> it = leafPartitions.iterator();
        Partition leafPartition1 = it.next();
        Partition leafPartition2 = it.next();
        int splitPoint;
        int minRowkey1 = (int) leafPartition1.getRegion().getRange("key1").getMin();
        int minRowkey2 = (int) leafPartition2.getRegion().getRange("key1").getMin();
        Integer maxRowKey1 = (Integer) leafPartition1.getRegion().getRange("key1").getMax();
        Integer maxRowKey2 = (Integer) leafPartition2.getRegion().getRange("key1").getMax();
        if (Integer.MIN_VALUE == minRowkey1) {
            splitPoint = maxRowKey1;
            assertEquals(maxRowKey1.intValue(), minRowkey2);
            assertNull(maxRowKey2);
        } else {
            splitPoint = maxRowKey2;
            assertEquals(Integer.MIN_VALUE, minRowkey2);
            assertEquals(maxRowKey2.intValue(), minRowkey1);
            assertNull(maxRowKey1);
        }
        assertTrue(Integer.MIN_VALUE < splitPoint && splitPoint < 99);
        //  - The leaf partitions should have the root partition as their parent
        //      and an empty array for the child partitions.
        leafPartitions.forEach(p -> assertEquals(p.getParentPartitionId(), rootPartition.getId()));
        leafPartitions.forEach(p -> assertEquals(p.getChildPartitionIds(), new ArrayList<>()));
        assertEquals(rootPartition, nonLeafPartitions.get(0));
    }

    @Test
    public void shouldSplitPartitionForIntMultidimensionalKeyOnSecondDimensionCorrectlyWhenMinIsMax()
            throws StateStoreException, IOException, IteratorException, InterruptedException, ObjectFactoryException {
        // Given
        Schema schema = new Schema();
        schema.setRowKeyFields(new Field("key1", new IntType()), new Field("key2", new IntType()));
        StateStore stateStore = getStateStore(schema);
        String path = folder.newFolder().getAbsolutePath();
        String path2 = folder.newFolder().getAbsolutePath();
        Partition rootPartition = stateStore.getAllPartitions().get(0);
        for (int i = 0; i < 10; i++) {
            List<Record> records = new ArrayList<>();
            for (int r = 0; r < 100; r++) {
                Record record = new Record();
                record.put("key1", 10);
                record.put("key2", r);
                records.add(record);
            }
            IngestRecordsFromIterator ingestRecordsFromIterator = new IngestRecordsFromIterator(new ObjectFactory(new InstanceProperties(), null, ""),
                    records.iterator(),
                    path,
                    1_000_000L,
                    1_000_000L,
                    ParquetWriter.DEFAULT_BLOCK_SIZE,
                    ParquetWriter.DEFAULT_PAGE_SIZE,
                    "zstd",
                    stateStore,
                    schema,
                    "",
                    path2,
                    null,
                    null,
                    1_000_000
            );
            ingestRecordsFromIterator.write();
        }
        SplitPartition partitionSplitter = new SplitPartition(stateStore, schema, new Configuration());

        // When
        List<String> fileNames = stateStore.getActiveFiles().stream()
                .filter(fi -> fi.getPartitionId().equals(rootPartition.getId()))
                .map(FileInfo::getFilename)
                .collect(Collectors.toList());
        partitionSplitter.splitPartition(rootPartition, fileNames);

        // Then
        //  - There should be 3 partitions
        List<Partition> partitions = stateStore.getAllPartitions();
        assertEquals(3, partitions.size());
        //  - There should be 1 non-leaf partition
        List<Partition> nonLeafPartitions = partitions.stream()
                .filter(p -> !p.isLeafPartition())
                .collect(Collectors.toList());
        assertEquals(1, nonLeafPartitions.size());
        //  - The root partition should have been split on the second dimension
        assertEquals(1, nonLeafPartitions.get(0).getDimension());
        //  - The leaf partitions should have been split on a value which is between
        //      0 and 100.
        Set<Partition> leafPartitions = partitions.stream()
                .filter(Partition::isLeafPartition)
                .collect(Collectors.toSet());
        Iterator<Partition> it = leafPartitions.iterator();
        Partition leafPartition1 = it.next();
        Partition leafPartition2 = it.next();
        int splitPoint;
        int minRowkey1 = (int) leafPartition1.getRegion().getRange("key2").getMin();
        int minRowkey2 = (int) leafPartition2.getRegion().getRange("key2").getMin();
        Integer maxRowKey1 = (Integer) leafPartition1.getRegion().getRange("key2").getMax();
        Integer maxRowKey2 = (Integer) leafPartition2.getRegion().getRange("key2").getMax();
        if (Integer.MIN_VALUE == minRowkey1) {
            splitPoint = maxRowKey1;
            assertEquals(maxRowKey1.intValue(), minRowkey2);
            assertNull(maxRowKey2);
        } else {
            splitPoint = maxRowKey2;
            assertEquals(Integer.MIN_VALUE, minRowkey2);
            assertEquals(maxRowKey2.intValue(), minRowkey1);
            assertNull(maxRowKey1);
        }
        assertTrue(Integer.MIN_VALUE < splitPoint && splitPoint < 99);
        //  - The leaf partitions should have the root partition as their parent
        //      and an empty array for the child partitions.
        leafPartitions.forEach(p -> assertEquals(p.getParentPartitionId(), rootPartition.getId()));
        leafPartitions.forEach(p -> assertEquals(p.getChildPartitionIds(), new ArrayList<>()));
        assertEquals(rootPartition, nonLeafPartitions.get(0));
    }
    
    @Test
    public void shouldSplitPartitionForIntMultidimensionalKeyOnSecondDimensionCorrectlyWhenMinIsMedian()
            throws StateStoreException, IOException, IteratorException, InterruptedException, ObjectFactoryException {
        // Given
        Schema schema = new Schema();
        schema.setRowKeyFields(new Field("key1", new IntType()), new Field("key2", new IntType()));
        StateStore stateStore = getStateStore(schema);
        String path = folder.newFolder().getAbsolutePath();
        String path2 = folder.newFolder().getAbsolutePath();
        Partition rootPartition = stateStore.getAllPartitions().get(0);
        for (int i = 0; i < 10; i++) {
            List<Record> records = new ArrayList<>();
            for (int r = 0; r < 100; r++) {
                Record record = new Record();
                // The majority of the values are 10; so min should equal median
                if (r < 75) {
                    record.put("key1", 10);
                } else {
                    record.put("key1", 20);
                }
                record.put("key2", r);
                records.add(record);
            }
            IngestRecordsFromIterator ingestRecordsFromIterator = new IngestRecordsFromIterator(new ObjectFactory(new InstanceProperties(), null, ""),
                    records.iterator(),
                    path,
                    1_000_000L,
                    1_000_000L,
                    ParquetWriter.DEFAULT_BLOCK_SIZE,
                    ParquetWriter.DEFAULT_PAGE_SIZE,
                    "zstd",
                    stateStore,
                    schema,
                    "",
                    path2,
                    null,
                    null,
                    1_000_000
            );
            ingestRecordsFromIterator.write();
        }
        SplitPartition partitionSplitter = new SplitPartition(stateStore, schema, new Configuration());

        // When
        List<String> fileNames = stateStore.getActiveFiles().stream()
                .filter(fi -> fi.getPartitionId().equals(rootPartition.getId()))
                .map(FileInfo::getFilename)
                .collect(Collectors.toList());
        partitionSplitter.splitPartition(rootPartition, fileNames);

        // Then
        //  - There should be 3 partitions
        List<Partition> partitions = stateStore.getAllPartitions();
        assertEquals(3, partitions.size());
        //  - There should be 1 non-leaf partition
        List<Partition> nonLeafPartitions = partitions.stream()
                .filter(p -> !p.isLeafPartition())
                .collect(Collectors.toList());
        assertEquals(1, nonLeafPartitions.size());
        //  - The root partition should have been split on the second dimension
        assertEquals(1, nonLeafPartitions.get(0).getDimension());
        //  - The leaf partitions should have been split on a value which is between
        //      0 and 100.
        Set<Partition> leafPartitions = partitions.stream()
                .filter(Partition::isLeafPartition)
                .collect(Collectors.toSet());
        Iterator<Partition> it = leafPartitions.iterator();
        Partition leafPartition1 = it.next();
        Partition leafPartition2 = it.next();
        int splitPoint;
        int minRowkey1 = (int) leafPartition1.getRegion().getRange("key2").getMin();
        int minRowkey2 = (int) leafPartition2.getRegion().getRange("key2").getMin();
        Integer maxRowKey1 = (Integer) leafPartition1.getRegion().getRange("key2").getMax();
        Integer maxRowKey2 = (Integer) leafPartition2.getRegion().getRange("key2").getMax();
        if (Integer.MIN_VALUE == minRowkey1) {
            splitPoint = maxRowKey1;
            assertEquals(maxRowKey1.intValue(), minRowkey2);
            assertNull(maxRowKey2);
        } else {
            splitPoint = maxRowKey2;
            assertEquals(Integer.MIN_VALUE, minRowkey2);
            assertEquals(maxRowKey2.intValue(), minRowkey1);
            assertNull(maxRowKey1);
        }
        assertTrue(Integer.MIN_VALUE < splitPoint && splitPoint < 99);
        //  - The leaf partitions should have the root partition as their parent
        //      and an empty array for the child partitions.
        leafPartitions.forEach(p -> assertEquals(p.getParentPartitionId(), rootPartition.getId()));
        leafPartitions.forEach(p -> assertEquals(p.getChildPartitionIds(), new ArrayList<>()));
        assertEquals(rootPartition, nonLeafPartitions.get(0));
    }

    @Test
    public void shouldSplitPartitionForLongKeyCorrectly()
            throws StateStoreException, IOException, IteratorException, InterruptedException, ObjectFactoryException {
        // Given
        Schema schema = new Schema();
        schema.setRowKeyFields(new Field("key", new LongType()));
        StateStore stateStore = getStateStore(schema);
        String path = folder.newFolder().getAbsolutePath();
        String path2 = folder.newFolder().getAbsolutePath();
        Partition rootPartition = stateStore.getAllPartitions().get(0);
        for (int i = 0; i < 10; i++) {
            List<Record> records = new ArrayList<>();
            for (long r = 100L * i; r < 100L * (i + 1); r++) {
                Record record = new Record();
                record.put("key", r);
                records.add(record);
            }
            IngestRecordsFromIterator ingestRecordsFromIterator = new IngestRecordsFromIterator(new ObjectFactory(new InstanceProperties(), null, ""),
                    records.iterator(),
                    path,
                    1_000_000L,
                    1_000_000L,
                    ParquetWriter.DEFAULT_BLOCK_SIZE,
                    ParquetWriter.DEFAULT_PAGE_SIZE,
                    "zstd",
                    stateStore,
                    schema,
                    "",
                    path2,
                    null,
                    null,
                    1_000_000
            );
            ingestRecordsFromIterator.write();
        }
        SplitPartition partitionSplitter = new SplitPartition(stateStore, schema, new Configuration());

        // When
        List<String> fileNames = stateStore.getActiveFiles().stream()
                .filter(fi -> fi.getPartitionId().equals(rootPartition.getId()))
                .map(FileInfo::getFilename)
                .collect(Collectors.toList());
        partitionSplitter.splitPartition(rootPartition, fileNames);

        // Then
        List<Partition> partitions = stateStore.getAllPartitions();
        assertEquals(3, partitions.size());
        List<Partition> nonLeafPartitions = partitions.stream()
                .filter(p -> !p.isLeafPartition())
                .collect(Collectors.toList());
        assertEquals(1, nonLeafPartitions.size());
        Set<Partition> leafPartitions = partitions.stream()
                .filter(Partition::isLeafPartition)
                .collect(Collectors.toSet());
        Iterator<Partition> it = leafPartitions.iterator();
        Partition leafPartition1 = it.next();
        Partition leafPartition2 = it.next();
        long splitPoint;
        long minRowkey1 = (long) leafPartition1.getRegion().getRange("key").getMin();
        long minRowkey2 = (long) leafPartition2.getRegion().getRange("key").getMin();
        Long maxRowKey1 = (Long) leafPartition1.getRegion().getRange("key").getMax();
        Long maxRowKey2 = (Long) leafPartition2.getRegion().getRange("key").getMax();
        if (minRowkey1 < minRowkey2) {
            splitPoint = maxRowKey1;
            assertEquals(minRowkey1, Long.MIN_VALUE);
            assertEquals((long) maxRowKey1, minRowkey2);
            assertNull(maxRowKey2);
        } else {
            splitPoint = maxRowKey2;
            assertEquals(minRowkey2, Long.MIN_VALUE);
            assertEquals((long) maxRowKey2, minRowkey1);
            assertNull(maxRowKey1);
        }
        assertTrue(400 < splitPoint && splitPoint < 600);
        leafPartitions.forEach(p -> assertEquals(p.getParentPartitionId(), rootPartition.getId()));
        leafPartitions.forEach(p -> assertEquals(p.getChildPartitionIds(), new ArrayList<>()));
        assertEquals(rootPartition, nonLeafPartitions.get(0));
    }

    @Test
    public void shouldSplitPartitionForStringKeyCorrectly()
            throws StateStoreException, IOException, IteratorException, InterruptedException, ObjectFactoryException {
        // Given
        Schema schema = new Schema();
        schema.setRowKeyFields(new Field("key", new StringType()));
        StateStore stateStore = getStateStore(schema);
        String path = folder.newFolder().getAbsolutePath();
        String path2 = folder.newFolder().getAbsolutePath();
        Partition rootPartition = stateStore.getAllPartitions().get(0);
        for (int i = 0; i < 10; i++) {
            List<Record> records = new ArrayList<>();
            for (int r = 0; r < 100; r++) {
                Record record = new Record();
                record.put("key", "A" + i + "" + r);
                records.add(record);
            }
            IngestRecordsFromIterator ingestRecordsFromIterator = new IngestRecordsFromIterator(new ObjectFactory(new InstanceProperties(), null, ""),
                    records.iterator(),
                    path,
                    1_000_000L,
                    1_000_000L,
                    ParquetWriter.DEFAULT_BLOCK_SIZE,
                    ParquetWriter.DEFAULT_PAGE_SIZE,
                    "zstd",
                    stateStore,
                    schema,
                    "",
                    path2,
                    null,
                    null,
                    1_000_000
            );
            ingestRecordsFromIterator.write();
        }
        SplitPartition partitionSplitter = new SplitPartition(stateStore, schema, new Configuration());

        // When
        List<String> fileNames = stateStore.getActiveFiles().stream()
                .filter(fi -> fi.getPartitionId().equals(rootPartition.getId()))
                .map(FileInfo::getFilename)
                .collect(Collectors.toList());
        partitionSplitter.splitPartition(rootPartition, fileNames);

        // Then
        List<Partition> partitions = stateStore.getAllPartitions();
        assertEquals(3, partitions.size());
        List<Partition> nonLeafPartitions = partitions.stream()
                .filter(p -> !p.isLeafPartition())
                .collect(Collectors.toList());
        assertEquals(1, nonLeafPartitions.size());
        Set<Partition> leafPartitions = partitions.stream()
                .filter(Partition::isLeafPartition)
                .collect(Collectors.toSet());
        Iterator<Partition> it = leafPartitions.iterator();
        Partition leafPartition1 = it.next();
        Partition leafPartition2 = it.next();
        String splitPoint;
        String minRowkey1 = (String) leafPartition1.getRegion().getRange("key").getMin();
        String minRowkey2 = (String) leafPartition2.getRegion().getRange("key").getMin();
        String maxRowKey1 = (String) leafPartition1.getRegion().getRange("key").getMax();
        String maxRowKey2 = (String) leafPartition2.getRegion().getRange("key").getMax();
        if ("".equals(minRowkey1)) {
            splitPoint = maxRowKey1;
            assertEquals(maxRowKey1, minRowkey2);
            assertNull(maxRowKey2);
        } else {
            splitPoint = maxRowKey2;
            assertEquals("", minRowkey2);
            assertEquals(maxRowKey2, minRowkey1);
            assertNull(maxRowKey1);
        }
        assertTrue("A00".compareTo(splitPoint) < 0 && splitPoint.compareTo("A9100") < 0);
        leafPartitions.forEach(p -> assertEquals(p.getParentPartitionId(), rootPartition.getId()));
        leafPartitions.forEach(p -> assertEquals(p.getChildPartitionIds(), new ArrayList<>()));
        assertEquals(rootPartition, nonLeafPartitions.get(0));
    }

    @Test
    public void shouldSplitPartitionForByteArrayKeyCorrectly()
            throws StateStoreException, IOException, IteratorException, InterruptedException, ObjectFactoryException {
        // Given
        Schema schema = new Schema();
        schema.setRowKeyFields(new Field("key", new ByteArrayType()));
        StateStore stateStore = getStateStore(schema);
        String path = folder.newFolder().getAbsolutePath();
        String path2 = folder.newFolder().getAbsolutePath();
        Partition rootPartition = stateStore.getAllPartitions().get(0);
        for (int i = 0; i < 10; i++) {
            List<Record> records = new ArrayList<>();
            for (int r = 0; r < 100; r++) {
                Record record = new Record();
                record.put("key", new byte[]{(byte) r});
                records.add(record);
            }
            IngestRecordsFromIterator ingestRecordsFromIterator = new IngestRecordsFromIterator(new ObjectFactory(new InstanceProperties(), null, ""),
                    records.iterator(),
                    path,
                    1_000_000L,
                    1_000_000L,
                    ParquetWriter.DEFAULT_BLOCK_SIZE,
                    ParquetWriter.DEFAULT_PAGE_SIZE,
                    "zstd",
                    stateStore,
                    schema,
                    "",
                    path2,
                    null,
                    null,
                    1_000_000
            );
            ingestRecordsFromIterator.write();
        }
        SplitPartition partitionSplitter = new SplitPartition(stateStore, schema, new Configuration());

        // When
        List<String> fileNames = stateStore.getActiveFiles().stream()
                .filter(fi -> fi.getPartitionId().equals(rootPartition.getId()))
                .map(FileInfo::getFilename)
                .collect(Collectors.toList());
        partitionSplitter.splitPartition(rootPartition, fileNames);

        // Then
        List<Partition> partitions = stateStore.getAllPartitions();
        assertEquals(3, partitions.size());
        List<Partition> nonLeafPartitions = partitions.stream()
                .filter(p -> !p.isLeafPartition())
                .collect(Collectors.toList());
        assertEquals(1, nonLeafPartitions.size());
        Set<Partition> leafPartitions = partitions.stream()
                .filter(Partition::isLeafPartition)
                .collect(Collectors.toSet());
        Iterator<Partition> it = leafPartitions.iterator();
        Partition leafPartition1 = it.next();
        Partition leafPartition2 = it.next();
        byte[] splitPoint;
        byte[] minRowkey1 = (byte[]) leafPartition1.getRegion().getRange("key").getMin();
        byte[] minRowkey2 = (byte[]) leafPartition2.getRegion().getRange("key").getMin();
        byte[] maxRowKey1 = null == leafPartition1.getRegion().getRange("key").getMax() ? null : (byte[]) leafPartition1.getRegion().getRange("key").getMax();
        byte[] maxRowKey2 = null == leafPartition2.getRegion().getRange("key").getMax() ? null : (byte[]) leafPartition2.getRegion().getRange("key").getMax();
        if (Arrays.equals(new byte[]{}, minRowkey1)) {
            splitPoint = maxRowKey1;
            assertArrayEquals(maxRowKey1, minRowkey2);
            assertNull(maxRowKey2);
        } else {
            splitPoint = maxRowKey2;
            assertArrayEquals(new byte[]{}, minRowkey2);
            assertArrayEquals(maxRowKey2, minRowkey1);
            assertNull(maxRowKey1);
        }
        ByteArray splitPointBA = ByteArray.wrap(splitPoint);
        assertTrue(ByteArray.wrap(new byte[]{}).compareTo(splitPointBA) < 0 && splitPointBA.compareTo(ByteArray.wrap(new byte[]{99})) < 0);
        leafPartitions.forEach(p -> assertEquals(p.getParentPartitionId(), rootPartition.getId()));
        leafPartitions.forEach(p -> assertEquals(p.getChildPartitionIds(), new ArrayList<>()));
        assertEquals(rootPartition, nonLeafPartitions.get(0));
    }

    @Test
    public void shouldNotSplitPartitionForByteArrayKeyIfItCannotBeSplitBecausePartitionIsOnePoint()
            throws StateStoreException, IOException, IteratorException, InterruptedException, ObjectFactoryException {
        // Given
        Schema schema = new Schema();
        Field field = new Field("key", new ByteArrayType());
        schema.setRowKeyFields(field);
        // Non-leaf partitions
        Range rootRange = new RangeFactory(schema).createRange(field, new byte[]{0}, null);
        Partition rootPartition = new Partition(
                schema.getRowKeyTypes(),
                new Region(rootRange),
                "root",
                false,
                null,
                null,
                0);
        Range range12 = new RangeFactory(schema).createRange(field, new byte[]{0}, new byte[]{51});
        Partition partition12 = new Partition(
                schema.getRowKeyTypes(),
                new Region(range12),
                "id12",
                false,
                rootPartition.getId(),
                null,
                0);
        // Leaf partitions
        Range range1 = new RangeFactory(schema).createRange(field, new byte[]{0}, new byte[]{50});
        Partition partition1 = new Partition(schema.getRowKeyTypes(),
                new Region(range1),
                "id1",
                true,
                partition12.getId(),
                null,
                -1);
        //  - Partition 2 only includes the key 0 (partitions do not include
        //      the maximum key), and so cannot be split.
        Range range2 = new RangeFactory(schema).createRange(field, new byte[]{50}, new byte[]{51});
        Partition partition2 = new Partition(schema.getRowKeyTypes(),
                new Region(range2),
                "id2",
                true,
                partition12.getId(),
                null,
                -1);
        Range range3 = new RangeFactory(schema).createRange(field, new byte[]{51}, null);
        Partition partition3 = new Partition(schema.getRowKeyTypes(),
                new Region(range3),
                "id3",
                true,
                rootPartition.getId(),
                null,
                -1);
        // Wire up partitions
        rootPartition.setChildPartitionIds(Arrays.asList(partition12.getId(), partition3.getId()));
        partition12.setChildPartitionIds(Arrays.asList(partition1.getId(), partition2.getId()));
        //
        List<Partition> partitions = Arrays.asList(rootPartition, partition12, partition1, partition2, partition3);
        StateStore stateStore = getStateStore(schema, partitions);
        String path = folder.newFolder().getAbsolutePath();
        String path2 = folder.newFolder().getAbsolutePath();
        for (Partition partition : partitions) {
            for (int i = 0; i < 10; i++) {
                List<Record> records = new ArrayList<>();
                for (int r = 0; r < 100; r++) {
                    Record record = new Record();
                    record.put("key", new byte[]{(byte) r});
                    records.add(record);
                }
                if (partition.equals(partition1)) {
                    int j = 0;
                    for (byte r = ((byte[]) partition.getRegion().getRange("key").getMin())[0];
                         r < ((byte[]) partition.getRegion().getRange("key").getMax())[0] && j < 10;
                         r++, j++) {
                        Record record = new Record();
                        record.put("key", new byte[]{r});
                        records.add(record);
                    }
                } else if (partition.equals(partition2)) {
                    for (int j = 0; j < 10; j++) {
                        Record record = new Record();
                        record.put("key", new byte[]{50});
                        records.add(record);
                    }
                } else {
                    for (int j = 51; j < 60; j++) {
                        Record record = new Record();
                        record.put("key", new byte[]{(byte) j});
                        records.add(record);
                    }
                }
                IngestRecordsFromIterator ingestRecordsFromIterator = new IngestRecordsFromIterator(new ObjectFactory(new InstanceProperties(), null, ""),
                        records.iterator(),
                        path,
                        1_000_000L,
                        1_000_000L,
                        ParquetWriter.DEFAULT_BLOCK_SIZE,
                        ParquetWriter.DEFAULT_PAGE_SIZE,
                        "zstd",
                        stateStore,
                        schema,
                        "",
                        path2,
                        null,
                        null,
                        1_000_000
                );
                ingestRecordsFromIterator.write();
            }
        }
        SplitPartition partitionSplitter = new SplitPartition(stateStore, schema, new Configuration());

        // When
        List<String> fileNames = stateStore.getActiveFiles().stream()
                .filter(fi -> fi.getPartitionId().equals(partition2.getId()))
                .map(FileInfo::getFilename)
                .collect(Collectors.toList());
        partitionSplitter.splitPartition(partition2, fileNames);

        // Then
        List<Partition> partitionsAfterSplit = stateStore.getAllPartitions();
        assertEquals(partitions.size(), partitionsAfterSplit.size());
        assertEquals(new HashSet<>(partitions), new HashSet<>(partitionsAfterSplit));
    }

    @Test
    public void shouldNotSplitPartitionForByteArrayKeyIfItCannotBeSplitBecauseDataIsConstant()
            throws StateStoreException, IOException, IteratorException, InterruptedException, ObjectFactoryException {
        // Given
        Schema schema = new Schema();
        Field field = new Field("key", new ByteArrayType());
        schema.setRowKeyFields(field);
        // Non-leaf partitions
        Range rootRange = new RangeFactory(schema).createRange(field, new byte[]{0}, null);
        Partition rootPartition = new Partition(
                schema.getRowKeyTypes(),
                new Region(rootRange),
                "root",
                false,
                null,
                null,
                0);
        Range range12 = new RangeFactory(schema).createRange(field, new byte[]{0}, new byte[]{100});
        Partition partition12 = new Partition(
                schema.getRowKeyTypes(),
                new Region(range12),
                "id12",
                false,
                rootPartition.getId(),
                null,
                0);
        // Leaf partitions
        Range range1 = new RangeFactory(schema).createRange(field, new byte[]{0}, new byte[]{50});
        Partition partition1 = new Partition(schema.getRowKeyTypes(),
                new Region(range1),
                "id1",
                true,
                partition12.getId(),
                null,
                -1);
        //  - Partition 2 only includes the key 0 (partitions do not include
        //      the maximum key), and so cannot be split.
        Range range2 = new RangeFactory(schema).createRange(field, new byte[]{50}, new byte[]{100});
        Partition partition2 = new Partition(schema.getRowKeyTypes(),
                new Region(range2),
                "id2",
                true,
                partition12.getId(),
                null,
                -1);
        Range range3 = new RangeFactory(schema).createRange(field, new byte[]{100}, null);
        Partition partition3 = new Partition(schema.getRowKeyTypes(),
                new Region(range3),
                "id3",
                true,
                rootPartition.getId(),
                null,
                -1);
        // Wire up partitions
        rootPartition.setChildPartitionIds(Arrays.asList(partition12.getId(), partition3.getId()));
        partition12.setChildPartitionIds(Arrays.asList(partition1.getId(), partition2.getId()));
        //
        List<Partition> partitions = Arrays.asList(rootPartition, partition12, partition1, partition2, partition3);
        StateStore stateStore = getStateStore(schema, partitions);
        String path = folder.newFolder().getAbsolutePath();
        String path2 = folder.newFolder().getAbsolutePath();
        for (Partition partition : partitions) {
            for (int i = 0; i < 10; i++) {
                List<Record> records = new ArrayList<>();
                if (partition.equals(partition1)) {
                    int j = 0;
                    for (byte r = ((byte[]) partition.getRegion().getRange("key").getMin())[0];
                         r < ((byte[]) partition.getRegion().getRange("key").getMax())[0] && j < 10;
                         r++, j++) {
                        Record record = new Record();
                        record.put("key", new byte[]{r});
                        records.add(record);
                    }
                } else if (partition.equals(partition2)) {
                    // Files in partition2 all have the same value for the key
                    for (int j = 0; j < 10; j++) {
                        Record record = new Record();
                        record.put("key", new byte[]{60});
                        records.add(record);
                    }
                } else {
                    for (int j = 100; j < 110; j++) {
                        Record record = new Record();
                        record.put("key", new byte[]{(byte) j});
                        records.add(record);
                    }
                }
                IngestRecordsFromIterator ingestRecordsFromIterator = new IngestRecordsFromIterator(new ObjectFactory(new InstanceProperties(), null, ""),
                        records.iterator(),
                        path,
                        1_000_000L,
                        1_000_000L,
                        ParquetWriter.DEFAULT_BLOCK_SIZE,
                        ParquetWriter.DEFAULT_PAGE_SIZE,
                        "zstd",
                        stateStore,
                        schema,
                        "",
                        path2,
                        null,
                        null,
                        1_000_000
                );
                ingestRecordsFromIterator.write();
            }
        }
        SplitPartition partitionSplitter = new SplitPartition(stateStore, schema, new Configuration());

        // When
        List<String> fileNames = stateStore.getActiveFiles().stream()
                .filter(fi -> fi.getPartitionId().equals(partition2.getId()))
                .map(FileInfo::getFilename)
                .collect(Collectors.toList());
        partitionSplitter.splitPartition(partition2, fileNames);

        // Then
        List<Partition> partitionsAfterSplit = stateStore.getAllPartitions();
        assertEquals(partitions.size(), partitionsAfterSplit.size());
        assertEquals(new HashSet<>(partitions), new HashSet<>(partitionsAfterSplit));
    }

    @Test
    public void shouldSplitPartitionForByteArrayMultidimensionalKeyOnFirstDimensionCorrectly()
            throws StateStoreException, IOException, IteratorException, InterruptedException, ObjectFactoryException {
        // Given
        Schema schema = new Schema();
        schema.setRowKeyFields(new Field("key1", new ByteArrayType()), new Field("key2", new ByteArrayType()));
        StateStore stateStore = getStateStore(schema);
        String path = folder.newFolder().getAbsolutePath();
        String path2 = folder.newFolder().getAbsolutePath();
        Partition rootPartition = stateStore.getAllPartitions().get(0);
        for (int i = 0; i < 10; i++) {
            List<Record> records = new ArrayList<>();
            for (int r = 0; r < 100; r++) {
                Record record = new Record();
                record.put("key1", new byte[]{(byte) r});
                record.put("key2", new byte[]{(byte) -100});
                records.add(record);
            }
            IngestRecordsFromIterator ingestRecordsFromIterator = new IngestRecordsFromIterator(new ObjectFactory(new InstanceProperties(), null, ""),
                    records.iterator(),
                    path,
                    1_000_000L,
                    1_000_000L,
                    ParquetWriter.DEFAULT_BLOCK_SIZE,
                    ParquetWriter.DEFAULT_PAGE_SIZE,
                    "zstd",
                    stateStore,
                    schema,
                    "",
                    path2,
                    null,
                    null,
                    1_000_000
            );
            ingestRecordsFromIterator.write();
        }
        SplitPartition partitionSplitter = new SplitPartition(stateStore, schema, new Configuration());

        // When
        List<String> fileNames = stateStore.getActiveFiles().stream()
                .filter(fi -> fi.getPartitionId().equals(rootPartition.getId()))
                .map(FileInfo::getFilename)
                .collect(Collectors.toList());
        partitionSplitter.splitPartition(rootPartition, fileNames);

        // Then
        //  - There should be 3 partitions
        List<Partition> partitions = stateStore.getAllPartitions();
        assertEquals(3, partitions.size());
        //  - There should be 1 non-leaf partition
        List<Partition> nonLeafPartitions = partitions.stream()
                .filter(p -> !p.isLeafPartition())
                .collect(Collectors.toList());
        assertEquals(1, nonLeafPartitions.size());
        //  - The root partition should have been split on the first dimension
        assertEquals(0, nonLeafPartitions.get(0).getDimension());
        //  - The leaf partitions should have been split on a value which is between
        //      0 and 100.
        Set<Partition> leafPartitions = partitions.stream()
                .filter(Partition::isLeafPartition)
                .collect(Collectors.toSet());
        Iterator<Partition> it = leafPartitions.iterator();
        Partition leafPartition1 = it.next();
        Partition leafPartition2 = it.next();
        byte[] splitPoint;
        byte[] minRowkey1 = (byte[]) leafPartition1.getRegion().getRange("key1").getMin();
        byte[] minRowkey2 = (byte[]) leafPartition2.getRegion().getRange("key1").getMin();
        byte[] maxRowKey1 = null == leafPartition1.getRegion().getRange("key1").getMax() ? null : (byte[]) leafPartition1.getRegion().getRange("key1").getMax();
        byte[] maxRowKey2 = null == leafPartition2.getRegion().getRange("key1").getMax() ? null : (byte[]) leafPartition2.getRegion().getRange("key1").getMax();
        if (Arrays.equals(new byte[]{}, minRowkey1)) {
            splitPoint = maxRowKey1;
            assertArrayEquals(maxRowKey1, minRowkey2);
            assertNull(maxRowKey2);
        } else {
            splitPoint = maxRowKey2;
            assertArrayEquals(new byte[]{}, minRowkey2);
            assertArrayEquals(maxRowKey2, minRowkey1);
            assertNull(maxRowKey1);
        }
        ByteArray splitPointBA = ByteArray.wrap(splitPoint);
        assertTrue(ByteArray.wrap(new byte[]{}).compareTo(splitPointBA) < 0 && splitPointBA.compareTo(ByteArray.wrap(new byte[]{99})) < 0);
        //  - The leaf partitions should have the root partition as their parent
        //      and an empty array for the child partitions.
        leafPartitions.forEach(p -> assertEquals(p.getParentPartitionId(), rootPartition.getId()));
        leafPartitions.forEach(p -> assertEquals(p.getChildPartitionIds(), new ArrayList<>()));
        assertEquals(rootPartition, nonLeafPartitions.get(0));
    }

    @Test
    public void shouldSplitPartitionForByteArrayMultidimensionalKeyOnSecondDimensionCorrectly()
            throws StateStoreException, IOException, IteratorException, InterruptedException, ObjectFactoryException {
        // Given
        Schema schema = new Schema();
        schema.setRowKeyFields(new Field("key1", new ByteArrayType()), new Field("key2", new ByteArrayType()));
        StateStore stateStore = getStateStore(schema);
        String path = folder.newFolder().getAbsolutePath();
        String path2 = folder.newFolder().getAbsolutePath();
        Partition rootPartition = stateStore.getAllPartitions().get(0);
        for (int i = 0; i < 10; i++) {
            List<Record> records = new ArrayList<>();
            for (int r = 0; r < 100; r++) {
                Record record = new Record();
                record.put("key1", new byte[]{(byte) -100});
                record.put("key2", new byte[]{(byte) r});
                records.add(record);
            }
            IngestRecordsFromIterator ingestRecordsFromIterator = new IngestRecordsFromIterator(new ObjectFactory(new InstanceProperties(), null, ""),
                    records.iterator(),
                    path,
                    1_000_000L,
                    1_000_000L,
                    ParquetWriter.DEFAULT_BLOCK_SIZE,
                    ParquetWriter.DEFAULT_PAGE_SIZE,
                    "zstd",
                    stateStore,
                    schema,
                    "",
                    path2,
                    null,
                    null,
                    1_000_000
            );
            ingestRecordsFromIterator.write();
        }
        SplitPartition partitionSplitter = new SplitPartition(stateStore, schema, new Configuration());

        // When
        List<String> fileNames = stateStore.getActiveFiles().stream()
                .filter(fi -> fi.getPartitionId().equals(rootPartition.getId()))
                .map(FileInfo::getFilename)
                .collect(Collectors.toList());
        partitionSplitter.splitPartition(rootPartition, fileNames);

        // Then
        List<Partition> partitions = stateStore.getAllPartitions();
        assertEquals(3, partitions.size());
        List<Partition> nonLeafPartitions = partitions.stream()
                .filter(p -> !p.isLeafPartition())
                .collect(Collectors.toList());
        assertEquals(1, nonLeafPartitions.size());
        assertEquals(1, nonLeafPartitions.get(0).getDimension());
        Set<Partition> leafPartitions = partitions.stream()
                .filter(Partition::isLeafPartition)
                .collect(Collectors.toSet());
        Iterator<Partition> it = leafPartitions.iterator();
        Partition leafPartition1 = it.next();
        Partition leafPartition2 = it.next();
        byte[] splitPoint;
        byte[] minRowkey1 = (byte[]) leafPartition1.getRegion().getRange("key2").getMin();
        byte[] minRowkey2 = (byte[]) leafPartition2.getRegion().getRange("key2").getMin();
        byte[] maxRowKey1 = null == leafPartition1.getRegion().getRange("key2").getMax() ? null : (byte[]) leafPartition1.getRegion().getRange("key2").getMax();
        byte[] maxRowKey2 = null == leafPartition2.getRegion().getRange("key2").getMax() ? null : (byte[]) leafPartition2.getRegion().getRange("key2").getMax();
        if (Arrays.equals(new byte[]{}, minRowkey1)) {
            splitPoint = maxRowKey1;
            assertArrayEquals(maxRowKey1, minRowkey2);
            assertNull(maxRowKey2);
        } else {
            splitPoint = maxRowKey2;
            assertArrayEquals(new byte[]{}, minRowkey2);
            assertArrayEquals(maxRowKey2, minRowkey1);
            assertNull(maxRowKey1);
        }
        ByteArray splitPointBA = ByteArray.wrap(splitPoint);
        assertTrue(ByteArray.wrap(new byte[]{}).compareTo(splitPointBA) < 0 && splitPointBA.compareTo(ByteArray.wrap(new byte[]{99})) < 0);
        leafPartitions.forEach(p -> assertEquals(p.getParentPartitionId(), rootPartition.getId()));
        leafPartitions.forEach(p -> assertEquals(p.getChildPartitionIds(), new ArrayList<>()));
        assertEquals(rootPartition, nonLeafPartitions.get(0));
    }
}
