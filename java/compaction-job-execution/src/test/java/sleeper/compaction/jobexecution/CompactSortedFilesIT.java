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
package sleeper.compaction.jobexecution;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.facebook.collections.ByteArray;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;
import org.junit.AfterClass;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.testcontainers.containers.GenericContainer;
import sleeper.compaction.job.CompactionJob;
import sleeper.configuration.jars.ObjectFactory;
import sleeper.configuration.jars.ObjectFactoryException;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.core.CommonTestConstants;
import sleeper.core.iterator.IteratorException;
import sleeper.core.iterator.impl.AgeOffIterator;
import sleeper.core.key.Key;
import sleeper.core.partition.Partition;
import sleeper.core.range.Range;
import sleeper.core.range.Range.RangeFactory;
import sleeper.core.range.Region;
import sleeper.core.record.Record;
import sleeper.core.record.RecordComparator;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.ByteArrayType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.StringType;
import sleeper.io.parquet.record.ParquetReaderIterator;
import sleeper.io.parquet.record.ParquetRecordReader;
import sleeper.io.parquet.record.ParquetRecordWriter;
import sleeper.io.parquet.record.SchemaConverter;
import sleeper.statestore.FileInfo;
import sleeper.statestore.StateStoreException;
import sleeper.statestore.dynamodb.DynamoDBStateStore;
import sleeper.statestore.dynamodb.DynamoDBStateStoreCreator;

public class CompactSortedFilesIT {
    private static final int DYNAMO_PORT = 8000;

    @ClassRule
    public static GenericContainer dynamoDb = new GenericContainer(CommonTestConstants.DYNAMODB_LOCAL_CONTAINER)
            .withExposedPorts(DYNAMO_PORT);

    public static AmazonDynamoDB dynamoDBClient;

    @BeforeClass
    public static void beforeAll() {
        AwsClientBuilder.EndpointConfiguration endpointConfiguration =
                new AwsClientBuilder.EndpointConfiguration("http://" + dynamoDb.getContainerIpAddress() + ":"
                        + dynamoDb.getMappedPort(DYNAMO_PORT), "us-west-2");
        dynamoDBClient = AmazonDynamoDBClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials("12345", "6789")))
                .withEndpointConfiguration(endpointConfiguration)
                .build();
    }

    @AfterClass
    public static void afterAll() {
        dynamoDBClient.shutdown();
        dynamoDBClient = null;
    }

    @Rule
    public TemporaryFolder folder = new TemporaryFolder(CommonTestConstants.TMP_DIRECTORY);

    @Test
    public void filesShouldMergeCorrectlyAndDynamoUpdatedLongKey() throws IOException, StateStoreException, IteratorException, ObjectFactoryException {
        // Given
        //  - Schema
        Schema schema = new Schema();
        schema.setRowKeyFields(new Field("key", new LongType()));
        schema.setValueFields(new Field("value1", new LongType()), new Field("value2", new LongType()));
        //  - Create two files of sorted data
        String folderName = folder.newFolder().getAbsolutePath();
        String file1 = folderName + "/file1.parquet";
        String file2 = folderName + "/file2.parquet";
        List<String> files = new ArrayList<>();
        files.add(file1);
        files.add(file2);
        FileInfo fileInfo1 = new FileInfo();
        fileInfo1.setRowKeyTypes(new LongType());
        fileInfo1.setFilename(file1);
        fileInfo1.setFileStatus(FileInfo.FileStatus.ACTIVE);
        fileInfo1.setPartitionId("1");
        fileInfo1.setNumberOfRecords(100L);
        fileInfo1.setMinRowKey(Key.create(0L));
        fileInfo1.setMaxRowKey(Key.create(198L));
        FileInfo fileInfo2 = new FileInfo();
        fileInfo2.setRowKeyTypes(new LongType());
        fileInfo2.setFilename(file2);
        fileInfo2.setFileStatus(FileInfo.FileStatus.ACTIVE);
        fileInfo2.setPartitionId("1");
        fileInfo2.setNumberOfRecords(100L);
        fileInfo2.setMinRowKey(Key.create(1L));
        fileInfo2.setMaxRowKey(Key.create(199L));
        List<FileInfo> fileInfos = new ArrayList<>();
        fileInfos.add(fileInfo1);
        fileInfos.add(fileInfo2);
        String outputFile = folderName + "/file3.parquet";
        SortedMap<Long, Record> data = new TreeMap<>();
        ParquetRecordWriter writer1 = new ParquetRecordWriter(new Path(file1), SchemaConverter.getSchema(schema), schema);
        for (int i = 0; i < 100; i++) {
            Record record = new Record();
            record.put("key", (long) 2 * i);
            record.put("value1", (long) 2 * i);
            record.put("value2", 987654321L);
            writer1.write(record);
            data.put((long) record.get("key"), record);
        }
        writer1.close();
        ParquetRecordWriter writer2 = new ParquetRecordWriter(new Path(file2), SchemaConverter.getSchema(schema), schema);
        for (int i = 0; i < 100; i++) {
            Record record = new Record();
            record.put("key", (long) 2 * i + 1);
            record.put("value1", 1001L);
            record.put("value2", 123456789L);
            writer2.write(record);
            data.put((long) record.get("key"), record);
        }
        writer2.close();
        //  - Create DynamoDBStateStore
        DynamoDBStateStoreCreator dynamoDBStateStoreCreator = new DynamoDBStateStoreCreator("fsmcadulk", schema, dynamoDBClient);
        DynamoDBStateStore dynamoStateStore = dynamoDBStateStoreCreator.create();
        dynamoStateStore.initialise();
        //  - Update Dynamo state store with details of files
        dynamoStateStore.addFiles(Arrays.asList(fileInfo1, fileInfo2));

        //  - Create CompactionJob and update status of files with compactionJob id
        CompactionJob compactionJob = new CompactionJob("table", "compactionJob-1");
        compactionJob.setInputFiles(files);
        compactionJob.setOutputFile(outputFile);
        compactionJob.setPartitionId("1");
        compactionJob.setIsSplittingJob(false);
        dynamoStateStore.atomicallyUpdateJobStatusOfFiles(compactionJob.getId(), fileInfos);

        // When
        //  - Merge two files
        CompactSortedFiles compactSortedFiles = new CompactSortedFiles(new InstanceProperties(), new ObjectFactory(new InstanceProperties(), null, ""),
                schema, SchemaConverter.getSchema(schema), compactionJob, dynamoStateStore,
                ParquetWriter.DEFAULT_BLOCK_SIZE, ParquetWriter.DEFAULT_PAGE_SIZE, "zstd", 25, 1000);
        CompactSortedFiles.CompactionJobSummary summary = compactSortedFiles.compact();

        // Then
        //  - Read output file and check that it contains the right results
        List<Record> results = new ArrayList<>();
        ParquetReaderIterator reader = new ParquetReaderIterator(new ParquetRecordReader(new Path(outputFile), schema));
        while (reader.hasNext()) {
            results.add(new Record(reader.next()));
        }
        reader.close();
        assertThat(summary.getLinesRead()).isEqualTo(data.values().size());
        assertThat(summary.getLinesWritten()).isEqualTo(data.values().size());
        List<Record> expectedResults = new ArrayList<>(data.values());
        assertThat(results).isEqualTo(expectedResults);
        // - Check DynamoDBStateStore has correct ready for GC files
        List<FileInfo> readyForGCFiles = new ArrayList<>();
        dynamoStateStore.getReadyForGCFiles().forEachRemaining(readyForGCFiles::add);
        readyForGCFiles = readyForGCFiles
                .stream()
                .sorted(Comparator.comparing(FileInfo::getFilename))
                .collect(Collectors.toList());
        assertThat(readyForGCFiles.size()).isEqualTo(2);

        assertThat(readyForGCFiles.get(0).getFilename()).isEqualTo(fileInfo1.getFilename());
        assertThat(readyForGCFiles.get(1).getFilename()).isEqualTo(fileInfo2.getFilename());
        assertThat(readyForGCFiles.get(0).getRowKeyTypes()).isEqualTo(fileInfo1.getRowKeyTypes());
        assertThat(readyForGCFiles.get(1).getRowKeyTypes()).isEqualTo(fileInfo2.getRowKeyTypes());
        assertThat(readyForGCFiles.get(0).getPartitionId()).isEqualTo(fileInfo1.getPartitionId());
        assertThat(readyForGCFiles.get(1).getPartitionId()).isEqualTo(fileInfo2.getPartitionId());
        assertThat(readyForGCFiles.get(0).getFileStatus()).isEqualTo(FileInfo.FileStatus.READY_FOR_GARBAGE_COLLECTION);
        assertThat(readyForGCFiles.get(1).getFileStatus()).isEqualTo(FileInfo.FileStatus.READY_FOR_GARBAGE_COLLECTION);
        // - Check DynamoDBStateStore has correct active files
        List<FileInfo> activeFiles = dynamoStateStore.getActiveFiles()
                .stream()
                .sorted(Comparator.comparing(FileInfo::getFilename))
                .collect(Collectors.toList());
        assertThat(activeFiles.size()).isEqualTo(1);
        FileInfo newFile = new FileInfo();
        newFile.setRowKeyTypes(new LongType());
        newFile.setFilename(outputFile);
        newFile.setFileStatus(FileInfo.FileStatus.ACTIVE);
        newFile.setPartitionId("1");
        newFile.setNumberOfRecords((long) expectedResults.size());
        long minKey = expectedResults.stream()
                .map(r -> (long) r.get("key"))
                .min(Comparator.naturalOrder())
                .get();
        newFile.setMinRowKey(Key.create(minKey));
        long maxKey = expectedResults.stream()
                .map(r -> (long) r.get("key"))
                .max(Comparator.naturalOrder())
                .get();
        newFile.setMaxRowKey(Key.create(maxKey));
        activeFiles.get(0).setLastStateStoreUpdateTime(null); // Set to null as we don't know what it should be
        assertThat(activeFiles.get(0)).isEqualTo(newFile);
    }

    @Test
    public void filesShouldMergeCorrectlyAndDynamoUpdatedStringKey() throws IOException, StateStoreException, ObjectFactoryException, IteratorException {
        // Given
        //  - Schema
        Schema schema = new Schema();
        schema.setRowKeyFields(new Field("key", new StringType()));
        schema.setValueFields(new Field("value1", new StringType()), new Field("value2", new LongType()));
        //  - Create two files of sorted data
        String folderName = folder.newFolder().getAbsolutePath();
        String file1 = folderName + "/file1.parquet";
        String file2 = folderName + "/file2.parquet";
        List<String> files = new ArrayList<>();
        files.add(file1);
        files.add(file2);
        FileInfo fileInfo1 = new FileInfo();
        fileInfo1.setRowKeyTypes(new StringType());
        fileInfo1.setFilename(file1);
        fileInfo1.setFileStatus(FileInfo.FileStatus.ACTIVE);
        fileInfo1.setPartitionId("1");
        fileInfo1.setNumberOfRecords(100L);
        fileInfo1.setMinRowKey(Key.create("0"));
        fileInfo1.setMaxRowKey(Key.create("98"));
        FileInfo fileInfo2 = new FileInfo();
        fileInfo2.setRowKeyTypes(new StringType());
        fileInfo2.setFilename(file2);
        fileInfo2.setFileStatus(FileInfo.FileStatus.ACTIVE);
        fileInfo2.setPartitionId("1");
        fileInfo2.setNumberOfRecords(100L);
        fileInfo2.setMinRowKey(Key.create("1"));
        fileInfo2.setMaxRowKey(Key.create("9"));
        List<FileInfo> fileInfos = Arrays.asList(fileInfo1, fileInfo2);
        String outputFile = folderName + "/file3.parquet";
        SortedMap<String, Record> data1 = new TreeMap<>();
        for (int i = 0; i < 100; i++) {
            Record record = new Record();
            record.put("key", "" + (2 * i));
            record.put("value1", "" + (2 * i));
            record.put("value2", 987654321L);
            data1.put((String) record.get("key"), record);
        }
        ParquetRecordWriter writer1 = new ParquetRecordWriter(new Path(file1), SchemaConverter.getSchema(schema), schema);
        for (Map.Entry<String, Record> entry : data1.entrySet()) {
            writer1.write(entry.getValue());
        }
        writer1.close();
        SortedMap<String, Record> data2 = new TreeMap<>();
        for (int i = 0; i < 100; i++) {
            Record record = new Record();
            record.put("key", "" + (2 * i + 1));
            record.put("value1", "" + 1001L);
            record.put("value2", 123456789L);
            data2.put((String) record.get("key"), record);
        }
        ParquetRecordWriter writer2 = new ParquetRecordWriter(new Path(file2), SchemaConverter.getSchema(schema), schema);
        for (Map.Entry<String, Record> entry : data2.entrySet()) {
            writer2.write(entry.getValue());
        }
        writer2.close();
        SortedMap<String, Record> data = new TreeMap<>();
        data.putAll(data1);
        data.putAll(data2);
        //  - Create DynamoDBStateStore
        DynamoDBStateStoreCreator dynamoDBStateStoreCreator = new DynamoDBStateStoreCreator("fsmcadusk", schema, dynamoDBClient);
        DynamoDBStateStore dynamoStateStore = dynamoDBStateStoreCreator.create();
        dynamoStateStore.initialise();
        //  - Update Dynamo state store with details of files
        dynamoStateStore.addFiles(Arrays.asList(fileInfo1, fileInfo2));

        //  - Create CompactionJob and update status of files with compactionJob id
        CompactionJob compactionJob = new CompactionJob("table", "compactionJob-1");
        compactionJob.setInputFiles(files);
        compactionJob.setOutputFile(outputFile);
        compactionJob.setPartitionId("1");
        compactionJob.setIsSplittingJob(false);
        dynamoStateStore.atomicallyUpdateJobStatusOfFiles(compactionJob.getId(), fileInfos);

        // When
        //  - Merge two files
        CompactSortedFiles compactSortedFiles = new CompactSortedFiles(new InstanceProperties(), new ObjectFactory(new InstanceProperties(), null, ""),
                schema, SchemaConverter.getSchema(schema), compactionJob, dynamoStateStore,
                ParquetWriter.DEFAULT_BLOCK_SIZE, ParquetWriter.DEFAULT_PAGE_SIZE, "zstd", 25, 1000);
        compactSortedFiles.compact();

        // Then
        //  - Read output file and check that it contains the right results
        List<Record> results = new ArrayList<>();
        ParquetReaderIterator reader = new ParquetReaderIterator(new ParquetRecordReader(new Path(outputFile), schema));
        while (reader.hasNext()) {
            results.add(new Record(reader.next()));
        }
        reader.close();
        List<Record> expectedResults = new ArrayList<>(data.values());
        assertThat(results).isEqualTo(expectedResults);
        // - Check DynamoDBStateStore has correct ready for GC files
        List<FileInfo> readyForGCFiles = new ArrayList<>();
        dynamoStateStore.getReadyForGCFiles().forEachRemaining(readyForGCFiles::add);
        readyForGCFiles = readyForGCFiles
                .stream()
                .sorted(Comparator.comparing(FileInfo::getFilename))
                .collect(Collectors.toList());
        assertThat(readyForGCFiles.size()).isEqualTo(2);

        assertThat(readyForGCFiles.get(0).getFilename()).isEqualTo(fileInfo1.getFilename());
        assertThat(readyForGCFiles.get(1).getFilename()).isEqualTo(fileInfo2.getFilename());
        assertThat(readyForGCFiles.get(0).getRowKeyTypes()).isEqualTo(fileInfo1.getRowKeyTypes());
        assertThat(readyForGCFiles.get(1).getRowKeyTypes()).isEqualTo(fileInfo2.getRowKeyTypes());
        assertThat(readyForGCFiles.get(0).getPartitionId()).isEqualTo(fileInfo1.getPartitionId());
        assertThat(readyForGCFiles.get(1).getPartitionId()).isEqualTo(fileInfo2.getPartitionId());
        assertThat(readyForGCFiles.get(0).getFileStatus()).isEqualTo(FileInfo.FileStatus.READY_FOR_GARBAGE_COLLECTION);
        assertThat(readyForGCFiles.get(1).getFileStatus()).isEqualTo(FileInfo.FileStatus.READY_FOR_GARBAGE_COLLECTION);
        // - Check DynamoDBStateStore has correct active files
        List<FileInfo> activeFiles = dynamoStateStore.getActiveFiles()
                .stream()
                .sorted(Comparator.comparing(FileInfo::getFilename))
                .collect(Collectors.toList());
        ;
        assertThat(activeFiles.size()).isEqualTo(1);
        FileInfo newFile = new FileInfo();
        newFile.setRowKeyTypes(new StringType());
        newFile.setFilename(outputFile);
        newFile.setFileStatus(FileInfo.FileStatus.ACTIVE);
        newFile.setPartitionId("1");
        newFile.setNumberOfRecords((long) expectedResults.size());
        String minKey = expectedResults.stream()
                .map(r -> (String) r.get("key"))
                .min(Comparator.naturalOrder())
                .get();
        newFile.setMinRowKey(Key.create(minKey));
        String maxKey = expectedResults.stream()
                .map(r -> (String) r.get("key"))
                .max(Comparator.naturalOrder())
                .get();
        newFile.setMaxRowKey(Key.create(maxKey));
        activeFiles.get(0).setLastStateStoreUpdateTime(null); // Set to null as we don't know what it should be
        assertThat(activeFiles.get(0)).isEqualTo(newFile);
    }

    @Test
    public void filesShouldMergeCorrectlyAndDynamoUpdatedByteArrayKey() throws IOException, StateStoreException, ObjectFactoryException, IteratorException {
        // Given
        //  - Schema
        Schema schema = new Schema();
        schema.setRowKeyFields(new Field("key", new ByteArrayType()));
        schema.setValueFields(new Field("value1", new ByteArrayType()), new Field("value2", new LongType()));
        //  - Create two files of sorted data
        String folderName = folder.newFolder().getAbsolutePath();
        String file1 = folderName + "/file1.parquet";
        String file2 = folderName + "/file2.parquet";
        List<String> files = new ArrayList<>();
        files.add(file1);
        files.add(file2);
        FileInfo fileInfo1 = new FileInfo();
        fileInfo1.setRowKeyTypes(new ByteArrayType());
        fileInfo1.setFilename(file1);
        fileInfo1.setFileStatus(FileInfo.FileStatus.ACTIVE);
        fileInfo1.setPartitionId("1");
        fileInfo1.setNumberOfRecords(100L);
        FileInfo fileInfo2 = new FileInfo();
        fileInfo2.setRowKeyTypes(new ByteArrayType());
        fileInfo2.setFilename(file2);
        fileInfo2.setFileStatus(FileInfo.FileStatus.ACTIVE);
        fileInfo2.setPartitionId("1");
        fileInfo2.setNumberOfRecords(100L);
        List<FileInfo> fileInfos = new ArrayList<>();
        fileInfos.add(fileInfo1);
        fileInfos.add(fileInfo2);
        String outputFile = folderName + "/file3.parquet";
        SortedMap<ByteArray, Record> data1 = new TreeMap<>();
        for (int i = 0; i < 100; i++) {
            Record record = new Record();
            record.put("key", new byte[]{(byte) (2 * i)});
            record.put("value1", new byte[]{(byte) (2 * i)});
            record.put("value2", 987654321L);
            data1.put(ByteArray.wrap((byte[]) record.get("key")), record);
        }
        fileInfo1.setMinRowKey(Key.create(data1.keySet().iterator().next().getArray()));
        Iterator<ByteArray> it = data1.keySet().iterator();
        ByteArray ba = it.next();
        while (it.hasNext()) {
            ba = it.next();
        }
        fileInfo1.setMaxRowKey(Key.create(ba.getArray()));
        ParquetRecordWriter writer1 = new ParquetRecordWriter(new Path(file1), SchemaConverter.getSchema(schema), schema);
        for (Map.Entry<ByteArray, Record> entry : data1.entrySet()) {
            writer1.write(entry.getValue());
        }
        writer1.close();
        SortedMap<ByteArray, Record> data2 = new TreeMap<>();
        for (int i = 0; i < 100; i++) {
            Record record = new Record();
            record.put("key", new byte[]{(byte) (2 * i + 1)});
            record.put("value1", new byte[]{101});
            record.put("value2", 123456789L);
            data2.put(ByteArray.wrap((byte[]) record.get("key")), record);
        }
        fileInfo2.setMinRowKey(Key.create(data2.keySet().iterator().next().getArray()));
        it = data2.keySet().iterator();
        ba = it.next();
        while (it.hasNext()) {
            ba = it.next();
        }
        fileInfo2.setMaxRowKey(Key.create(ba.getArray()));
        ParquetRecordWriter writer2 = new ParquetRecordWriter(new Path(file2), SchemaConverter.getSchema(schema), schema);
        for (Map.Entry<ByteArray, Record> entry : data2.entrySet()) {
            writer2.write(entry.getValue());
        }
        writer2.close();
        SortedMap<ByteArray, Record> data = new TreeMap<>();
        data.putAll(data1);
        data.putAll(data2);
        //  - Create DynamoDBStateStore
        DynamoDBStateStoreCreator dynamoDBStateStoreCreator = new DynamoDBStateStoreCreator("fsmcadubak", schema, dynamoDBClient);
        DynamoDBStateStore dynamoStateStore = dynamoDBStateStoreCreator.create();
        dynamoStateStore.initialise();
        //  - Update Dynamo state store with details of files
        dynamoStateStore.addFiles(Arrays.asList(fileInfo1, fileInfo2));

        //  - Create CompactionJob and update status of files with compactionJob id
        CompactionJob compactionJob = new CompactionJob("table", "compactionJob-1");
        compactionJob.setInputFiles(files);
        compactionJob.setOutputFile(outputFile);
        compactionJob.setPartitionId("1");
        compactionJob.setIsSplittingJob(false);
        dynamoStateStore.atomicallyUpdateJobStatusOfFiles(compactionJob.getId(), fileInfos);

        // When
        //  - Merge two files
        CompactSortedFiles compactSortedFiles = new CompactSortedFiles(new InstanceProperties(), new ObjectFactory(new InstanceProperties(), null, ""),
                schema, SchemaConverter.getSchema(schema), compactionJob, dynamoStateStore,
                ParquetWriter.DEFAULT_BLOCK_SIZE, ParquetWriter.DEFAULT_PAGE_SIZE, "zstd", 25, 1000);
        compactSortedFiles.compact();

        // Then
        //  - Read output file and check that it contains the right results
        List<Record> results = new ArrayList<>();
        ParquetReaderIterator reader = new ParquetReaderIterator(new ParquetRecordReader(new Path(outputFile), schema));
        while (reader.hasNext()) {
            results.add(new Record(reader.next()));
        }
        reader.close();
        List<Record> expectedResults = data.values().stream()
                .map(r -> {
                    Record transformedRecord = new Record();
                    transformedRecord.put("key", r.get("key"));
                    transformedRecord.put("value1", r.get("value1"));
                    transformedRecord.put("value2", r.get("value2"));
                    return transformedRecord;
                })
                .collect(Collectors.toList());
        assertThat(results).isEqualTo(expectedResults);
        // - Check DynamoDBStateStore has correct ready for GC files
        List<FileInfo> readyForGCFiles = new ArrayList<>();
        dynamoStateStore.getReadyForGCFiles().forEachRemaining(readyForGCFiles::add);
        readyForGCFiles = readyForGCFiles
                .stream()
                .sorted(Comparator.comparing(FileInfo::getFilename))
                .collect(Collectors.toList());
        assertThat(readyForGCFiles.size()).isEqualTo(2);

        assertThat(readyForGCFiles.get(0).getFilename()).isEqualTo(fileInfo1.getFilename());
        assertThat(readyForGCFiles.get(1).getFilename()).isEqualTo(fileInfo2.getFilename());
        assertThat(readyForGCFiles.get(0).getRowKeyTypes()).isEqualTo(fileInfo1.getRowKeyTypes());
        assertThat(readyForGCFiles.get(1).getRowKeyTypes()).isEqualTo(fileInfo2.getRowKeyTypes());
        assertThat(readyForGCFiles.get(0).getPartitionId()).isEqualTo(fileInfo1.getPartitionId());
        assertThat(readyForGCFiles.get(1).getPartitionId()).isEqualTo(fileInfo2.getPartitionId());
        assertThat(readyForGCFiles.get(0).getFileStatus()).isEqualTo(FileInfo.FileStatus.READY_FOR_GARBAGE_COLLECTION);
        assertThat(readyForGCFiles.get(1).getFileStatus()).isEqualTo(FileInfo.FileStatus.READY_FOR_GARBAGE_COLLECTION);
        // - Check DynamoDBStateStore has correct active files
        List<FileInfo> activeFiles = dynamoStateStore.getActiveFiles()
                .stream()
                .sorted(Comparator.comparing(FileInfo::getFilename))
                .collect(Collectors.toList());
        assertThat(activeFiles.size()).isEqualTo(1);
        FileInfo newFile = new FileInfo();
        newFile.setRowKeyTypes(new ByteArrayType());
        newFile.setFilename(outputFile);
        newFile.setFileStatus(FileInfo.FileStatus.ACTIVE);
        newFile.setPartitionId("1");
        newFile.setNumberOfRecords((long) expectedResults.size());
        byte[] minKey = expectedResults.stream()
                .map(r -> ByteArray.wrap((byte[]) r.get("key")))
                .min(Comparator.naturalOrder())
                .map(b -> b.getArray())
                .get();
        newFile.setMinRowKey(Key.create(minKey));
        byte[] maxKey = expectedResults.stream()
                .map(r -> ByteArray.wrap((byte[]) r.get("key")))
                .max(Comparator.naturalOrder())
                .map(b -> b.getArray())
                .get();
        newFile.setMaxRowKey(Key.create(maxKey));
        activeFiles.get(0).setLastStateStoreUpdateTime(null); // Set to null as we don't know what it should be
        assertThat(activeFiles.get(0)).isEqualTo(newFile);
    }

    @Test
    public void filesShouldMergeCorrectlyWhenSomeAreEmpty() throws IOException, StateStoreException, ObjectFactoryException, IteratorException {
        // Given
        //  - Schema
        Schema schema = new Schema();
        schema.setRowKeyFields(new Field("key", new LongType()));
        schema.setValueFields(new Field("value1", new LongType()), new Field("value2", new LongType()));
        //  - Create two files of sorted data
        String folderName = folder.newFolder().getAbsolutePath();
        String file1 = folderName + "/file1.parquet";
        String file2 = folderName + "/file2.parquet";
        List<String> files = new ArrayList<>();
        files.add(file1);
        files.add(file2);
        FileInfo fileInfo1 = new FileInfo();
        fileInfo1.setRowKeyTypes(new LongType());
        fileInfo1.setFilename(file1);
        fileInfo1.setFileStatus(FileInfo.FileStatus.ACTIVE);
        fileInfo1.setPartitionId("1");
        fileInfo1.setNumberOfRecords(100L);
        fileInfo1.setMinRowKey(Key.create(0L));
        fileInfo1.setMaxRowKey(Key.create(198L));
        FileInfo fileInfo2 = new FileInfo();
        fileInfo2.setRowKeyTypes(new LongType());
        fileInfo2.setFilename(file2);
        fileInfo2.setFileStatus(FileInfo.FileStatus.ACTIVE);
        fileInfo2.setPartitionId("1");
        fileInfo2.setNumberOfRecords(0L);
        // Don't set min/max row keys for files with no lines
        List<FileInfo> fileInfos = new ArrayList<>();
        fileInfos.add(fileInfo1);
        fileInfos.add(fileInfo2);
        String outputFile = folderName + "/file3.parquet";
        SortedMap<Long, Record> data = new TreeMap<>();
        ParquetRecordWriter writer1 = new ParquetRecordWriter(new Path(file1), SchemaConverter.getSchema(schema), schema);
        for (int i = 0; i < 100; i++) {
            Record record = new Record();
            record.put("key", (long) 2 * i);
            record.put("value1", (long) 2 * i);
            record.put("value2", 987654321L);
            writer1.write(record);
            data.put((long) record.get("key"), record);
        }
        writer1.close();
        ParquetRecordWriter writer2 = new ParquetRecordWriter(new Path(file2), SchemaConverter.getSchema(schema), schema);
        //      - File 2 is empty
        writer2.close();
        //  - Create DynamoDBStateStore
        DynamoDBStateStoreCreator dynamoDBStateStoreCreator = new DynamoDBStateStoreCreator("fsmcwsae", schema, dynamoDBClient);
        DynamoDBStateStore dynamoStateStore = dynamoDBStateStoreCreator.create();
        dynamoStateStore.initialise();
        //  - Update Dynamo state store with details of files
        dynamoStateStore.addFiles(Arrays.asList(fileInfo1, fileInfo2));
        //  - Create CompactionJob and update status of files with compactionJob id
        CompactionJob compactionJob = new CompactionJob("table", "compactionJob-1");
        compactionJob.setInputFiles(files);
        compactionJob.setOutputFile(outputFile);
        compactionJob.setPartitionId("1");
        dynamoStateStore.atomicallyUpdateJobStatusOfFiles(compactionJob.getId(), fileInfos);

        // When
        //  - Merge two files
        CompactSortedFiles compactSortedFiles = new CompactSortedFiles(new InstanceProperties(), new ObjectFactory(new InstanceProperties(), null, ""),
                schema, SchemaConverter.getSchema(schema), compactionJob, dynamoStateStore,
                ParquetWriter.DEFAULT_BLOCK_SIZE, ParquetWriter.DEFAULT_PAGE_SIZE, "zstd", 25, 1000);
        compactSortedFiles.compact();

        // Then
        //  - Read output file and check that it contains the right results
        List<Record> results = new ArrayList<>();
        ParquetReaderIterator reader = new ParquetReaderIterator(new ParquetRecordReader(new Path(outputFile), schema));
        while (reader.hasNext()) {
            results.add(new Record(reader.next()));
        }
        reader.close();
        List<Record> expectedResults = new ArrayList<>(data.values());
        assertThat(results).isEqualTo(expectedResults);
        // - Check DynamoDBStateStore has correct ready for GC files
        List<FileInfo> readyForGCFiles = new ArrayList<>();
        dynamoStateStore.getReadyForGCFiles().forEachRemaining(readyForGCFiles::add);
        readyForGCFiles = readyForGCFiles
                .stream()
                .sorted(Comparator.comparing(FileInfo::getFilename))
                .collect(Collectors.toList());
        assertThat(readyForGCFiles.size()).isEqualTo(2);

        assertThat(readyForGCFiles.get(0).getFilename()).isEqualTo(fileInfo1.getFilename());
        assertThat(readyForGCFiles.get(1).getFilename()).isEqualTo(fileInfo2.getFilename());
        assertThat(readyForGCFiles.get(0).getRowKeyTypes()).isEqualTo(fileInfo1.getRowKeyTypes());
        assertThat(readyForGCFiles.get(1).getRowKeyTypes()).isEqualTo(fileInfo2.getRowKeyTypes());
        assertThat(readyForGCFiles.get(0).getPartitionId()).isEqualTo(fileInfo1.getPartitionId());
        assertThat(readyForGCFiles.get(1).getPartitionId()).isEqualTo(fileInfo2.getPartitionId());
        assertThat(readyForGCFiles.get(0).getFileStatus()).isEqualTo(FileInfo.FileStatus.READY_FOR_GARBAGE_COLLECTION);
        assertThat(readyForGCFiles.get(1).getFileStatus()).isEqualTo(FileInfo.FileStatus.READY_FOR_GARBAGE_COLLECTION);

        // - Check DynamoDBStateStore has correct active files
        List<FileInfo> activeFiles = dynamoStateStore.getActiveFiles()
                .stream()
                .sorted(Comparator.comparing(FileInfo::getFilename))
                .collect(Collectors.toList());
        assertThat(activeFiles.size()).isEqualTo(1);
        FileInfo newFile = new FileInfo();
        newFile.setRowKeyTypes(new LongType());
        newFile.setFilename(outputFile);
        newFile.setFileStatus(FileInfo.FileStatus.ACTIVE);
        newFile.setPartitionId("1");
        newFile.setNumberOfRecords((long) expectedResults.size());
        long minKey = expectedResults.stream()
                .map(r -> (long) r.get("key"))
                .min(Comparator.naturalOrder())
                .get();
        newFile.setMinRowKey(Key.create(minKey));
        long maxKey = expectedResults.stream()
                .map(r -> (long) r.get("key"))
                .max(Comparator.naturalOrder())
                .get();
        newFile.setMaxRowKey(Key.create(maxKey));
        activeFiles.get(0).setLastStateStoreUpdateTime(null); // Set to null as we don't know what it should be
        assertThat(activeFiles.get(0)).isEqualTo(newFile);
    }

    @Test
    public void filesShouldMergeCorrectlyWhenAllAreEmpty() throws IOException, StateStoreException, ObjectFactoryException, IteratorException {
        // Given
        //  - Schema
        Schema schema = new Schema();
        schema.setRowKeyFields(new Field("key", new LongType()));
        schema.setValueFields(new Field("value1", new LongType()), new Field("value2", new LongType()));
        //  - Create two empty files
        String folderName = folder.newFolder().getAbsolutePath();
        String file1 = folderName + "/file1.parquet";
        String file2 = folderName + "/file2.parquet";
        List<String> files = new ArrayList<>();
        files.add(file1);
        files.add(file2);
        FileInfo fileInfo1 = new FileInfo();
        fileInfo1.setRowKeyTypes(new LongType());
        fileInfo1.setFilename(file1);
        fileInfo1.setFileStatus(FileInfo.FileStatus.ACTIVE);
        fileInfo1.setPartitionId("1");
        fileInfo1.setNumberOfRecords(0L);
        FileInfo fileInfo2 = new FileInfo();
        fileInfo2.setRowKeyTypes(new LongType());
        fileInfo2.setFilename(file2);
        fileInfo2.setFileStatus(FileInfo.FileStatus.ACTIVE);
        fileInfo2.setPartitionId("1");
        fileInfo2.setNumberOfRecords(0L);
        List<FileInfo> fileInfos = new ArrayList<>();
        fileInfos.add(fileInfo1);
        fileInfos.add(fileInfo2);
        String outputFile = folderName + "/file3.parquet";
        SortedMap<Long, Record> data = new TreeMap<>();
        ParquetRecordWriter writer1 = new ParquetRecordWriter(new Path(file1), SchemaConverter.getSchema(schema), schema);
        writer1.close();
        ParquetRecordWriter writer2 = new ParquetRecordWriter(new Path(file2), SchemaConverter.getSchema(schema), schema);
        writer2.close();
        //  - Create DynamoDBStateStore
        DynamoDBStateStoreCreator dynamoDBStateStoreCreator = new DynamoDBStateStoreCreator("fsmcwaae", schema, dynamoDBClient);
        DynamoDBStateStore dynamoStateStore = dynamoDBStateStoreCreator.create();
        dynamoStateStore.initialise();
        //  - Update Dynamo state store with details of files
        dynamoStateStore.addFiles(Arrays.asList(fileInfo1, fileInfo2));
        //  - Create CompactionJob and update status of files with compactionJob id
        CompactionJob compactionJob = new CompactionJob("table", "compactionJob-1");
        compactionJob.setInputFiles(files);
        compactionJob.setOutputFile(outputFile);
        compactionJob.setPartitionId("1");
        dynamoStateStore.atomicallyUpdateJobStatusOfFiles(compactionJob.getId(), fileInfos);

        // When
        //  - Merge two files
        CompactSortedFiles compactSortedFiles = new CompactSortedFiles(new InstanceProperties(), new ObjectFactory(new InstanceProperties(), null, ""),
                schema, SchemaConverter.getSchema(schema), compactionJob, dynamoStateStore,
                ParquetWriter.DEFAULT_BLOCK_SIZE, ParquetWriter.DEFAULT_PAGE_SIZE, "zstd", 25, 1000);
        compactSortedFiles.compact();

        // Then
        //  - Read output file and check that it contains the right results
        List<Record> results = new ArrayList<>();
        ParquetReaderIterator reader = new ParquetReaderIterator(new ParquetRecordReader(new Path(outputFile), schema));
        while (reader.hasNext()) {
            results.add(new Record(reader.next()));
        }
        reader.close();
        List<Record> expectedResults = new ArrayList<>(data.values());
        assertThat(results).isEqualTo(expectedResults);
        // - Check DynamoDBStateStore has correct ready for GC files
        List<FileInfo> readyForGCFiles = new ArrayList<>();
        dynamoStateStore.getReadyForGCFiles().forEachRemaining(readyForGCFiles::add);
        readyForGCFiles = readyForGCFiles
                .stream()
                .sorted(Comparator.comparing(FileInfo::getFilename))
                .collect(Collectors.toList());
        assertThat(readyForGCFiles.size()).isEqualTo(2);

        assertThat(readyForGCFiles.get(0).getFilename()).isEqualTo(fileInfo1.getFilename());
        assertThat(readyForGCFiles.get(1).getFilename()).isEqualTo(fileInfo2.getFilename());
        assertThat(readyForGCFiles.get(0).getRowKeyTypes()).isEqualTo(fileInfo1.getRowKeyTypes());
        assertThat(readyForGCFiles.get(1).getRowKeyTypes()).isEqualTo(fileInfo2.getRowKeyTypes());
        assertThat(readyForGCFiles.get(0).getPartitionId()).isEqualTo(fileInfo1.getPartitionId());
        assertThat(readyForGCFiles.get(1).getPartitionId()).isEqualTo(fileInfo2.getPartitionId());
        assertThat(readyForGCFiles.get(0).getFileStatus()).isEqualTo(FileInfo.FileStatus.READY_FOR_GARBAGE_COLLECTION);
        assertThat(readyForGCFiles.get(1).getFileStatus()).isEqualTo(FileInfo.FileStatus.READY_FOR_GARBAGE_COLLECTION);
        // - Check DynamoDBStateStore has correct active files
        List<FileInfo> activeFiles = dynamoStateStore.getActiveFiles()
                .stream()
                .sorted(Comparator.comparing(FileInfo::getFilename))
                .collect(Collectors.toList());
        assertThat(activeFiles.size()).isEqualTo(1);
        FileInfo newFile = new FileInfo();
        newFile.setRowKeyTypes(new LongType());
        newFile.setFilename(outputFile);
        newFile.setFileStatus(FileInfo.FileStatus.ACTIVE);
        newFile.setPartitionId("1");
        newFile.setNumberOfRecords(0L);
        newFile.setMinRowKey(null);
        newFile.setMaxRowKey(null);
        activeFiles.get(0).setLastStateStoreUpdateTime(null); // Set to null as we don't know what it should be
        assertThat(activeFiles.get(0)).isEqualTo(newFile);
    }

    @Test
    public void filesShouldMergeAndSplitCorrectlyAndDynamoUpdated() throws IOException, StateStoreException, ObjectFactoryException, IteratorException {
        // Given
        //  - Schema
        Schema schema = new Schema();
        Field field = new Field("key", new LongType());
        schema.setRowKeyFields(field);
        schema.setValueFields(new Field("value1", new LongType()), new Field("value2", new LongType()));
        //  - Create DynamoDBStateStore
        DynamoDBStateStoreCreator dynamoDBStateStoreCreator = new DynamoDBStateStoreCreator("fsmascadu", schema, dynamoDBClient);
        DynamoDBStateStore dynamoStateStore = dynamoDBStateStoreCreator.create();
        dynamoStateStore.initialise();
        //  - Get root partition
        Partition rootPartition = dynamoStateStore.getAllPartitions().get(0);
        //  - Create two files of sorted data
        String folderName = folder.newFolder().getAbsolutePath();
        String file1 = folderName + "/file1.parquet";
        String file2 = folderName + "/file2.parquet";
        List<String> files = new ArrayList<>();
        files.add(file1);
        files.add(file2);
        FileInfo fileInfo1 = new FileInfo();
        fileInfo1.setRowKeyTypes(new LongType());
        fileInfo1.setFilename(file1);
        fileInfo1.setFileStatus(FileInfo.FileStatus.ACTIVE);
        fileInfo1.setPartitionId(rootPartition.getId());
        fileInfo1.setNumberOfRecords(100L);
        fileInfo1.setMinRowKey(Key.create(0L));
        fileInfo1.setMaxRowKey(Key.create(198L));
        FileInfo fileInfo2 = new FileInfo();
        fileInfo2.setRowKeyTypes(new LongType());
        fileInfo2.setFilename(file2);
        fileInfo2.setFileStatus(FileInfo.FileStatus.ACTIVE);
        fileInfo2.setPartitionId(rootPartition.getId());
        fileInfo2.setNumberOfRecords(100L);
        fileInfo2.setMinRowKey(Key.create(1L));
        fileInfo2.setMaxRowKey(Key.create(199L));
        List<FileInfo> fileInfos = new ArrayList<>();
        fileInfos.add(fileInfo1);
        fileInfos.add(fileInfo2);
        String leftOutputFile = folderName + "/file3-left.parquet";
        String rightOutputFile = folderName + "/file3-right.parquet";
        SortedMap<Long, Record> data = new TreeMap<>();
        ParquetRecordWriter writer1 = new ParquetRecordWriter(new Path(file1), SchemaConverter.getSchema(schema), schema);
        for (int i = 0; i < 100; i++) {
            Record record = new Record();
            record.put("key", (long) 2 * i);
            record.put("value1", 1000L);
            record.put("value2", 987654321L);
            writer1.write(record);
            data.put((long) record.get("key"), record);
        }
        writer1.close();
        ParquetRecordWriter writer2 = new ParquetRecordWriter(new Path(file2), SchemaConverter.getSchema(schema), schema);
        for (int i = 0; i < 100; i++) {
            Record record = new Record();
            record.put("key", (long) 2 * i + 1);
            record.put("value1", 1001L);
            record.put("value2", 123456789L);
            writer2.write(record);
            data.put((long) record.get("key"), record);
        }
        writer2.close();
        //  - Split root partition
        rootPartition.setLeafPartition(false);
        Partition leftPartition = new Partition();
        leftPartition.setLeafPartition(true);

        Range leftRange = new Range.RangeFactory(schema).createRange(field, Long.MIN_VALUE, 100L);
        leftPartition.setRegion(new Region(leftRange));
        leftPartition.setId(Long.MIN_VALUE + "---100");
        leftPartition.setParentPartitionId(rootPartition.getId());
        leftPartition.setChildPartitionIds(new ArrayList<>());
        Partition rightPartition = new Partition();
        rightPartition.setLeafPartition(true);
        Range rightRange = new Range.RangeFactory(schema).createRange(field, 100L, null);
        rightPartition.setRegion(new Region(rightRange));
        rightPartition.setId("100---");
        rightPartition.setParentPartitionId(rootPartition.getId());
        rightPartition.setChildPartitionIds(new ArrayList<>());
        rootPartition.setChildPartitionIds(Arrays.asList(leftPartition.getId(), rightPartition.getId()));
        dynamoStateStore.atomicallyUpdatePartitionAndCreateNewOnes(rootPartition, leftPartition, rightPartition);
        //  - Update Dynamo state store with details of files
        dynamoStateStore.addFiles(Arrays.asList(fileInfo1, fileInfo2));
        //  - Create CompactionJob and update status of files with compactionJob id
        CompactionJob compactionJob = new CompactionJob("table", "compactionJob-1");
        compactionJob.setInputFiles(files);
        compactionJob.setOutputFiles(new MutablePair<>(leftOutputFile, rightOutputFile));
        compactionJob.setPartitionId(rootPartition.getId());
        compactionJob.setChildPartitions(Arrays.asList(leftPartition.getId(), rightPartition.getId()));
        compactionJob.setIsSplittingJob(true);
        compactionJob.setSplitPoint(100L);
        compactionJob.setDimension(0);
        dynamoStateStore.atomicallyUpdateJobStatusOfFiles(compactionJob.getId(), fileInfos);

        // When
        //  - Merge two files
        CompactSortedFiles compactSortedFiles = new CompactSortedFiles(new InstanceProperties(), new ObjectFactory(new InstanceProperties(), null, ""),
                schema, SchemaConverter.getSchema(schema), compactionJob, dynamoStateStore,
                ParquetWriter.DEFAULT_BLOCK_SIZE, ParquetWriter.DEFAULT_PAGE_SIZE, "zstd", 25, 1000);
        compactSortedFiles.compact();

        // Then
        //  - Read output files and check that they contains the right results
        List<Record> leftResults = new ArrayList<>();
        ParquetReaderIterator reader = new ParquetReaderIterator(new ParquetRecordReader(new Path(leftOutputFile), schema));
        while (reader.hasNext()) {
            leftResults.add(new Record(reader.next()));
        }
        reader.close();
        List<Record> rightResults = new ArrayList<>();
        reader = new ParquetReaderIterator(new ParquetRecordReader(new Path(rightOutputFile), schema));
        while (reader.hasNext()) {
            rightResults.add(new Record(reader.next()));
        }
        reader.close();
        List<Record> leftExpectedResults = data.values().stream()
                .filter(r -> ((long) r.get("key")) < 100L)
                .collect(Collectors.toList());
        assertThat(leftResults).isEqualTo(leftExpectedResults);
        List<Record> rightExpectedResults = data.values().stream()
                .filter(r -> ((long) r.get("key")) >= 100L)
                .collect(Collectors.toList());
        assertThat(rightResults).isEqualTo(rightExpectedResults);
        // - Check DynamoDBStateStore has correct ready for GC files
        List<FileInfo> readyForGCFiles = new ArrayList<>();
        dynamoStateStore.getReadyForGCFiles().forEachRemaining(readyForGCFiles::add);
        readyForGCFiles = readyForGCFiles
                .stream()
                .sorted(Comparator.comparing(FileInfo::getFilename))
                .collect(Collectors.toList());
        assertThat(readyForGCFiles.size()).isEqualTo(2);

        assertThat(readyForGCFiles.get(0).getFilename()).isEqualTo(fileInfo1.getFilename());
        assertThat(readyForGCFiles.get(1).getFilename()).isEqualTo(fileInfo2.getFilename());
        assertThat(readyForGCFiles.get(0).getRowKeyTypes()).isEqualTo(fileInfo1.getRowKeyTypes());
        assertThat(readyForGCFiles.get(1).getRowKeyTypes()).isEqualTo(fileInfo2.getRowKeyTypes());
        assertThat(readyForGCFiles.get(0).getPartitionId()).isEqualTo(fileInfo1.getPartitionId());
        assertThat(readyForGCFiles.get(1).getPartitionId()).isEqualTo(fileInfo2.getPartitionId());
        assertThat(readyForGCFiles.get(0).getFileStatus()).isEqualTo(FileInfo.FileStatus.READY_FOR_GARBAGE_COLLECTION);
        assertThat(readyForGCFiles.get(1).getFileStatus()).isEqualTo(FileInfo.FileStatus.READY_FOR_GARBAGE_COLLECTION);
        // - Check DynamoDBStateStore has correct active files
        List<FileInfo> activeFiles = dynamoStateStore.getActiveFiles()
                .stream()
                .sorted(Comparator.comparing(FileInfo::getFilename))
                .collect(Collectors.toList());
        assertThat(activeFiles.size()).isEqualTo(2);

        FileInfo leftNewFile = new FileInfo();
        leftNewFile.setRowKeyTypes(new LongType());
        leftNewFile.setFilename(leftOutputFile);
        leftNewFile.setFileStatus(FileInfo.FileStatus.ACTIVE);
        leftNewFile.setPartitionId(leftPartition.getId());
        leftNewFile.setNumberOfRecords((long) leftExpectedResults.size());
        long minKeyLeft = leftExpectedResults.stream()
                .map(r -> (long) r.get("key"))
                .min(Comparator.naturalOrder())
                .get();
        leftNewFile.setMinRowKey(Key.create(minKeyLeft));
        long maxKeyLeft = leftExpectedResults.stream()
                .map(r -> (long) r.get("key"))
                .max(Comparator.naturalOrder())
                .get();
        leftNewFile.setMaxRowKey(Key.create(maxKeyLeft));
        activeFiles.get(0).setLastStateStoreUpdateTime(null); // Set to null as we don't know what it should be
        assertThat(activeFiles.get(0)).isEqualTo(leftNewFile);
        FileInfo rightNewFile = new FileInfo();
        rightNewFile.setRowKeyTypes(new LongType());
        rightNewFile.setFilename(rightOutputFile);
        rightNewFile.setFileStatus(FileInfo.FileStatus.ACTIVE);
        rightNewFile.setPartitionId(rightPartition.getId());
        rightNewFile.setNumberOfRecords((long) rightExpectedResults.size());
        long minKeyRight = rightExpectedResults.stream()
                .map(r -> (long) r.get("key"))
                .min(Comparator.naturalOrder())
                .get();
        rightNewFile.setMinRowKey(Key.create(minKeyRight));
        long maxKeyRight = rightExpectedResults.stream()
                .map(r -> (long) r.get("key"))
                .max(Comparator.naturalOrder())
                .get();
        rightNewFile.setMaxRowKey(Key.create(maxKeyRight));
        activeFiles.get(1).setLastStateStoreUpdateTime(null); // Set to null as we don't know what it should be
        assertThat(activeFiles.get(1)).isEqualTo(rightNewFile);
    }

    @Test
    public void filesShouldMergeAndSplitCorrectlyWith2DimKeySplitOnFirstKey() throws IOException, StateStoreException, ObjectFactoryException, IteratorException {
        // Given
        //  - Schema
        Schema schema = new Schema();
        Field field1 = new Field("key1", new LongType());
        Field field2 = new Field("key2", new StringType());
        schema.setRowKeyFields(field1, field2);
        schema.setValueFields(new Field("value1", new LongType()), new Field("value2", new LongType()));
        //  - Create DynamoDBStateStore
        DynamoDBStateStoreCreator dynamoDBStateStoreCreator = new DynamoDBStateStoreCreator("fsmascw2dksofk", schema, dynamoDBClient);
        DynamoDBStateStore dynamoStateStore = dynamoDBStateStoreCreator.create();
        dynamoStateStore.initialise();
        //  - Get root partition
        Partition rootPartition = dynamoStateStore.getAllPartitions().get(0);
        //  - Create two files of sorted data
        String folderName = folder.newFolder().getAbsolutePath();
        String file1 = folderName + "/file1.parquet";
        String file2 = folderName + "/file2.parquet";
        List<String> files = new ArrayList<>();
        files.add(file1);
        files.add(file2);
        FileInfo fileInfo1 = new FileInfo();
        fileInfo1.setRowKeyTypes(new LongType(), new StringType());
        fileInfo1.setFilename(file1);
        fileInfo1.setFileStatus(FileInfo.FileStatus.ACTIVE);
        fileInfo1.setPartitionId(rootPartition.getId());
        fileInfo1.setNumberOfRecords(100L);
        fileInfo1.setMinRowKey(Key.create(Arrays.asList(0L, "A")));
        fileInfo1.setMaxRowKey(Key.create(Arrays.asList(198L, "A")));
        FileInfo fileInfo2 = new FileInfo();
        fileInfo2.setRowKeyTypes(new LongType(), new StringType());
        fileInfo2.setFilename(file2);
        fileInfo2.setFileStatus(FileInfo.FileStatus.ACTIVE);
        fileInfo2.setPartitionId(rootPartition.getId());
        fileInfo2.setNumberOfRecords(100L);
        fileInfo2.setMinRowKey(Key.create(Arrays.asList(1L, "A")));
        fileInfo2.setMaxRowKey(Key.create(Arrays.asList(199L, "A")));
        List<FileInfo> fileInfos = new ArrayList<>();
        fileInfos.add(fileInfo1);
        fileInfos.add(fileInfo2);
        String leftOutputFile = folderName + "/file3-left.parquet";
        String rightOutputFile = folderName + "/file3-right.parquet";
        List<Record> data = new ArrayList<>();
        ParquetRecordWriter writer1 = new ParquetRecordWriter(new Path(file1), SchemaConverter.getSchema(schema), schema);
        for (int i = 0; i < 100; i++) {
            Record record = new Record();
            record.put("key1", (long) 2 * i);
            record.put("key2", "A");
            record.put("value1", 1000L);
            record.put("value2", 987654321L);
            writer1.write(record);
            data.add(record);
        }
        writer1.close();
        ParquetRecordWriter writer2 = new ParquetRecordWriter(new Path(file2), SchemaConverter.getSchema(schema), schema);
        for (int i = 0; i < 100; i++) {
            Record record = new Record();
            record.put("key1", (long) 2 * i + 1);
            record.put("key2", "A");
            record.put("value1", 1001L);
            record.put("value2", 123456789L);
            writer2.write(record);
            data.add(record);
        }

        List<Record> leftPartitionRecords = data.stream()
                .filter(r -> ((long) r.get("key1")) < 100L)
                .sorted(new RecordComparator(schema))
                .collect(Collectors.toList());
        List<Record> rightPartitionRecords = data.stream()
                .filter(r -> ((long) r.get("key1")) >= 100L)
                .sorted(new RecordComparator(schema))
                .collect(Collectors.toList());
        writer2.close();
        //  - Split root partition
        rootPartition.setLeafPartition(false);
        Partition leftPartition = new Partition();
        leftPartition.setRowKeyTypes(new LongType(), new StringType());
        leftPartition.setLeafPartition(true);
        Range leftRange = new RangeFactory(schema)
                .createRange(field1, leftPartitionRecords.get(0).get("key1"), leftPartitionRecords.get(leftPartitionRecords.size() - 1).get("key1"));
        leftPartition.setRegion(new Region(leftRange));
        leftPartition.setId("left");
        leftPartition.setParentPartitionId(rootPartition.getId());
        leftPartition.setChildPartitionIds(new ArrayList<>());
        Partition rightPartition = new Partition();
        rightPartition.setRowKeyTypes(new LongType(), new StringType());
        rightPartition.setLeafPartition(true);
        Range rightRange = new RangeFactory(schema)
                .createRange(field1, rightPartitionRecords.get(0).get("key1"), rightPartitionRecords.get(leftPartitionRecords.size() - 1).get("key1"));
        rightPartition.setRegion(new Region(rightRange));
        rightPartition.setId("right");
        rightPartition.setParentPartitionId(rootPartition.getId());
        rightPartition.setChildPartitionIds(new ArrayList<>());
        rootPartition.setChildPartitionIds(Arrays.asList(leftPartition.getId(), rightPartition.getId()));
        dynamoStateStore.atomicallyUpdatePartitionAndCreateNewOnes(rootPartition, leftPartition, rightPartition);
        //  - Update Dynamo state store with details of files
        dynamoStateStore.addFiles(Arrays.asList(fileInfo1, fileInfo2));
        //  - Create CompactionJob and update status of files with compactionJob id
        CompactionJob compactionJob = new CompactionJob("table", "compactionJob-1");
        compactionJob.setInputFiles(files);
        compactionJob.setOutputFiles(new MutablePair<>(leftOutputFile, rightOutputFile));
        compactionJob.setPartitionId(rootPartition.getId());
        compactionJob.setChildPartitions(Arrays.asList(leftPartition.getId(), rightPartition.getId()));
        compactionJob.setIsSplittingJob(true);
        compactionJob.setSplitPoint(100L);
        compactionJob.setDimension(0);
        dynamoStateStore.atomicallyUpdateJobStatusOfFiles(compactionJob.getId(), fileInfos);

        // When
        //  - Merge two files
        CompactSortedFiles compactSortedFiles = new CompactSortedFiles(new InstanceProperties(), new ObjectFactory(new InstanceProperties(), null, ""),
                schema, SchemaConverter.getSchema(schema), compactionJob, dynamoStateStore,
                ParquetWriter.DEFAULT_BLOCK_SIZE, ParquetWriter.DEFAULT_PAGE_SIZE, "zstd", 25, 1000);
        compactSortedFiles.compact();

        // Then
        //  - Read output files and check that they contains the right results
        List<Record> leftResults = new ArrayList<>();
        ParquetReaderIterator reader = new ParquetReaderIterator(new ParquetRecordReader(new Path(leftOutputFile), schema));
        while (reader.hasNext()) {
            leftResults.add(new Record(reader.next()));
        }
        reader.close();
        List<Record> rightResults = new ArrayList<>();
        reader = new ParquetReaderIterator(new ParquetRecordReader(new Path(rightOutputFile), schema));
        while (reader.hasNext()) {
            rightResults.add(new Record(reader.next()));
        }
        reader.close();
        List<Record> leftExpectedResults = leftPartitionRecords;
        assertThat(leftResults).isEqualTo(leftExpectedResults);
        List<Record> rightExpectedResults = rightPartitionRecords;
        assertThat(rightResults).isEqualTo(rightExpectedResults);
        // - Check DynamoDBStateStore has correct ready for GC files
        List<FileInfo> readyForGCFiles = new ArrayList<>();
        dynamoStateStore.getReadyForGCFiles().forEachRemaining(readyForGCFiles::add);
        readyForGCFiles = readyForGCFiles
                .stream()
                .sorted(Comparator.comparing(FileInfo::getFilename))
                .collect(Collectors.toList());
        assertThat(readyForGCFiles.size()).isEqualTo(2);

        assertThat(readyForGCFiles.get(0).getFilename()).isEqualTo(fileInfo1.getFilename());
        assertThat(readyForGCFiles.get(1).getFilename()).isEqualTo(fileInfo2.getFilename());
        assertThat(readyForGCFiles.get(0).getRowKeyTypes()).isEqualTo(fileInfo1.getRowKeyTypes());
        assertThat(readyForGCFiles.get(1).getRowKeyTypes()).isEqualTo(fileInfo2.getRowKeyTypes());
        assertThat(readyForGCFiles.get(0).getPartitionId()).isEqualTo(fileInfo1.getPartitionId());
        assertThat(readyForGCFiles.get(1).getPartitionId()).isEqualTo(fileInfo2.getPartitionId());
        assertThat(readyForGCFiles.get(0).getFileStatus()).isEqualTo(FileInfo.FileStatus.READY_FOR_GARBAGE_COLLECTION);
        assertThat(readyForGCFiles.get(1).getFileStatus()).isEqualTo(FileInfo.FileStatus.READY_FOR_GARBAGE_COLLECTION);
        // - Check DynamoDBStateStore has correct active files
        List<FileInfo> activeFiles = dynamoStateStore.getActiveFiles()
                .stream()
                .sorted(Comparator.comparing(FileInfo::getFilename))
                .collect(Collectors.toList());
        assertThat(activeFiles.size()).isEqualTo(2);
        FileInfo leftNewFile = new FileInfo();
        leftNewFile.setRowKeyTypes(new LongType(), new StringType());
        leftNewFile.setFilename(leftOutputFile);
        leftNewFile.setFileStatus(FileInfo.FileStatus.ACTIVE);
        leftNewFile.setPartitionId(leftPartition.getId());
        leftNewFile.setNumberOfRecords((long) leftExpectedResults.size());
        leftNewFile.setMinRowKey(Key.create(leftExpectedResults.get(0).get(schema.getRowKeyFieldNames().get(0))));
        leftNewFile.setMaxRowKey(Key.create(leftExpectedResults.get(leftExpectedResults.size() - 1).get(schema.getRowKeyFieldNames().get(0))));
        activeFiles.get(0).setLastStateStoreUpdateTime(null); // Set to null as we don't know what it should be
        assertThat(activeFiles.get(0)).isEqualTo(leftNewFile);
        FileInfo rightNewFile = new FileInfo();
        rightNewFile.setRowKeyTypes(new LongType(), new StringType());
        rightNewFile.setFilename(rightOutputFile);
        rightNewFile.setFileStatus(FileInfo.FileStatus.ACTIVE);
        rightNewFile.setPartitionId(rightPartition.getId());
        rightNewFile.setNumberOfRecords((long) rightExpectedResults.size());
        rightNewFile.setMinRowKey(Key.create(rightExpectedResults.get(0).get(schema.getRowKeyFieldNames().get(0))));
        rightNewFile.setMaxRowKey(Key.create(rightExpectedResults.get(rightExpectedResults.size() - 1).get(schema.getRowKeyFieldNames().get(0))));
        activeFiles.get(1).setLastStateStoreUpdateTime(null); // Set to null as we don't know what it should be
        assertThat(activeFiles.get(1)).isEqualTo(rightNewFile);
    }

    @Test
    public void filesShouldMergeAndSplitCorrectlyWith2DimKeySplitOnSecondKey() throws IOException, StateStoreException, ObjectFactoryException, IteratorException {
        // Given
        //  - Schema
        Schema schema = new Schema();
        Field field1 = new Field("key1", new LongType());
        Field field2 = new Field("key2", new StringType());
        schema.setRowKeyFields(field1, field2);
        schema.setValueFields(new Field("value1", new LongType()), new Field("value2", new LongType()));
        //  - Create DynamoDBStateStore
        DynamoDBStateStoreCreator dynamoDBStateStoreCreator = new DynamoDBStateStoreCreator("fsmascw2dksosk", schema, dynamoDBClient);
        DynamoDBStateStore dynamoStateStore = dynamoDBStateStoreCreator.create();
        dynamoStateStore.initialise();
        //  - Get root partition
        Partition rootPartition = dynamoStateStore.getAllPartitions().get(0);
        //  - Create two files of sorted data
        String folderName = folder.newFolder().getAbsolutePath();
        String file1 = folderName + "/file1.parquet";
        String file2 = folderName + "/file2.parquet";
        List<String> files = new ArrayList<>();
        files.add(file1);
        files.add(file2);
        FileInfo fileInfo1 = new FileInfo();
        fileInfo1.setRowKeyTypes(new LongType(), new StringType());
        fileInfo1.setFilename(file1);
        fileInfo1.setFileStatus(FileInfo.FileStatus.ACTIVE);
        fileInfo1.setPartitionId(rootPartition.getId());
        fileInfo1.setNumberOfRecords(100L);
        fileInfo1.setMinRowKey(Key.create(Arrays.asList(0L, "A")));
        fileInfo1.setMaxRowKey(Key.create(Arrays.asList(198L, "B")));
        FileInfo fileInfo2 = new FileInfo();
        fileInfo2.setRowKeyTypes(new LongType(), new StringType());
        fileInfo2.setFilename(file2);
        fileInfo2.setFileStatus(FileInfo.FileStatus.ACTIVE);
        fileInfo2.setPartitionId(rootPartition.getId());
        fileInfo2.setNumberOfRecords(100L);
        fileInfo2.setMinRowKey(Key.create(Arrays.asList(1L, "A")));
        fileInfo2.setMaxRowKey(Key.create(Arrays.asList(199L, "B")));
        List<FileInfo> fileInfos = new ArrayList<>();
        fileInfos.add(fileInfo1);
        fileInfos.add(fileInfo2);
        String leftOutputFile = folderName + "/file3-left.parquet";
        String rightOutputFile = folderName + "/file3-right.parquet";
        List<Record> data = new ArrayList<>();
        ParquetRecordWriter writer1 = new ParquetRecordWriter(new Path(file1), SchemaConverter.getSchema(schema), schema);
        for (int i = 0; i < 100; i++) {
            Record record = new Record();
            record.put("key1", (long) 2 * i);
            record.put("key2", i % 2 == 0 ? "A" : "B");
            record.put("value1", 1000L);
            record.put("value2", 987654321L);
            writer1.write(record);
            data.add(record);
        }
        writer1.close();
        ParquetRecordWriter writer2 = new ParquetRecordWriter(new Path(file2), SchemaConverter.getSchema(schema), schema);
        for (int i = 0; i < 100; i++) {
            Record record = new Record();
            record.put("key1", (long) 2 * i + 1);
            record.put("key2", i % 2 == 0 ? "A" : "B");
            record.put("value1", 1001L);
            record.put("value2", 123456789L);
            writer2.write(record);
            data.add(record);
        }

        List<Record> leftPartitionRecords = data.stream()
                .filter(r -> ((String) r.get("key2")).compareTo("A2") < 0)
                .sorted(new RecordComparator(schema))
                .collect(Collectors.toList());
        List<Record> rightPartitionRecords = data.stream()
                .filter(r -> ((String) r.get("key2")).compareTo("A2") >= 0)
                .sorted(new RecordComparator(schema))
                .collect(Collectors.toList());
        writer2.close();
        //  - Split root partition
        rootPartition.setLeafPartition(false);
        Partition leftPartition = new Partition();
        leftPartition.setRowKeyTypes(new LongType(), new StringType());
        leftPartition.setLeafPartition(true);
        Range leftRange = new RangeFactory(schema)
                .createRange(field1, leftPartitionRecords.get(0).get("key1"), leftPartitionRecords.get(leftPartitionRecords.size() - 1).get("key1"));
        leftPartition.setRegion(new Region(leftRange));
        leftPartition.setId("left");
        leftPartition.setParentPartitionId(rootPartition.getId());
        leftPartition.setChildPartitionIds(new ArrayList<>());
        Partition rightPartition = new Partition();
        rightPartition.setRowKeyTypes(new LongType(), new StringType());
        rightPartition.setLeafPartition(true);
        Range rightRange = new RangeFactory(schema)
                .createRange(field1, rightPartitionRecords.get(0).get("key1"), rightPartitionRecords.get(leftPartitionRecords.size() - 1).get("key1"));
        rightPartition.setRegion(new Region(rightRange));
        rightPartition.setId("right");
        rightPartition.setParentPartitionId(rootPartition.getId());
        rightPartition.setChildPartitionIds(new ArrayList<>());
        rootPartition.setChildPartitionIds(Arrays.asList(leftPartition.getId(), rightPartition.getId()));
        dynamoStateStore.atomicallyUpdatePartitionAndCreateNewOnes(rootPartition, leftPartition, rightPartition);
        //  - Update Dynamo state store with details of files
        dynamoStateStore.addFiles(Arrays.asList(fileInfo1, fileInfo2));
        //  - Create CompactionJob and update status of files with compactionJob id
        CompactionJob compactionJob = new CompactionJob("table", "compactionJob-1");
        compactionJob.setInputFiles(files);
        compactionJob.setOutputFiles(new MutablePair<>(leftOutputFile, rightOutputFile));
        compactionJob.setPartitionId(rootPartition.getId());
        compactionJob.setChildPartitions(Arrays.asList(leftPartition.getId(), rightPartition.getId()));
        compactionJob.setIsSplittingJob(true);
        compactionJob.setSplitPoint("A2");
        compactionJob.setDimension(1);
        dynamoStateStore.atomicallyUpdateJobStatusOfFiles(compactionJob.getId(), fileInfos);

        // When
        //  - Merge two files
        CompactSortedFiles compactSortedFiles = new CompactSortedFiles(new InstanceProperties(), new ObjectFactory(new InstanceProperties(), null, ""),
                schema, SchemaConverter.getSchema(schema), compactionJob, dynamoStateStore,
                ParquetWriter.DEFAULT_BLOCK_SIZE, ParquetWriter.DEFAULT_PAGE_SIZE, "zstd", 25, 1000);
        compactSortedFiles.compact();

        // Then
        //  - Read output files and check that they contains the right results
        List<Record> leftResults = new ArrayList<>();
        ParquetReaderIterator reader = new ParquetReaderIterator(new ParquetRecordReader(new Path(leftOutputFile), schema));
        while (reader.hasNext()) {
            leftResults.add(new Record(reader.next()));
        }
        reader.close();
        List<Record> rightResults = new ArrayList<>();
        reader = new ParquetReaderIterator(new ParquetRecordReader(new Path(rightOutputFile), schema));
        while (reader.hasNext()) {
            rightResults.add(new Record(reader.next()));
        }
        reader.close();
        List<Record> leftExpectedResults = leftPartitionRecords;
        assertThat(leftResults).isEqualTo(leftExpectedResults);
        List<Record> rightExpectedResults = rightPartitionRecords;
        assertThat(rightResults).isEqualTo(rightExpectedResults);
        // - Check DynamoDBStateStore has correct ready for GC files
        List<FileInfo> readyForGCFiles = new ArrayList<>();
        dynamoStateStore.getReadyForGCFiles().forEachRemaining(readyForGCFiles::add);
        readyForGCFiles = readyForGCFiles
                .stream()
                .sorted(Comparator.comparing(FileInfo::getFilename))
                .collect(Collectors.toList());
        assertThat(readyForGCFiles.size()).isEqualTo(2);

        assertThat(readyForGCFiles.get(0).getFilename()).isEqualTo(fileInfo1.getFilename());
        assertThat(readyForGCFiles.get(1).getFilename()).isEqualTo(fileInfo2.getFilename());
        assertThat(readyForGCFiles.get(0).getRowKeyTypes()).isEqualTo(fileInfo1.getRowKeyTypes());
        assertThat(readyForGCFiles.get(1).getRowKeyTypes()).isEqualTo(fileInfo2.getRowKeyTypes());
        assertThat(readyForGCFiles.get(0).getPartitionId()).isEqualTo(fileInfo1.getPartitionId());
        assertThat(readyForGCFiles.get(1).getPartitionId()).isEqualTo(fileInfo2.getPartitionId());
        assertThat(readyForGCFiles.get(0).getFileStatus()).isEqualTo(FileInfo.FileStatus.READY_FOR_GARBAGE_COLLECTION);
        assertThat(readyForGCFiles.get(1).getFileStatus()).isEqualTo(FileInfo.FileStatus.READY_FOR_GARBAGE_COLLECTION);
        // - Check DynamoDBStateStore has correct active files
        List<FileInfo> activeFiles = dynamoStateStore.getActiveFiles()
                .stream()
                .sorted(Comparator.comparing(FileInfo::getFilename))
                .collect(Collectors.toList());
        assertThat(activeFiles.size()).isEqualTo(2);
        FileInfo leftNewFile = new FileInfo();
        leftNewFile.setRowKeyTypes(new LongType(), new StringType());
        leftNewFile.setFilename(leftOutputFile);
        leftNewFile.setFileStatus(FileInfo.FileStatus.ACTIVE);
        leftNewFile.setPartitionId(leftPartition.getId());
        leftNewFile.setNumberOfRecords((long) leftExpectedResults.size());
        leftNewFile.setMinRowKey(Key.create(leftExpectedResults.get(0).get(schema.getRowKeyFieldNames().get(0))));
        leftNewFile.setMaxRowKey(Key.create(leftExpectedResults.get(leftExpectedResults.size() - 1).get(schema.getRowKeyFieldNames().get(0))));
        activeFiles.get(0).setLastStateStoreUpdateTime(null); // Set to null as we don't know what it should be
        assertThat(activeFiles.get(0)).isEqualTo(leftNewFile);
        FileInfo rightNewFile = new FileInfo();
        rightNewFile.setRowKeyTypes(new LongType(), new StringType());
        rightNewFile.setFilename(rightOutputFile);
        rightNewFile.setFileStatus(FileInfo.FileStatus.ACTIVE);
        rightNewFile.setPartitionId(rightPartition.getId());
        rightNewFile.setNumberOfRecords((long) rightExpectedResults.size());
        rightNewFile.setMinRowKey(Key.create(rightExpectedResults.get(0).get(schema.getRowKeyFieldNames().get(0))));
        rightNewFile.setMaxRowKey(Key.create(rightExpectedResults.get(rightExpectedResults.size() - 1).get(schema.getRowKeyFieldNames().get(0))));
        activeFiles.get(1).setLastStateStoreUpdateTime(null); // Set to null as we don't know what it should be
        assertThat(activeFiles.get(1)).isEqualTo(rightNewFile);
    }

    @Test
    public void filesShouldMergeAndSplitCorrectlyWhenOneChildFileIsEmpty() throws IOException, StateStoreException, ObjectFactoryException, IteratorException {
        // Given
        //  - Schema
        Schema schema = new Schema();
        Field field = new Field("key", new LongType());
        schema.setRowKeyFields(field);
        schema.setValueFields(new Field("value1", new LongType()), new Field("value2", new LongType()));
        //  - Create DynamoDBStateStore
        DynamoDBStateStoreCreator dynamoDBStateStoreCreator = new DynamoDBStateStoreCreator("fsmascwocfie", schema, dynamoDBClient);
        DynamoDBStateStore dynamoStateStore = dynamoDBStateStoreCreator.create();
        dynamoStateStore.initialise();
        //  - Get root partition
        Partition rootPartition = dynamoStateStore.getAllPartitions().get(0);
        //  - Create two files of sorted data
        String folderName = folder.newFolder().getAbsolutePath();
        String file1 = folderName + "/file1.parquet";
        String file2 = folderName + "/file2.parquet";
        List<String> files = new ArrayList<>();
        files.add(file1);
        files.add(file2);
        FileInfo fileInfo1 = new FileInfo();
        fileInfo1.setRowKeyTypes(new LongType());
        fileInfo1.setFilename(file1);
        fileInfo1.setFileStatus(FileInfo.FileStatus.ACTIVE);
        fileInfo1.setPartitionId(rootPartition.getId());
        fileInfo1.setNumberOfRecords(100L);
        fileInfo1.setMinRowKey(Key.create(0L));
        fileInfo1.setMaxRowKey(Key.create(198L));
        FileInfo fileInfo2 = new FileInfo();
        fileInfo2.setRowKeyTypes(new LongType());
        fileInfo2.setFilename(file2);
        fileInfo2.setFileStatus(FileInfo.FileStatus.ACTIVE);
        fileInfo2.setPartitionId(rootPartition.getId());
        fileInfo2.setNumberOfRecords(100L);
        fileInfo2.setMinRowKey(Key.create(1L));
        fileInfo2.setMaxRowKey(Key.create(199L));
        List<FileInfo> fileInfos = new ArrayList<>();
        fileInfos.add(fileInfo1);
        fileInfos.add(fileInfo2);
        String leftOutputFile = folderName + "/file3-left.parquet";
        String rightOutputFile = folderName + "/file3-right.parquet";
        SortedMap<Long, Record> data = new TreeMap<>();
        //  - All data in file1 and file2 has key < 100 so after the splitting compaction the right file should be empty
        ParquetRecordWriter writer1 = new ParquetRecordWriter(new Path(file1), SchemaConverter.getSchema(schema), schema);
        for (int i = 0; i < 50; i++) {
            Record record = new Record();
            record.put("key", (long) 2 * i);
            record.put("value1", 1000L);
            record.put("value2", 987654321L);
            writer1.write(record);
            data.put((long) record.get("key"), record);
        }
        writer1.close();
        ParquetRecordWriter writer2 = new ParquetRecordWriter(new Path(file2), SchemaConverter.getSchema(schema), schema);
        for (int i = 0; i < 50; i++) {
            Record record = new Record();
            record.put("key", (long) 2 * i + 1);
            record.put("value1", 1001L);
            record.put("value2", 123456789L);
            writer2.write(record);
            data.put((long) record.get("key"), record);
        }
        writer2.close();
        //  - Split root partition
        rootPartition.setLeafPartition(false);
        Partition leftPartition = new Partition();
        leftPartition.setLeafPartition(true);
        Range leftRange = new RangeFactory(schema).createRange(field, Long.MIN_VALUE, 100L);
        leftPartition.setRegion(new Region(leftRange));
        leftPartition.setId(Long.MIN_VALUE + "---100");
        leftPartition.setParentPartitionId(rootPartition.getId());
        leftPartition.setChildPartitionIds(new ArrayList<>());
        Partition rightPartition = new Partition();
        rightPartition.setLeafPartition(true);
        Range rightRange = new RangeFactory(schema).createRange(field, 100L, null);
        rightPartition.setRegion(new Region(rightRange));
        rightPartition.setId("100---");
        rightPartition.setParentPartitionId(rootPartition.getId());
        rightPartition.setChildPartitionIds(new ArrayList<>());
        rootPartition.setChildPartitionIds(Arrays.asList(leftPartition.getId(), rightPartition.getId()));
        dynamoStateStore.atomicallyUpdatePartitionAndCreateNewOnes(rootPartition, leftPartition, rightPartition);
        //  - Update Dynamo state store with details of files
        dynamoStateStore.addFiles(Arrays.asList(fileInfo1, fileInfo2));
        //  - Create CompactionJob and update status of files with compactionJob id
        CompactionJob compactionJob = new CompactionJob("table", "compactionJob-1");
        compactionJob.setInputFiles(files);
        compactionJob.setOutputFiles(new MutablePair<>(leftOutputFile, rightOutputFile));
        compactionJob.setPartitionId(rootPartition.getId());
        compactionJob.setChildPartitions(Arrays.asList(leftPartition.getId(), rightPartition.getId()));
        compactionJob.setIsSplittingJob(true);
        compactionJob.setSplitPoint(100L);
        compactionJob.setDimension(0);
        dynamoStateStore.atomicallyUpdateJobStatusOfFiles(compactionJob.getId(), fileInfos);

        // When
        //  - Merge two files
        CompactSortedFiles compactSortedFiles = new CompactSortedFiles(new InstanceProperties(), new ObjectFactory(new InstanceProperties(), null, ""),
                schema, SchemaConverter.getSchema(schema), compactionJob, dynamoStateStore,
                ParquetWriter.DEFAULT_BLOCK_SIZE, ParquetWriter.DEFAULT_PAGE_SIZE, "zstd", 25, 1000);
        compactSortedFiles.compact();

        // Then
        //  - Read output files and check that they contain the right results
        List<Record> leftResults = new ArrayList<>();
        ParquetReaderIterator reader = new ParquetReaderIterator(new ParquetRecordReader(new Path(leftOutputFile), schema));
        while (reader.hasNext()) {
            leftResults.add(new Record(reader.next()));
        }
        reader.close();
        List<Record> rightResults = new ArrayList<>();
        reader = new ParquetReaderIterator(new ParquetRecordReader(new Path(rightOutputFile), schema));
        while (reader.hasNext()) {
            rightResults.add(new Record(reader.next()));
        }
        reader.close();
        List<Record> leftExpectedResults = data.values().stream()
                .filter(r -> ((long) r.get("key")) < 100L)
                .collect(Collectors.toList());
        assertThat(leftResults).isEqualTo(leftExpectedResults);
        assertThat(rightResults).isEqualTo(Collections.emptyList());
        // - Check DynamoDBStateStore has correct ready for GC files
        List<FileInfo> readyForGCFiles = new ArrayList<>();
        dynamoStateStore.getReadyForGCFiles().forEachRemaining(readyForGCFiles::add);
        readyForGCFiles = readyForGCFiles
                .stream()
                .sorted(Comparator.comparing(FileInfo::getFilename))
                .collect(Collectors.toList());
        assertThat(readyForGCFiles.size()).isEqualTo(2);

        assertThat(readyForGCFiles.get(0).getFilename()).isEqualTo(fileInfo1.getFilename());
        assertThat(readyForGCFiles.get(1).getFilename()).isEqualTo(fileInfo2.getFilename());
        assertThat(readyForGCFiles.get(0).getRowKeyTypes()).isEqualTo(fileInfo1.getRowKeyTypes());
        assertThat(readyForGCFiles.get(1).getRowKeyTypes()).isEqualTo(fileInfo2.getRowKeyTypes());
        assertThat(readyForGCFiles.get(0).getPartitionId()).isEqualTo(fileInfo1.getPartitionId());
        assertThat(readyForGCFiles.get(1).getPartitionId()).isEqualTo(fileInfo2.getPartitionId());
        assertThat(readyForGCFiles.get(0).getFileStatus()).isEqualTo(FileInfo.FileStatus.READY_FOR_GARBAGE_COLLECTION);
        assertThat(readyForGCFiles.get(1).getFileStatus()).isEqualTo(FileInfo.FileStatus.READY_FOR_GARBAGE_COLLECTION);
        // - Check DynamoDBStateStore has correct active files
        List<FileInfo> activeFiles = dynamoStateStore.getActiveFiles()
                .stream()
                .sorted(Comparator.comparing(FileInfo::getFilename))
                .collect(Collectors.toList());
        assertThat(activeFiles.size()).isEqualTo(2);

        FileInfo leftNewFile = new FileInfo();
        leftNewFile.setRowKeyTypes(new LongType());
        leftNewFile.setFilename(leftOutputFile);
        leftNewFile.setFileStatus(FileInfo.FileStatus.ACTIVE);
        leftNewFile.setPartitionId(leftPartition.getId());
        leftNewFile.setNumberOfRecords((long) leftExpectedResults.size());
        long minKeyLeft = leftExpectedResults.stream()
                .map(r -> (long) r.get("key"))
                .min(Comparator.naturalOrder())
                .get();
        leftNewFile.setMinRowKey(Key.create(minKeyLeft));
        long maxKeyLeft = leftExpectedResults.stream()
                .map(r -> (long) r.get("key"))
                .max(Comparator.naturalOrder())
                .get();
        leftNewFile.setMaxRowKey(Key.create(maxKeyLeft));
        activeFiles.get(0).setLastStateStoreUpdateTime(null); // Set to null as we don't know what it should be
        assertThat(activeFiles.get(0)).isEqualTo(leftNewFile);
        FileInfo rightNewFile = new FileInfo();
        rightNewFile.setRowKeyTypes(new LongType());
        rightNewFile.setFilename(rightOutputFile);
        rightNewFile.setFileStatus(FileInfo.FileStatus.ACTIVE);
        rightNewFile.setPartitionId(rightPartition.getId());
        rightNewFile.setNumberOfRecords(0L);
        rightNewFile.setMinRowKey(null);
        rightNewFile.setMaxRowKey(null);
        activeFiles.get(1).setLastStateStoreUpdateTime(null); // Set to null as we don't know what it should be
        assertThat(activeFiles.get(1)).isEqualTo(rightNewFile);
    }

    @Test
    public void filesShouldMergeAndApplyIteratorCorrectlyLongKey() throws IOException, StateStoreException, ObjectFactoryException, IteratorException {
        // Given
        //  - Schema
        Schema schema = new Schema();
        schema.setRowKeyFields(new Field("key", new LongType()));
        schema.setValueFields(new Field("timestamp", new LongType()), new Field("value", new LongType()));
        //  - Create two files of sorted data
        String folderName = folder.newFolder().getAbsolutePath();
        String file1 = folderName + "/file1.parquet";
        String file2 = folderName + "/file2.parquet";
        List<String> files = new ArrayList<>();
        files.add(file1);
        files.add(file2);
        FileInfo fileInfo1 = new FileInfo();
        fileInfo1.setRowKeyTypes(new LongType());
        fileInfo1.setFilename(file1);
        fileInfo1.setFileStatus(FileInfo.FileStatus.ACTIVE);
        fileInfo1.setPartitionId("1");
        fileInfo1.setNumberOfRecords(100L);
        fileInfo1.setMinRowKey(Key.create(0L));
        fileInfo1.setMaxRowKey(Key.create(198L));
        FileInfo fileInfo2 = new FileInfo();
        fileInfo2.setRowKeyTypes(new LongType());
        fileInfo2.setFilename(file2);
        fileInfo2.setFileStatus(FileInfo.FileStatus.ACTIVE);
        fileInfo2.setPartitionId("1");
        fileInfo2.setNumberOfRecords(100L);
        fileInfo2.setMinRowKey(Key.create(1L));
        fileInfo2.setMaxRowKey(Key.create(199L));
        List<FileInfo> fileInfos = new ArrayList<>();
        fileInfos.add(fileInfo1);
        fileInfos.add(fileInfo2);
        String outputFile = folderName + "/file3.parquet";
        SortedMap<Long, Record> data = new TreeMap<>();
        ParquetRecordWriter writer1 = new ParquetRecordWriter(new Path(file1), SchemaConverter.getSchema(schema), schema);
        for (int i = 0; i < 100; i++) {
            Record record = new Record();
            record.put("key", (long) 2 * i);
            record.put("timestamp", i % 2 == 0 ? System.currentTimeMillis() : 0L);
            record.put("value", 987654321L);
            writer1.write(record);
            data.put((long) record.get("key"), record);
        }
        writer1.close();
        ParquetRecordWriter writer2 = new ParquetRecordWriter(new Path(file2), SchemaConverter.getSchema(schema), schema);
        for (int i = 0; i < 100; i++) {
            Record record = new Record();
            record.put("key", (long) 2 * i + 1);
            record.put("timestamp", i % 2 == 0 ? System.currentTimeMillis() : 0L);
            record.put("value", 123456789L);
            writer2.write(record);
            data.put((long) record.get("key"), record);
        }
        writer2.close();
        //  - Create DynamoDBStateStore
        DynamoDBStateStoreCreator dynamoDBStateStoreCreator = new DynamoDBStateStoreCreator("fsmaaiclk", schema, dynamoDBClient);
        DynamoDBStateStore dynamoStateStore = dynamoDBStateStoreCreator.create();
        dynamoStateStore.initialise();
        //  - Update Dynamo state store with details of files
        dynamoStateStore.addFiles(Arrays.asList(fileInfo1, fileInfo2));

        //  - Create CompactionJob and update status of files with compactionJob id
        CompactionJob compactionJob = new CompactionJob("table", "compactionJob-1");
        compactionJob.setInputFiles(files);
        compactionJob.setOutputFile(outputFile);
        compactionJob.setPartitionId("1");
        compactionJob.setIsSplittingJob(false);
        compactionJob.setIteratorClassName(AgeOffIterator.class.getName());
        compactionJob.setIteratorConfig("timestamp,1000000");
        dynamoStateStore.atomicallyUpdateJobStatusOfFiles(compactionJob.getId(), fileInfos);

        // When
        //  - Merge two files
        CompactSortedFiles compactSortedFiles = new CompactSortedFiles(new InstanceProperties(), new ObjectFactory(new InstanceProperties(), null, ""),
                schema, SchemaConverter.getSchema(schema), compactionJob, dynamoStateStore,
                ParquetWriter.DEFAULT_BLOCK_SIZE, ParquetWriter.DEFAULT_PAGE_SIZE, "zstd", 25, 1000);
        compactSortedFiles.compact();

        // Then
        //  - Read output file and check that it contains the right results
        List<Record> results = new ArrayList<>();
        ParquetReaderIterator reader = new ParquetReaderIterator(new ParquetRecordReader(new Path(outputFile), schema));
        while (reader.hasNext()) {
            results.add(new Record(reader.next()));
        }
        reader.close();
        List<Record> expectedResults = data
                .values()
                .stream()
                .filter(r -> System.currentTimeMillis() - ((long) r.get("timestamp")) < 1000000)
                .collect(Collectors.toList());
        assertThat(results).isEqualTo(expectedResults);
        // - Check DynamoDBStateStore has correct ready for GC files
        List<FileInfo> readyForGCFiles = new ArrayList<>();
        dynamoStateStore.getReadyForGCFiles().forEachRemaining(readyForGCFiles::add);
        readyForGCFiles = readyForGCFiles
                .stream()
                .sorted(Comparator.comparing(FileInfo::getFilename))
                .collect(Collectors.toList());
        assertThat(readyForGCFiles.size()).isEqualTo(2);

        assertThat(readyForGCFiles.get(0).getFilename()).isEqualTo(fileInfo1.getFilename());
        assertThat(readyForGCFiles.get(1).getFilename()).isEqualTo(fileInfo2.getFilename());
        assertThat(readyForGCFiles.get(0).getRowKeyTypes()).isEqualTo(fileInfo1.getRowKeyTypes());
        assertThat(readyForGCFiles.get(1).getRowKeyTypes()).isEqualTo(fileInfo2.getRowKeyTypes());
        assertThat(readyForGCFiles.get(0).getPartitionId()).isEqualTo(fileInfo1.getPartitionId());
        assertThat(readyForGCFiles.get(1).getPartitionId()).isEqualTo(fileInfo2.getPartitionId());
        assertThat(readyForGCFiles.get(0).getFileStatus()).isEqualTo(FileInfo.FileStatus.READY_FOR_GARBAGE_COLLECTION);
        assertThat(readyForGCFiles.get(1).getFileStatus()).isEqualTo(FileInfo.FileStatus.READY_FOR_GARBAGE_COLLECTION);
        // - Check DynamoDBStateStore has correct active files
        List<FileInfo> activeFiles = dynamoStateStore.getActiveFiles()
                .stream()
                .sorted(Comparator.comparing(FileInfo::getFilename))
                .collect(Collectors.toList());
        ;
        assertThat(activeFiles.size()).isEqualTo(1);
        FileInfo newFile = new FileInfo();
        newFile.setRowKeyTypes(new LongType());
        newFile.setFilename(outputFile);
        newFile.setFileStatus(FileInfo.FileStatus.ACTIVE);
        newFile.setPartitionId("1");
        newFile.setNumberOfRecords((long) expectedResults.size());
        long minKey = expectedResults.stream()
                .map(r -> (long) r.get("key"))
                .min(Comparator.naturalOrder())
                .get();
        newFile.setMinRowKey(Key.create(minKey));
        long maxKey = expectedResults.stream()
                .map(r -> (long) r.get("key"))
                .max(Comparator.naturalOrder())
                .get();
        newFile.setMaxRowKey(Key.create(maxKey));
        activeFiles.get(0).setLastStateStoreUpdateTime(null); // Set to null as we don't know what it should be
        assertThat(activeFiles.get(0)).isEqualTo(newFile);
    }

    @Test
    public void filesShouldMergeAndSplitAndApplyIteratorCorrectlyLongKey() throws IOException, StateStoreException, ObjectFactoryException, IteratorException {
        // Given
        //  - Schema
        Schema schema = new Schema();
        Field field = new Field("key", new LongType());
        schema.setRowKeyFields(field);
        schema.setValueFields(new Field("timestamp", new LongType()), new Field("value", new LongType()));
        //  - Create DynamoDBStateStore
        DynamoDBStateStoreCreator dynamoDBStateStoreCreator = new DynamoDBStateStoreCreator("fsmasaaicadu", schema, dynamoDBClient);
        DynamoDBStateStore dynamoStateStore = dynamoDBStateStoreCreator.create();
        dynamoStateStore.initialise();
        //  - Get root partition
        Partition rootPartition = dynamoStateStore.getAllPartitions().get(0);
        //  - Create two files of sorted data
        String folderName = folder.newFolder().getAbsolutePath();
        String file1 = folderName + "/file1.parquet";
        String file2 = folderName + "/file2.parquet";
        List<String> files = new ArrayList<>();
        files.add(file1);
        files.add(file2);
        FileInfo fileInfo1 = new FileInfo();
        fileInfo1.setRowKeyTypes(new LongType());
        fileInfo1.setFilename(file1);
        fileInfo1.setFileStatus(FileInfo.FileStatus.ACTIVE);
        fileInfo1.setPartitionId(rootPartition.getId());
        fileInfo1.setNumberOfRecords(100L);
        fileInfo1.setMinRowKey(Key.create(0L));
        fileInfo1.setMaxRowKey(Key.create(198L));
        FileInfo fileInfo2 = new FileInfo();
        fileInfo2.setRowKeyTypes(new LongType());
        fileInfo2.setFilename(file2);
        fileInfo2.setFileStatus(FileInfo.FileStatus.ACTIVE);
        fileInfo2.setPartitionId(rootPartition.getId());
        fileInfo2.setNumberOfRecords(100L);
        fileInfo2.setMinRowKey(Key.create(1L));
        fileInfo2.setMaxRowKey(Key.create(199L));
        List<FileInfo> fileInfos = new ArrayList<>();
        fileInfos.add(fileInfo1);
        fileInfos.add(fileInfo2);
        String leftOutputFile = folderName + "/file3-left.parquet";
        String rightOutputFile = folderName + "/file3-right.parquet";
        SortedMap<Long, Record> data = new TreeMap<>();
        ParquetRecordWriter writer1 = new ParquetRecordWriter(new Path(file1), SchemaConverter.getSchema(schema), schema);
        for (int i = 0; i < 100; i++) {
            Record record = new Record();
            record.put("key", (long) 2 * i);
            record.put("timestamp", i % 2 == 0 ? System.currentTimeMillis() : 0L);
            record.put("value", 987654321L);
            writer1.write(record);
            data.put((long) record.get("key"), record);
        }
        writer1.close();
        ParquetRecordWriter writer2 = new ParquetRecordWriter(new Path(file2), SchemaConverter.getSchema(schema), schema);
        for (int i = 0; i < 100; i++) {
            Record record = new Record();
            record.put("key", (long) 2 * i + 1);
            record.put("timestamp", i % 2 == 0 ? System.currentTimeMillis() : 0L);
            record.put("value", 123456789L);
            writer2.write(record);
            data.put((long) record.get("key"), record);
        }
        writer2.close();
        //  - Split root partition
        rootPartition.setLeafPartition(false);
        Partition leftPartition = new Partition();
        leftPartition.setLeafPartition(true);
        Range leftRange = new RangeFactory(schema).createRange(field, Long.MIN_VALUE, 100L);
        leftPartition.setRegion(new Region(leftRange));
        leftPartition.setId(Long.MIN_VALUE + "---100");
        leftPartition.setParentPartitionId(rootPartition.getId());
        leftPartition.setChildPartitionIds(new ArrayList<>());
        Partition rightPartition = new Partition();
        rightPartition.setLeafPartition(true);
        Range rightRange = new RangeFactory(schema).createRange(field, 100L, null);
        rightPartition.setRegion(new Region(rightRange));
        rightPartition.setId("100---");
        rightPartition.setParentPartitionId(rootPartition.getId());
        rightPartition.setChildPartitionIds(new ArrayList<>());
        rootPartition.setChildPartitionIds(Arrays.asList(leftPartition.getId(), rightPartition.getId()));
        dynamoStateStore.atomicallyUpdatePartitionAndCreateNewOnes(rootPartition, leftPartition, rightPartition);
        //  - Update Dynamo state store with details of files
        dynamoStateStore.addFiles(Arrays.asList(fileInfo1, fileInfo2));
        //  - Create CompactionJob and update status of files with compactionJob id
        CompactionJob compactionJob = new CompactionJob("table", "compactionJob-1");
        compactionJob.setInputFiles(files);
        compactionJob.setOutputFiles(new MutablePair<>(leftOutputFile, rightOutputFile));
        compactionJob.setPartitionId(rootPartition.getId());
        compactionJob.setChildPartitions(Arrays.asList(leftPartition.getId(), rightPartition.getId()));
        compactionJob.setIsSplittingJob(true);
        compactionJob.setSplitPoint(100L);
        compactionJob.setDimension(0);
        compactionJob.setIteratorClassName(AgeOffIterator.class.getName());
        compactionJob.setIteratorConfig("timestamp,1000000");
        dynamoStateStore.atomicallyUpdateJobStatusOfFiles(compactionJob.getId(), fileInfos);

        // When
        //  - Merge two files
        CompactSortedFiles compactSortedFiles = new CompactSortedFiles(new InstanceProperties(), new ObjectFactory(new InstanceProperties(), null, ""),
                schema, SchemaConverter.getSchema(schema), compactionJob, dynamoStateStore,
                ParquetWriter.DEFAULT_BLOCK_SIZE, ParquetWriter.DEFAULT_PAGE_SIZE, "zstd", 25, 1000);
        compactSortedFiles.compact();

        // Then
        //  - Read output files and check that they contains the right results
        List<Record> leftResults = new ArrayList<>();
        ParquetReaderIterator reader = new ParquetReaderIterator(new ParquetRecordReader(new Path(leftOutputFile), schema));
        while (reader.hasNext()) {
            leftResults.add(new Record(reader.next()));
        }
        reader.close();
        List<Record> rightResults = new ArrayList<>();
        reader = new ParquetReaderIterator(new ParquetRecordReader(new Path(rightOutputFile), schema));
        while (reader.hasNext()) {
            rightResults.add(new Record(reader.next()));
        }
        reader.close();
        List<Record> leftExpectedResults = data.values().stream()
                .filter(r -> ((long) r.get("key")) < 100L)
                .filter(r -> System.currentTimeMillis() - ((long) r.get("timestamp")) < 1000000)
                .collect(Collectors.toList());
        assertThat(leftResults).isEqualTo(leftExpectedResults);
        List<Record> rightExpectedResults = data.values().stream()
                .filter(r -> ((long) r.get("key")) >= 100L)
                .filter(r -> System.currentTimeMillis() - ((long) r.get("timestamp")) < 1000000)
                .collect(Collectors.toList());
        assertThat(rightResults).isEqualTo(rightExpectedResults);
        // - Check DynamoDBStateStore has correct ready for GC files
        List<FileInfo> readyForGCFiles = new ArrayList<>();
        dynamoStateStore.getReadyForGCFiles().forEachRemaining(readyForGCFiles::add);
        readyForGCFiles = readyForGCFiles
                .stream()
                .sorted(Comparator.comparing(FileInfo::getFilename))
                .collect(Collectors.toList());
        assertThat(readyForGCFiles.size()).isEqualTo(2);

        assertThat(readyForGCFiles.get(0).getFilename()).isEqualTo(fileInfo1.getFilename());
        assertThat(readyForGCFiles.get(1).getFilename()).isEqualTo(fileInfo2.getFilename());
        assertThat(readyForGCFiles.get(0).getRowKeyTypes()).isEqualTo(fileInfo1.getRowKeyTypes());
        assertThat(readyForGCFiles.get(1).getRowKeyTypes()).isEqualTo(fileInfo2.getRowKeyTypes());
        assertThat(readyForGCFiles.get(0).getPartitionId()).isEqualTo(fileInfo1.getPartitionId());
        assertThat(readyForGCFiles.get(1).getPartitionId()).isEqualTo(fileInfo2.getPartitionId());
        assertThat(readyForGCFiles.get(0).getFileStatus()).isEqualTo(FileInfo.FileStatus.READY_FOR_GARBAGE_COLLECTION);
        assertThat(readyForGCFiles.get(1).getFileStatus()).isEqualTo(FileInfo.FileStatus.READY_FOR_GARBAGE_COLLECTION);
        // - Check DynamoDBStateStore has correct active files
        List<FileInfo> activeFiles = dynamoStateStore.getActiveFiles()
                .stream()
                .sorted(Comparator.comparing(FileInfo::getFilename))
                .collect(Collectors.toList());
        assertThat(activeFiles.size()).isEqualTo(2);

        FileInfo leftNewFile = new FileInfo();
        leftNewFile.setRowKeyTypes(new LongType());
        leftNewFile.setFilename(leftOutputFile);
        leftNewFile.setFileStatus(FileInfo.FileStatus.ACTIVE);
        leftNewFile.setPartitionId(leftPartition.getId());
        leftNewFile.setNumberOfRecords((long) leftExpectedResults.size());
        long minKeyLeft = leftExpectedResults.stream()
                .map(r -> (long) r.get("key"))
                .min(Comparator.naturalOrder())
                .get();
        leftNewFile.setMinRowKey(Key.create(minKeyLeft));
        long maxKeyLeft = leftExpectedResults.stream()
                .map(r -> (long) r.get("key"))
                .max(Comparator.naturalOrder())
                .get();
        leftNewFile.setMaxRowKey(Key.create(maxKeyLeft));
        activeFiles.get(0).setLastStateStoreUpdateTime(null); // Set to null as we don't know what it should be
        assertThat(activeFiles.get(0)).isEqualTo(leftNewFile);
        FileInfo rightNewFile = new FileInfo();
        rightNewFile.setRowKeyTypes(new LongType());
        rightNewFile.setFilename(rightOutputFile);
        rightNewFile.setFileStatus(FileInfo.FileStatus.ACTIVE);
        rightNewFile.setPartitionId(rightPartition.getId());
        rightNewFile.setNumberOfRecords((long) rightExpectedResults.size());
        long minKeyRight = rightExpectedResults.stream()
                .map(r -> (long) r.get("key"))
                .min(Comparator.naturalOrder())
                .get();
        rightNewFile.setMinRowKey(Key.create(minKeyRight));
        long maxKeyRight = rightExpectedResults.stream()
                .map(r -> (long) r.get("key"))
                .max(Comparator.naturalOrder())
                .get();
        rightNewFile.setMaxRowKey(Key.create(maxKeyRight));
        activeFiles.get(1).setLastStateStoreUpdateTime(null); // Set to null as we don't know what it should be
        assertThat(activeFiles.get(1)).isEqualTo(rightNewFile);
    }
}
