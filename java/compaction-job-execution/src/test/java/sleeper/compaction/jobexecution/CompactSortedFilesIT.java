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
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.hadoop.fs.Path;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.testcontainers.containers.GenericContainer;
import sleeper.compaction.job.CompactionFactory;
import sleeper.compaction.job.CompactionJob;
import sleeper.configuration.jars.ObjectFactoryException;
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
import sleeper.statestore.StateStore;
import sleeper.statestore.StateStoreException;
import sleeper.statestore.dynamodb.DynamoDBStateStore;
import sleeper.statestore.dynamodb.DynamoDBStateStoreCreator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.compaction.jobexecution.CompactSortedFilesTestData.combineSortedBySingleByteArrayKey;
import static sleeper.compaction.jobexecution.CompactSortedFilesTestData.combineSortedBySingleKey;
import static sleeper.compaction.jobexecution.CompactSortedFilesTestData.keyAndTwoValuesSortedEvenByteArrays;
import static sleeper.compaction.jobexecution.CompactSortedFilesTestData.keyAndTwoValuesSortedEvenLongs;
import static sleeper.compaction.jobexecution.CompactSortedFilesTestData.keyAndTwoValuesSortedEvenStrings;
import static sleeper.compaction.jobexecution.CompactSortedFilesTestData.keyAndTwoValuesSortedOddByteArrays;
import static sleeper.compaction.jobexecution.CompactSortedFilesTestData.keyAndTwoValuesSortedOddLongs;
import static sleeper.compaction.jobexecution.CompactSortedFilesTestData.keyAndTwoValuesSortedOddStrings;
import static sleeper.compaction.jobexecution.CompactSortedFilesTestData.readDataFile;
import static sleeper.compaction.jobexecution.CompactSortedFilesTestUtils.assertReadyForGC;
import static sleeper.compaction.jobexecution.CompactSortedFilesTestUtils.createCompactSortedFiles;
import static sleeper.compaction.jobexecution.CompactSortedFilesTestUtils.createSchemaWithKeyTimestampValue;
import static sleeper.compaction.jobexecution.CompactSortedFilesTestUtils.createSchemaWithTwoTypedValuesAndKeyFields;
import static sleeper.compaction.jobexecution.CompactSortedFilesTestUtils.createSchemaWithTypesForKeyAndTwoValues;
import static sleeper.compaction.jobexecution.CompactSortedFilesTestUtils.createStateStore;

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
    private String folderName;
    private CompactionFactory compactionFactory;

    @Before
    public void setUp() throws Exception {
        folderName = folder.newFolder().getAbsolutePath();
        compactionFactory = CompactionFactory.withTableName("table").outputFilePrefix(folderName).build();
    }

    @Test
    public void filesShouldMergeCorrectlyAndDynamoUpdatedLongKey() throws Exception {
        // Given
        Schema schema = createSchemaWithTypesForKeyAndTwoValues(new LongType(), new LongType(), new LongType());
        StateStore stateStore = createStateStore("fsmcadulk", schema, dynamoDBClient);
        CompactSortedFilesTestDataHelper dataHelper = new CompactSortedFilesTestDataHelper(schema, stateStore);

        List<Record> data1 = keyAndTwoValuesSortedEvenLongs();
        List<Record> data2 = keyAndTwoValuesSortedOddLongs();
        dataHelper.writeLeafFile(folderName + "/file1.parquet", data1, 0L, 198L);
        dataHelper.writeLeafFile(folderName + "/file2.parquet", data2, 1L, 199L);

        CompactionJob compactionJob = compactionFactory.createCompactionJob(
                dataHelper.allFileInfos(), dataHelper.singlePartition().getId());
        dataHelper.addFilesToStateStoreForJob(compactionJob);

        // When
        CompactSortedFiles compactSortedFiles = createCompactSortedFiles(schema, compactionJob, stateStore);
        CompactSortedFiles.CompactionJobSummary summary = compactSortedFiles.compact();

        // Then
        //  - Read output file and check that it contains the right results
        List<Record> expectedResults = combineSortedBySingleKey(data1, data2);
        assertThat(summary.getLinesRead()).isEqualTo(expectedResults.size());
        assertThat(summary.getLinesWritten()).isEqualTo(expectedResults.size());
        assertThat(readDataFile(schema, compactionJob.getOutputFile())).isEqualTo(expectedResults);

        // - Check DynamoDBStateStore has correct ready for GC files
        assertReadyForGC(stateStore, dataHelper.allFileInfos());

        // - Check DynamoDBStateStore has correct active files
        assertThat(stateStore.getActiveFiles())
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("lastStateStoreUpdateTime")
                .containsExactly(dataHelper.expectedLeafFile(compactionJob.getOutputFile(), 200L, 0L, 199L));
    }

    @Test
    public void shouldGenerate200EvenAndOddStrings() {
        List<Record> data = combineSortedBySingleKey(
                keyAndTwoValuesSortedEvenStrings(),
                keyAndTwoValuesSortedOddStrings());
        assertThat(data).hasSize(200);
        assertThat(Stream.of(0, 1, 26, 27, 198, 199).map(n -> data.get(n).get("key")))
                .containsExactly("aa", "ab", "ba", "bb", "hq", "hr");
    }

    @Test
    public void filesShouldMergeCorrectlyAndDynamoUpdatedStringKey() throws Exception {
        // Given
        Schema schema = createSchemaWithTypesForKeyAndTwoValues(new StringType(), new StringType(), new LongType());
        StateStore stateStore = createStateStore("fsmcadusk", schema, dynamoDBClient);
        CompactSortedFilesTestDataHelper dataHelper = new CompactSortedFilesTestDataHelper(schema, stateStore);

        List<Record> data1 = keyAndTwoValuesSortedEvenStrings();
        List<Record> data2 = keyAndTwoValuesSortedOddStrings();
        dataHelper.writeLeafFile(folderName + "/file1.parquet", data1, "aa", "hq");
        dataHelper.writeLeafFile(folderName + "/file2.parquet", data2, "ab", "hr");

        CompactionJob compactionJob = compactionFactory.createCompactionJob(
                dataHelper.allFileInfos(), dataHelper.singlePartition().getId());
        dataHelper.addFilesToStateStoreForJob(compactionJob);

        // When
        CompactSortedFiles compactSortedFiles = createCompactSortedFiles(schema, compactionJob, stateStore);
        CompactSortedFiles.CompactionJobSummary summary = compactSortedFiles.compact();

        // Then
        //  - Read output file and check that it contains the right results
        List<Record> expectedResults = combineSortedBySingleKey(data1, data2);
        assertThat(summary.getLinesRead()).isEqualTo(expectedResults.size());
        assertThat(summary.getLinesWritten()).isEqualTo(expectedResults.size());
        assertThat(readDataFile(schema, compactionJob.getOutputFile())).isEqualTo(expectedResults);

        // - Check DynamoDBStateStore has correct ready for GC files
        assertReadyForGC(stateStore, dataHelper.allFileInfos());

        // - Check DynamoDBStateStore has correct active files
        assertThat(stateStore.getActiveFiles())
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("lastStateStoreUpdateTime")
                .containsExactly(dataHelper.expectedLeafFile(compactionJob.getOutputFile(), 200L, "aa", "hr"));
    }

    @Test
    public void shouldGenerate200EvenAndOddByteArrays() {
        List<Record> data = combineSortedBySingleByteArrayKey(
                keyAndTwoValuesSortedEvenByteArrays(),
                keyAndTwoValuesSortedOddByteArrays());
        assertThat(Stream.of(0, 1, 128, 129, 198, 199).map(n -> data.get(n).get("key")))
                .containsExactly(
                        new byte[]{0, 0}, new byte[]{0, 1},
                        new byte[]{1, 0}, new byte[]{1, 1},
                        new byte[]{1, 70}, new byte[]{1, 71});
    }

    @Test
    public void filesShouldMergeCorrectlyAndDynamoUpdatedByteArrayKey() throws Exception {
        // Given
        Schema schema = createSchemaWithTypesForKeyAndTwoValues(new ByteArrayType(), new ByteArrayType(), new LongType());
        StateStore stateStore = createStateStore("fsmcadubak", schema, dynamoDBClient);
        CompactSortedFilesTestDataHelper dataHelper = new CompactSortedFilesTestDataHelper(schema, stateStore);

        List<Record> data1 = keyAndTwoValuesSortedEvenByteArrays();
        List<Record> data2 = keyAndTwoValuesSortedOddByteArrays();
        dataHelper.writeLeafFile(folderName + "/file1.parquet", data1, new byte[]{0, 0}, new byte[]{1, 70});
        dataHelper.writeLeafFile(folderName + "/file2.parquet", data2, new byte[]{0, 1}, new byte[]{1, 71});

        CompactionJob compactionJob = compactionFactory.createCompactionJob(
                dataHelper.allFileInfos(), dataHelper.singlePartition().getId());
        dataHelper.addFilesToStateStoreForJob(compactionJob);

        // When
        CompactSortedFiles compactSortedFiles = createCompactSortedFiles(schema, compactionJob, stateStore);
        CompactSortedFiles.CompactionJobSummary summary = compactSortedFiles.compact();

        // Then
        //  - Read output file and check that it contains the right results
        List<Record> expectedResults = combineSortedBySingleByteArrayKey(data1, data2);
        assertThat(summary.getLinesRead()).isEqualTo(expectedResults.size());
        assertThat(summary.getLinesWritten()).isEqualTo(expectedResults.size());
        assertThat(readDataFile(schema, compactionJob.getOutputFile())).isEqualTo(expectedResults);

        // - Check DynamoDBStateStore has correct ready for GC files
        assertReadyForGC(stateStore, dataHelper.allFileInfos());

        // - Check DynamoDBStateStore has correct active files
        assertThat(stateStore.getActiveFiles())
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("lastStateStoreUpdateTime")
                .containsExactly(dataHelper.expectedLeafFile(compactionJob.getOutputFile(), 200L, new byte[]{0, 0}, new byte[]{1, 71}));
    }

    @Test
    public void filesShouldMergeCorrectlyWhenSomeAreEmpty() throws IOException, StateStoreException, ObjectFactoryException, IteratorException {
        // Given
        Schema schema = createSchemaWithTypesForKeyAndTwoValues(new LongType(), new LongType(), new LongType());
        //  - Create two files of sorted data
        String folderName = folder.newFolder().getAbsolutePath();
        String file1 = folderName + "/file1.parquet";
        String file2 = folderName + "/file2.parquet";
        List<String> files = new ArrayList<>();
        files.add(file1);
        files.add(file2);
        FileInfo fileInfo1 = FileInfo.builder()
                .rowKeyTypes(new LongType())
                .filename(file1)
                .fileStatus(FileInfo.FileStatus.ACTIVE)
                .partitionId("1")
                .numberOfRecords(100L)
                .minRowKey(Key.create(0L))
                .maxRowKey(Key.create(198L))
                .build();
        FileInfo fileInfo2 = FileInfo.builder()
                .rowKeyTypes(new LongType())
                .filename(file2)
                .fileStatus(FileInfo.FileStatus.ACTIVE)
                .partitionId("1")
                .numberOfRecords(0L)
                .build();
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
        CompactSortedFiles compactSortedFiles = createCompactSortedFiles(schema, compactionJob, dynamoStateStore);
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
        assertReadyForGC(dynamoStateStore, fileInfo1, fileInfo2);

        // - Check DynamoDBStateStore has correct active files
        FileInfo newFile = FileInfo.builder()
                .rowKeyTypes(new LongType())
                .filename(outputFile)
                .fileStatus(FileInfo.FileStatus.ACTIVE)
                .partitionId("1")
                .numberOfRecords((long) expectedResults.size())
                .build();
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
        assertThat(dynamoStateStore.getActiveFiles())
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("lastStateStoreUpdateTime")
                .containsExactly(newFile);
    }

    @Test
    public void filesShouldMergeCorrectlyWhenAllAreEmpty() throws IOException, StateStoreException, ObjectFactoryException, IteratorException {
        // Given
        Schema schema = createSchemaWithTypesForKeyAndTwoValues(new LongType(), new LongType(), new LongType());
        //  - Create two empty files
        String folderName = folder.newFolder().getAbsolutePath();
        String file1 = folderName + "/file1.parquet";
        String file2 = folderName + "/file2.parquet";
        List<String> files = new ArrayList<>();
        files.add(file1);
        files.add(file2);
        FileInfo fileInfo1 = FileInfo.builder()
                .rowKeyTypes(new LongType())
                .filename(file1)
                .fileStatus(FileInfo.FileStatus.ACTIVE)
                .partitionId("1")
                .numberOfRecords(0L)
                .build();
        FileInfo fileInfo2 = FileInfo.builder()
                .rowKeyTypes(new LongType())
                .filename(file2)
                .fileStatus(FileInfo.FileStatus.ACTIVE)
                .partitionId("1")
                .numberOfRecords(0L)
                .build();
        List<FileInfo> fileInfos = new ArrayList<>();
        fileInfos.add(fileInfo1);
        fileInfos.add(fileInfo2);
        String outputFile = folderName + "/file3.parquet";
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
        CompactSortedFiles compactSortedFiles = createCompactSortedFiles(schema, compactionJob, dynamoStateStore);
        compactSortedFiles.compact();

        // Then
        //  - Read output file and check that it contains the right results
        List<Record> results = new ArrayList<>();
        ParquetReaderIterator reader = new ParquetReaderIterator(new ParquetRecordReader(new Path(outputFile), schema));
        while (reader.hasNext()) {
            results.add(new Record(reader.next()));
        }
        reader.close();
        assertThat(results).isEmpty();

        // - Check DynamoDBStateStore has correct ready for GC files
        assertReadyForGC(dynamoStateStore, fileInfo1, fileInfo2);

        // - Check DynamoDBStateStore has correct active files
        FileInfo newFile = FileInfo.builder()
                .rowKeyTypes(new LongType())
                .filename(outputFile)
                .fileStatus(FileInfo.FileStatus.ACTIVE)
                .partitionId("1")
                .numberOfRecords(0L)
                .minRowKey(null)
                .maxRowKey(null)
                .build();
        assertThat(dynamoStateStore.getActiveFiles())
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("lastStateStoreUpdateTime")
                .containsExactly(newFile);
    }

    @Test
    public void filesShouldMergeAndSplitCorrectlyAndDynamoUpdated() throws IOException, StateStoreException, ObjectFactoryException, IteratorException {
        // Given
        Field field = new Field("key", new LongType());
        Schema schema = createSchemaWithTwoTypedValuesAndKeyFields(new LongType(), new LongType(), field);
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
        FileInfo fileInfo1 = FileInfo.builder()
                .rowKeyTypes(new LongType())
                .filename(file1)
                .fileStatus(FileInfo.FileStatus.ACTIVE)
                .partitionId(rootPartition.getId())
                .numberOfRecords(100L)
                .minRowKey(Key.create(0L))
                .maxRowKey(Key.create(198L))
                .build();
        FileInfo fileInfo2 = FileInfo.builder()
                .rowKeyTypes(new LongType())
                .filename(file2)
                .fileStatus(FileInfo.FileStatus.ACTIVE)
                .partitionId(rootPartition.getId())
                .numberOfRecords(100L)
                .minRowKey(Key.create(1L))
                .maxRowKey(Key.create(199L))
                .build();
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
        Range leftRange = new RangeFactory(schema).createRange(field, Long.MIN_VALUE, 100L);
        Partition leftPartition = Partition.builder()
                .leafPartition(true)
                .region(new Region(leftRange))
                .id(Long.MIN_VALUE + "---100")
                .parentPartitionId(rootPartition.getId())
                .childPartitionIds(new ArrayList<>())
                .build();
        Range rightRange = new RangeFactory(schema).createRange(field, 100L, null);
        Partition rightPartition = Partition.builder()
                .leafPartition(true)
                .region(new Region(rightRange))
                .id("100---")
                .parentPartitionId(rootPartition.getId())
                .childPartitionIds(new ArrayList<>())
                .build();
        rootPartition.setLeafPartition(false);
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
        CompactSortedFiles compactSortedFiles = createCompactSortedFiles(schema, compactionJob, dynamoStateStore);
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
        assertReadyForGC(dynamoStateStore, fileInfo1, fileInfo2);

        // - Check DynamoDBStateStore has correct active files
        FileInfo leftNewFile = FileInfo.builder()
                .rowKeyTypes(new LongType())
                .filename(leftOutputFile)
                .fileStatus(FileInfo.FileStatus.ACTIVE)
                .partitionId(leftPartition.getId())
                .numberOfRecords((long) leftExpectedResults.size())
                .build();
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
        FileInfo rightNewFile = FileInfo.builder()
                .rowKeyTypes(new LongType())
                .filename(rightOutputFile)
                .fileStatus(FileInfo.FileStatus.ACTIVE)
                .partitionId(rightPartition.getId())
                .numberOfRecords((long) rightExpectedResults.size())
                .build();
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
        assertThat(dynamoStateStore.getActiveFiles())
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("lastStateStoreUpdateTime")
                .containsExactlyInAnyOrder(leftNewFile, rightNewFile);
    }

    @Test
    public void filesShouldMergeAndSplitCorrectlyWith2DimKeySplitOnFirstKey() throws IOException, StateStoreException, ObjectFactoryException, IteratorException {
        // Given
        Field field1 = new Field("key1", new LongType());
        Field field2 = new Field("key2", new StringType());
        Schema schema = createSchemaWithTwoTypedValuesAndKeyFields(new LongType(), new LongType(), field1, field2);
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
        FileInfo fileInfo1 = FileInfo.builder()
                .rowKeyTypes(new LongType(), new StringType())
                .filename(file1)
                .fileStatus(FileInfo.FileStatus.ACTIVE)
                .partitionId(rootPartition.getId())
                .numberOfRecords(100L)
                .minRowKey(Key.create(Arrays.asList(0L, "A")))
                .maxRowKey(Key.create(Arrays.asList(198L, "A")))
                .build();
        FileInfo fileInfo2 = FileInfo.builder()
                .rowKeyTypes(new LongType(), new StringType())
                .filename(file2)
                .fileStatus(FileInfo.FileStatus.ACTIVE)
                .partitionId(rootPartition.getId())
                .numberOfRecords(100L)
                .minRowKey(Key.create(Arrays.asList(1L, "A")))
                .maxRowKey(Key.create(Arrays.asList(199L, "A")))
                .build();
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
        Range leftRange = new RangeFactory(schema).createRange(
                field1,
                leftPartitionRecords.get(0).get("key1"),
                leftPartitionRecords.get(leftPartitionRecords.size() - 1).get("key1"));
        Partition leftPartition = Partition.builder()
                .rowKeyTypes(new LongType(), new StringType())
                .leafPartition(true)
                .region(new Region(leftRange))
                .id("left")
                .parentPartitionId(rootPartition.getId())
                .childPartitionIds(new ArrayList<>())
                .build();
        Range rightRange = new RangeFactory(schema).createRange(
                field1,
                rightPartitionRecords.get(0).get("key1"),
                rightPartitionRecords.get(leftPartitionRecords.size() - 1).get("key1"));
        Partition rightPartition = Partition.builder()
                .rowKeyTypes(new LongType(), new StringType())
                .leafPartition(true)
                .region(new Region(rightRange))
                .id("right")
                .parentPartitionId(rootPartition.getId())
                .childPartitionIds(new ArrayList<>())
                .build();
        rootPartition.setLeafPartition(false);
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
        CompactSortedFiles compactSortedFiles = createCompactSortedFiles(schema, compactionJob, dynamoStateStore);
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
        assertReadyForGC(dynamoStateStore, fileInfo1, fileInfo2);

        // - Check DynamoDBStateStore has correct active files
        FileInfo leftNewFile = FileInfo.builder()
                .rowKeyTypes(new LongType(), new StringType())
                .filename(leftOutputFile)
                .fileStatus(FileInfo.FileStatus.ACTIVE)
                .partitionId(leftPartition.getId())
                .numberOfRecords((long) leftExpectedResults.size())
                .minRowKey(Key.create(leftExpectedResults.get(0).get(schema.getRowKeyFieldNames().get(0))))
                .maxRowKey(Key.create(leftExpectedResults.get(leftExpectedResults.size() - 1).get(schema.getRowKeyFieldNames().get(0))))
                .build();
        FileInfo rightNewFile = FileInfo.builder()
                .rowKeyTypes(new LongType(), new StringType())
                .filename(rightOutputFile)
                .fileStatus(FileInfo.FileStatus.ACTIVE)
                .partitionId(rightPartition.getId())
                .numberOfRecords((long) rightExpectedResults.size())
                .minRowKey(Key.create(rightExpectedResults.get(0).get(schema.getRowKeyFieldNames().get(0))))
                .maxRowKey(Key.create(rightExpectedResults.get(rightExpectedResults.size() - 1).get(schema.getRowKeyFieldNames().get(0))))
                .build();
        assertThat(dynamoStateStore.getActiveFiles())
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("lastStateStoreUpdateTime")
                .containsExactlyInAnyOrder(leftNewFile, rightNewFile);
    }

    @Test
    public void filesShouldMergeAndSplitCorrectlyWith2DimKeySplitOnSecondKey() throws IOException, StateStoreException, ObjectFactoryException, IteratorException {
        // Given
        Field field1 = new Field("key1", new LongType());
        Field field2 = new Field("key2", new StringType());
        Schema schema = createSchemaWithTwoTypedValuesAndKeyFields(new LongType(), new LongType(), field1, field2);
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
        FileInfo fileInfo1 = FileInfo.builder()
                .rowKeyTypes(new LongType(), new StringType())
                .filename(file1)
                .fileStatus(FileInfo.FileStatus.ACTIVE)
                .partitionId(rootPartition.getId())
                .numberOfRecords(100L)
                .minRowKey(Key.create(Arrays.asList(0L, "A")))
                .maxRowKey(Key.create(Arrays.asList(198L, "B")))
                .build();
        FileInfo fileInfo2 = FileInfo.builder()
                .rowKeyTypes(new LongType(), new StringType())
                .filename(file2)
                .fileStatus(FileInfo.FileStatus.ACTIVE)
                .partitionId(rootPartition.getId())
                .numberOfRecords(100L)
                .minRowKey(Key.create(Arrays.asList(1L, "A")))
                .maxRowKey(Key.create(Arrays.asList(199L, "B")))
                .build();
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
        Range leftRange = new RangeFactory(schema).createRange(
                field1,
                leftPartitionRecords.get(0).get("key1"),
                leftPartitionRecords.get(leftPartitionRecords.size() - 1).get("key1"));
        Partition leftPartition = Partition.builder()
                .rowKeyTypes(new LongType(), new StringType())
                .leafPartition(true)
                .region(new Region(leftRange))
                .id("left")
                .parentPartitionId(rootPartition.getId())
                .childPartitionIds(new ArrayList<>())
                .build();
        Range rightRange = new RangeFactory(schema).createRange(
                field1,
                rightPartitionRecords.get(0).get("key1"),
                rightPartitionRecords.get(leftPartitionRecords.size() - 1).get("key1"));
        Partition rightPartition = Partition.builder()
                .rowKeyTypes(new LongType(), new StringType())
                .leafPartition(true)
                .region(new Region(rightRange))
                .id("right")
                .parentPartitionId(rootPartition.getId())
                .childPartitionIds(new ArrayList<>())
                .build();
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
        CompactSortedFiles compactSortedFiles = createCompactSortedFiles(schema, compactionJob, dynamoStateStore);
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
        assertReadyForGC(dynamoStateStore, fileInfo1, fileInfo2);

        // - Check DynamoDBStateStore has correct active files
        FileInfo leftNewFile = FileInfo.builder()
                .rowKeyTypes(new LongType(), new StringType())
                .filename(leftOutputFile)
                .fileStatus(FileInfo.FileStatus.ACTIVE)
                .partitionId(leftPartition.getId())
                .numberOfRecords((long) leftExpectedResults.size())
                .minRowKey(Key.create(leftExpectedResults.get(0).get(schema.getRowKeyFieldNames().get(0))))
                .maxRowKey(Key.create(leftExpectedResults.get(leftExpectedResults.size() - 1).get(schema.getRowKeyFieldNames().get(0))))
                .build();
        FileInfo rightNewFile = FileInfo.builder()
                .rowKeyTypes(new LongType(), new StringType())
                .filename(rightOutputFile)
                .fileStatus(FileInfo.FileStatus.ACTIVE)
                .partitionId(rightPartition.getId())
                .numberOfRecords((long) rightExpectedResults.size())
                .minRowKey(Key.create(rightExpectedResults.get(0).get(schema.getRowKeyFieldNames().get(0))))
                .maxRowKey(Key.create(rightExpectedResults.get(rightExpectedResults.size() - 1).get(schema.getRowKeyFieldNames().get(0))))
                .build();
        assertThat(dynamoStateStore.getActiveFiles())
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("lastStateStoreUpdateTime")
                .containsExactlyInAnyOrder(leftNewFile, rightNewFile);
    }

    @Test
    public void filesShouldMergeAndSplitCorrectlyWhenOneChildFileIsEmpty() throws IOException, StateStoreException, ObjectFactoryException, IteratorException {
        // Given
        Field field = new Field("key", new LongType());
        Schema schema = createSchemaWithTwoTypedValuesAndKeyFields(new LongType(), new LongType(), field);
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
        FileInfo fileInfo1 = FileInfo.builder()
                .rowKeyTypes(new LongType())
                .filename(file1)
                .fileStatus(FileInfo.FileStatus.ACTIVE)
                .partitionId(rootPartition.getId())
                .numberOfRecords(100L)
                .minRowKey(Key.create(0L))
                .maxRowKey(Key.create(198L))
                .build();
        FileInfo fileInfo2 = FileInfo.builder()
                .rowKeyTypes(new LongType())
                .filename(file2)
                .fileStatus(FileInfo.FileStatus.ACTIVE)
                .partitionId(rootPartition.getId())
                .numberOfRecords(100L)
                .minRowKey(Key.create(1L))
                .maxRowKey(Key.create(199L))
                .build();
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
        Range leftRange = new RangeFactory(schema).createRange(field, Long.MIN_VALUE, 100L);
        Partition leftPartition = Partition.builder()
                .leafPartition(true)
                .region(new Region(leftRange))
                .id(Long.MIN_VALUE + "---100")
                .parentPartitionId(rootPartition.getId())
                .childPartitionIds(new ArrayList<>())
                .build();
        Range rightRange = new RangeFactory(schema).createRange(field, 100L, null);
        Partition rightPartition = Partition.builder()
                .leafPartition(true)
                .region(new Region(rightRange))
                .id("100---")
                .parentPartitionId(rootPartition.getId())
                .childPartitionIds(new ArrayList<>())
                .build();
        rootPartition.setLeafPartition(false);
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
        CompactSortedFiles compactSortedFiles = createCompactSortedFiles(schema, compactionJob, dynamoStateStore);
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
        assertReadyForGC(dynamoStateStore, fileInfo1, fileInfo2);

        // - Check DynamoDBStateStore has correct active files
        FileInfo leftNewFile = FileInfo.builder()
                .rowKeyTypes(new LongType())
                .filename(leftOutputFile)
                .fileStatus(FileInfo.FileStatus.ACTIVE)
                .partitionId(leftPartition.getId())
                .numberOfRecords((long) leftExpectedResults.size())
                .build();
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
        FileInfo rightNewFile = FileInfo.builder()
                .rowKeyTypes(new LongType())
                .filename(rightOutputFile)
                .fileStatus(FileInfo.FileStatus.ACTIVE)
                .partitionId(rightPartition.getId())
                .numberOfRecords(0L)
                .minRowKey(null)
                .maxRowKey(null)
                .build();
        assertThat(dynamoStateStore.getActiveFiles())
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("lastStateStoreUpdateTime")
                .containsExactlyInAnyOrder(leftNewFile, rightNewFile);
    }

    @Test
    public void filesShouldMergeAndApplyIteratorCorrectlyLongKey() throws IOException, StateStoreException, ObjectFactoryException, IteratorException {
        // Given
        Schema schema = CompactSortedFilesTestUtils.createSchemaWithKeyTimestampValue();
        //  - Create two files of sorted data
        String folderName = folder.newFolder().getAbsolutePath();
        String file1 = folderName + "/file1.parquet";
        String file2 = folderName + "/file2.parquet";
        List<String> files = new ArrayList<>();
        files.add(file1);
        files.add(file2);
        FileInfo fileInfo1 = FileInfo.builder()
                .rowKeyTypes(new LongType())
                .filename(file1)
                .fileStatus(FileInfo.FileStatus.ACTIVE)
                .partitionId("1")
                .numberOfRecords(100L)
                .minRowKey(Key.create(0L))
                .maxRowKey(Key.create(198L))
                .build();
        FileInfo fileInfo2 = FileInfo.builder()
                .rowKeyTypes(new LongType())
                .filename(file2)
                .fileStatus(FileInfo.FileStatus.ACTIVE)
                .partitionId("1")
                .numberOfRecords(100L)
                .minRowKey(Key.create(1L))
                .maxRowKey(Key.create(199L))
                .build();
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
        CompactSortedFiles compactSortedFiles = createCompactSortedFiles(schema, compactionJob, dynamoStateStore);
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
        assertReadyForGC(dynamoStateStore, fileInfo1, fileInfo2);

        // - Check DynamoDBStateStore has correct active files
        FileInfo newFile = FileInfo.builder()
                .rowKeyTypes(new LongType())
                .filename(outputFile)
                .fileStatus(FileInfo.FileStatus.ACTIVE)
                .partitionId("1")
                .numberOfRecords((long) expectedResults.size())
                .build();
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
        assertThat(dynamoStateStore.getActiveFiles())
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("lastStateStoreUpdateTime")
                .containsExactly(newFile);
    }

    @Test
    public void filesShouldMergeAndSplitAndApplyIteratorCorrectlyLongKey() throws IOException, StateStoreException, ObjectFactoryException, IteratorException {
        // Given
        //  - Schema
        Field field = new Field("key", new LongType());
        Schema schema = createSchemaWithKeyTimestampValue(field);
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
        FileInfo fileInfo1 = FileInfo.builder()
                .rowKeyTypes(new LongType())
                .filename(file1)
                .fileStatus(FileInfo.FileStatus.ACTIVE)
                .partitionId(rootPartition.getId())
                .numberOfRecords(100L)
                .minRowKey(Key.create(0L))
                .maxRowKey(Key.create(198L))
                .build();
        FileInfo fileInfo2 = FileInfo.builder()
                .rowKeyTypes(new LongType())
                .filename(file2)
                .fileStatus(FileInfo.FileStatus.ACTIVE)
                .partitionId(rootPartition.getId())
                .numberOfRecords(100L)
                .minRowKey(Key.create(1L))
                .maxRowKey(Key.create(199L))
                .build();
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
        Range leftRange = new RangeFactory(schema).createRange(field, Long.MIN_VALUE, 100L);
        Partition leftPartition = Partition.builder()
                .leafPartition(true)
                .region(new Region(leftRange))
                .id(Long.MIN_VALUE + "---100")
                .parentPartitionId(rootPartition.getId())
                .childPartitionIds(new ArrayList<>())
                .build();
        Range rightRange = new RangeFactory(schema).createRange(field, 100L, null);
        Partition rightPartition = Partition.builder()
                .leafPartition(true)
                .region(new Region(rightRange))
                .id("100---")
                .parentPartitionId(rootPartition.getId())
                .childPartitionIds(new ArrayList<>())
                .build();
        rootPartition.setLeafPartition(false);
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
        CompactSortedFiles compactSortedFiles = createCompactSortedFiles(schema, compactionJob, dynamoStateStore);
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
        assertReadyForGC(dynamoStateStore, fileInfo1, fileInfo2);

        // - Check DynamoDBStateStore has correct active files
        FileInfo leftNewFile = FileInfo.builder()
                .rowKeyTypes(new LongType())
                .filename(leftOutputFile)
                .fileStatus(FileInfo.FileStatus.ACTIVE)
                .partitionId(leftPartition.getId())
                .numberOfRecords((long) leftExpectedResults.size())
                .build();
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
        FileInfo rightNewFile = FileInfo.builder()
                .rowKeyTypes(new LongType())
                .filename(rightOutputFile)
                .fileStatus(FileInfo.FileStatus.ACTIVE)
                .partitionId(rightPartition.getId())
                .numberOfRecords((long) rightExpectedResults.size())
                .build();
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
        assertThat(dynamoStateStore.getActiveFiles())
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("lastStateStoreUpdateTime")
                .containsExactlyInAnyOrder(leftNewFile, rightNewFile);
    }
}
