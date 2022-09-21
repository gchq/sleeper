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
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.testcontainers.containers.GenericContainer;
import sleeper.compaction.job.CompactionJob;
import sleeper.compaction.job.CompactionJobFactory;
import sleeper.core.CommonTestConstants;
import sleeper.core.iterator.impl.AgeOffIterator;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.record.Record;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.ByteArrayType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.StringType;
import sleeper.statestore.StateStore;
import sleeper.statestore.StateStoreException;
import sleeper.statestore.dynamodb.DynamoDBStateStoreCreator;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

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
import static sleeper.compaction.jobexecution.CompactSortedFilesTestData.specifiedAndTwoValuesFromEvens;
import static sleeper.compaction.jobexecution.CompactSortedFilesTestData.specifiedAndTwoValuesFromOdds;
import static sleeper.compaction.jobexecution.CompactSortedFilesTestData.specifiedFromEvens;
import static sleeper.compaction.jobexecution.CompactSortedFilesTestData.specifiedFromOdds;
import static sleeper.compaction.jobexecution.CompactSortedFilesTestUtils.assertReadyForGC;
import static sleeper.compaction.jobexecution.CompactSortedFilesTestUtils.createCompactSortedFiles;
import static sleeper.compaction.jobexecution.CompactSortedFilesTestUtils.createSchemaWithTwoTypedValuesAndKeyFields;
import static sleeper.compaction.jobexecution.CompactSortedFilesTestUtils.createSchemaWithTypesForKeyAndTwoValues;

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

    @Before
    public void setUp() throws Exception {
        folderName = folder.newFolder().getAbsolutePath();
    }

    private CompactionJobFactory compactionFactory() {
        return compactionFactoryBuilder().build();
    }

    private CompactionJobFactory.Builder compactionFactoryBuilder() {
        return CompactionJobFactory.withTableName("table").outputFilePrefix(folderName);
    }

    private static StateStore createInitStateStore(String tablenameStub, Schema schema, AmazonDynamoDB dynamoDBClient) throws StateStoreException {
        StateStore dynamoStateStore = createStateStore(tablenameStub, schema, dynamoDBClient);
        dynamoStateStore.initialise();
        return dynamoStateStore;
    }

    private static StateStore createStateStore(String tablenameStub, Schema schema, AmazonDynamoDB dynamoDBClient) throws StateStoreException {
        return new DynamoDBStateStoreCreator(tablenameStub, schema, dynamoDBClient).create();
    }

    @Test
    public void filesShouldMergeCorrectlyAndDynamoUpdatedLongKey() throws Exception {
        // Given
        Schema schema = createSchemaWithTypesForKeyAndTwoValues(new LongType(), new LongType(), new LongType());
        StateStore stateStore = createInitStateStore("fsmcadulk", schema, dynamoDBClient);
        CompactSortedFilesTestDataHelper dataHelper = new CompactSortedFilesTestDataHelper(schema, stateStore);

        List<Record> data1 = keyAndTwoValuesSortedEvenLongs();
        List<Record> data2 = keyAndTwoValuesSortedOddLongs();
        dataHelper.writeLeafFile(folderName + "/file1.parquet", data1, 0L, 198L);
        dataHelper.writeLeafFile(folderName + "/file2.parquet", data2, 1L, 199L);

        CompactionJob compactionJob = compactionFactory().createCompactionJob(
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
    public void shouldGenerateTestData200EvenAndOddStrings() {
        // When
        List<Record> evens = keyAndTwoValuesSortedEvenStrings();
        List<Record> odds = keyAndTwoValuesSortedOddStrings();
        List<Record> combined = combineSortedBySingleKey(evens, odds);

        // Then
        assertThat(evens).hasSize(100).elements(0, 99).extracting(e -> e.get("key"))
                .containsExactly("aa", "hq");
        assertThat(odds).hasSize(100).elements(0, 99).extracting(e -> e.get("key"))
                .containsExactly("ab", "hr");
        assertThat(combined).hasSize(200)
                .elements(0, 1, 26, 27, 198, 199).extracting(e -> e.get("key"))
                .containsExactly("aa", "ab", "ba", "bb", "hq", "hr");
    }

    @Test
    public void filesShouldMergeCorrectlyAndDynamoUpdatedStringKey() throws Exception {
        // Given
        Schema schema = createSchemaWithTypesForKeyAndTwoValues(new StringType(), new StringType(), new LongType());
        StateStore stateStore = createInitStateStore("fsmcadusk", schema, dynamoDBClient);
        CompactSortedFilesTestDataHelper dataHelper = new CompactSortedFilesTestDataHelper(schema, stateStore);

        List<Record> data1 = keyAndTwoValuesSortedEvenStrings();
        List<Record> data2 = keyAndTwoValuesSortedOddStrings();
        dataHelper.writeLeafFile(folderName + "/file1.parquet", data1, "aa", "hq");
        dataHelper.writeLeafFile(folderName + "/file2.parquet", data2, "ab", "hr");

        CompactionJob compactionJob = compactionFactory().createCompactionJob(
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
    public void shouldGenerateTestData200EvenAndOddByteArrays() {
        // When
        List<Record> evens = keyAndTwoValuesSortedEvenByteArrays();
        List<Record> odds = keyAndTwoValuesSortedOddByteArrays();
        List<Record> combined = combineSortedBySingleByteArrayKey(evens, odds);

        // Then
        assertThat(evens).hasSize(100)
                .elements(0, 99).extracting(e -> e.get("key"))
                .containsExactly(new byte[]{0, 0}, new byte[]{1, 70});
        assertThat(odds).hasSize(100)
                .elements(0, 99).extracting(e -> e.get("key"))
                .containsExactly(new byte[]{0, 1}, new byte[]{1, 71});
        assertThat(combined).hasSize(200)
                .elements(0, 1, 128, 129, 198, 199).extracting(e -> e.get("key"))
                .containsExactly(
                        new byte[]{0, 0}, new byte[]{0, 1},
                        new byte[]{1, 0}, new byte[]{1, 1},
                        new byte[]{1, 70}, new byte[]{1, 71});
    }

    @Test
    public void filesShouldMergeCorrectlyAndDynamoUpdatedByteArrayKey() throws Exception {
        // Given
        Schema schema = createSchemaWithTypesForKeyAndTwoValues(new ByteArrayType(), new ByteArrayType(), new LongType());
        StateStore stateStore = createInitStateStore("fsmcadubak", schema, dynamoDBClient);
        CompactSortedFilesTestDataHelper dataHelper = new CompactSortedFilesTestDataHelper(schema, stateStore);

        List<Record> data1 = keyAndTwoValuesSortedEvenByteArrays();
        List<Record> data2 = keyAndTwoValuesSortedOddByteArrays();
        dataHelper.writeLeafFile(folderName + "/file1.parquet", data1, new byte[]{0, 0}, new byte[]{1, 70});
        dataHelper.writeLeafFile(folderName + "/file2.parquet", data2, new byte[]{0, 1}, new byte[]{1, 71});

        CompactionJob compactionJob = compactionFactory().createCompactionJob(
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
    public void filesShouldMergeCorrectlyWhenSomeAreEmpty() throws Exception {
        // Given
        Schema schema = createSchemaWithTypesForKeyAndTwoValues(new LongType(), new LongType(), new LongType());
        StateStore stateStore = createInitStateStore("fsmcwsae", schema, dynamoDBClient);
        CompactSortedFilesTestDataHelper dataHelper = new CompactSortedFilesTestDataHelper(schema, stateStore);

        List<Record> data = keyAndTwoValuesSortedEvenLongs();
        dataHelper.writeLeafFile(folderName + "/file1.parquet", data, 0L, 198L);
        dataHelper.writeLeafFile(folderName + "/file2.parquet", Collections.emptyList(), null, null);

        CompactionJob compactionJob = compactionFactory().createCompactionJob(
                dataHelper.allFileInfos(), dataHelper.singlePartition().getId());
        dataHelper.addFilesToStateStoreForJob(compactionJob);

        // When
        CompactSortedFiles compactSortedFiles = createCompactSortedFiles(schema, compactionJob, stateStore);
        CompactSortedFiles.CompactionJobSummary summary = compactSortedFiles.compact();

        // Then
        //  - Read output file and check that it contains the right results
        assertThat(summary.getLinesRead()).isEqualTo(data.size());
        assertThat(summary.getLinesWritten()).isEqualTo(data.size());
        assertThat(readDataFile(schema, compactionJob.getOutputFile())).isEqualTo(data);

        // - Check DynamoDBStateStore has correct ready for GC files
        assertReadyForGC(stateStore, dataHelper.allFileInfos());

        // - Check DynamoDBStateStore has correct active files
        assertThat(stateStore.getActiveFiles())
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("lastStateStoreUpdateTime")
                .containsExactly(dataHelper.expectedLeafFile(compactionJob.getOutputFile(), 100L, 0L, 198L));
    }

    @Test
    public void filesShouldMergeCorrectlyWhenAllAreEmpty() throws Exception {
        // Given
        Schema schema = createSchemaWithTypesForKeyAndTwoValues(new LongType(), new LongType(), new LongType());
        StateStore stateStore = createInitStateStore("fsmcwaae", schema, dynamoDBClient);
        CompactSortedFilesTestDataHelper dataHelper = new CompactSortedFilesTestDataHelper(schema, stateStore);

        dataHelper.writeLeafFile(folderName + "/file1.parquet", Collections.emptyList(), null, null);
        dataHelper.writeLeafFile(folderName + "/file2.parquet", Collections.emptyList(), null, null);

        CompactionJob compactionJob = compactionFactory().createCompactionJob(
                dataHelper.allFileInfos(), dataHelper.singlePartition().getId());
        dataHelper.addFilesToStateStoreForJob(compactionJob);

        // When
        CompactSortedFiles compactSortedFiles = createCompactSortedFiles(schema, compactionJob, stateStore);
        CompactSortedFiles.CompactionJobSummary summary = compactSortedFiles.compact();

        // Then
        //  - Read output file and check that it contains the right results
        assertThat(summary.getLinesRead()).isZero();
        assertThat(summary.getLinesWritten()).isZero();
        assertThat(readDataFile(schema, compactionJob.getOutputFile())).isEmpty();

        // - Check DynamoDBStateStore has correct ready for GC files
        assertReadyForGC(stateStore, dataHelper.allFileInfos());

        // - Check DynamoDBStateStore has correct active files
        assertThat(stateStore.getActiveFiles())
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("lastStateStoreUpdateTime")
                .containsExactly(dataHelper.expectedLeafFile(compactionJob.getOutputFile(), 0L, null, null));
    }

    @Test
    public void filesShouldMergeAndSplitCorrectlyAndDynamoUpdated() throws Exception {
        // Given
        Schema schema = createSchemaWithTypesForKeyAndTwoValues(new LongType(), new LongType(), new LongType());
        StateStore stateStore = createStateStore("fsmascadu", schema, dynamoDBClient);
        stateStore.initialise(new PartitionsBuilder(schema)
                .leavesWithSplits(Arrays.asList("A", "B"), Collections.singletonList(100L))
                .parentJoining("C", "A", "B")
                .buildList());
        CompactSortedFilesTestDataHelper dataHelper = new CompactSortedFilesTestDataHelper(schema, stateStore);

        List<Record> data1 = keyAndTwoValuesSortedEvenLongs();
        List<Record> data2 = keyAndTwoValuesSortedOddLongs();
        dataHelper.writeRootFile(folderName + "/file1.parquet", data1, 0L, 198L);
        dataHelper.writeRootFile(folderName + "/file2.parquet", data2, 1L, 199L);

        CompactionJob compactionJob = compactionFactory().createSplittingCompactionJob(
                dataHelper.allFileInfos(), "C", "A", "B", 100L, 0);
        dataHelper.addFilesToStateStoreForJob(compactionJob);

        // When
        CompactSortedFiles compactSortedFiles = createCompactSortedFiles(schema, compactionJob, stateStore);
        CompactSortedFiles.CompactionJobSummary summary = compactSortedFiles.compact();

        // Then
        //  - Read output files and check that they contain the right results
        List<Record> expectedResults = combineSortedBySingleKey(data1, data2);
        assertThat(summary.getLinesRead()).isEqualTo(200L);
        assertThat(summary.getLinesWritten()).isEqualTo(200L);
        assertThat(readDataFile(schema, compactionJob.getOutputFiles().getLeft())).isEqualTo(expectedResults.subList(0, 100));
        assertThat(readDataFile(schema, compactionJob.getOutputFiles().getRight())).isEqualTo(expectedResults.subList(100, 200));

        // - Check DynamoDBStateStore has correct ready for GC files
        assertReadyForGC(stateStore, dataHelper.allFileInfos());

        // - Check DynamoDBStateStore has correct active files
        assertThat(stateStore.getActiveFiles())
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("lastStateStoreUpdateTime")
                .containsExactlyInAnyOrder(
                        dataHelper.expectedPartitionFile("A", compactionJob.getOutputFiles().getLeft(), 100L, 0L, 99L),
                        dataHelper.expectedPartitionFile("B", compactionJob.getOutputFiles().getRight(), 100L, 100L, 199L));
    }

    @Test
    public void filesShouldMergeAndSplitCorrectlyWith2DimKeySplitOnFirstKey() throws Exception {
        // Given
        Field field1 = new Field("key1", new LongType());
        Field field2 = new Field("key2", new StringType());
        Schema schema = createSchemaWithTwoTypedValuesAndKeyFields(new LongType(), new LongType(), field1, field2);
        StateStore stateStore = createStateStore("fsmascw2dksofk", schema, dynamoDBClient);
        stateStore.initialise(new PartitionsBuilder(schema)
                .leavesWithSplits(Arrays.asList("A", "B"), Collections.singletonList(100L))
                .parentJoining("C", "A", "B")
                .buildList());
        CompactSortedFilesTestDataHelper dataHelper = new CompactSortedFilesTestDataHelper(schema, stateStore);

        List<Record> data1 = specifiedAndTwoValuesFromEvens((even, record) -> {
            record.put(field1.getName(), (long) even);
            record.put(field2.getName(), "A");
        });
        List<Record> data2 = specifiedAndTwoValuesFromOdds((odd, record) -> {
            record.put(field1.getName(), (long) odd);
            record.put(field2.getName(), "A");
        });
        dataHelper.writeRootFile(folderName + "/file1.parquet", data1, 0L, 198L);
        dataHelper.writeRootFile(folderName + "/file2.parquet", data2, 1L, 199L);

        CompactionJob compactionJob = compactionFactory().createSplittingCompactionJob(
                dataHelper.allFileInfos(), "C", "A", "B", 100L, 0);
        dataHelper.addFilesToStateStoreForJob(compactionJob);

        // When
        CompactSortedFiles compactSortedFiles = createCompactSortedFiles(schema, compactionJob, stateStore);
        CompactSortedFiles.CompactionJobSummary summary = compactSortedFiles.compact();

        // Then
        //  - Read output files and check that they contain the right results
        List<Record> expectedResults = combineSortedBySingleKey(data1, data2, record -> record.get(field1.getName()));
        assertThat(summary.getLinesRead()).isEqualTo(200L);
        assertThat(summary.getLinesWritten()).isEqualTo(200L);
        assertThat(readDataFile(schema, compactionJob.getOutputFiles().getLeft())).isEqualTo(expectedResults.subList(0, 100));
        assertThat(readDataFile(schema, compactionJob.getOutputFiles().getRight())).isEqualTo(expectedResults.subList(100, 200));

        // - Check DynamoDBStateStore has correct ready for GC files
        assertReadyForGC(stateStore, dataHelper.allFileInfos());

        // - Check DynamoDBStateStore has correct active files
        assertThat(stateStore.getActiveFiles())
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("lastStateStoreUpdateTime")
                .containsExactlyInAnyOrder(
                        dataHelper.expectedPartitionFile("A", compactionJob.getOutputFiles().getLeft(), 100L, 0L, 99L),
                        dataHelper.expectedPartitionFile("B", compactionJob.getOutputFiles().getRight(), 100L, 100L, 199L));
    }

    @Test
    public void filesShouldMergeAndSplitCorrectlyWith2DimKeySplitOnSecondKey() throws Exception {
        // Given
        Field field1 = new Field("key1", new LongType());
        Field field2 = new Field("key2", new StringType());
        Schema schema = createSchemaWithTwoTypedValuesAndKeyFields(new LongType(), new LongType(), field1, field2);
        StateStore stateStore = createStateStore("fsmascw2dksosk", schema, dynamoDBClient);
        stateStore.initialise(new PartitionsBuilder(schema)
                .leavesWithSplitsOnDimension(1, Arrays.asList("A", "B"), Collections.singletonList("A2"))
                .parentJoining("C", "A", "B")
                .buildList());
        CompactSortedFilesTestDataHelper dataHelper = new CompactSortedFilesTestDataHelper(schema, stateStore);

        List<Record> data1 = specifiedAndTwoValuesFromEvens((even, record) -> {
            record.put(field1.getName(), (long) even);
            record.put(field2.getName(), "A");
        });
        List<Record> data2 = specifiedAndTwoValuesFromOdds((odd, record) -> {
            record.put(field1.getName(), (long) odd);
            record.put(field2.getName(), "B");
        });
        dataHelper.writeRootFile(folderName + "/file1.parquet", data1, 0L, 198L);
        dataHelper.writeRootFile(folderName + "/file2.parquet", data2, 1L, 199L);

        CompactionJob compactionJob = compactionFactory().createSplittingCompactionJob(
                dataHelper.allFileInfos(), "C", "A", "B", "A2", 1);
        dataHelper.addFilesToStateStoreForJob(compactionJob);

        // When
        CompactSortedFiles compactSortedFiles = createCompactSortedFiles(schema, compactionJob, stateStore);
        CompactSortedFiles.CompactionJobSummary summary = compactSortedFiles.compact();

        // Then
        //  - Read output files and check that they contain the right results
        assertThat(summary.getLinesRead()).isEqualTo(200L);
        assertThat(summary.getLinesWritten()).isEqualTo(200L);
        assertThat(readDataFile(schema, compactionJob.getOutputFiles().getLeft())).isEqualTo(data1);
        assertThat(readDataFile(schema, compactionJob.getOutputFiles().getRight())).isEqualTo(data2);

        // - Check DynamoDBStateStore has correct ready for GC files
        assertReadyForGC(stateStore, dataHelper.allFileInfos());

        // - Check DynamoDBStateStore has correct active files
        assertThat(stateStore.getActiveFiles())
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("lastStateStoreUpdateTime")
                .containsExactlyInAnyOrder(
                        dataHelper.expectedPartitionFile("A", compactionJob.getOutputFiles().getLeft(), 100L, 0L, 198L),
                        dataHelper.expectedPartitionFile("B", compactionJob.getOutputFiles().getRight(), 100L, 1L, 199L));
    }

    @Test
    public void filesShouldMergeAndSplitCorrectlyWhenOneChildFileIsEmpty() throws Exception {
        // Given
        Schema schema = createSchemaWithTypesForKeyAndTwoValues(new LongType(), new LongType(), new LongType());
        StateStore stateStore = createStateStore("fsmascwocfie", schema, dynamoDBClient);
        stateStore.initialise(new PartitionsBuilder(schema)
                .leavesWithSplits(Arrays.asList("A", "B"), Collections.singletonList(200L))
                .parentJoining("C", "A", "B")
                .buildList());
        CompactSortedFilesTestDataHelper dataHelper = new CompactSortedFilesTestDataHelper(schema, stateStore);

        List<Record> data1 = keyAndTwoValuesSortedEvenLongs();
        List<Record> data2 = keyAndTwoValuesSortedOddLongs();
        dataHelper.writeRootFile(folderName + "/file1.parquet", data1, 0L, 198L);
        dataHelper.writeRootFile(folderName + "/file2.parquet", data2, 1L, 199L);

        CompactionJob compactionJob = compactionFactory().createSplittingCompactionJob(
                dataHelper.allFileInfos(), "C", "A", "B", 200L, 0);
        dataHelper.addFilesToStateStoreForJob(compactionJob);

        // When
        CompactSortedFiles compactSortedFiles = createCompactSortedFiles(schema, compactionJob, stateStore);
        CompactSortedFiles.CompactionJobSummary summary = compactSortedFiles.compact();

        // Then
        //  - Read output files and check that they contain the right results
        List<Record> expectedResults = combineSortedBySingleKey(data1, data2);
        assertThat(summary.getLinesRead()).isEqualTo(200L);
        assertThat(summary.getLinesWritten()).isEqualTo(200L);
        assertThat(readDataFile(schema, compactionJob.getOutputFiles().getLeft())).isEqualTo(expectedResults);
        assertThat(readDataFile(schema, compactionJob.getOutputFiles().getRight())).isEmpty();

        // - Check DynamoDBStateStore has correct ready for GC files
        assertReadyForGC(stateStore, dataHelper.allFileInfos());

        // - Check DynamoDBStateStore has correct active files
        assertThat(stateStore.getActiveFiles())
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("lastStateStoreUpdateTime")
                .containsExactlyInAnyOrder(
                        dataHelper.expectedPartitionFile("A", compactionJob.getOutputFiles().getLeft(), 200L, 0L, 199L),
                        dataHelper.expectedPartitionFile("B", compactionJob.getOutputFiles().getRight(), 0L, null, null));
    }

    @Test
    public void filesShouldMergeAndApplyIteratorCorrectlyLongKey() throws Exception {
        // Given
        Schema schema = CompactSortedFilesTestUtils.createSchemaWithKeyTimestampValue();
        StateStore stateStore = createInitStateStore("fsmaaiclk", schema, dynamoDBClient);
        CompactSortedFilesTestDataHelper dataHelper = new CompactSortedFilesTestDataHelper(schema, stateStore);

        List<Record> data1 = specifiedFromEvens((even, record) -> {
            record.put("key", (long) even);
            record.put("timestamp", System.currentTimeMillis());
            record.put("value", 987654321L);
        });
        List<Record> data2 = specifiedFromOdds((odd, record) -> {
            record.put("key", (long) odd);
            record.put("timestamp", 0L);
            record.put("value", 123456789L);
        });
        dataHelper.writeLeafFile(folderName + "/file1.parquet", data1, 0L, 198L);
        dataHelper.writeLeafFile(folderName + "/file2.parquet", data2, 1L, 199L);

        CompactionJob compactionJob = compactionFactoryBuilder()
                .iteratorClassName(AgeOffIterator.class.getName())
                .iteratorConfig("timestamp,1000000")
                .build().createCompactionJob(dataHelper.allFileInfos(), dataHelper.singlePartition().getId());
        dataHelper.addFilesToStateStoreForJob(compactionJob);

        // When
        CompactSortedFiles compactSortedFiles = createCompactSortedFiles(schema, compactionJob, stateStore);
        CompactSortedFiles.CompactionJobSummary summary = compactSortedFiles.compact();

        // Then
        //  - Read output files and check that they contain the right results
        assertThat(summary.getLinesRead()).isEqualTo(200L);
        assertThat(summary.getLinesWritten()).isEqualTo(100L);
        assertThat(readDataFile(schema, compactionJob.getOutputFile())).isEqualTo(data1);

        // - Check DynamoDBStateStore has correct ready for GC files
        assertReadyForGC(stateStore, dataHelper.allFileInfos());

        // - Check DynamoDBStateStore has correct active files
        assertThat(stateStore.getActiveFiles())
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("lastStateStoreUpdateTime")
                .containsExactly(dataHelper.expectedLeafFile(compactionJob.getOutputFile(), 100L, 0L, 198L));
    }

    @Test
    public void filesShouldMergeAndSplitAndApplyIteratorCorrectlyLongKey() throws Exception {
        // Given
        Schema schema = CompactSortedFilesTestUtils.createSchemaWithKeyTimestampValue();
        StateStore stateStore = createStateStore("fsmasaaiclk", schema, dynamoDBClient);
        stateStore.initialise(new PartitionsBuilder(schema)
                .leavesWithSplits(Arrays.asList("A", "B"), Collections.singletonList(100L))
                .parentJoining("C", "A", "B")
                .buildList());
        CompactSortedFilesTestDataHelper dataHelper = new CompactSortedFilesTestDataHelper(schema, stateStore);

        List<Record> data1 = specifiedFromEvens((even, record) -> {
            record.put("key", (long) even);
            record.put("timestamp", System.currentTimeMillis());
            record.put("value", 987654321L);
        });
        List<Record> data2 = specifiedFromOdds((odd, record) -> {
            record.put("key", (long) odd);
            record.put("timestamp", 0L);
            record.put("value", 123456789L);
        });
        dataHelper.writeRootFile(folderName + "/file1.parquet", data1, 0L, 198L);
        dataHelper.writeRootFile(folderName + "/file2.parquet", data2, 1L, 199L);

        CompactionJob compactionJob = compactionFactoryBuilder()
                .iteratorClassName(AgeOffIterator.class.getName())
                .iteratorConfig("timestamp,1000000")
                .build().createSplittingCompactionJob(dataHelper.allFileInfos(), "C", "A", "B", 100L, 0);
        dataHelper.addFilesToStateStoreForJob(compactionJob);

        // When
        CompactSortedFiles compactSortedFiles = createCompactSortedFiles(schema, compactionJob, stateStore);
        CompactSortedFiles.CompactionJobSummary summary = compactSortedFiles.compact();

        // Then
        //  - Read output files and check that they contain the right results
        assertThat(summary.getLinesRead()).isEqualTo(200L);
        assertThat(summary.getLinesWritten()).isEqualTo(100L);
        assertThat(readDataFile(schema, compactionJob.getOutputFiles().getLeft())).isEqualTo(data1.subList(0, 50));
        assertThat(readDataFile(schema, compactionJob.getOutputFiles().getRight())).isEqualTo(data1.subList(50, 100));

        // - Check DynamoDBStateStore has correct ready for GC files
        assertReadyForGC(stateStore, dataHelper.allFileInfos());

        // - Check DynamoDBStateStore has correct active files
        assertThat(stateStore.getActiveFiles())
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("lastStateStoreUpdateTime")
                .containsExactlyInAnyOrder(
                        dataHelper.expectedPartitionFile("A", compactionJob.getOutputFiles().getLeft(), 50L, 0L, 98L),
                        dataHelper.expectedPartitionFile("B", compactionJob.getOutputFiles().getRight(), 50L, 100L, 198L));
    }
}
