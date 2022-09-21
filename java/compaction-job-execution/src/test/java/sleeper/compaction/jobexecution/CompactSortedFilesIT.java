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
import sleeper.compaction.job.CompactionFactory;
import sleeper.compaction.job.CompactionJob;
import sleeper.core.CommonTestConstants;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.record.Record;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.LongType;
import sleeper.statestore.StateStore;
import sleeper.statestore.StateStoreException;
import sleeper.statestore.dynamodb.DynamoDBStateStoreCreator;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.compaction.jobexecution.CompactSortedFilesTestData.combineSortedBySingleKey;
import static sleeper.compaction.jobexecution.CompactSortedFilesTestData.keyAndTwoValuesSortedEvenLongs;
import static sleeper.compaction.jobexecution.CompactSortedFilesTestData.keyAndTwoValuesSortedOddLongs;
import static sleeper.compaction.jobexecution.CompactSortedFilesTestData.readDataFile;
import static sleeper.compaction.jobexecution.CompactSortedFilesTestUtils.assertReadyForGC;
import static sleeper.compaction.jobexecution.CompactSortedFilesTestUtils.createCompactSortedFiles;
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

    private CompactionFactory compactionFactory() {
        return compactionFactoryBuilder().build();
    }

    private CompactionFactory.Builder compactionFactoryBuilder() {
        return CompactionFactory.withTableName("table").outputFilePrefix(folderName);
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

}
