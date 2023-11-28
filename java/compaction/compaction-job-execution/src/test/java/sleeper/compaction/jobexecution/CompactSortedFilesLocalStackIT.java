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
package sleeper.compaction.jobexecution;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import sleeper.compaction.job.CompactionJob;
import sleeper.compaction.job.CompactionJobStatusStore;
import sleeper.compaction.jobexecution.testutils.CompactSortedFilesTestBase;
import sleeper.compaction.jobexecution.testutils.CompactSortedFilesTestDataHelper;
import sleeper.compaction.status.store.job.CompactionJobStatusStoreFactory;
import sleeper.compaction.status.store.job.DynamoDBCompactionJobStatusStoreCreator;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.CommonTestConstants;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.record.Record;
import sleeper.core.record.process.RecordsProcessedSummary;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.LongType;
import sleeper.core.statestore.StateStore;
import sleeper.statestore.StateStoreFactory;
import sleeper.statestore.s3.S3StateStoreCreator;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.compaction.jobexecution.testutils.CompactSortedFilesTestData.combineSortedBySingleKey;
import static sleeper.compaction.jobexecution.testutils.CompactSortedFilesTestData.keyAndTwoValuesSortedEvenLongs;
import static sleeper.compaction.jobexecution.testutils.CompactSortedFilesTestData.keyAndTwoValuesSortedOddLongs;
import static sleeper.compaction.jobexecution.testutils.CompactSortedFilesTestData.readDataFile;
import static sleeper.compaction.jobexecution.testutils.CompactSortedFilesTestUtils.assertReadyForGC;
import static sleeper.compaction.jobexecution.testutils.CompactSortedFilesTestUtils.createCompactSortedFiles;
import static sleeper.compaction.jobexecution.testutils.CompactSortedFilesTestUtils.createSchemaWithTypesForKeyAndTwoValues;
import static sleeper.configuration.properties.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.configuration.properties.table.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.configuration.properties.table.TableProperty.GARBAGE_COLLECTOR_DELAY_BEFORE_DELETION;
import static sleeper.configuration.testutils.LocalStackAwsV1ClientHelper.buildAwsV1Client;
import static sleeper.io.parquet.utils.HadoopConfigurationLocalStackUtils.getHadoopConfiguration;

@Testcontainers
public class CompactSortedFilesLocalStackIT extends CompactSortedFilesTestBase {

    @Container
    public static LocalStackContainer localStackContainer = new LocalStackContainer(DockerImageName.parse(CommonTestConstants.LOCALSTACK_DOCKER_IMAGE)).withServices(
            LocalStackContainer.Service.S3, LocalStackContainer.Service.DYNAMODB);
    private static AmazonDynamoDB dynamoDBClient;
    private static AmazonS3 s3Client;
    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final CompactionJobStatusStore jobStatusStore = CompactionJobStatusStoreFactory.getStatusStore(dynamoDBClient, instanceProperties);

    @BeforeAll
    public static void beforeAll() {
        dynamoDBClient = buildAwsV1Client(localStackContainer, LocalStackContainer.Service.DYNAMODB, AmazonDynamoDBClientBuilder.standard());
        s3Client = buildAwsV1Client(localStackContainer, LocalStackContainer.Service.S3, AmazonS3ClientBuilder.standard());
    }

    @AfterAll
    public static void afterAll() {
        dynamoDBClient.shutdown();
        dynamoDBClient = null;
    }

    @BeforeEach
    void setUp() {
        s3Client.createBucket(instanceProperties.get(DATA_BUCKET));
        DynamoDBCompactionJobStatusStoreCreator.create(instanceProperties, dynamoDBClient);
        new S3StateStoreCreator(instanceProperties, dynamoDBClient).create();
    }

    private StateStore createStateStore(Schema schema) {
        TableProperties tableProperties = createTestTableProperties(instanceProperties, schema);
        tableProperties.set(GARBAGE_COLLECTOR_DELAY_BEFORE_DELETION, "0");
        return new StateStoreFactory(dynamoDBClient, instanceProperties, getHadoopConfiguration(localStackContainer))
                .getStateStore(tableProperties);
    }

    @Test
    public void shouldUpdateStateStoreAfterRunningStandardCompaction() throws Exception {
        // Given
        Schema schema = createSchemaWithTypesForKeyAndTwoValues(new LongType(), new LongType(), new LongType());
        StateStore stateStore = createStateStore(schema);
        stateStore.initialise();
        CompactSortedFilesTestDataHelper dataHelper = new CompactSortedFilesTestDataHelper(schema, stateStore);

        List<Record> data1 = keyAndTwoValuesSortedEvenLongs();
        List<Record> data2 = keyAndTwoValuesSortedOddLongs();
        dataHelper.writeRootFile(dataFolderName + "/file1.parquet", data1);
        dataHelper.writeRootFile(dataFolderName + "/file2.parquet", data2);

        CompactionJob compactionJob = compactionFactory().createCompactionJob(
                dataHelper.allFileInfos(), dataHelper.singlePartition().getId());
        dataHelper.addFilesToStateStoreForJob(compactionJob);

        // When
        CompactSortedFiles compactSortedFiles = createCompactSortedFiles(schema, compactionJob, stateStore, jobStatusStore, DEFAULT_TASK_ID);
        RecordsProcessedSummary summary = compactSortedFiles.compact();

        // Then
        //  - Read output file and check that it contains the right results
        List<Record> expectedResults = combineSortedBySingleKey(data1, data2);
        assertThat(summary.getRecordsRead()).isEqualTo(expectedResults.size());
        assertThat(summary.getRecordsWritten()).isEqualTo(expectedResults.size());
        assertThat(readDataFile(schema, compactionJob.getOutputFile())).isEqualTo(expectedResults);

        // - Check StateStore has correct ready for GC files
        assertReadyForGC(stateStore, dataHelper.allFileInfos());

        // - Check StateStore has correct active files
        assertThat(stateStore.getActiveFiles())
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("lastStateStoreUpdateTime")
                .containsExactly(dataHelper.expectedRootFile(compactionJob.getOutputFile(), 200L));
    }

    @Test
    public void shouldUpdateStateStoreAfterRunningSplittingCompaction() throws Exception {
        // Given
        Schema schema = createSchemaWithTypesForKeyAndTwoValues(new LongType(), new LongType(), new LongType());
        StateStore stateStore = createStateStore(schema);
        stateStore.initialise(new PartitionsBuilder(schema)
                .rootFirst("A")
                .splitToNewChildren("A", "B", "C", 100L)
                .buildList());
        CompactSortedFilesTestDataHelper dataHelper = new CompactSortedFilesTestDataHelper(schema, stateStore);

        List<Record> data1 = keyAndTwoValuesSortedEvenLongs();
        List<Record> data2 = keyAndTwoValuesSortedOddLongs();
        dataHelper.writeRootFile(dataFolderName + "/file1.parquet", data1);
        dataHelper.writeRootFile(dataFolderName + "/file2.parquet", data2);

        CompactionJob compactionJob = compactionFactory().createSplittingCompactionJob(
                dataHelper.allFileInfos(), "A", "B", "C", 100L, 0);
        dataHelper.addFilesToStateStoreForJob(compactionJob);

        // When
        CompactSortedFiles compactSortedFiles = createCompactSortedFiles(schema, compactionJob, stateStore, jobStatusStore, DEFAULT_TASK_ID);
        RecordsProcessedSummary summary = compactSortedFiles.compact();

        // Then
        //  - Read output files and check that they contain the right results
        List<Record> expectedResults = combineSortedBySingleKey(data1, data2);
        assertThat(summary.getRecordsRead()).isEqualTo(200L);
        assertThat(summary.getRecordsWritten()).isEqualTo(200L);
        assertThat(readDataFile(schema, compactionJob.getOutputFiles().getLeft())).isEqualTo(expectedResults.subList(0, 100));
        assertThat(readDataFile(schema, compactionJob.getOutputFiles().getRight())).isEqualTo(expectedResults.subList(100, 200));

        // - Check StateStore has correct ready for GC files
        assertReadyForGC(stateStore, dataHelper.allFileInfos());

        // - Check StateStore has correct active files
        assertThat(stateStore.getActiveFiles())
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("lastStateStoreUpdateTime")
                .containsExactlyInAnyOrder(
                        dataHelper.expectedPartitionFile("B", compactionJob.getOutputFiles().getLeft(), 100L),
                        dataHelper.expectedPartitionFile("C", compactionJob.getOutputFiles().getRight(), 100L));
    }

}
