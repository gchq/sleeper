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
import software.amazon.awssdk.services.s3.S3AsyncClient;

import sleeper.compaction.job.CompactionJob;
import sleeper.compaction.job.CompactionJobStatusStore;
import sleeper.compaction.jobexecution.testutils.CompactSortedFilesTestBase;
import sleeper.compaction.status.store.job.CompactionJobStatusStoreFactory;
import sleeper.compaction.status.store.job.DynamoDBCompactionJobStatusStoreCreator;
import sleeper.configuration.jars.ObjectFactory;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.CommonTestConstants;
import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.record.Record;
import sleeper.core.record.process.RecordsProcessedSummary;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.LongType;
import sleeper.core.statestore.FileInfo;
import sleeper.core.statestore.FileInfoFactory;
import sleeper.core.statestore.SplitFileInfo;
import sleeper.core.statestore.StateStore;
import sleeper.statestore.FixedStateStoreProvider;
import sleeper.statestore.StateStoreFactory;
import sleeper.statestore.s3.S3StateStoreCreator;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.testcontainers.containers.localstack.LocalStackContainer.Service.DYNAMODB;
import static org.testcontainers.containers.localstack.LocalStackContainer.Service.S3;
import static sleeper.compaction.jobexecution.testutils.CompactSortedFilesTestData.combineSortedBySingleKey;
import static sleeper.compaction.jobexecution.testutils.CompactSortedFilesTestData.keyAndTwoValuesSortedEvenLongs;
import static sleeper.compaction.jobexecution.testutils.CompactSortedFilesTestData.keyAndTwoValuesSortedOddLongs;
import static sleeper.compaction.jobexecution.testutils.CompactSortedFilesTestData.readDataFile;
import static sleeper.compaction.jobexecution.testutils.CompactSortedFilesTestUtils.createSchemaWithTypesForKeyAndTwoValues;
import static sleeper.configuration.properties.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.configuration.properties.table.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.configuration.properties.table.TableProperty.GARBAGE_COLLECTOR_DELAY_BEFORE_DELETION;
import static sleeper.configuration.testutils.LocalStackAwsV1ClientHelper.buildAwsV1Client;
import static sleeper.ingest.testutils.LocalStackAwsV2ClientHelper.buildAwsV2Client;
import static sleeper.io.parquet.utils.HadoopConfigurationLocalStackUtils.getHadoopConfiguration;

@Testcontainers
public class CompactSortedFilesLocalStackIT extends CompactSortedFilesTestBase {

    @Container
    public static LocalStackContainer localStackContainer = new LocalStackContainer(
            DockerImageName.parse(CommonTestConstants.LOCALSTACK_DOCKER_IMAGE)).withServices(S3, DYNAMODB);
    private static AmazonDynamoDB dynamoDBClient;
    private static AmazonS3 s3Client;
    private static S3AsyncClient s3AsyncClient;
    private CompactionJobStatusStore jobStatusStore;

    @BeforeAll
    public static void beforeAll() {
        dynamoDBClient = buildAwsV1Client(localStackContainer, DYNAMODB, AmazonDynamoDBClientBuilder.standard());
        s3Client = buildAwsV1Client(localStackContainer, S3, AmazonS3ClientBuilder.standard());
        s3AsyncClient = buildAwsV2Client(localStackContainer, S3, S3AsyncClient.builder());
    }

    @AfterAll
    public static void afterAll() {
        dynamoDBClient.shutdown();
        s3Client.shutdown();
        s3AsyncClient.close();
    }

    @BeforeEach
    void setUp() {
        instanceProperties.resetAndValidate(createTestInstanceProperties().getProperties());
        s3Client.createBucket(instanceProperties.get(DATA_BUCKET));
        new S3StateStoreCreator(instanceProperties, dynamoDBClient).create();
        DynamoDBCompactionJobStatusStoreCreator.create(instanceProperties, dynamoDBClient);
        jobStatusStore = CompactionJobStatusStoreFactory.getStatusStore(dynamoDBClient, instanceProperties);
    }

    protected FileInfo ingestRecordsGetFile(StateStore stateStore, List<Record> records) throws Exception {
        return ingestRecordsGetFile(records, builder -> builder
                .stateStoreProvider(new FixedStateStoreProvider(tableProperties, stateStore))
                .hadoopConfiguration(getHadoopConfiguration(localStackContainer))
                .s3AsyncClient(s3AsyncClient));
    }

    private StateStore createStateStore(Schema schema) {
        TableProperties tableProperties = createTestTableProperties(instanceProperties, schema);
        tableProperties.set(GARBAGE_COLLECTOR_DELAY_BEFORE_DELETION, "0");
        return new StateStoreFactory(dynamoDBClient, instanceProperties, getHadoopConfiguration(localStackContainer))
                .getStateStore(tableProperties);
    }

    private CompactSortedFiles createCompactSortedFiles(Schema schema, CompactionJob compactionJob, StateStore stateStore) throws Exception {
        tableProperties.setSchema(schema);
        return new CompactSortedFiles(instanceProperties, tableProperties, ObjectFactory.noUserJars(),
                compactionJob, stateStore, jobStatusStore, DEFAULT_TASK_ID);
    }

    @Test
    public void shouldUpdateStateStoreAfterRunningStandardCompaction() throws Exception {
        // Given
        Schema schema = createSchemaWithTypesForKeyAndTwoValues(new LongType(), new LongType(), new LongType());
        tableProperties.setSchema(schema);
        StateStore stateStore = createStateStore(schema);
        PartitionTree tree = new PartitionsBuilder(schema).singlePartition("root").buildTree();
        stateStore.initialise(tree.getAllPartitions());

        List<Record> data1 = keyAndTwoValuesSortedEvenLongs();
        List<Record> data2 = keyAndTwoValuesSortedOddLongs();
        FileInfo file1 = ingestRecordsGetFile(stateStore, data1);
        FileInfo file2 = ingestRecordsGetFile(stateStore, data2);

        CompactionJob compactionJob = compactionFactory().createCompactionJob(List.of(file1, file2), "root");

        // When
        CompactSortedFiles compactSortedFiles = createCompactSortedFiles(schema, compactionJob, stateStore);
        RecordsProcessedSummary summary = compactSortedFiles.compact();

        // Then
        //  - Read output file and check that it contains the right results
        List<Record> expectedResults = combineSortedBySingleKey(data1, data2);
        assertThat(summary.getRecordsRead()).isEqualTo(expectedResults.size());
        assertThat(summary.getRecordsWritten()).isEqualTo(expectedResults.size());
        assertThat(readDataFile(schema, compactionJob.getOutputFile())).isEqualTo(expectedResults);

        // - Check StateStore has correct ready for GC files
        assertThat(stateStore.getReadyForGCFilenamesBefore(Instant.ofEpochMilli(Long.MAX_VALUE)))
                .containsExactly(file1.getFilename(), file2.getFilename());

        // - Check StateStore has correct active files
        assertThat(stateStore.getActiveFiles())
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("lastStateStoreUpdateTime")
                .containsExactly(FileInfoFactory.from(tree).rootFile(compactionJob.getOutputFile(), 200L));
    }

    @Test
    public void shouldUpdateStateStoreAfterRunningSplittingCompaction() throws Exception {
        // Given
        Schema schema = createSchemaWithTypesForKeyAndTwoValues(new LongType(), new LongType(), new LongType());
        tableProperties.setSchema(schema);
        StateStore stateStore = createStateStore(schema);
        PartitionsBuilder partitions = new PartitionsBuilder(schema).rootFirst("A");
        stateStore.initialise(partitions.buildList());

        List<Record> data1 = keyAndTwoValuesSortedEvenLongs();
        List<Record> data2 = keyAndTwoValuesSortedOddLongs();
        FileInfo file1 = ingestRecordsGetFile(stateStore, data1);
        FileInfo file2 = ingestRecordsGetFile(stateStore, data2);

        partitions.splitToNewChildren("A", "B", "C", 100L)
                .applySplit(stateStore, "A");

        CompactionJob compactionJob = compactionFactory().createSplittingCompactionJob(
                List.of(file1, file2), "A", "B", "C");

        // When
        CompactSortedFiles compactSortedFiles = createCompactSortedFiles(schema, compactionJob, stateStore);
        RecordsProcessedSummary summary = compactSortedFiles.compact();

        // Then
        //  - Read output files and check that they contain the right results
        String file1LeftOutput = jobPartitionFilename(compactionJob, "B", 0);
        String file1RightOutput = jobPartitionFilename(compactionJob, "C", 0);
        String file2LeftOutput = jobPartitionFilename(compactionJob, "B", 1);
        String file2RightOutput = jobPartitionFilename(compactionJob, "C", 1);
        assertThat(summary.getRecordsRead()).isEqualTo(400L);
        assertThat(summary.getRecordsWritten()).isEqualTo(400L);
        assertThat(readDataFile(schema, file1LeftOutput)).isEqualTo(data1);
        assertThat(readDataFile(schema, file1RightOutput)).isEqualTo(data1);
        assertThat(readDataFile(schema, file2LeftOutput)).isEqualTo(data2);
        assertThat(readDataFile(schema, file2RightOutput)).isEqualTo(data2);

        // - Check StateStore has correct ready for GC files
        assertThat(stateStore.getReadyForGCFilenamesBefore(Instant.ofEpochMilli(Long.MAX_VALUE)))
                .containsExactly(file1.getFilename(), file2.getFilename());

        // - Check StateStore has correct active files
        assertThat(stateStore.getActiveFiles())
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("lastStateStoreUpdateTime")
                .containsExactlyInAnyOrder(
                        SplitFileInfo.copyToChildPartition(file1, "B", file1LeftOutput),
                        SplitFileInfo.copyToChildPartition(file1, "C", file1RightOutput),
                        SplitFileInfo.copyToChildPartition(file2, "B", file2LeftOutput),
                        SplitFileInfo.copyToChildPartition(file2, "C", file2RightOutput));
    }

}
