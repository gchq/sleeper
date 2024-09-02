/*
 * Copyright 2022-2024 Crown Copyright
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
package sleeper.compaction.job.execution;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.apache.hadoop.conf.Configuration;
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
import sleeper.compaction.job.execution.testutils.CompactionRunnerTestBase;
import sleeper.compaction.job.execution.testutils.CompactionRunnerTestData;
import sleeper.compaction.status.store.job.DynamoDBCompactionJobStatusStoreCreator;
import sleeper.core.CommonTestConstants;
import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.record.Record;
import sleeper.core.record.process.RecordsProcessed;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.LongType;
import sleeper.core.statestore.FileReference;
import sleeper.sketches.testutils.SketchesDeciles;
import sleeper.statestore.StateStoreFactory;
import sleeper.statestore.transactionlog.TransactionLogStateStoreCreator;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.testcontainers.containers.localstack.LocalStackContainer.Service.DYNAMODB;
import static org.testcontainers.containers.localstack.LocalStackContainer.Service.S3;
import static sleeper.compaction.job.execution.testutils.CompactionRunnerTestUtils.assignJobIdToInputFiles;
import static sleeper.compaction.job.execution.testutils.CompactionRunnerTestUtils.createSchemaWithTypesForKeyAndTwoValues;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.configuration.properties.instance.CommonProperty.FILE_SYSTEM;
import static sleeper.configuration.properties.instance.CommonProperty.ID;
import static sleeper.configuration.properties.instance.DefaultProperty.DEFAULT_INGEST_PARTITION_FILE_WRITER_TYPE;
import static sleeper.configuration.properties.table.TableProperty.GARBAGE_COLLECTOR_DELAY_BEFORE_DELETION;
import static sleeper.configuration.testutils.LocalStackAwsV1ClientHelper.buildAwsV1Client;
import static sleeper.ingest.testutils.LocalStackAwsV2ClientHelper.buildAwsV2Client;
import static sleeper.io.parquet.utils.HadoopConfigurationLocalStackUtils.getHadoopConfiguration;

@Testcontainers
public class JavaCompactionRunnerLocalStackIT extends CompactionRunnerTestBase {

    @Container
    public static LocalStackContainer localStackContainer = new LocalStackContainer(
            DockerImageName.parse(CommonTestConstants.LOCALSTACK_DOCKER_IMAGE)).withServices(S3, DYNAMODB);
    private static AmazonDynamoDB dynamoDBClient;
    private static AmazonS3 s3Client;
    private static S3AsyncClient s3AsyncClient;
    private static Configuration configuration;

    @BeforeAll
    public static void beforeAll() {
        dynamoDBClient = buildAwsV1Client(localStackContainer, DYNAMODB, AmazonDynamoDBClientBuilder.standard());
        s3Client = buildAwsV1Client(localStackContainer, S3, AmazonS3ClientBuilder.standard());
        s3AsyncClient = buildAwsV2Client(localStackContainer, S3, S3AsyncClient.builder());
        configuration = getHadoopConfiguration(localStackContainer);
    }

    @AfterAll
    public static void afterAll() {
        dynamoDBClient.shutdown();
        s3Client.shutdown();
        s3AsyncClient.close();
    }

    @BeforeEach
    void setUp() {
        String dataBucket = "data-bucket-" + instanceProperties.get(ID);
        instanceProperties.set(FILE_SYSTEM, "s3a://");
        instanceProperties.set(DATA_BUCKET, dataBucket);
        instanceProperties.unset(DEFAULT_INGEST_PARTITION_FILE_WRITER_TYPE);
        s3Client.createBucket(dataBucket);
        new TransactionLogStateStoreCreator(instanceProperties, dynamoDBClient).create();
        DynamoDBCompactionJobStatusStoreCreator.create(instanceProperties, dynamoDBClient);
    }

    @Test
    public void shouldRunCompactionJob() throws Exception {
        // Given
        Schema schema = createSchemaWithTypesForKeyAndTwoValues(new LongType(), new LongType(), new LongType());
        tableProperties.setSchema(schema);
        createStateStore();
        PartitionTree tree = new PartitionsBuilder(schema).singlePartition("root").buildTree();
        stateStore.initialise(tree.getAllPartitions());

        List<Record> data1 = CompactionRunnerTestData.keyAndTwoValuesSortedEvenLongs();
        List<Record> data2 = CompactionRunnerTestData.keyAndTwoValuesSortedOddLongs();
        FileReference file1 = ingestRecordsGetFile(data1);
        FileReference file2 = ingestRecordsGetFile(data2);

        CompactionJob compactionJob = compactionFactory().createCompactionJob(List.of(file1, file2), "root");
        assignJobIdToInputFiles(stateStore, compactionJob);

        // When
        RecordsProcessed summary = compact(compactionJob, configuration);

        // Then
        //  - Read output file and check that it contains the right results
        List<Record> expectedResults = CompactionRunnerTestData.combineSortedBySingleKey(data1, data2);
        assertThat(summary.getRecordsRead()).isEqualTo(expectedResults.size());
        assertThat(summary.getRecordsWritten()).isEqualTo(expectedResults.size());
        assertThat(CompactionRunnerTestData.readDataFile(schema, compactionJob.getOutputFile())).isEqualTo(expectedResults);
        assertThat(SketchesDeciles.from(readSketches(schema, compactionJob.getOutputFile())))
                .isEqualTo(SketchesDeciles.builder()
                        .field("key", builder -> builder
                                .min(0L).max(199L)
                                .rank(0.1, 20L).rank(0.2, 40L).rank(0.3, 60L)
                                .rank(0.4, 80L).rank(0.5, 100L).rank(0.6, 120L)
                                .rank(0.7, 140L).rank(0.8, 160L).rank(0.9, 180L))
                        .build());
    }

    protected FileReference ingestRecordsGetFile(List<Record> records) throws Exception {
        return ingestRecordsGetFile(records, builder -> builder
                .hadoopConfiguration(configuration)
                .s3AsyncClient(s3AsyncClient));
    }

    private void createStateStore() {
        tableProperties.set(GARBAGE_COLLECTOR_DELAY_BEFORE_DELETION, "0");
        stateStore = new StateStoreFactory(instanceProperties, s3Client, dynamoDBClient, configuration)
                .getStateStore(tableProperties);
    }
}
