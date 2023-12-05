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
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.GetQueueAttributesRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.model.SetQueueAttributesRequest;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import sleeper.compaction.job.CompactionJob;
import sleeper.compaction.job.CompactionJobSerDe;
import sleeper.compaction.job.CompactionJobStatusStore;
import sleeper.compaction.job.CompactionOutputFileNameFactory;
import sleeper.compaction.status.store.job.CompactionJobStatusStoreFactory;
import sleeper.compaction.status.store.job.DynamoDBCompactionJobStatusStoreCreator;
import sleeper.compaction.status.store.task.CompactionTaskStatusStoreFactory;
import sleeper.compaction.status.store.task.DynamoDBCompactionTaskStatusStoreCreator;
import sleeper.compaction.task.CompactionTaskStatusStore;
import sleeper.compaction.task.CompactionTaskType;
import sleeper.configuration.jars.ObjectFactory;
import sleeper.configuration.properties.PropertiesReloader;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.instance.InstanceProperty;
import sleeper.configuration.properties.table.S3TableProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.configuration.properties.table.TablePropertiesStore;
import sleeper.configuration.table.index.DynamoDBTableIndexCreator;
import sleeper.core.CommonTestConstants;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.record.Record;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.LongType;
import sleeper.core.statestore.FileInfo;
import sleeper.core.statestore.FileInfoFactory;
import sleeper.core.statestore.SplitFileInfo;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;
import sleeper.ingest.IngestFactory;
import sleeper.ingest.impl.IngestCoordinator;
import sleeper.statestore.FixedStateStoreProvider;
import sleeper.statestore.StateStoreProvider;
import sleeper.statestore.s3.S3StateStoreCreator;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static sleeper.configuration.properties.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.COMPACTION_JOB_DLQ_URL;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.COMPACTION_JOB_QUEUE_URL;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.configuration.properties.instance.CommonProperty.FILE_SYSTEM;
import static sleeper.configuration.properties.instance.IngestProperty.INGEST_PARTITION_FILE_WRITER_TYPE;
import static sleeper.configuration.properties.table.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.configuration.properties.table.TableProperty.COMPACTION_FILES_BATCH_SIZE;
import static sleeper.configuration.properties.table.TableProperty.TABLE_ID;
import static sleeper.configuration.testutils.LocalStackAwsV1ClientHelper.buildAwsV1Client;
import static sleeper.io.parquet.utils.HadoopConfigurationLocalStackUtils.getHadoopConfiguration;

@Testcontainers
public class CompactSortedFilesRunnerLocalStackIT {

    @Container
    public static LocalStackContainer localStackContainer = new LocalStackContainer(DockerImageName.parse(CommonTestConstants.LOCALSTACK_DOCKER_IMAGE)).withServices(
            LocalStackContainer.Service.S3, LocalStackContainer.Service.SQS, LocalStackContainer.Service.DYNAMODB);

    private final AmazonS3 s3 = buildAwsV1Client(localStackContainer, LocalStackContainer.Service.S3, AmazonS3ClientBuilder.standard());
    private final AmazonDynamoDB dynamoDB = buildAwsV1Client(localStackContainer, LocalStackContainer.Service.DYNAMODB, AmazonDynamoDBClientBuilder.standard());
    private final AmazonSQS sqs = buildAwsV1Client(localStackContainer, LocalStackContainer.Service.SQS, AmazonSQSClientBuilder.standard());
    private final InstanceProperties instanceProperties = createInstance();
    private final Configuration configuration = getHadoopConfiguration(localStackContainer);
    private final StateStoreProvider stateStoreProvider = new StateStoreProvider(dynamoDB, instanceProperties, configuration);
    private final TablePropertiesStore tablePropertiesStore = S3TableProperties.getStore(instanceProperties, s3, dynamoDB);
    private final TablePropertiesProvider tablePropertiesProvider = new TablePropertiesProvider(instanceProperties, s3, dynamoDB);
    private final Schema schema = createSchema();
    private final TableProperties tableProperties = createTable();
    private final String tableId = tableProperties.get(TABLE_ID);
    private final CompactionJobStatusStore jobStatusStore = CompactionJobStatusStoreFactory.getStatusStore(dynamoDB, instanceProperties);
    private final CompactionTaskStatusStore taskStatusStore = CompactionTaskStatusStoreFactory.getStatusStore(dynamoDB, instanceProperties);

    private InstanceProperties createInstance() {
        InstanceProperties instanceProperties = createTestInstanceProperties();
        instanceProperties.set(FILE_SYSTEM, "");
        instanceProperties.set(INGEST_PARTITION_FILE_WRITER_TYPE, "direct");
        s3.createBucket(instanceProperties.get(CONFIG_BUCKET));
        s3.createBucket(instanceProperties.get(DATA_BUCKET));
        instanceProperties.saveToS3(s3);
        DynamoDBTableIndexCreator.create(dynamoDB, instanceProperties);
        new S3StateStoreCreator(instanceProperties, dynamoDB).create();

        return instanceProperties;
    }

    private Schema createSchema() {
        return Schema.builder()
                .rowKeyFields(new Field("key", new LongType()))
                .valueFields(new Field("value1", new LongType()), new Field("value2", new LongType()))
                .build();
    }

    private TableProperties createTable() {
        TableProperties tableProperties = createTestTableProperties(instanceProperties, schema);
        tableProperties.set(COMPACTION_FILES_BATCH_SIZE, "5");
        tablePropertiesStore.save(tableProperties);
        try {
            stateStoreProvider.getStateStore(tableProperties).initialise();
        } catch (StateStoreException e) {
            throw new RuntimeException(e);
        }
        return tableProperties;
    }

    private StateStore getStateStore() {
        return stateStoreProvider.getStateStore(tableProperties);
    }

    @AfterEach
    void tearDown() {
        s3.shutdown();
        dynamoDB.shutdown();
        sqs.shutdown();
    }

    @BeforeEach
    void setUp() {
        DynamoDBCompactionJobStatusStoreCreator.create(instanceProperties, dynamoDB);
        DynamoDBCompactionTaskStatusStoreCreator.create(instanceProperties, dynamoDB);
    }

    @TempDir
    public java.nio.file.Path tempDir;

    @Nested
    @DisplayName("Standard compactions")
    class StandardCompactions {
        @Test
        void shouldDeleteMessagesIfJobSuccessful() throws Exception {
            // Given
            configureJobQueuesWithMaxReceiveCount(10);
            // - Create four files of sorted data
            StateStore stateStore = getStateStore();
            FileInfo fileInfo1 = ingestFileWith100Records(i ->
                    new Record(Map.of(
                            "key", (long) 2 * i,
                            "value1", (long) 2 * i,
                            "value2", 987654321L)));
            FileInfo fileInfo2 = ingestFileWith100Records(i ->
                    new Record(Map.of(
                            "key", (long) 2 * i + 1,
                            "value1", 1001L,
                            "value2", 123456789L)));
            FileInfo fileInfo3 = ingestFileWith100Records(i ->
                    new Record(Map.of(
                            "key", (long) 2 * i,
                            "value1", (long) 2 * i,
                            "value2", 987654321L)));
            FileInfo fileInfo4 = ingestFileWith100Records(i ->
                    new Record(Map.of(
                            "key", (long) 2 * i + 1,
                            "value1", 1001L,
                            "value2", 123456789L)));

            // - Create two compaction jobs and put on queue
            CompactionJob job1 = compactionJobForFiles("job1", "output1.parquet", fileInfo1, fileInfo2);
            CompactionJob job2 = compactionJobForFiles("job2", "output2.parquet", fileInfo3, fileInfo4);
            String job1Json = CompactionJobSerDe.serialiseToString(job1);
            String job2Json = CompactionJobSerDe.serialiseToString(job2);
            SendMessageRequest sendMessageRequest = new SendMessageRequest()
                    .withQueueUrl(instanceProperties.get(COMPACTION_JOB_QUEUE_URL))
                    .withMessageBody(job1Json);
            sqs.sendMessage(sendMessageRequest);
            sendMessageRequest = new SendMessageRequest()
                    .withQueueUrl(instanceProperties.get(COMPACTION_JOB_QUEUE_URL))
                    .withMessageBody(job2Json);
            sqs.sendMessage(sendMessageRequest);

            // When
            createJobRunner("task-id").run();

            // Then
            // - There should be no messages left on the queue
            assertThat(messagesOnQueue(COMPACTION_JOB_QUEUE_URL)).isEmpty();
            // - Check DynamoDBStateStore has correct active files
            List<FileInfo> activeFiles = stateStore.getActiveFiles();
            assertThat(activeFiles)
                    .extracting(FileInfo::getFilename)
                    .containsExactlyInAnyOrder(job1.getOutputFile(), job2.getOutputFile());
        }

        @Test
        void shouldPutMessageBackOnSQSQueueIfJobFailed() throws Exception {
            // Given
            configureJobQueuesWithMaxReceiveCount(10);
            StateStore stateStore = getStateStore();
            FileInfoFactory factory = FileInfoFactory.from(schema, stateStore);
            // - Create a compaction job for a non-existent file
            String jobJson = sendCompactionJobForFilesGetJson("job1", "output1.parquet",
                    factory.rootFile("not-a-file.parquet", 0L));

            // When
            createJobRunner("task-id").run();

            // Then
            // - The compaction job should be put back on the queue
            assertThat(messagesOnQueue(COMPACTION_JOB_QUEUE_URL)).containsExactly(jobJson);
            // - No active files should be in the state store
            assertThat(stateStore.getActiveFiles()).isEmpty();
        }

        @Test
        void shouldMoveMessageToDLQIfJobFailedTooManyTimes() throws Exception {
            // Given
            configureJobQueuesWithMaxReceiveCount(2);
            StateStore stateStore = getStateStore();
            FileInfoFactory factory = FileInfoFactory.from(schema, stateStore);
            // - Create a compaction job for a non-existent file
            String jobJson = sendCompactionJobForFilesGetJson("job1", "output1.parquet",
                    factory.rootFile("not-a-file.parquet", 0L));


            // When
            createJobRunner("task-id").run();
            createJobRunner("task-id").run();
            createJobRunner("task-id").run();

            // Then
            // - The compaction job should no longer be on the job queue
            assertThat(messagesOnQueue(COMPACTION_JOB_QUEUE_URL)).isEmpty();
            // - The compaction job should be on the DLQ
            assertThat(messagesOnQueue(COMPACTION_JOB_DLQ_URL))
                    .containsExactly(jobJson);
            // - No active files should be in the state store
            assertThat(stateStore.getActiveFiles()).isEmpty();
        }

        @Test
        void shouldPutMessageBackOnSQSQueueIfStateStoreUpdateFailed() throws Exception {
            // Given
            configureJobQueuesWithMaxReceiveCount(2);
            StateStore stateStore = mock(StateStore.class);
            doThrow(new StateStoreException("Failed to update state store"))
                    .when(stateStore).atomicallyUpdateFilesToReadyForGCAndCreateNewActiveFile(any(), any());
            FileInfo fileInfo1 = ingestFileWith100Records();
            FileInfo fileInfo2 = ingestFileWith100Records();
            String jobJson = sendCompactionJobForFilesGetJson("job1", "output1.parquet", fileInfo1, fileInfo2);

            // When
            createJobRunner("task-id", new FixedStateStoreProvider(tableProperties, stateStore)).run();

            // Then
            // - The compaction job should be put back on the queue
            assertThat(messagesOnQueue(COMPACTION_JOB_QUEUE_URL))
                    .containsExactly(jobJson);
            // - No active files should be in the state store
            assertThat(stateStore.getActiveFiles()).isEmpty();
        }

        @Test
        void shouldMoveMessageToDLQIfStateStoreUpdateFailedTooManyTimes() throws Exception {
            // Given
            configureJobQueuesWithMaxReceiveCount(2);
            StateStore stateStore = mock(StateStore.class);
            doThrow(new StateStoreException("Failed to update state store"))
                    .when(stateStore).atomicallyUpdateFilesToReadyForGCAndCreateNewActiveFile(any(), any());
            FileInfo fileInfo1 = ingestFileWith100Records();
            FileInfo fileInfo2 = ingestFileWith100Records();
            String jobJson = sendCompactionJobForFilesGetJson("job1", "output1.parquet", fileInfo1, fileInfo2);

            // When
            StateStoreProvider provider = new FixedStateStoreProvider(tableProperties, stateStore);
            createJobRunner("task-id", provider).run();
            createJobRunner("task-id", provider).run();
            createJobRunner("task-id", provider).run();

            // Then
            // - The compaction job should no longer be on the job queue
            assertThat(messagesOnQueue(COMPACTION_JOB_QUEUE_URL)).isEmpty();
            // - The compaction job should be on the DLQ
            assertThat(messagesOnQueue(COMPACTION_JOB_DLQ_URL))
                    .containsExactly(jobJson);
            // - No active files should be in the state store
            assertThat(stateStore.getActiveFiles()).isEmpty();
        }
    }

    @Nested
    @DisplayName("Splitting compactions")
    class SplittingCompactions {
        @Test
        void shouldDeleteMessagesIfJobSuccessful() throws Exception {
            // Given
            configureJobQueuesWithMaxReceiveCount(10);
            PartitionsBuilder partitions = new PartitionsBuilder(schema);
            StateStore stateStore = getStateStore();
            stateStore.initialise(partitions.rootFirst("root").buildList());
            FileInfo fileInfo = ingestFileWith100Records(i ->
                    new Record(Map.of(
                            "key", (long) 2 * i,
                            "value1", (long) 2 * i,
                            "value2", 987654321L)));
            partitions.splitToNewChildren("root", "L", "R", 100L)
                    .applySplit(stateStore, "root");
            CompactionJob job1 = splittingJobForFiles("job1", fileInfo);
            String job1Json = CompactionJobSerDe.serialiseToString(job1);
            SendMessageRequest sendMessageRequest = new SendMessageRequest()
                    .withQueueUrl(instanceProperties.get(COMPACTION_JOB_QUEUE_URL))
                    .withMessageBody(job1Json);
            sqs.sendMessage(sendMessageRequest);

            // When
            createJobRunner("task-id").run();

            // Then
            // - There should be no messages left on the queue
            assertThat(messagesOnQueue(COMPACTION_JOB_QUEUE_URL)).isEmpty();
            // - Check DynamoDBStateStore has correct active files
            assertThat(stateStore.getActiveFiles())
                    .usingRecursiveFieldByFieldElementComparatorIgnoringFields("lastStateStoreUpdateTime")
                    .containsExactly(
                            SplitFileInfo.copyToChildPartition(fileInfo, "L", jobPartitionFilename(job1, "L", 0)),
                            SplitFileInfo.copyToChildPartition(fileInfo, "R", jobPartitionFilename(job1, "R", 0)));
        }

        @Test
        void shouldPutMessageBackOnSQSQueueIfJobFailed() throws Exception {
            // Given
            configureJobQueuesWithMaxReceiveCount(10);
            StateStore stateStore = getStateStore();
            FileInfoFactory factory = FileInfoFactory.from(schema, stateStore);
            // - Create a compaction job for a non-existent file
            String jobJson = sendSplittingJobForFilesGetJson("job1", factory.rootFile("not-a-file.parquet", 0L));

            // When
            createJobRunner("task-id").run();

            // Then
            // - The compaction job should be put back on the queue
            assertThat(messagesOnQueue(COMPACTION_JOB_QUEUE_URL)).containsExactly(jobJson);
            // - No active files should be in the state store
            assertThat(stateStore.getActiveFiles()).isEmpty();
        }

        @Test
        void shouldMoveMessageToDLQIfJobFailedTooManyTimes() throws Exception {
            // Given
            configureJobQueuesWithMaxReceiveCount(2);
            StateStore stateStore = getStateStore();
            FileInfoFactory factory = FileInfoFactory.from(schema, stateStore);
            // - Create a compaction job for a non-existent file
            String jobJson = sendSplittingJobForFilesGetJson("job1", factory.rootFile("not-a-file.parquet", 0L));


            // When
            createJobRunner("task-id").run();
            createJobRunner("task-id").run();
            createJobRunner("task-id").run();

            // Then
            // - The compaction job should no longer be on the job queue
            assertThat(messagesOnQueue(COMPACTION_JOB_QUEUE_URL)).isEmpty();
            // - The compaction job should be on the DLQ
            assertThat(messagesOnQueue(COMPACTION_JOB_DLQ_URL))
                    .containsExactly(jobJson);
            // - No active files should be in the state store
            assertThat(stateStore.getActiveFiles()).isEmpty();
        }

        @Test
        void shouldPutMessageBackOnSQSQueueIfStateStoreUpdateFailed() throws Exception {
            // Given
            configureJobQueuesWithMaxReceiveCount(2);
            StateStore stateStore = mock(StateStore.class);
            doThrow(new StateStoreException("Failed to update state store"))
                    .when(stateStore).atomicallyUpdateFilesToReadyForGCAndCreateNewActiveFiles(any(), any());
            FileInfo fileInfo1 = ingestFileWith100Records();
            String jobJson = sendSplittingJobForFilesGetJson("job1", fileInfo1);

            // When
            createJobRunner("task-id", new FixedStateStoreProvider(tableProperties, stateStore)).run();

            // Then
            // - The compaction job should be put back on the queue
            assertThat(messagesOnQueue(COMPACTION_JOB_QUEUE_URL))
                    .containsExactly(jobJson);
            // - No active files should be in the state store
            assertThat(stateStore.getActiveFiles()).isEmpty();
        }

        @Test
        void shouldMoveMessageToDLQIfStateStoreUpdateFailedTooManyTimes() throws Exception {
            // Given
            configureJobQueuesWithMaxReceiveCount(2);
            StateStore stateStore = mock(StateStore.class);
            doThrow(new StateStoreException("Failed to update state store"))
                    .when(stateStore).atomicallyUpdateFilesToReadyForGCAndCreateNewActiveFile(any(), any());
            FileInfo fileInfo1 = ingestFileWith100Records();
            String jobJson = sendSplittingJobForFilesGetJson("job1", fileInfo1);

            // When
            StateStoreProvider provider = new FixedStateStoreProvider(tableProperties, stateStore);
            createJobRunner("task-id", provider).run();
            createJobRunner("task-id", provider).run();
            createJobRunner("task-id", provider).run();

            // Then
            // - The compaction job should no longer be on the job queue
            assertThat(messagesOnQueue(COMPACTION_JOB_QUEUE_URL)).isEmpty();
            // - The compaction job should be on the DLQ
            assertThat(messagesOnQueue(COMPACTION_JOB_DLQ_URL))
                    .containsExactly(jobJson);
            // - No active files should be in the state store
            assertThat(stateStore.getActiveFiles()).isEmpty();
        }
    }

    private Stream<String> messagesOnQueue(InstanceProperty queueProperty) {
        return sqs.receiveMessage(new ReceiveMessageRequest()
                        .withQueueUrl(instanceProperties.get(queueProperty))
                        .withWaitTimeSeconds(2))
                .getMessages().stream()
                .map(Message::getBody);
    }

    private void configureJobQueuesWithMaxReceiveCount(int maxReceiveCount) {
        String jobQueueUrl = sqs.createQueue(UUID.randomUUID().toString()).getQueueUrl();
        String jobDlqUrl = sqs.createQueue(UUID.randomUUID().toString()).getQueueUrl();
        String jobDlqArn = sqs.getQueueAttributes(new GetQueueAttributesRequest()
                .withQueueUrl(jobDlqUrl)
                .withAttributeNames("QueueArn")).getAttributes().get("QueueArn");
        sqs.setQueueAttributes(new SetQueueAttributesRequest()
                .withQueueUrl(jobQueueUrl)
                .addAttributesEntry("RedrivePolicy",
                        "{\"maxReceiveCount\":\"" + maxReceiveCount + "\", "
                                + "\"deadLetterTargetArn\":\"" + jobDlqArn + "\"}"));
        instanceProperties.set(COMPACTION_JOB_QUEUE_URL, jobQueueUrl);
        instanceProperties.set(COMPACTION_JOB_DLQ_URL, jobDlqUrl);
    }

    private CompactSortedFilesRunner createJobRunner(String taskId) {
        return createJobRunner(taskId, stateStoreProvider);
    }

    private CompactSortedFilesRunner createJobRunner(String taskId, StateStoreProvider stateStoreProvider) {
        return new CompactSortedFilesRunner(
                instanceProperties, ObjectFactory.noUserJars(),
                tablePropertiesProvider, PropertiesReloader.neverReload(), stateStoreProvider, jobStatusStore, taskStatusStore,
                taskId, instanceProperties.get(COMPACTION_JOB_QUEUE_URL), sqs, null, CompactionTaskType.COMPACTION, 1, 0);
    }

    private FileInfo ingestFileWith100Records() throws Exception {
        return ingestFileWith100Records(i ->
                new Record(Map.of(
                        "key", (long) 2 * i,
                        "value1", (long) 2 * i,
                        "value2", 987654321L)));
    }

    private FileInfo ingestFileWith100Records(Function<Integer, Record> recordCreator) throws Exception {
        IngestFactory ingestFactory = IngestFactory.builder()
                .objectFactory(ObjectFactory.noUserJars())
                .hadoopConfiguration(configuration)
                .localDir(tempDir.toString())
                .stateStoreProvider(new FixedStateStoreProvider(tableProperties, getStateStore()))
                .instanceProperties(instanceProperties)
                .build();
        IngestCoordinator<Record> coordinator = ingestFactory.createIngestCoordinator(tableProperties);
        for (int i = 0; i < 100; i++) {
            coordinator.write(recordCreator.apply(i));
        }
        return coordinator.closeReturningResult().getFileInfoList().get(0);
    }

    private String sendCompactionJobForFilesGetJson(String jobId, String outputFilename, FileInfo... fileInfos) throws IOException {
        return sendJobForFilesGetJson(compactionJobForFiles(jobId, outputFilename, fileInfos));
    }

    private String sendSplittingJobForFilesGetJson(String jobId, FileInfo... fileInfos) throws IOException {
        return sendJobForFilesGetJson(splittingJobForFiles(jobId, fileInfos));
    }

    private String sendJobForFilesGetJson(CompactionJob job) throws IOException {
        String jobJson = CompactionJobSerDe.serialiseToString(job);
        SendMessageRequest sendMessageRequest = new SendMessageRequest()
                .withQueueUrl(instanceProperties.get(COMPACTION_JOB_QUEUE_URL))
                .withMessageBody(jobJson);
        sqs.sendMessage(sendMessageRequest);
        return jobJson;
    }

    private CompactionJob compactionJobForFiles(String jobId, String outputFilename, FileInfo... fileInfos) {
        return CompactionJob.builder()
                .tableId(tableId)
                .jobId(jobId)
                .partitionId("root")
                .inputFileInfos(List.of(fileInfos))
                .isSplittingJob(false)
                .outputFile(tempDir + "/" + outputFilename).build();
    }

    private CompactionJob splittingJobForFiles(String jobId, FileInfo... fileInfos) {
        return CompactionJob.builder()
                .tableId(tableId)
                .jobId(jobId)
                .partitionId("root")
                .inputFileInfos(List.of(fileInfos))
                .isSplittingJob(true)
                .childPartitions(List.of("L", "R"))
                .build();
    }

    private String jobPartitionFilename(CompactionJob job, String partitionId, int index) {
        return CompactionOutputFileNameFactory.forTable(instanceProperties, tableProperties)
                .jobPartitionFile(job.getId(), partitionId, index);
    }
}
