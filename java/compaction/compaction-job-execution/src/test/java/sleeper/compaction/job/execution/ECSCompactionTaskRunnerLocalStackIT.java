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
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.GetQueueAttributesRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.model.SetQueueAttributesRequest;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetReader;
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

import sleeper.compaction.core.job.CompactionJob;
import sleeper.compaction.core.job.CompactionJobSerDe;
import sleeper.compaction.core.job.commit.CompactionJobCommitRequest;
import sleeper.compaction.core.job.commit.CompactionJobCommitRequestSerDe;
import sleeper.compaction.core.job.commit.CompactionJobCommitterOrSendToLambda;
import sleeper.compaction.core.task.CompactionTask;
import sleeper.compaction.core.task.StateStoreWaitForFiles;
import sleeper.compaction.tracker.job.CompactionJobTrackerFactory;
import sleeper.compaction.tracker.job.DynamoDBCompactionJobTrackerCreator;
import sleeper.compaction.tracker.task.CompactionTaskTrackerFactory;
import sleeper.compaction.tracker.task.DynamoDBCompactionTaskTrackerCreator;
import sleeper.configuration.properties.S3InstanceProperties;
import sleeper.configuration.properties.S3TableProperties;
import sleeper.configuration.table.index.DynamoDBTableIndexCreator;
import sleeper.core.CommonTestConstants;
import sleeper.core.properties.PropertiesReloader;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.instance.InstanceProperty;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertiesProvider;
import sleeper.core.properties.table.TablePropertiesStore;
import sleeper.core.record.Record;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.LongType;
import sleeper.core.statestore.CheckFileAssignmentsRequest;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.ReplaceFileReferencesRequest;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreProvider;
import sleeper.core.statestore.exception.ReplaceRequestsFailedException;
import sleeper.core.statestore.testutils.FixedStateStoreProvider;
import sleeper.core.tracker.compaction.job.CompactionJobTracker;
import sleeper.core.tracker.compaction.task.CompactionTaskTracker;
import sleeper.core.tracker.job.RecordsProcessed;
import sleeper.core.tracker.job.RecordsProcessedSummary;
import sleeper.core.util.ObjectFactory;
import sleeper.ingest.runner.IngestFactory;
import sleeper.ingest.runner.impl.IngestCoordinator;
import sleeper.parquet.record.ParquetRecordReader;
import sleeper.statestore.StateStoreFactory;
import sleeper.statestore.transactionlog.TransactionLogStateStoreCreator;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static sleeper.compaction.job.execution.testutils.CompactionRunnerTestUtils.assignJobIdsToInputFiles;
import static sleeper.configuration.testutils.LocalStackAwsV1ClientHelper.buildAwsV1Client;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.COMPACTION_JOB_DLQ_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.COMPACTION_JOB_QUEUE_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.STATESTORE_COMMITTER_QUEUE_URL;
import static sleeper.core.properties.instance.CommonProperty.FILE_SYSTEM;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_JOB_FAILED_VISIBILITY_TIMEOUT_IN_SECONDS;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_KEEP_ALIVE_PERIOD_IN_SECONDS;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_QUEUE_VISIBILITY_TIMEOUT_IN_SECONDS;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_TASK_DELAY_BEFORE_RETRY_IN_SECONDS;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_TASK_MAX_CONSECUTIVE_FAILURES;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_TASK_MAX_IDLE_TIME_IN_SECONDS;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_TASK_WAIT_TIME_IN_SECONDS;
import static sleeper.core.properties.instance.TableDefaultProperty.DEFAULT_INGEST_PARTITION_FILE_WRITER_TYPE;
import static sleeper.core.properties.table.TableProperty.COMPACTION_FILES_BATCH_SIZE;
import static sleeper.core.properties.table.TableProperty.COMPACTION_JOB_COMMIT_ASYNC;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.parquet.utils.HadoopConfigurationLocalStackUtils.getHadoopConfiguration;

@Testcontainers
public class ECSCompactionTaskRunnerLocalStackIT {

    @Container
    public static LocalStackContainer localStackContainer = new LocalStackContainer(DockerImageName.parse(CommonTestConstants.LOCALSTACK_DOCKER_IMAGE)).withServices(
            LocalStackContainer.Service.S3, LocalStackContainer.Service.SQS, LocalStackContainer.Service.DYNAMODB);

    private final AmazonS3 s3 = buildAwsV1Client(localStackContainer, LocalStackContainer.Service.S3, AmazonS3ClientBuilder.standard());
    private final AmazonDynamoDB dynamoDB = buildAwsV1Client(localStackContainer, LocalStackContainer.Service.DYNAMODB, AmazonDynamoDBClientBuilder.standard());
    private final AmazonSQS sqs = buildAwsV1Client(localStackContainer, LocalStackContainer.Service.SQS, AmazonSQSClientBuilder.standard());
    private final InstanceProperties instanceProperties = createInstance();
    private final Configuration configuration = getHadoopConfiguration(localStackContainer);
    private final StateStoreProvider stateStoreProvider = StateStoreFactory.createProvider(instanceProperties, s3, dynamoDB, configuration);
    private final TablePropertiesStore tablePropertiesStore = S3TableProperties.createStore(instanceProperties, s3, dynamoDB);
    private final TablePropertiesProvider tablePropertiesProvider = S3TableProperties.createProvider(instanceProperties, s3, dynamoDB);
    private final Schema schema = createSchema();
    private final TableProperties tableProperties = createTable();
    private final String tableId = tableProperties.get(TABLE_ID);
    private final CompactionJobTracker jobTracker = CompactionJobTrackerFactory.getTracker(dynamoDB, instanceProperties);
    private final CompactionTaskTracker taskTracker = CompactionTaskTrackerFactory.getTracker(dynamoDB, instanceProperties);

    @AfterEach
    void tearDown() {
        s3.shutdown();
        dynamoDB.shutdown();
        sqs.shutdown();
    }

    @BeforeEach
    void setUp() {
        DynamoDBCompactionJobTrackerCreator.create(instanceProperties, dynamoDB);
        DynamoDBCompactionTaskTrackerCreator.create(instanceProperties, dynamoDB);
    }

    @TempDir
    public java.nio.file.Path tempDir;

    @Nested
    @DisplayName("Handle messages on job queue")
    class HandleMessagesOnJobQueue {
        @BeforeEach
        void setup() {
            tableProperties.set(COMPACTION_JOB_COMMIT_ASYNC, "false");
            tablePropertiesStore.save(tableProperties);
        }

        @Test
        void shouldDeleteMessagesIfJobSuccessful() throws Exception {
            // Given
            configureJobQueuesWithMaxReceiveCount(1);
            // - Create four files of sorted data
            StateStore stateStore = getStateStore();
            FileReference fileReference1 = ingestFileWith100Records(i -> new Record(Map.of(
                    "key", (long) 2 * i,
                    "value1", (long) 2 * i,
                    "value2", 987654321L)));
            FileReference fileReference2 = ingestFileWith100Records(i -> new Record(Map.of(
                    "key", (long) 2 * i + 1,
                    "value1", 1001L,
                    "value2", 123456789L)));
            FileReference fileReference3 = ingestFileWith100Records(i -> new Record(Map.of(
                    "key", (long) 2 * i,
                    "value1", (long) 2 * i,
                    "value2", 987654321L)));
            FileReference fileReference4 = ingestFileWith100Records(i -> new Record(Map.of(
                    "key", (long) 2 * i + 1,
                    "value1", 1001L,
                    "value2", 123456789L)));

            // - Create two compaction jobs and put on queue
            CompactionJob job1 = compactionJobForFiles("job1", "output1.parquet", fileReference1, fileReference2);
            CompactionJob job2 = compactionJobForFiles("job2", "output2.parquet", fileReference3, fileReference4);
            assignJobIdsToInputFiles(stateStore, job1, job2);
            sendJob(job1);
            sendJob(job2);

            // When
            createTask("task-id").run();

            // Then
            // - There should be no messages left on the queue
            assertThat(messagesOnQueue(COMPACTION_JOB_QUEUE_URL)).isEmpty();
            // - Check DynamoDBStateStore has correct file references
            assertThat(stateStore.getFileReferences())
                    .extracting(FileReference::getFilename)
                    .containsExactlyInAnyOrder(job1.getOutputFile(), job2.getOutputFile());
        }

        @Test
        void shouldPutMessageBackOnSQSQueueIfJobFailed() throws Exception {
            // Given
            configureJobQueuesWithMaxReceiveCount(2);
            StateStore stateStore = getStateStore();
            // - Create a compaction job for a non-existent file
            String jobJson = sendCompactionJobForFilesGetJson("job1", "output1.parquet", "not-a-file.parquet");

            // When
            createTask("task-id").run();

            // Then
            // - The compaction job should be put back on the queue
            assertThat(messagesOnQueue(COMPACTION_JOB_QUEUE_URL))
                    .map(Message::getBody)
                    .containsExactly(jobJson);
            // - No file references should be in the state store
            assertThat(stateStore.getFileReferences()).isEmpty();
        }

        @Test
        void shouldMoveMessageToDLQIfJobFailedTooManyTimes() throws Exception {
            // Given
            configureJobQueuesWithMaxReceiveCount(1);
            StateStore stateStore = getStateStore();
            // - Create a compaction job for a non-existent file
            String jobJson = sendCompactionJobForFilesGetJson("job1", "output1.parquet", "not-a-file.parquet");

            // When
            createTask("task-id").run();

            // Then
            // - The compaction job should no longer be on the job queue
            assertThat(messagesOnQueue(COMPACTION_JOB_QUEUE_URL)).isEmpty();
            // - The compaction job should be on the DLQ
            assertThat(messagesOnQueue(COMPACTION_JOB_DLQ_URL))
                    .map(Message::getBody)
                    .containsExactly(jobJson);
            // - No file references should be in the state store
            assertThat(stateStore.getFileReferences()).isEmpty();
        }

        @Test
        void shouldPutMessageBackOnSQSQueueIfStateStoreUpdateFailed() throws Exception {
            // Given
            configureJobQueuesWithMaxReceiveCount(2);
            StateStore stateStore = mock(StateStore.class);
            doAnswer(invocation -> {
                List<ReplaceFileReferencesRequest> requests = invocation.getArgument(0);
                throw new ReplaceRequestsFailedException(requests, new IllegalStateException("Failed to update state store"));
            }).when(stateStore).atomicallyReplaceFileReferencesWithNewOnes(anyList());
            FileReference fileReference1 = ingestFileWith100Records();
            FileReference fileReference2 = ingestFileWith100Records();
            when(stateStore.isAssigned(List.of(CheckFileAssignmentsRequest.isJobAssignedToFilesOnPartition(
                    "job1", List.of(fileReference1.getFilename(), fileReference2.getFilename()), "root"))))
                    .thenReturn(true);
            String jobJson = sendCompactionJobForFilesGetJson("job1", "output1.parquet", fileReference1, fileReference2);

            // When
            createTask("task-id", new FixedStateStoreProvider(tableProperties, stateStore)).run();

            // Then
            // - The compaction job should be put back on the queue
            assertThat(messagesOnQueue(COMPACTION_JOB_QUEUE_URL))
                    .map(Message::getBody)
                    .containsExactly(jobJson);
        }

        @Test
        void shouldMoveMessageToDLQIfStateStoreUpdateFailedTooManyTimes() throws Exception {
            // Given
            configureJobQueuesWithMaxReceiveCount(1);
            StateStore stateStore = mock(StateStore.class);
            doAnswer(invocation -> {
                List<ReplaceFileReferencesRequest> requests = invocation.getArgument(0);
                throw new ReplaceRequestsFailedException(requests, new IllegalStateException("Failed to update state store"));
            }).when(stateStore).atomicallyReplaceFileReferencesWithNewOnes(anyList());
            FileReference fileReference1 = ingestFileWith100Records();
            FileReference fileReference2 = ingestFileWith100Records();
            when(stateStore.isAssigned(List.of(CheckFileAssignmentsRequest.isJobAssignedToFilesOnPartition(
                    "job1", List.of(fileReference1.getFilename(), fileReference2.getFilename()), "root"))))
                    .thenReturn(true);
            String jobJson = sendCompactionJobForFilesGetJson("job1", "output1.parquet", fileReference1, fileReference2);

            // When
            StateStoreProvider provider = new FixedStateStoreProvider(tableProperties, stateStore);
            createTask("task-id", provider).run();

            // Then
            // - The compaction job should no longer be on the job queue
            assertThat(messagesOnQueue(COMPACTION_JOB_QUEUE_URL)).isEmpty();
            // - The compaction job should be on the DLQ
            assertThat(messagesOnQueue(COMPACTION_JOB_DLQ_URL))
                    .map(Message::getBody)
                    .containsExactly(jobJson);
        }
    }

    @Test
    void shouldSendCommitRequestToQueueIfAsyncCommitsEnabled() throws Exception {
        // Given
        tableProperties.set(COMPACTION_JOB_COMMIT_ASYNC, "true");
        tablePropertiesStore.save(tableProperties);
        configureJobQueuesWithMaxReceiveCount(1);
        StateStore stateStore = getStateStore();
        FileReference fileReference = ingestFileWith100Records();
        List<Record> expectedRecords = IntStream.range(0, 100)
                .mapToObj(defaultRecordCreator()::apply)
                .collect(Collectors.toList());
        CompactionJob job = compactionJobForFiles("job1", "output1.parquet", fileReference);
        assignJobIdsToInputFiles(stateStore, job);
        sendJob(job);
        Queue<Instant> times = new LinkedList<>(List.of(
                Instant.parse("2024-05-09T12:52:00Z"),      // Start task
                Instant.parse("2024-05-09T12:55:00Z"),      // Job started
                Instant.parse("2024-05-09T12:56:00Z"),      // Job finished
                Instant.parse("2024-05-09T12:58:00Z")));    // Finished task
        Queue<String> jobRunIds = new LinkedList<>(List.of("job-run-id"));

        // When
        createTaskWithRunIdsAndTimes("task-id", jobRunIds::poll, times::poll).run();

        // Then
        // - The compaction job should not be on the input queue or DLQ
        assertThat(messagesOnQueue(COMPACTION_JOB_QUEUE_URL)).isEmpty();
        assertThat(messagesOnQueue(COMPACTION_JOB_DLQ_URL)).isEmpty();
        // - A compaction commit request should be on the job commit queue
        assertThat(messagesOnQueue(STATESTORE_COMMITTER_QUEUE_URL))
                .extracting(Message::getBody, this::getMessageGroupId)
                .containsExactly(tuple(
                        commitRequestOnQueue(job, "task-id", "job-run-id",
                                new RecordsProcessedSummary(new RecordsProcessed(100, 100),
                                        Instant.parse("2024-05-09T12:55:00Z"),
                                        Instant.parse("2024-05-09T12:56:00Z"))),
                        tableId));
        // - Check new output file has been created with the correct records
        assertThat(readRecords("output1.parquet", schema))
                .containsExactlyElementsOf(expectedRecords);
        // - Check DynamoDBStateStore does not yet have correct file references
        assertThat(stateStore.getFileReferences())
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("lastStateStoreUpdateTime")
                .containsExactly(onJob(job, fileReference));
    }

    private InstanceProperties createInstance() {
        InstanceProperties instanceProperties = createTestInstanceProperties();
        instanceProperties.set(FILE_SYSTEM, "");
        instanceProperties.set(DEFAULT_INGEST_PARTITION_FILE_WRITER_TYPE, "direct");
        instanceProperties.setNumber(COMPACTION_TASK_WAIT_TIME_IN_SECONDS, 0);
        instanceProperties.setNumber(COMPACTION_JOB_FAILED_VISIBILITY_TIMEOUT_IN_SECONDS, 0);
        instanceProperties.setNumber(COMPACTION_TASK_DELAY_BEFORE_RETRY_IN_SECONDS, 0);
        instanceProperties.setNumber(COMPACTION_TASK_MAX_IDLE_TIME_IN_SECONDS, 0);
        instanceProperties.setNumber(COMPACTION_TASK_MAX_CONSECUTIVE_FAILURES, 1);
        instanceProperties.setNumber(COMPACTION_KEEP_ALIVE_PERIOD_IN_SECONDS, 1);
        instanceProperties.setNumber(COMPACTION_QUEUE_VISIBILITY_TIMEOUT_IN_SECONDS, 1);
        s3.createBucket(instanceProperties.get(CONFIG_BUCKET));
        s3.createBucket(instanceProperties.get(DATA_BUCKET));
        S3InstanceProperties.saveToS3(s3, instanceProperties);
        DynamoDBTableIndexCreator.create(dynamoDB, instanceProperties);
        new TransactionLogStateStoreCreator(instanceProperties, dynamoDB).create();

        return instanceProperties;
    }

    private static Schema createSchema() {
        return Schema.builder()
                .rowKeyFields(new Field("key", new LongType()))
                .valueFields(new Field("value1", new LongType()), new Field("value2", new LongType()))
                .build();
    }

    private TableProperties createTable() {
        TableProperties tableProperties = createTestTableProperties(instanceProperties, schema);
        tableProperties.set(COMPACTION_FILES_BATCH_SIZE, "5");
        tablePropertiesStore.save(tableProperties);
        stateStoreProvider.getStateStore(tableProperties).initialise();

        return tableProperties;
    }

    private StateStore getStateStore() {
        return stateStoreProvider.getStateStore(tableProperties);
    }

    private String getMessageGroupId(Message message) {
        return message.getAttributes().get("MessageGroupId");
    }

    private Stream<Message> messagesOnQueue(InstanceProperty queueProperty) {
        return sqs.receiveMessage(new ReceiveMessageRequest()
                .withQueueUrl(instanceProperties.get(queueProperty))
                .withAttributeNames("MessageGroupId")
                .withWaitTimeSeconds(2))
                .getMessages().stream();
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
                        "{\"maxReceiveCount\":\"" + maxReceiveCount + "\", " + "\"deadLetterTargetArn\":\"" + jobDlqArn + "\"}"));
        instanceProperties.set(COMPACTION_JOB_QUEUE_URL, jobQueueUrl);
        instanceProperties.set(COMPACTION_JOB_DLQ_URL, jobDlqUrl);
        configureJobCommitterQueues(maxReceiveCount);
    }

    private void configureJobCommitterQueues(int maxReceiveCount) {
        String jobCommitQueueUrl = sqs.createQueue(new CreateQueueRequest()
                .withQueueName(UUID.randomUUID().toString() + ".fifo")
                .withAttributes(Map.of("FifoQueue", "true"))).getQueueUrl();
        instanceProperties.set(STATESTORE_COMMITTER_QUEUE_URL, jobCommitQueueUrl);
    }

    private CompactionTask createTaskWithRunIdsAndTimes(
            String taskId, Supplier<String> jobRunIdSupplier, Supplier<Instant> timeSupplier) {
        return createTask(taskId, stateStoreProvider, jobRunIdSupplier, timeSupplier);
    }

    private CompactionTask createTask(String taskId) {
        return createTask(taskId, stateStoreProvider, () -> UUID.randomUUID().toString(), Instant::now);
    }

    private CompactionTask createTask(String taskId, StateStoreProvider stateStoreProvider) {
        return createTask(taskId, stateStoreProvider, () -> UUID.randomUUID().toString(), Instant::now);
    }

    private CompactionTask createTask(
            String taskId, StateStoreProvider stateStoreProvider,
            Supplier<String> jobRunIdSupplier, Supplier<Instant> timeSupplier) {
        DefaultCompactionRunnerFactory selector = new DefaultCompactionRunnerFactory(
                ObjectFactory.noUserJars(), configuration);
        CompactionJobCommitterOrSendToLambda committer = ECSCompactionTaskRunner.committerOrSendToLambda(
                tablePropertiesProvider, stateStoreProvider, jobTracker,
                instanceProperties, sqs);
        StateStoreWaitForFiles waitForFiles = new StateStoreWaitForFiles(tablePropertiesProvider, stateStoreProvider, jobTracker);
        CompactionTask task = new CompactionTask(instanceProperties, tablePropertiesProvider,
                PropertiesReloader.neverReload(), stateStoreProvider, new SqsCompactionQueueHandler(sqs, instanceProperties),
                waitForFiles, committer, jobTracker, taskTracker, selector, taskId,
                jobRunIdSupplier, timeSupplier, duration -> {
                });
        return task;
    }

    private Function<Integer, Record> defaultRecordCreator() {
        return i -> new Record(Map.of(
                "key", (long) 2 * i,
                "value1", (long) 2 * i,
                "value2", 987654321L));
    }

    private FileReference ingestFileWith100Records() throws Exception {
        return ingestFileWith100Records(defaultRecordCreator());
    }

    private FileReference ingestFileWith100Records(Function<Integer, Record> recordCreator) throws Exception {
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
        return coordinator.closeReturningResult().getFileReferenceList().get(0);
    }

    private String sendCompactionJobForFilesGetJson(String jobId, String outputFilename, FileReference... fileReferences) throws IOException {
        return sendJob(compactionJobForFiles(jobId, outputFilename, List.of(fileReferences).stream()
                .map(FileReference::getFilename)
                .collect(Collectors.toList())));
    }

    private String sendCompactionJobForFilesGetJson(String jobId, String outputFilename, String... inputFilenames) throws IOException {
        return sendJob(compactionJobForFiles(jobId, outputFilename, List.of(inputFilenames)));
    }

    private String sendJob(CompactionJob job) throws IOException {
        String jobJson = new CompactionJobSerDe().toJson(job);
        SendMessageRequest sendMessageRequest = new SendMessageRequest()
                .withQueueUrl(instanceProperties.get(COMPACTION_JOB_QUEUE_URL))
                .withMessageBody(jobJson);
        sqs.sendMessage(sendMessageRequest);
        return jobJson;
    }

    private CompactionJob compactionJobForFiles(String jobId, String outputFilename, FileReference... fileReferences) {
        return compactionJobForFiles(jobId, outputFilename, List.of(fileReferences).stream()
                .map(FileReference::getFilename)
                .collect(Collectors.toList()));

    }

    private CompactionJob compactionJobForFiles(String jobId, String outputFilename, List<String> inputFilenames) {
        return CompactionJob.builder()
                .tableId(tableId)
                .jobId(jobId)
                .partitionId("root")
                .inputFiles(inputFilenames)
                .outputFile(tempDir + "/" + outputFilename).build();
    }

    private String commitRequestOnQueue(CompactionJob job, String taskId, String jobRunId, RecordsProcessedSummary summary) {
        return new CompactionJobCommitRequestSerDe().toJson(new CompactionJobCommitRequest(job, taskId, jobRunId, summary));
    }

    private FileReference onJob(CompactionJob job, FileReference reference) {
        return reference.toBuilder().jobId(job.getId()).build();
    }

    private List<Record> readRecords(String filename, Schema schema) {
        try (ParquetReader<Record> reader = new ParquetRecordReader(new Path(tempDir.resolve(filename).toString()), schema)) {
            List<Record> records = new ArrayList<>();
            for (Record record = reader.read(); record != null; record = reader.read()) {
                records.add(new Record(record));
            }
            return records;
        } catch (IOException e) {
            throw new RuntimeException("Failed reading records", e);
        }
    }
}
