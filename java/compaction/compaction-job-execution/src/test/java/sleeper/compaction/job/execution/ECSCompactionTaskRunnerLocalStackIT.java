/*
 * Copyright 2022-2025 Crown Copyright
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

import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetReader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import software.amazon.awssdk.services.sqs.model.GetQueueAttributesRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.MessageSystemAttributeName;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import software.amazon.awssdk.services.sqs.model.SetQueueAttributesRequest;

import sleeper.compaction.core.job.CompactionJob;
import sleeper.compaction.core.job.CompactionJobCommitterOrSendToLambda;
import sleeper.compaction.core.job.CompactionJobFactory;
import sleeper.compaction.core.job.CompactionJobSerDe;
import sleeper.compaction.core.job.commit.CompactionCommitMessage;
import sleeper.compaction.core.job.commit.CompactionCommitMessageSerDe;
import sleeper.compaction.core.task.CompactionTask;
import sleeper.compaction.core.task.StateStoreWaitForFiles;
import sleeper.compaction.tracker.job.CompactionJobTrackerFactory;
import sleeper.compaction.tracker.job.DynamoDBCompactionJobTrackerCreator;
import sleeper.compaction.tracker.task.CompactionTaskTrackerFactory;
import sleeper.compaction.tracker.task.DynamoDBCompactionTaskTrackerCreator;
import sleeper.configuration.properties.S3InstanceProperties;
import sleeper.configuration.properties.S3TableProperties;
import sleeper.configuration.table.index.DynamoDBTableIndexCreator;
import sleeper.core.properties.PropertiesReloader;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.instance.InstanceProperty;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertiesProvider;
import sleeper.core.properties.table.TablePropertiesStore;
import sleeper.core.row.Row;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.LongType;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreProvider;
import sleeper.core.statestore.commit.StateStoreCommitRequest;
import sleeper.core.statestore.commit.StateStoreCommitRequestSerDe;
import sleeper.core.statestore.testutils.FixedStateStoreProvider;
import sleeper.core.statestore.testutils.InMemoryTransactionLogStateStore;
import sleeper.core.statestore.testutils.InMemoryTransactionLogStore;
import sleeper.core.statestore.testutils.InMemoryTransactionLogsPerTable;
import sleeper.core.statestore.transactionlog.transaction.impl.ReplaceFileReferencesTransaction;
import sleeper.core.tracker.compaction.job.CompactionJobTracker;
import sleeper.core.tracker.compaction.task.CompactionTaskTracker;
import sleeper.core.tracker.job.run.JobRunSummary;
import sleeper.core.tracker.job.run.RowsProcessed;
import sleeper.core.util.ObjectFactory;
import sleeper.dynamodb.tools.DynamoDBUtils;
import sleeper.ingest.runner.IngestFactory;
import sleeper.ingest.runner.impl.IngestCoordinator;
import sleeper.localstack.test.LocalStackTestBase;
import sleeper.parquet.row.ParquetRowReaderFactory;
import sleeper.sketches.store.S3SketchesStore;
import sleeper.statestore.StateStoreFactory;
import sleeper.statestore.transactionlog.TransactionLogStateStoreCreator;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;
import static sleeper.compaction.job.execution.testutils.CompactionRunnerTestUtils.assignJobIdsToInputFiles;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.COMPACTION_COMMIT_QUEUE_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.COMPACTION_JOB_DLQ_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.COMPACTION_JOB_QUEUE_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.STATESTORE_COMMITTER_QUEUE_URL;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_JOB_FAILED_VISIBILITY_TIMEOUT_IN_SECONDS;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_KEEP_ALIVE_PERIOD_IN_SECONDS;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_QUEUE_VISIBILITY_TIMEOUT_IN_SECONDS;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_TASK_DELAY_BEFORE_RETRY_IN_SECONDS;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_TASK_MAX_CONSECUTIVE_FAILURES;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_TASK_MAX_IDLE_TIME_IN_SECONDS;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_TASK_WAIT_TIME_IN_SECONDS;
import static sleeper.core.properties.instance.CompactionProperty.DEFAULT_COMPACTION_FILES_BATCH_SIZE;
import static sleeper.core.properties.instance.TableDefaultProperty.DEFAULT_INGEST_PARTITION_FILE_WRITER_TYPE;
import static sleeper.core.properties.table.TableProperty.COMPACTION_JOB_ASYNC_BATCHING;
import static sleeper.core.properties.table.TableProperty.COMPACTION_JOB_COMMIT_ASYNC;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.statestore.testutils.StateStoreUpdatesWrapper.update;
import static sleeper.core.testutils.SupplierTestHelper.fixIds;
import static sleeper.core.testutils.SupplierTestHelper.supplyTimes;
import static sleeper.core.util.ThreadSleepTestHelper.noWaits;

public class ECSCompactionTaskRunnerLocalStackIT extends LocalStackTestBase {

    private final InstanceProperties instanceProperties = createInstanceProperties();
    private final TablePropertiesStore tablePropertiesStore = S3TableProperties.createStore(instanceProperties, s3Client, dynamoClient);
    private final TablePropertiesProvider tablePropertiesProvider = S3TableProperties.createProvider(instanceProperties, s3Client, dynamoClient);
    private StateStoreProvider stateStoreProvider = StateStoreFactory.createProvider(instanceProperties, s3Client, dynamoClient);
    private final Schema schema = Schema.builder()
            .rowKeyFields(new Field("key", new LongType()))
            .valueFields(new Field("value1", new LongType()), new Field("value2", new LongType()))
            .build();
    private final TableProperties tableProperties = createTestTableProperties(instanceProperties, schema);
    private final String tableId = tableProperties.get(TABLE_ID);
    private final CompactionJobTracker jobTracker = CompactionJobTrackerFactory.getTracker(dynamoClient, instanceProperties);
    private final CompactionTaskTracker taskTracker = CompactionTaskTrackerFactory.getTracker(dynamoClient, instanceProperties);
    private InMemoryTransactionLogsPerTable inMemoryTransactionLogsPerTable;

    @BeforeEach
    void setUp() {
        createBucket(instanceProperties.get(CONFIG_BUCKET));
        createBucket(instanceProperties.get(DATA_BUCKET));
        S3InstanceProperties.saveToS3(s3Client, instanceProperties);
        DynamoDBTableIndexCreator.create(dynamoClient, instanceProperties);
        new TransactionLogStateStoreCreator(instanceProperties, dynamoClient).create();
        tablePropertiesStore.save(tableProperties);
        update(stateStoreProvider.getStateStore(tableProperties)).initialise(schema);
        DynamoDBCompactionJobTrackerCreator.create(instanceProperties, dynamoClient);
        DynamoDBCompactionTaskTrackerCreator.create(instanceProperties, dynamoClient);
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
            FileReference fileReference1 = ingestFileWith100Rows(i -> new Row(Map.of(
                    "key", (long) 2 * i,
                    "value1", (long) 2 * i,
                    "value2", 987654321L)));
            FileReference fileReference2 = ingestFileWith100Rows(i -> new Row(Map.of(
                    "key", (long) 2 * i + 1,
                    "value1", 1001L,
                    "value2", 123456789L)));
            FileReference fileReference3 = ingestFileWith100Rows(i -> new Row(Map.of(
                    "key", (long) 2 * i,
                    "value1", (long) 2 * i,
                    "value2", 987654321L)));
            FileReference fileReference4 = ingestFileWith100Rows(i -> new Row(Map.of(
                    "key", (long) 2 * i + 1,
                    "value1", 1001L,
                    "value2", 123456789L)));

            // - Create two compaction jobs and put on queue
            CompactionJob job1 = compactionJobForFiles("job1", fileReference1, fileReference2);
            CompactionJob job2 = compactionJobForFiles("job2", fileReference3, fileReference4);
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
                    .map(Message::body)
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
                    .map(Message::body)
                    .containsExactly(jobJson);
            // - No file references should be in the state store
            assertThat(stateStore.getFileReferences()).isEmpty();
        }

        @Test
        void shouldPutMessageBackOnSQSQueueIfStateStoreUpdateFailed() throws Exception {
            // Given
            useInMemoryStateStore();
            configureJobQueuesWithMaxReceiveCount(2);
            FileReference fileReference1 = ingestFileWith100Rows();
            FileReference fileReference2 = ingestFileWith100Rows();
            CompactionJob job = compactionJobForFiles("job1", fileReference1, fileReference2);
            assignJobIdsToInputFiles(getStateStore(), job);
            String jobJson = sendJob(job);
            inMemoryFilesLogStore().atStartOfAddTransaction(() -> {
                throw new RuntimeException("Test error message thrown");
            });

            // When
            createTask("task-id").run();

            // Then
            // - The compaction job should be put back on the queue
            assertThat(messagesOnQueue(COMPACTION_JOB_QUEUE_URL))
                    .map(Message::body)
                    .containsExactly(jobJson);
        }

        @Test
        void shouldMoveMessageToDLQIfStateStoreUpdateFailedTooManyTimes() throws Exception {
            // Given
            useInMemoryStateStore();
            configureJobQueuesWithMaxReceiveCount(1);
            FileReference fileReference1 = ingestFileWith100Rows();
            FileReference fileReference2 = ingestFileWith100Rows();
            CompactionJob job = compactionJobForFiles("job1", fileReference1, fileReference2);
            assignJobIdsToInputFiles(getStateStore(), job);
            String jobJson = sendJob(job);
            inMemoryFilesLogStore().atStartOfAddTransaction(() -> {
                throw new RuntimeException("Test error message thrown");
            });

            // When
            createTask("task-id").run();

            // Then
            // - The compaction job should no longer be on the job queue
            assertThat(messagesOnQueue(COMPACTION_JOB_QUEUE_URL)).isEmpty();
            // - The compaction job should be on the DLQ
            assertThat(messagesOnQueue(COMPACTION_JOB_DLQ_URL))
                    .map(Message::body)
                    .containsExactly(jobJson);
        }
    }

    @Test
    void shouldSendCommitRequestToBatcherQueueIfEnabled() throws Exception {
        // Given
        tableProperties.set(COMPACTION_JOB_COMMIT_ASYNC, "true");
        tableProperties.set(COMPACTION_JOB_ASYNC_BATCHING, "true");
        tablePropertiesStore.save(tableProperties);
        configureJobQueuesWithMaxReceiveCount(1);
        StateStore stateStore = getStateStore();
        FileReference fileReference = ingestFileWith100Rows();
        List<Row> expectedRows = IntStream.range(0, 100)
                .mapToObj(defaultRowCreator()::apply)
                .collect(Collectors.toList());
        CompactionJob job = compactionJobForFiles("job1", fileReference);
        assignJobIdsToInputFiles(stateStore, job);
        sendJob(job);
        Supplier<Instant> times = supplyTimes(
                Instant.parse("2024-05-09T12:52:00Z"),      // Start task
                Instant.parse("2024-05-09T12:55:00Z"),      // Job started
                Instant.parse("2024-05-09T12:56:00Z"),      // Job finished
                Instant.parse("2024-05-09T12:58:00Z"));    // Finished task
        Supplier<String> jobRunIds = fixIds("job-run-id");

        // When
        createTaskWithRunIdsAndTimes("task-id", jobRunIds, times).run();

        // Then
        // - The compaction job should not be on the input queue or DLQ
        assertThat(messagesOnQueue(COMPACTION_JOB_QUEUE_URL)).isEmpty();
        assertThat(messagesOnQueue(COMPACTION_JOB_DLQ_URL)).isEmpty();
        // - A compaction commit request should be on the job commit queue
        assertThat(messagesOnQueue(COMPACTION_COMMIT_QUEUE_URL))
                .extracting(Message::body)
                .containsExactly(
                        batchedCommitRequestOnQueue(job, "task-id", "job-run-id",
                                new JobRunSummary(new RowsProcessed(100, 100),
                                        Instant.parse("2024-05-09T12:55:00Z"),
                                        Instant.parse("2024-05-09T12:56:00Z"))));
        // - Check new output file has been created with the correct rows
        assertThat(readRows(job.getOutputFile(), schema))
                .containsExactlyElementsOf(expectedRows);
        // - Check DynamoDBStateStore does not yet have correct file references
        assertThat(stateStore.getFileReferences())
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("lastStateStoreUpdateTime")
                .containsExactly(onJob(job, fileReference));
    }

    @Test
    void shouldSendCommitRequestToStateStoreQueueIfBatchingDisabled() throws Exception {
        // Given
        tableProperties.set(COMPACTION_JOB_COMMIT_ASYNC, "true");
        tableProperties.set(COMPACTION_JOB_ASYNC_BATCHING, "false");
        tablePropertiesStore.save(tableProperties);
        configureJobQueuesWithMaxReceiveCount(1);
        StateStore stateStore = getStateStore();
        FileReference fileReference = ingestFileWith100Rows();
        List<Row> expectedRows = IntStream.range(0, 100)
                .mapToObj(defaultRowCreator()::apply)
                .collect(Collectors.toList());
        CompactionJob job = compactionJobForFiles("job1", fileReference);
        assignJobIdsToInputFiles(stateStore, job);
        sendJob(job);
        Supplier<Instant> times = supplyTimes(
                Instant.parse("2024-05-09T12:52:00Z"),      // Start task
                Instant.parse("2024-05-09T12:55:00Z"),      // Job started
                Instant.parse("2024-05-09T12:56:00Z"),      // Job finished
                Instant.parse("2024-05-09T12:58:00Z"));    // Finished task
        Supplier<String> jobRunIds = fixIds("job-run-id");

        // When
        createTaskWithRunIdsAndTimes("task-id", jobRunIds, times).run();

        // Then
        // - The compaction job should not be on the input queue or DLQ
        assertThat(messagesOnQueue(COMPACTION_JOB_QUEUE_URL)).isEmpty();
        assertThat(messagesOnQueue(COMPACTION_JOB_DLQ_URL)).isEmpty();
        // - A compaction commit request should be on the job commit queue
        assertThat(messagesOnQueue(STATESTORE_COMMITTER_QUEUE_URL))
                .extracting(Message::body, this::getMessageGroupId)
                .containsExactly(tuple(
                        commitRequestOnQueue(job, "task-id", "job-run-id",
                                new JobRunSummary(new RowsProcessed(100, 100),
                                        Instant.parse("2024-05-09T12:55:00Z"),
                                        Instant.parse("2024-05-09T12:56:00Z"))),
                        tableId));
        // - Check new output file has been created with the correct rows
        assertThat(readRows(job.getOutputFile(), schema))
                .containsExactlyElementsOf(expectedRows);
        // - Check DynamoDBStateStore does not yet have correct file references
        assertThat(stateStore.getFileReferences())
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("lastStateStoreUpdateTime")
                .containsExactly(onJob(job, fileReference));
    }

    private InstanceProperties createInstanceProperties() {
        InstanceProperties instanceProperties = createTestInstanceProperties();
        instanceProperties.set(DEFAULT_INGEST_PARTITION_FILE_WRITER_TYPE, "direct");
        instanceProperties.setNumber(COMPACTION_TASK_WAIT_TIME_IN_SECONDS, 0);
        instanceProperties.setNumber(COMPACTION_JOB_FAILED_VISIBILITY_TIMEOUT_IN_SECONDS, 0);
        instanceProperties.setNumber(COMPACTION_TASK_DELAY_BEFORE_RETRY_IN_SECONDS, 0);
        instanceProperties.setNumber(COMPACTION_TASK_MAX_IDLE_TIME_IN_SECONDS, 0);
        instanceProperties.setNumber(COMPACTION_TASK_MAX_CONSECUTIVE_FAILURES, 1);
        instanceProperties.setNumber(COMPACTION_KEEP_ALIVE_PERIOD_IN_SECONDS, 1);
        instanceProperties.setNumber(COMPACTION_QUEUE_VISIBILITY_TIMEOUT_IN_SECONDS, 1);
        instanceProperties.setNumber(DEFAULT_COMPACTION_FILES_BATCH_SIZE, 5);
        return instanceProperties;
    }

    private StateStore getStateStore() {
        return stateStoreProvider.getStateStore(tableProperties);
    }

    private void useInMemoryStateStore() {
        inMemoryTransactionLogsPerTable = new InMemoryTransactionLogsPerTable();
        stateStoreProvider = InMemoryTransactionLogStateStore.createProvider(instanceProperties, inMemoryTransactionLogsPerTable);
        update(getStateStore()).initialise(schema);
    }

    private InMemoryTransactionLogStore inMemoryFilesLogStore() {
        return inMemoryTransactionLogsPerTable.forTable(tableProperties).getFilesLogStore();
    }

    private String getMessageGroupId(Message message) {
        return message.attributes().get(MessageSystemAttributeName.MESSAGE_GROUP_ID);
    }

    private Stream<Message> messagesOnQueue(InstanceProperty queueProperty) {
        return receiveMessagesAndMessageGroupId(instanceProperties.get(queueProperty));
    }

    private void configureJobQueuesWithMaxReceiveCount(int maxReceiveCount) {
        String jobQueueUrl = createSqsQueueGetUrl();
        String jobDlqUrl = createSqsQueueGetUrl();
        String jobDlqArn = sqsClient.getQueueAttributes(GetQueueAttributesRequest.builder()
                .queueUrl(jobDlqUrl)
                .attributeNames(List.of(QueueAttributeName.QUEUE_ARN)).build()).attributes().get(QueueAttributeName.QUEUE_ARN);
        sqsClient.setQueueAttributes(SetQueueAttributesRequest.builder()
                .queueUrl(jobQueueUrl)
                .attributes(Map.of(
                        QueueAttributeName.REDRIVE_POLICY, "{\"maxReceiveCount\":\"" + maxReceiveCount + "\", " +
                                "\"deadLetterTargetArn\":\"" + jobDlqArn + "\"}"))
                .build());
        instanceProperties.set(COMPACTION_JOB_QUEUE_URL, jobQueueUrl);
        instanceProperties.set(COMPACTION_JOB_DLQ_URL, jobDlqUrl);
        configureJobCommitterQueues(maxReceiveCount);
    }

    private void configureJobCommitterQueues(int maxReceiveCount) {
        String jobCommitQueueUrl = createSqsQueueGetUrl();
        String stateStoreCommitQueueUrl = createFifoQueueGetUrl();
        instanceProperties.set(COMPACTION_COMMIT_QUEUE_URL, jobCommitQueueUrl);
        instanceProperties.set(STATESTORE_COMMITTER_QUEUE_URL, stateStoreCommitQueueUrl);
    }

    private CompactionTask createTaskWithRunIdsAndTimes(
            String taskId, Supplier<String> jobRunIdSupplier, Supplier<Instant> timeSupplier) {
        return createTask(taskId, stateStoreProvider, jobRunIdSupplier, timeSupplier);
    }

    private CompactionTask createTask(String taskId) {
        return createTask(taskId, stateStoreProvider, () -> UUID.randomUUID().toString(), Instant::now);
    }

    private CompactionTask createTask(
            String taskId, StateStoreProvider stateStoreProvider,
            Supplier<String> jobRunIdSupplier, Supplier<Instant> timeSupplier) {
        DefaultCompactionRunnerFactory selector = new DefaultCompactionRunnerFactory(
                ObjectFactory.noUserJars(), hadoopConf, new S3SketchesStore(s3Client, s3TransferManager));
        CompactionJobCommitterOrSendToLambda committer = ECSCompactionTaskRunner.committerOrSendToLambda(
                tablePropertiesProvider, stateStoreProvider, jobTracker,
                instanceProperties, sqsClient);
        StateStoreWaitForFiles waitForFiles = new StateStoreWaitForFiles(
                tablePropertiesProvider, stateStoreProvider, jobTracker, DynamoDBUtils.retryOnThrottlingException());
        CompactionTask task = new CompactionTask(instanceProperties, tablePropertiesProvider,
                PropertiesReloader.neverReload(), stateStoreProvider, new SqsCompactionQueueHandler(sqsClient, instanceProperties),
                waitForFiles, committer, jobTracker, taskTracker, selector, taskId,
                jobRunIdSupplier, timeSupplier, noWaits());
        return task;
    }

    private Function<Integer, Row> defaultRowCreator() {
        return i -> new Row(Map.of(
                "key", (long) 2 * i,
                "value1", (long) 2 * i,
                "value2", 987654321L));
    }

    private FileReference ingestFileWith100Rows() throws Exception {
        return ingestFileWith100Rows(defaultRowCreator());
    }

    private FileReference ingestFileWith100Rows(Function<Integer, Row> rowCreator) throws Exception {
        IngestFactory ingestFactory = IngestFactory.builder()
                .objectFactory(ObjectFactory.noUserJars())
                .hadoopConfiguration(hadoopConf)
                .localDir(tempDir.toString())
                .stateStoreProvider(FixedStateStoreProvider.singleTable(tableProperties, getStateStore()))
                .instanceProperties(instanceProperties)
                .s3AsyncClient(s3AsyncClient)
                .build();
        IngestCoordinator<Row> coordinator = ingestFactory.createIngestCoordinator(tableProperties);
        for (int i = 0; i < 100; i++) {
            coordinator.write(rowCreator.apply(i));
        }
        return coordinator.closeReturningResult().getFileReferenceList().get(0);
    }

    private String sendCompactionJobForFilesGetJson(String jobId, String outputFilename, String... inputFilenames) throws IOException {
        return sendJob(compactionJobForFiles(jobId, List.of(inputFilenames)));
    }

    private String sendJob(CompactionJob job) throws IOException {
        String jobJson = new CompactionJobSerDe().toJson(job);
        SendMessageRequest sendMessageRequest = SendMessageRequest.builder()
                .queueUrl(instanceProperties.get(COMPACTION_JOB_QUEUE_URL))
                .messageBody(jobJson).build();
        sqsClient.sendMessage(sendMessageRequest);
        return jobJson;
    }

    private CompactionJob compactionJobForFiles(String jobId, FileReference... fileReferences) {
        return compactionJobForFiles(jobId, List.of(fileReferences).stream()
                .map(FileReference::getFilename)
                .collect(Collectors.toList()));

    }

    private CompactionJob compactionJobForFiles(String jobId, List<String> inputFilenames) {
        CompactionJobFactory factory = new CompactionJobFactory(instanceProperties, tableProperties);
        return factory.createCompactionJobWithFilenames(jobId, inputFilenames, "root");
    }

    private String commitRequestOnQueue(CompactionJob job, String taskId, String jobRunId, JobRunSummary summary) {
        return new StateStoreCommitRequestSerDe(tablePropertiesProvider)
                .toJson(StateStoreCommitRequest.create(tableId,
                        new ReplaceFileReferencesTransaction(List.of(
                                job.replaceFileReferencesRequestBuilder(summary.getRowsProcessed().getRowsWritten())
                                        .taskId(taskId)
                                        .jobRunId(jobRunId)
                                        .build()))));
    }

    private String batchedCommitRequestOnQueue(CompactionJob job, String taskId, String jobRunId, JobRunSummary summary) {
        return new CompactionCommitMessageSerDe()
                .toJson(new CompactionCommitMessage(tableId,
                        job.replaceFileReferencesRequestBuilder(summary.getRowsProcessed().getRowsWritten())
                                .taskId(taskId)
                                .jobRunId(jobRunId)
                                .build()));
    }

    private FileReference onJob(CompactionJob job, FileReference reference) {
        return reference.toBuilder().jobId(job.getId()).build();
    }

    private List<Row> readRows(String filename, Schema schema) {
        try (ParquetReader<Row> reader = ParquetRowReaderFactory.parquetRowReaderBuilder(new Path(filename), schema).build()) {
            List<Row> rows = new ArrayList<>();
            for (Row row = reader.read(); row != null; row = reader.read()) {
                rows.add(new Row(row));
            }
            return rows;
        } catch (IOException e) {
            throw new RuntimeException("Failed reading rows", e);
        }
    }
}
