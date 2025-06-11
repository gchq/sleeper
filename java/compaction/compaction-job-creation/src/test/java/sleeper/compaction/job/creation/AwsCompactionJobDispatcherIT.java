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
package sleeper.compaction.job.creation;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;

import sleeper.compaction.core.job.CompactionJob;
import sleeper.compaction.core.job.CompactionJobFactory;
import sleeper.compaction.core.job.CompactionJobSerDe;
import sleeper.compaction.core.job.dispatch.CompactionJobDispatchRequest;
import sleeper.compaction.core.job.dispatch.CompactionJobDispatchRequestSerDe;
import sleeper.compaction.core.job.dispatch.CompactionJobDispatcher;
import sleeper.compaction.tracker.job.DynamoDBCompactionJobTrackerCreator;
import sleeper.configurationv2.properties.S3InstanceProperties;
import sleeper.configurationv2.properties.S3TableProperties;
import sleeper.configurationv2.table.index.DynamoDBTableIndexCreator;
import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreProvider;
import sleeper.localstack.test.LocalStackTestBase;
import sleeper.statestore.StateStoreFactory;
import sleeper.statestore.transactionlog.TransactionLogStateStoreCreator;

import java.time.Instant;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.COMPACTION_JOB_QUEUE_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.COMPACTION_PENDING_DLQ_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.COMPACTION_PENDING_QUEUE_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.core.properties.table.TableProperty.COMPACTION_JOB_SEND_RETRY_DELAY_SECS;
import static sleeper.core.properties.table.TableProperty.COMPACTION_JOB_SEND_TIMEOUT_SECS;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.schema.SchemaTestHelper.createSchemaWithKey;
import static sleeper.core.statestore.testutils.StateStoreUpdatesWrapper.update;

public class AwsCompactionJobDispatcherIT extends LocalStackTestBase {

    InstanceProperties instanceProperties = createInstance();
    StateStoreProvider stateStoreProvider = StateStoreFactory.createProvider(instanceProperties, s3ClientV2, dynamoClientV2);
    Schema schema = createSchemaWithKey("key");
    PartitionTree partitions = new PartitionsBuilder(schema).singlePartition("root").buildTree();
    TableProperties tableProperties = addTable(instanceProperties, schema, partitions);
    FileReferenceFactory fileFactory = FileReferenceFactory.from(partitions);
    CompactionJobFactory compactionFactory = new CompactionJobFactory(instanceProperties, tableProperties);
    StateStore stateStore = stateStoreProvider.getStateStore(tableProperties);

    @Test
    void shouldSendCompactionJobsInABatchWhenAllFilesAreAssigned() {

        // Given
        FileReference file1 = fileFactory.rootFile("test1.parquet", 1234);
        FileReference file2 = fileFactory.rootFile("test2.parquet", 5678);
        CompactionJob job1 = compactionFactory.createCompactionJob("test-job-1", List.of(file1), "root");
        CompactionJob job2 = compactionFactory.createCompactionJob("test-job-2", List.of(file2), "root");
        update(stateStore).addFiles(List.of(file1, file2));
        assignJobIds(List.of(job1, job2));

        CompactionJobDispatchRequest request = generateBatchRequestAtTime(
                "test-batch", Instant.parse("2024-11-15T10:30:00Z"));
        putCompactionJobBatch(request, List.of(job1, job2));

        // When
        dispatchWithNoRetry(request);

        // Then
        assertThat(receiveCompactionJobs()).containsExactly(job1, job2);
        assertThatBatchFileWasDeleted();
    }

    @Test
    void shouldReturnBatchToTheQueueIfTheFilesForTheBatchAreUnassigned() throws Exception {

        // Given
        FileReference file1 = fileFactory.rootFile("test3.parquet", 1234);
        FileReference file2 = fileFactory.rootFile("test4.parquet", 5678);
        CompactionJob job1 = compactionFactory.createCompactionJob("test-job-3", List.of(file1), "root");
        CompactionJob job2 = compactionFactory.createCompactionJob("test-job-4", List.of(file2), "root");
        update(stateStore).addFiles(List.of(file1, file2));

        tableProperties.setNumber(COMPACTION_JOB_SEND_TIMEOUT_SECS, 123);
        tableProperties.setNumber(COMPACTION_JOB_SEND_RETRY_DELAY_SECS, 0);
        saveTableProperties();

        CompactionJobDispatchRequest request = generateBatchRequestAtTime(
                "test-batch", Instant.parse("2024-11-21T10:20:00Z"));
        Instant retryTime = Instant.parse("2024-11-21T10:22:00Z");
        putCompactionJobBatch(request, List.of(job1, job2));

        // When
        dispatchWithTimeAtRetryCheck(request, retryTime);

        // Then
        assertThat(recievePendingBatches()).containsExactly(request);
        assertThatBatchFileWasNotDeleted();
    }

    @Test
    void shouldSendBatchToDeadLetterQueueIfExpiredAndTheFilesForTheBatchAreUnassigned() throws Exception {

        // Given
        FileReference file1 = fileFactory.rootFile("test3.parquet", 1234);
        FileReference file2 = fileFactory.rootFile("test4.parquet", 5678);
        CompactionJob job1 = compactionFactory.createCompactionJob("test-job-3", List.of(file1), "root");
        CompactionJob job2 = compactionFactory.createCompactionJob("test-job-4", List.of(file2), "root");
        update(stateStore).addFiles(List.of(file1, file2));

        tableProperties.setNumber(COMPACTION_JOB_SEND_TIMEOUT_SECS, 123);
        tableProperties.setNumber(COMPACTION_JOB_SEND_RETRY_DELAY_SECS, 0);
        saveTableProperties();

        CompactionJobDispatchRequest request = generateBatchRequestAtTime(
                "test-batch", Instant.parse("2024-11-21T10:20:00Z"));
        Instant retryTime = Instant.parse("2024-11-21T10:23:00Z");
        putCompactionJobBatch(request, List.of(job1, job2));

        // When
        dispatchWithTimeAtRetryCheck(request, retryTime);

        // Then
        assertThat(recievePendingBatches()).isEmpty();
        assertThat(receiveDeadLetters()).containsExactly(request);
        assertThatBatchFileWasNotDeleted();
    }

    private InstanceProperties createInstance() {
        InstanceProperties instanceProperties = createTestInstanceProperties();
        instanceProperties.set(COMPACTION_JOB_QUEUE_URL, createSqsQueueGetUrl());
        instanceProperties.set(COMPACTION_PENDING_QUEUE_URL, createSqsQueueGetUrl());
        instanceProperties.set(COMPACTION_PENDING_DLQ_URL, createSqsQueueGetUrl());

        DynamoDBTableIndexCreator.create(dynamoClientV2, instanceProperties);
        new TransactionLogStateStoreCreator(instanceProperties, dynamoClientV2).create();
        DynamoDBCompactionJobTrackerCreator.create(instanceProperties, dynamoClientV2);

        createBucket(instanceProperties.get(CONFIG_BUCKET));
        createBucket(instanceProperties.get(DATA_BUCKET));
        S3InstanceProperties.saveToS3(s3ClientV2, instanceProperties);

        return instanceProperties;
    }

    private TableProperties addTable(InstanceProperties instanceProperties, Schema schema, PartitionTree partitions) {
        TableProperties tableProperties = createTestTableProperties(instanceProperties, schema);
        S3TableProperties.createStore(instanceProperties, s3ClientV2, dynamoClientV2)
                .createTable(tableProperties);
        update(stateStoreProvider.getStateStore(tableProperties))
                .initialise(partitions.getAllPartitions());
        return tableProperties;
    }

    private void saveTableProperties() {
        S3TableProperties.createStore(instanceProperties, s3ClientV2, dynamoClientV2)
                .save(tableProperties);
    }

    private void assignJobIds(List<CompactionJob> jobs) {
        for (CompactionJob job : jobs) {
            update(stateStore).assignJobIds(List.of(job.createAssignJobIdRequest()));
        }
    }

    private CompactionJobDispatchRequest generateBatchRequestAtTime(String batchId, Instant timeNow) {
        return CompactionJobDispatchRequest.forTableWithBatchIdAtTime(
                tableProperties, batchId, timeNow);
    }

    private void putCompactionJobBatch(CompactionJobDispatchRequest request, List<CompactionJob> jobs) {
        putObject(
                instanceProperties.get(DATA_BUCKET),
                request.getBatchKey(),
                new CompactionJobSerDe().toJson(jobs));
    }

    private void dispatchWithNoRetry(CompactionJobDispatchRequest request) {
        dispatcher(List.of()).dispatch(request);
    }

    private void dispatchWithTimeAtRetryCheck(CompactionJobDispatchRequest request, Instant time) {
        dispatcher(List.of(time)).dispatch(request);
    }

    private CompactionJobDispatcher dispatcher(List<Instant> times) {
        return AwsCompactionJobDispatcher.from(s3ClientV2, dynamoClientV2, sqsClientV2, instanceProperties, times.iterator()::next);
    }

    private List<CompactionJob> receiveCompactionJobs() {
        ReceiveMessageResponse response = sqsClientV2.receiveMessage(ReceiveMessageRequest.builder()
                .queueUrl(instanceProperties.get(COMPACTION_JOB_QUEUE_URL))
                .maxNumberOfMessages(10)
                .build());
        return response.messages().stream()
                .map(Message::body)
                .map(new CompactionJobSerDe()::fromJson).toList();
    }

    private List<CompactionJobDispatchRequest> recievePendingBatches() {
        ReceiveMessageResponse response = sqsClientV2.receiveMessage(ReceiveMessageRequest.builder()
                .queueUrl(instanceProperties.get(COMPACTION_PENDING_QUEUE_URL))
                .maxNumberOfMessages(10)
                .build());
        return response.messages().stream()
                .map(Message::body)
                .map(new CompactionJobDispatchRequestSerDe()::fromJson).toList();
    }

    private List<CompactionJobDispatchRequest> receiveDeadLetters() {
        ReceiveMessageResponse response = sqsClientV2.receiveMessage(ReceiveMessageRequest.builder()
                .queueUrl(instanceProperties.get(COMPACTION_PENDING_DLQ_URL))
                .maxNumberOfMessages(10)
                .build());
        return response.messages().stream()
                .map(Message::body)
                .map(new CompactionJobDispatchRequestSerDe()::fromJson).toList();
    }

    private void assertThatBatchFileWasNotDeleted() {
        Set<String> contents = listObjectKeys(instanceProperties.get(DATA_BUCKET));
        assertThat(contents).isNotEmpty();
    }

    private void assertThatBatchFileWasDeleted() {
        Set<String> contents = listObjectKeys(instanceProperties.get(DATA_BUCKET));
        assertThat(contents).isEmpty();
    }

}
