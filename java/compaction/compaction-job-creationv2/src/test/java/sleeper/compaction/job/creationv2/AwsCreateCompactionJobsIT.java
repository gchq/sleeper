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
package sleeper.compaction.job.creationv2;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;

import sleeper.compaction.core.job.CompactionJob;
import sleeper.compaction.core.job.CompactionJobSerDe;
import sleeper.compaction.core.job.creation.CreateCompactionJobs;
import sleeper.compaction.core.job.creation.CreateJobsTestUtils;
import sleeper.compaction.core.job.dispatch.CompactionJobDispatchRequest;
import sleeper.compaction.core.job.dispatch.CompactionJobDispatchRequestSerDe;
import sleeper.configurationv2.properties.S3InstanceProperties;
import sleeper.configurationv2.table.index.DynamoDBTableIndexCreator;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.testutils.FixedTablePropertiesProvider;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreProvider;
import sleeper.core.statestore.commit.StateStoreCommitRequest;
import sleeper.core.statestore.commit.StateStoreCommitRequestSerDe;
import sleeper.core.statestore.transactionlog.transaction.impl.AssignJobIdsTransaction;
import sleeper.core.util.ObjectFactory;
import sleeper.core.util.ObjectFactoryException;
import sleeper.localstack.test.LocalStackTestBase;
import sleeper.statestorev2.StateStoreFactory;
import sleeper.statestorev2.transactionlog.TransactionLogStateStoreCreator;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.compaction.core.job.creation.CreateJobsTestUtils.assertAllReferencesHaveJobId;
import static sleeper.compaction.core.job.creation.CreateJobsTestUtils.createInstanceProperties;
import static sleeper.compaction.core.job.creation.CreateJobsTestUtils.createTableProperties;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.COMPACTION_JOB_QUEUE_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.COMPACTION_PENDING_QUEUE_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.STATESTORE_COMMITTER_QUEUE_URL;
import static sleeper.core.properties.instance.CommonProperty.FILE_SYSTEM;
import static sleeper.core.properties.table.TableProperty.COMPACTION_JOB_ID_ASSIGNMENT_COMMIT_ASYNC;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.statestore.testutils.StateStoreUpdatesWrapper.update;

public class AwsCreateCompactionJobsIT extends LocalStackTestBase {

    private final InstanceProperties instanceProperties = createInstance();
    private final Schema schema = CreateJobsTestUtils.createSchema();
    private final StateStoreProvider stateStoreProvider = StateStoreFactory.createProvider(instanceProperties, s3ClientV2, dynamoClientV2, s3TransferManager);
    private final TableProperties tableProperties = createTableProperties(schema, instanceProperties);
    private final StateStore stateStore = createAndInitialiseStateStore(tableProperties);

    @Test
    public void shouldCompactAllFilesInSinglePartition() throws Exception {
        // Given
        FileReferenceFactory fileReferenceFactory = FileReferenceFactory.from(stateStore);
        FileReference fileReference1 = fileReferenceFactory.rootFile("file1", 200L);
        FileReference fileReference2 = fileReferenceFactory.rootFile("file2", 200L);
        FileReference fileReference3 = fileReferenceFactory.rootFile("file3", 200L);
        FileReference fileReference4 = fileReferenceFactory.rootFile("file4", 200L);
        update(stateStore).addFiles(Arrays.asList(fileReference1, fileReference2, fileReference3, fileReference4));

        // When
        createJobs();

        // Then
        assertThat(stateStore.getFileReferencesWithNoJobId()).isEmpty();
        String jobId = assertAllReferencesHaveJobId(stateStore.getFileReferences());
        assertThat(receivePendingBatches()).singleElement().satisfies(batch -> {
            assertThat(loadBatchJobs(batch)).singleElement().satisfies(job -> {
                assertThat(job.getId()).isEqualTo(jobId);
                assertThat(job.getInputFiles()).containsExactlyInAnyOrder("file1", "file2", "file3", "file4");
                assertThat(job.getPartitionId()).isEqualTo("root");
            });
        });
    }

    @Test
    public void shouldSendAssignJobIdRequestToSqs() throws Exception {
        // Given
        tableProperties.set(COMPACTION_JOB_ID_ASSIGNMENT_COMMIT_ASYNC, "true");
        FileReferenceFactory fileReferenceFactory = FileReferenceFactory.from(stateStore);
        FileReference fileReference1 = fileReferenceFactory.rootFile("file1", 200L);
        FileReference fileReference2 = fileReferenceFactory.rootFile("file2", 200L);
        FileReference fileReference3 = fileReferenceFactory.rootFile("file3", 200L);
        FileReference fileReference4 = fileReferenceFactory.rootFile("file4", 200L);
        List<FileReference> files = List.of(fileReference1, fileReference2, fileReference3, fileReference4);
        update(stateStore).addFiles(files);

        // When
        createJobs();

        // Then
        assertThat(stateStore.getFileReferencesWithNoJobId())
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("lastStateStoreUpdateTime")
                .containsExactlyInAnyOrderElementsOf(files);
        List<CompactionJobDispatchRequest> batches = receivePendingBatches();
        List<StateStoreCommitRequest> jobIdAssignmentRequests = receiveJobIdAssignmentRequests();
        assertThat(batches).singleElement().satisfies(batch -> {
            assertThat(loadBatchJobs(batch)).singleElement().satisfies(job -> {
                assertThat(job.getInputFiles()).containsExactlyInAnyOrder("file1", "file2", "file3", "file4");
                assertThat(job.getPartitionId()).isEqualTo("root");
                assertThat(jobIdAssignmentRequests).containsExactly(
                        StateStoreCommitRequest.create(tableProperties.get(TABLE_ID), new AssignJobIdsTransaction(
                                List.of(job.createAssignJobIdRequest()))));
            });
        });
    }

    private List<CompactionJobDispatchRequest> receivePendingBatches() {
        ReceiveMessageResponse response = sqsClientV2.receiveMessage(ReceiveMessageRequest.builder()
                .queueUrl(instanceProperties.get(COMPACTION_PENDING_QUEUE_URL))
                .maxNumberOfMessages(10)
                .build());
        return response.messages().stream()
                .map(Message::body)
                .map(new CompactionJobDispatchRequestSerDe()::fromJson)
                .toList();
    }

    private List<CompactionJob> loadBatchJobs(CompactionJobDispatchRequest batch) {
        return new CompactionJobSerDe().batchFromJson(s3ClientV2.getObject(
                GetObjectRequest.builder()
                        .bucket(instanceProperties.get(DATA_BUCKET))
                        .key(batch.getBatchKey())
                        .build(),
                ResponseTransformer.toBytes()).asUtf8String());
    }

    private List<StateStoreCommitRequest> receiveJobIdAssignmentRequests() {
        return receiveAssignJobIdQueueMessage().messages().stream()
                .map(this::readAssignJobIdRequest)
                .collect(Collectors.toList());
    }

    private ReceiveMessageResponse receiveAssignJobIdQueueMessage() {
        return sqsClientV2.receiveMessage(ReceiveMessageRequest.builder()
                .queueUrl(instanceProperties.get(STATESTORE_COMMITTER_QUEUE_URL))
                .maxNumberOfMessages(10)
                .build());
    }

    private StateStoreCommitRequest readAssignJobIdRequest(Message message) {
        return new StateStoreCommitRequestSerDe(tableProperties).fromJson(message.body());
    }

    private InstanceProperties createInstance() {
        InstanceProperties instanceProperties = createInstanceProperties();
        instanceProperties.set(STATESTORE_COMMITTER_QUEUE_URL, createFifoQueueGetUrl());
        instanceProperties.set(COMPACTION_JOB_QUEUE_URL, sqsClientV2.createQueue(CreateQueueRequest.builder()
                .queueName(UUID.randomUUID().toString()).build()).queueUrl());
        instanceProperties.set(FILE_SYSTEM, "");
        DynamoDBTableIndexCreator.create(dynamoClientV2, instanceProperties);
        new TransactionLogStateStoreCreator(instanceProperties, dynamoClientV2).create();

        createBucket(instanceProperties.get(CONFIG_BUCKET));
        createBucket(instanceProperties.get(DATA_BUCKET));
        instanceProperties.set(COMPACTION_PENDING_QUEUE_URL,
                sqsClientV2.createQueue(CreateQueueRequest.builder()
                        .queueName(UUID.randomUUID().toString()).build()).queueUrl());
        S3InstanceProperties.saveToS3(s3ClientV2, instanceProperties);

        return instanceProperties;
    }

    private StateStore createAndInitialiseStateStore(TableProperties tableProperties) {
        StateStore stateStore = stateStoreProvider.getStateStore(tableProperties);
        update(stateStore).initialise(tableProperties.getSchema());
        return stateStore;
    }

    private void createJobs() throws Exception {
        jobCreator().createJobsWithStrategy(tableProperties);
    }

    private CreateCompactionJobs jobCreator() throws ObjectFactoryException {
        return AwsCreateCompactionJobs.from(ObjectFactory.noUserJars(),
                instanceProperties, new FixedTablePropertiesProvider(tableProperties), stateStoreProvider, s3ClientV2, sqsClientV2);
    }
}
