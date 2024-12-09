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
package sleeper.compaction.job.creation;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.CreateQueueResult;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import sleeper.compaction.core.job.CompactionJob;
import sleeper.compaction.core.job.CompactionJobSerDe;
import sleeper.compaction.core.job.CompactionJobStatusStore;
import sleeper.compaction.core.job.commit.CompactionJobIdAssignmentCommitRequest;
import sleeper.compaction.core.job.commit.CompactionJobIdAssignmentCommitRequestSerDe;
import sleeper.compaction.core.job.creation.CreateCompactionJobs;
import sleeper.compaction.core.job.creation.CreateJobsTestUtils;
import sleeper.compaction.core.job.dispatch.CompactionJobDispatchRequest;
import sleeper.compaction.core.job.dispatch.CompactionJobDispatchRequestSerDe;
import sleeper.configuration.properties.S3InstanceProperties;
import sleeper.configuration.table.index.DynamoDBTableIndexCreator;
import sleeper.core.CommonTestConstants;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreProvider;
import sleeper.core.util.ObjectFactory;
import sleeper.core.util.ObjectFactoryException;
import sleeper.parquet.utils.HadoopConfigurationLocalStackUtils;
import sleeper.statestore.StateStoreFactory;
import sleeper.statestore.transactionlog.TransactionLogStateStoreCreator;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.compaction.core.job.commit.CompactionJobIdAssignmentCommitRequestTestHelper.requestToAssignFilesToJobs;
import static sleeper.compaction.core.job.creation.CreateJobsTestUtils.assertAllReferencesHaveJobId;
import static sleeper.compaction.core.job.creation.CreateJobsTestUtils.createInstanceProperties;
import static sleeper.compaction.core.job.creation.CreateJobsTestUtils.createTableProperties;
import static sleeper.configuration.testutils.LocalStackAwsV1ClientHelper.buildAwsV1Client;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.COMPACTION_JOB_QUEUE_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.COMPACTION_PENDING_QUEUE_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.STATESTORE_COMMITTER_QUEUE_URL;
import static sleeper.core.properties.instance.CommonProperty.FILE_SYSTEM;
import static sleeper.core.properties.table.TableProperty.COMPACTION_JOB_ID_ASSIGNMENT_COMMIT_ASYNC;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;

@Testcontainers
public class AwsCreateCompactionJobsIT {

    @Container
    public static LocalStackContainer localStackContainer = new LocalStackContainer(DockerImageName.parse(CommonTestConstants.LOCALSTACK_DOCKER_IMAGE)).withServices(
            LocalStackContainer.Service.S3, LocalStackContainer.Service.SQS, LocalStackContainer.Service.DYNAMODB);

    private final AmazonS3 s3 = buildAwsV1Client(localStackContainer, LocalStackContainer.Service.S3, AmazonS3ClientBuilder.standard());
    private final AmazonDynamoDB dynamoDB = buildAwsV1Client(localStackContainer, LocalStackContainer.Service.DYNAMODB, AmazonDynamoDBClientBuilder.standard());
    private final AmazonSQS sqs = buildAwsV1Client(localStackContainer, LocalStackContainer.Service.SQS, AmazonSQSClientBuilder.standard());
    private final InstanceProperties instanceProperties = createInstance();
    private final Schema schema = CreateJobsTestUtils.createSchema();
    private final StateStoreProvider stateStoreProvider = StateStoreFactory.createProvider(instanceProperties, s3, dynamoDB,
            HadoopConfigurationLocalStackUtils.getHadoopConfiguration(localStackContainer));
    private final TableProperties tableProperties = createTableProperties(schema, instanceProperties);
    private final StateStore stateStore = createAndInitialiseStateStore(tableProperties);

    @AfterEach
    void tearDown() {
        s3.shutdown();
        dynamoDB.shutdown();
        sqs.shutdown();
    }

    @Test
    public void shouldCompactAllFilesInSinglePartition() throws Exception {
        // Given
        FileReferenceFactory fileReferenceFactory = FileReferenceFactory.from(stateStore);
        FileReference fileReference1 = fileReferenceFactory.rootFile("file1", 200L);
        FileReference fileReference2 = fileReferenceFactory.rootFile("file2", 200L);
        FileReference fileReference3 = fileReferenceFactory.rootFile("file3", 200L);
        FileReference fileReference4 = fileReferenceFactory.rootFile("file4", 200L);
        stateStore.addFiles(Arrays.asList(fileReference1, fileReference2, fileReference3, fileReference4));

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
        stateStore.addFiles(files);

        // When
        createJobs();

        // Then
        assertThat(stateStore.getFileReferencesWithNoJobId())
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("lastStateStoreUpdateTime")
                .containsExactlyInAnyOrderElementsOf(files);
        List<CompactionJobDispatchRequest> batches = receivePendingBatches();
        List<CompactionJobIdAssignmentCommitRequest> jobIdAssignmentRequests = receiveJobIdAssignmentRequests();
        assertThat(batches).singleElement().satisfies(batch -> {
            assertThat(loadBatchJobs(batch)).singleElement().satisfies(job -> {
                assertThat(job.getInputFiles()).containsExactlyInAnyOrder("file1", "file2", "file3", "file4");
                assertThat(job.getPartitionId()).isEqualTo("root");
                assertThat(jobIdAssignmentRequests).containsExactly(
                        requestToAssignFilesToJobs(List.of(job), tableProperties.get(TABLE_ID)));
            });
        });
    }

    private List<CompactionJobDispatchRequest> receivePendingBatches() {
        ReceiveMessageResult result = sqs.receiveMessage(new ReceiveMessageRequest()
                .withQueueUrl(instanceProperties.get(COMPACTION_PENDING_QUEUE_URL))
                .withMaxNumberOfMessages(10));
        return result.getMessages().stream()
                .map(Message::getBody)
                .map(new CompactionJobDispatchRequestSerDe()::fromJson)
                .toList();
    }

    private List<CompactionJob> loadBatchJobs(CompactionJobDispatchRequest batch) {
        String json = s3.getObjectAsString(instanceProperties.get(DATA_BUCKET), batch.getBatchKey());
        return new CompactionJobSerDe().batchFromJson(json);
    }

    private List<CompactionJobIdAssignmentCommitRequest> receiveJobIdAssignmentRequests() {
        return receiveAssignJobIdQueueMessage().getMessages().stream()
                .map(this::readAssignJobIdRequest)
                .collect(Collectors.toList());
    }

    private ReceiveMessageResult receiveAssignJobIdQueueMessage() {
        ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest()
                .withQueueUrl(instanceProperties.get(STATESTORE_COMMITTER_QUEUE_URL))
                .withMaxNumberOfMessages(10);
        return sqs.receiveMessage(receiveMessageRequest);
    }

    private CompactionJobIdAssignmentCommitRequest readAssignJobIdRequest(Message message) {
        return new CompactionJobIdAssignmentCommitRequestSerDe().fromJson(message.getBody());
    }

    private InstanceProperties createInstance() {
        InstanceProperties instanceProperties = createInstanceProperties();
        instanceProperties.set(STATESTORE_COMMITTER_QUEUE_URL, createFifoQueueGetUrl());
        instanceProperties.set(COMPACTION_JOB_QUEUE_URL, sqs.createQueue(UUID.randomUUID().toString()).getQueueUrl());
        instanceProperties.set(FILE_SYSTEM, "");
        DynamoDBTableIndexCreator.create(dynamoDB, instanceProperties);
        new TransactionLogStateStoreCreator(instanceProperties, dynamoDB).create();

        s3.createBucket(instanceProperties.get(CONFIG_BUCKET));
        s3.createBucket(instanceProperties.get(DATA_BUCKET));
        instanceProperties.set(COMPACTION_PENDING_QUEUE_URL,
                sqs.createQueue(UUID.randomUUID().toString()).getQueueUrl());
        S3InstanceProperties.saveToS3(s3, instanceProperties);

        return instanceProperties;
    }

    private String createFifoQueueGetUrl() {
        CreateQueueResult result = sqs.createQueue(new CreateQueueRequest()
                .withQueueName(UUID.randomUUID().toString() + ".fifo")
                .withAttributes(Map.of("FifoQueue", "true")));
        return result.getQueueUrl();
    }

    private StateStore createAndInitialiseStateStore(TableProperties tableProperties) {
        StateStore stateStore = stateStoreProvider.getStateStore(tableProperties);
        stateStore.initialise();
        return stateStore;
    }

    private void createJobs() throws Exception {
        jobCreator().createJobsWithStrategy(tableProperties);
    }

    private CreateCompactionJobs jobCreator() throws ObjectFactoryException {
        return AwsCreateCompactionJobs.from(ObjectFactory.noUserJars(),
                instanceProperties, stateStoreProvider, CompactionJobStatusStore.NONE,
                s3, sqs);
    }
}
