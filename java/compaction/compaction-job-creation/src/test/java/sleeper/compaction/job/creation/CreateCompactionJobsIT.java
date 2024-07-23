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
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import sleeper.compaction.job.CompactionJob;
import sleeper.compaction.job.CompactionJobSerDe;
import sleeper.compaction.job.CompactionJobStatusStore;
import sleeper.compaction.job.creation.CreateCompactionJobs.Mode;
import sleeper.compaction.job.creation.commit.AssignJobIdToFiles.AssignJobIdQueueSender;
import sleeper.configuration.jars.ObjectFactory;
import sleeper.configuration.jars.ObjectFactoryException;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.table.index.DynamoDBTableIndexCreator;
import sleeper.core.CommonTestConstants;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;
import sleeper.io.parquet.utils.HadoopConfigurationLocalStackUtils;
import sleeper.statestore.StateStoreProvider;
import sleeper.statestore.transactionlog.TransactionLogStateStoreCreator;

import java.io.IOException;
import java.util.Arrays;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.compaction.job.creation.CreateJobsTestUtils.assertAllReferencesHaveJobId;
import static sleeper.compaction.job.creation.CreateJobsTestUtils.createInstanceProperties;
import static sleeper.compaction.job.creation.CreateJobsTestUtils.createTableProperties;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.COMPACTION_JOB_QUEUE_URL;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.configuration.properties.instance.CommonProperty.FILE_SYSTEM;
import static sleeper.configuration.testutils.LocalStackAwsV1ClientHelper.buildAwsV1Client;

@Testcontainers
public class CreateCompactionJobsIT {

    @Container
    public static LocalStackContainer localStackContainer = new LocalStackContainer(DockerImageName.parse(CommonTestConstants.LOCALSTACK_DOCKER_IMAGE)).withServices(
            LocalStackContainer.Service.S3, LocalStackContainer.Service.SQS, LocalStackContainer.Service.DYNAMODB, LocalStackContainer.Service.IAM);

    private final AmazonS3 s3 = buildAwsV1Client(localStackContainer, LocalStackContainer.Service.S3, AmazonS3ClientBuilder.standard());
    private final AmazonDynamoDB dynamoDB = buildAwsV1Client(localStackContainer, LocalStackContainer.Service.DYNAMODB, AmazonDynamoDBClientBuilder.standard());
    private final AmazonSQS sqs = buildAwsV1Client(localStackContainer, LocalStackContainer.Service.SQS, AmazonSQSClientBuilder.standard());
    private final InstanceProperties instanceProperties = createInstance();
    private final Schema schema = CreateJobsTestUtils.createSchema();
    private final StateStoreProvider stateStoreProvider = new StateStoreProvider(instanceProperties, s3, dynamoDB,
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
        jobCreator().createJobs(tableProperties);

        // Then
        assertThat(stateStore.getFileReferencesWithNoJobId()).isEmpty();
        String jobId = assertAllReferencesHaveJobId(stateStore.getFileReferences());
        assertThat(receiveJobQueueMessage().getMessages())
                .extracting(this::readJobMessage).singleElement().satisfies(job -> {
                    assertThat(job.getId()).isEqualTo(jobId);
                    assertThat(job.getInputFiles()).containsExactlyInAnyOrder("file1", "file2", "file3", "file4");
                    assertThat(job.getPartitionId()).isEqualTo("root");
                });
    }

    private ReceiveMessageResult receiveJobQueueMessage() {
        ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest()
                .withQueueUrl(instanceProperties.get(COMPACTION_JOB_QUEUE_URL))
                .withMaxNumberOfMessages(10);
        return sqs.receiveMessage(receiveMessageRequest);
    }

    private CompactionJob readJobMessage(Message message) {
        try {
            return CompactionJobSerDe.deserialiseFromString(message.getBody());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private InstanceProperties createInstance() {
        InstanceProperties instanceProperties = createInstanceProperties();
        instanceProperties.set(COMPACTION_JOB_QUEUE_URL, sqs.createQueue(UUID.randomUUID().toString()).getQueueUrl());
        instanceProperties.set(FILE_SYSTEM, "");
        DynamoDBTableIndexCreator.create(dynamoDB, instanceProperties);
        new TransactionLogStateStoreCreator(instanceProperties, dynamoDB).create();

        s3.createBucket(instanceProperties.get(CONFIG_BUCKET));
        s3.createBucket(instanceProperties.get(DATA_BUCKET));
        instanceProperties.saveToS3(s3);

        return instanceProperties;
    }

    private StateStore createAndInitialiseStateStore(TableProperties tableProperties) {
        StateStore stateStore = stateStoreProvider.getStateStore(tableProperties);
        try {
            stateStore.initialise();
        } catch (StateStoreException e) {
            throw new RuntimeException(e);
        }
        return stateStore;
    }

    private CreateCompactionJobs jobCreator() throws ObjectFactoryException {
        return new CreateCompactionJobs(new ObjectFactory(instanceProperties, s3, null),
                instanceProperties, stateStoreProvider,
                new SendCompactionJobToSqs(instanceProperties, sqs)::send,
                CompactionJobStatusStore.NONE, Mode.STRATEGY,
                AssignJobIdQueueSender.bySqs(sqs, instanceProperties));
    }
}
