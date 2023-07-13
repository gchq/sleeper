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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import sleeper.compaction.job.CompactionJob;
import sleeper.compaction.job.CompactionJobSerDe;
import sleeper.compaction.status.store.job.CompactionJobStatusStoreFactory;
import sleeper.compaction.status.store.job.DynamoDBCompactionJobStatusStoreCreator;
import sleeper.configuration.jars.ObjectFactory;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.core.CommonTestConstants;
import sleeper.core.partition.Partition;
import sleeper.core.schema.Schema;
import sleeper.statestore.FileInfo;
import sleeper.statestore.FileInfoFactory;
import sleeper.statestore.StateStore;
import sleeper.statestore.StateStoreProvider;
import sleeper.table.job.TableCreator;
import sleeper.table.job.TableLister;

import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.compaction.job.creation.CreateJobsTestUtils.assertAllFilesHaveJobId;
import static sleeper.compaction.job.creation.CreateJobsTestUtils.createInstanceProperties;
import static sleeper.compaction.job.creation.CreateJobsTestUtils.createTableProperties;
import static sleeper.configuration.properties.CommonProperty.FILE_SYSTEM;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.COMPACTION_JOB_QUEUE_URL;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;

@Testcontainers
public class CreateJobsIT {

    @Container
    public static LocalStackContainer localStackContainer = new LocalStackContainer(DockerImageName.parse(CommonTestConstants.LOCALSTACK_DOCKER_IMAGE)).withServices(
            LocalStackContainer.Service.S3, LocalStackContainer.Service.SQS, LocalStackContainer.Service.DYNAMODB, LocalStackContainer.Service.IAM
    );

    private final AmazonS3 s3 = createS3Client();
    private final AmazonSQS sqs = createSQSClient();
    private final InstanceProperties instanceProperties = createProperties(s3);
    private final Schema schema = CreateJobsTestUtils.createSchema();
    private StateStore stateStore;
    private CreateJobs createJobs;
    private CompactionJobSerDe compactionJobSerDe;

    @BeforeEach
    public void setUp() throws Exception {
        AmazonDynamoDB dynamoDB = createDynamoClient();
        TableProperties tableProperties = createTable(s3, dynamoDB, instanceProperties, schema);
        TablePropertiesProvider tablePropertiesProvider = new TablePropertiesProvider(s3, instanceProperties);
        StateStoreProvider stateStoreProvider = new StateStoreProvider(dynamoDB, instanceProperties);
        stateStore = stateStoreProvider.getStateStore(tableProperties);
        stateStore.initialise();
        DynamoDBCompactionJobStatusStoreCreator.create(instanceProperties, dynamoDB);
        compactionJobSerDe = new CompactionJobSerDe(tablePropertiesProvider);
        createJobs = new CreateJobs(new ObjectFactory(instanceProperties, s3, null),
                instanceProperties, tablePropertiesProvider, stateStoreProvider, sqs,
                new TableLister(s3, instanceProperties),
                CompactionJobStatusStoreFactory.getStatusStore(dynamoDB, instanceProperties));
    }

    @Test
    public void shouldCompactAllFilesInSinglePartition() throws Exception {
        // Given
        List<Partition> partitions = stateStore.getAllPartitions();
        FileInfoFactory fileInfoFactory = new FileInfoFactory(schema, partitions, Instant.now());
        FileInfo fileInfo1 = fileInfoFactory.leafFile("file1", 200L, 12L, 34L);
        FileInfo fileInfo2 = fileInfoFactory.leafFile("file2", 200L, 56L, 78L);
        FileInfo fileInfo3 = fileInfoFactory.leafFile("file3", 200L, 90L, 123L);
        FileInfo fileInfo4 = fileInfoFactory.leafFile("file4", 200L, 456L, 789L);
        stateStore.addFiles(Arrays.asList(fileInfo1, fileInfo2, fileInfo3, fileInfo4));

        // When
        createJobs.createJobs();

        // Then
        assertThat(stateStore.getActiveFilesWithNoJobId()).isEmpty();
        String jobId = assertAllFilesHaveJobId(stateStore.getActiveFiles());
        assertThat(receiveJobQueueMessage().getMessages())
                .extracting(this::readJobMessage).singleElement().satisfies(job -> {
                    assertThat(job.getId()).isEqualTo(jobId);
                    assertThat(job.getInputFiles()).containsExactlyInAnyOrder("file1", "file2", "file3", "file4");
                    assertThat(job.getPartitionId()).isEqualTo(partitions.get(0).getId());
                    assertThat(job.isSplittingJob()).isFalse();
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
            return compactionJobSerDe.deserialiseFromString(message.getBody());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static AmazonS3 createS3Client() {
        return AmazonS3ClientBuilder.standard()
                .withEndpointConfiguration(localStackContainer.getEndpointConfiguration(LocalStackContainer.Service.S3))
                .withCredentials(localStackContainer.getDefaultCredentialsProvider())
                .build();
    }

    private static AmazonSQS createSQSClient() {
        return AmazonSQSClientBuilder.standard()
                .withEndpointConfiguration(localStackContainer.getEndpointConfiguration(LocalStackContainer.Service.SQS))
                .withCredentials(localStackContainer.getDefaultCredentialsProvider())
                .build();
    }

    private static AmazonDynamoDB createDynamoClient() {
        return AmazonDynamoDBClientBuilder.standard()
                .withCredentials(localStackContainer.getDefaultCredentialsProvider())
                .withEndpointConfiguration(localStackContainer.getEndpointConfiguration(LocalStackContainer.Service.DYNAMODB))
                .build();
    }

    private static InstanceProperties createProperties(AmazonS3 s3) {
        String queue = UUID.randomUUID().toString();

        AmazonSQS sqs = createSQSClient();
        String queueUrl = sqs.createQueue(queue).getQueueUrl();
        sqs.shutdown();

        InstanceProperties instanceProperties = createInstanceProperties();
        instanceProperties.set(COMPACTION_JOB_QUEUE_URL, queueUrl);
        instanceProperties.set(FILE_SYSTEM, "");

        s3.createBucket(instanceProperties.get(CONFIG_BUCKET));

        return instanceProperties;
    }

    private static TableProperties createTable(AmazonS3 s3, AmazonDynamoDB dynamoDB, InstanceProperties instanceProperties, Schema schema) throws IOException {
        TableProperties tableProperties = createTableProperties(schema, instanceProperties);
        TableCreator tableCreator = new TableCreator(s3, dynamoDB, instanceProperties);
        tableCreator.createTable(tableProperties);

        tableProperties.loadFromS3(s3, tableProperties.get(TABLE_NAME));
        return tableProperties;
    }

}
