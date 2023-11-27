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
import org.junit.jupiter.api.AfterEach;
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
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.S3TableProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.configuration.properties.table.TablePropertiesStore;
import sleeper.configuration.table.index.DynamoDBTableIndexCreator;
import sleeper.core.CommonTestConstants;
import sleeper.core.partition.Partition;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.FileInfo;
import sleeper.core.statestore.FileInfoFactory;
import sleeper.core.statestore.StateStore;
import sleeper.statestore.StateStoreProvider;
import sleeper.statestore.s3.S3StateStoreCreator;

import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.compaction.job.creation.CreateJobsTestUtils.assertAllFilesHaveJobId;
import static sleeper.compaction.job.creation.CreateJobsTestUtils.createInstanceProperties;
import static sleeper.compaction.job.creation.CreateJobsTestUtils.createTableProperties;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.COMPACTION_JOB_QUEUE_URL;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.configuration.properties.instance.CommonProperty.FILE_SYSTEM;
import static sleeper.configuration.testutils.LocalStackAwsV1ClientHelper.buildAwsV1Client;
import static sleeper.io.parquet.utils.HadoopConfigurationLocalStackUtils.getHadoopConfiguration;

@Testcontainers
public class CreateJobsIT {

    @Container
    public static LocalStackContainer localStackContainer = new LocalStackContainer(DockerImageName.parse(CommonTestConstants.LOCALSTACK_DOCKER_IMAGE)).withServices(
            LocalStackContainer.Service.S3, LocalStackContainer.Service.SQS, LocalStackContainer.Service.DYNAMODB, LocalStackContainer.Service.IAM
    );

    private final AmazonS3 s3 = buildAwsV1Client(localStackContainer, LocalStackContainer.Service.S3, AmazonS3ClientBuilder.standard());
    private final AmazonDynamoDB dynamoDB = buildAwsV1Client(localStackContainer, LocalStackContainer.Service.DYNAMODB, AmazonDynamoDBClientBuilder.standard());
    private final AmazonSQS sqs = buildAwsV1Client(localStackContainer, LocalStackContainer.Service.SQS, AmazonSQSClientBuilder.standard());
    private final InstanceProperties instanceProperties = createInstance();
    private final Schema schema = CreateJobsTestUtils.createSchema();
    private final TablePropertiesStore tablePropertiesStore = S3TableProperties.getStore(instanceProperties, s3, dynamoDB);
    private StateStore stateStore;
    private CreateJobs createJobs;
    private CompactionJobSerDe compactionJobSerDe;

    @BeforeEach
    public void setUp() throws Exception {
        TableProperties tableProperties = createTable(schema);
        TablePropertiesProvider tablePropertiesProvider = new TablePropertiesProvider(instanceProperties, s3, dynamoDB);
        StateStoreProvider stateStoreProvider = new StateStoreProvider(dynamoDB, instanceProperties, getHadoopConfiguration(localStackContainer));
        stateStore = stateStoreProvider.getStateStore(tableProperties);
        stateStore.initialise();
        compactionJobSerDe = new CompactionJobSerDe(tablePropertiesProvider);
        createJobs = new CreateJobs(new ObjectFactory(instanceProperties, s3, null),
                instanceProperties, tablePropertiesProvider, stateStoreProvider, sqs,
                CompactionJobStatusStoreFactory.getStatusStore(dynamoDB, instanceProperties));
    }

    @AfterEach
    void tearDown() {
        s3.shutdown();
        dynamoDB.shutdown();
        sqs.shutdown();
    }

    @Test
    public void shouldCompactAllFilesInSinglePartition() throws Exception {
        // Given
        List<Partition> partitions = stateStore.getAllPartitions();
        FileInfoFactory fileInfoFactory = new FileInfoFactory(schema, partitions, Instant.now());
        FileInfo fileInfo1 = fileInfoFactory.rootFile("file1", 200L);
        FileInfo fileInfo2 = fileInfoFactory.rootFile("file2", 200L);
        FileInfo fileInfo3 = fileInfoFactory.rootFile("file3", 200L);
        FileInfo fileInfo4 = fileInfoFactory.rootFile("file4", 200L);
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

    private InstanceProperties createInstance() {
        InstanceProperties instanceProperties = createInstanceProperties();
        instanceProperties.set(COMPACTION_JOB_QUEUE_URL, sqs.createQueue(UUID.randomUUID().toString()).getQueueUrl());
        instanceProperties.set(FILE_SYSTEM, "");
        DynamoDBTableIndexCreator.create(dynamoDB, instanceProperties);
        new S3StateStoreCreator(instanceProperties, dynamoDB).create();
        DynamoDBCompactionJobStatusStoreCreator.create(instanceProperties, dynamoDB);

        s3.createBucket(instanceProperties.get(CONFIG_BUCKET));
        s3.createBucket(instanceProperties.get(DATA_BUCKET));
        instanceProperties.saveToS3(s3);

        return instanceProperties;
    }

    private TableProperties createTable(Schema schema) {
        TableProperties tableProperties = createTableProperties(schema, instanceProperties);
        tablePropertiesStore.save(tableProperties);
        return tableProperties;
    }

}
