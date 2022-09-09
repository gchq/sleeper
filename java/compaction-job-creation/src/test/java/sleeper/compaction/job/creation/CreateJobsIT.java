/*
 * Copyright 2022 Crown Copyright
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
import org.junit.ClassRule;
import org.junit.Test;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.utility.DockerImageName;
import sleeper.compaction.job.CompactionJob;
import sleeper.compaction.job.CompactionJobSerDe;
import sleeper.configuration.jars.ObjectFactory;
import sleeper.configuration.jars.ObjectFactoryException;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.core.CommonTestConstants;
import sleeper.core.partition.Partition;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.LongType;
import sleeper.statestore.FileInfo;
import sleeper.statestore.FileInfoFactory;
import sleeper.statestore.StateStore;
import sleeper.statestore.StateStoreException;
import sleeper.table.job.TableCreator;
import sleeper.table.job.TableLister;
import sleeper.table.util.StateStoreProvider;

import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.COMPACTION_JOB_QUEUE_URL;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.FILE_SYSTEM;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.ID;
import static sleeper.configuration.properties.table.TableProperty.COMPACTION_FILES_BATCH_SIZE;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;

public class CreateJobsIT {

    @ClassRule
    public static LocalStackContainer localStackContainer = new LocalStackContainer(DockerImageName.parse(CommonTestConstants.LOCALSTACK_DOCKER_IMAGE)).withServices(
            LocalStackContainer.Service.S3, LocalStackContainer.Service.SQS, LocalStackContainer.Service.DYNAMODB, LocalStackContainer.Service.IAM
    );

    private AmazonS3 createS3Client() {
        return AmazonS3ClientBuilder.standard()
                .withEndpointConfiguration(localStackContainer.getEndpointConfiguration(LocalStackContainer.Service.S3))
                .withCredentials(localStackContainer.getDefaultCredentialsProvider())
                .build();
    }

    private AmazonSQS createSQSClient() {
        return AmazonSQSClientBuilder.standard()
                .withEndpointConfiguration(localStackContainer.getEndpointConfiguration(LocalStackContainer.Service.SQS))
                .withCredentials(localStackContainer.getDefaultCredentialsProvider())
                .build();
    }

    private AmazonDynamoDB createDynamoClient() {
        return AmazonDynamoDBClientBuilder.standard()
                .withCredentials(localStackContainer.getDefaultCredentialsProvider())
                .withEndpointConfiguration(localStackContainer.getEndpointConfiguration(LocalStackContainer.Service.DYNAMODB))
                .build();
    }

    private Schema createSchema() {
        return Schema.builder()
                .rowKeyFields(new Field("key", new LongType()))
                .valueFields(
                        new Field("value1", new LongType()),
                        new Field("value2", new LongType()))
                .build();
    }

    private InstanceProperties createProperties(AmazonS3 s3) {
        AmazonSQS sqs = createSQSClient();
        String queue = UUID.randomUUID().toString();

        String queueUrl = sqs.createQueue(queue).getQueueUrl();

        sqs.shutdown();

        InstanceProperties instanceProperties = new InstanceProperties();
        instanceProperties.set(ID, UUID.randomUUID().toString());
        instanceProperties.set(CONFIG_BUCKET, UUID.randomUUID().toString());
        instanceProperties.set(COMPACTION_JOB_QUEUE_URL, queueUrl);
        instanceProperties.set(FILE_SYSTEM, "");

        s3.createBucket(instanceProperties.get(CONFIG_BUCKET));

        return instanceProperties;
    }

    private TableProperties createTable(AmazonS3 s3, AmazonDynamoDB dynamoDB, InstanceProperties instanceProperties, String tableName, Schema schema) throws IOException {
        TableProperties tableProperties = new TableProperties(instanceProperties);
        tableProperties.set(TABLE_NAME, tableName);
        tableProperties.setSchema(schema);
        tableProperties.set(COMPACTION_FILES_BATCH_SIZE, "5");
        TableCreator tableCreator = new TableCreator(s3, dynamoDB, instanceProperties);
        tableCreator.createTable(tableProperties);

        tableProperties.loadFromS3(s3, tableName);
        return tableProperties;
    }

    @Test
    public void shouldCreateJobs() throws StateStoreException, IOException, IllegalAccessException, InstantiationException, ClassNotFoundException, ObjectFactoryException {
        // Given
        AmazonS3 s3 = createS3Client();
        AmazonDynamoDB dynamoDB = createDynamoClient();
        AmazonSQS sqsClient = createSQSClient();
        String tableName = UUID.randomUUID().toString();
        InstanceProperties instanceProperties = createProperties(s3);
        Schema schema = createSchema();
        TableProperties tableProperties = createTable(s3, dynamoDB, instanceProperties, tableName, schema);
        TablePropertiesProvider tablePropertiesProvider = new TablePropertiesProvider(s3, instanceProperties);
        TableLister tableLister = new TableLister(s3, instanceProperties);
        StateStoreProvider stateStoreProvider = new StateStoreProvider(dynamoDB, new InstanceProperties());
        StateStore stateStore = stateStoreProvider.getStateStore(tableProperties);
        stateStore.initialise();
        List<Partition> partitions = stateStore.getAllPartitions();
        Partition partition = partitions.get(0);
        FileInfoFactory fileInfoFactory = new FileInfoFactory(schema, partitions, Instant.now());
        FileInfo fileInfo1 = fileInfoFactory.leafFile("file1", 200L, 12L, 34L);
        FileInfo fileInfo2 = fileInfoFactory.leafFile("file2", 200L, 56L, 78L);
        FileInfo fileInfo3 = fileInfoFactory.leafFile("file3", 200L, 90L, 123L);
        FileInfo fileInfo4 = fileInfoFactory.leafFile("file4", 200L, 456L, 789L);
        stateStore.addFiles(Arrays.asList(fileInfo1, fileInfo2, fileInfo3, fileInfo4));
        CreateJobs createJobs = new CreateJobs(new ObjectFactory(instanceProperties, s3, null), instanceProperties, tablePropertiesProvider, stateStoreProvider, dynamoDB, sqsClient, tableLister);

        // When
        createJobs.createJobs();

        // Then
        assertThat(stateStore.getActiveFilesWithNoJobId()).isEmpty();
        List<FileInfo> activeFiles = stateStore.getActiveFiles();
        Set<String> jobIds = new HashSet<>();
        for (int i = 0; i < 4; i++) {
            String jobId = activeFiles.get(i).getJobId();
            assertThat(jobId).isNotNull();
            jobIds.add(jobId);
        }
        assertThat(jobIds).hasSize(1);
        ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest()
                .withQueueUrl(instanceProperties.get(COMPACTION_JOB_QUEUE_URL))
                .withMaxNumberOfMessages(10);
        ReceiveMessageResult receiveMessageResult = sqsClient.receiveMessage(receiveMessageRequest);
        assertThat(receiveMessageResult.getMessages()).hasSize(1);
        Message message = receiveMessageResult.getMessages().get(0);
        CompactionJobSerDe compactionJobSerDe = new CompactionJobSerDe(tablePropertiesProvider);
        CompactionJob compactionJob = compactionJobSerDe.deserialiseFromString(message.getBody());
        assertThat(compactionJob.getInputFiles()).containsExactlyInAnyOrder("file1", "file2", "file3", "file4");
        assertThat(compactionJob.getPartitionId()).isEqualTo(partition.getId());
        assertThat(compactionJob.isSplittingJob()).isFalse();
    }
}
