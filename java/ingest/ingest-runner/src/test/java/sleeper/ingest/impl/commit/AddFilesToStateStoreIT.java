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
package sleeper.ingest.impl.commit;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.CreateQueueResult;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import sleeper.core.CommonTestConstants;
import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.statestore.commit.StateStoreCommitRequestInS3;
import sleeper.core.statestore.commit.StateStoreCommitRequestInS3SerDe;
import sleeper.ingest.core.job.commit.IngestAddFilesCommitRequest;
import sleeper.ingest.core.job.commit.IngestAddFilesCommitRequestSerDe;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.configuration.testutils.LocalStackAwsV1ClientHelper.buildAwsV1Client;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.STATESTORE_COMMITTER_QUEUE_URL;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;

@Testcontainers
public class AddFilesToStateStoreIT {
    @Container
    public static LocalStackContainer localStackContainer = new LocalStackContainer(DockerImageName.parse(CommonTestConstants.LOCALSTACK_DOCKER_IMAGE))
            .withServices(LocalStackContainer.Service.S3, LocalStackContainer.Service.SQS);
    private final AmazonS3 s3 = buildAwsV1Client(localStackContainer, LocalStackContainer.Service.S3, AmazonS3ClientBuilder.standard());
    private final AmazonSQS sqs = buildAwsV1Client(localStackContainer, LocalStackContainer.Service.SQS, AmazonSQSClientBuilder.standard());
    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final Schema schema = schemaWithKey("key");
    private final PartitionTree partitionTree = new PartitionsBuilder(schema).singlePartition("root").buildTree();
    private final FileReferenceFactory factory = FileReferenceFactory.from(partitionTree);
    private final TableProperties tableProperties = createTestTableProperties(instanceProperties, schemaWithKey("key"));

    @BeforeEach
    void setUp() {
        s3.createBucket(instanceProperties.get(DATA_BUCKET));
        instanceProperties.set(STATESTORE_COMMITTER_QUEUE_URL, createFifoQueueGetUrl());
    }

    @Test
    void shouldSendCommitRequestToSqsWhenRequestIsSmallEnough() throws Exception {
        // Given
        FileReference file1 = factory.rootFile("test.parquet", 123L);

        // When
        bySqs().addFiles(List.of(file1));

        // Then
        assertThat(receiveAddFilesCommitMessages())
                .containsExactly(
                        IngestAddFilesCommitRequest.builder()
                                .fileReferences(List.of(file1))
                                .tableId(tableProperties.get(TABLE_ID))
                                .build());
    }

    @Test
    void shouldUploadCommitRequestToS3WhenRequestIsTooLarge() throws Exception {
        // Given
        List<FileReference> fileReferences = IntStream.range(0, 1350)
                .mapToObj(i -> factory.rootFile("s3a://test-data-bucket/test-table/data/partition_root/test-file" + i + ".parquet", 100L))
                .collect(Collectors.toList());
        Supplier<String> s3FileNameSupplier = () -> "test-add-files-commit";

        // When
        bySqs(s3FileNameSupplier).addFiles(fileReferences);

        // Then
        String expectedS3Key = StateStoreCommitRequestInS3.createFileS3Key(
                tableProperties.get(TABLE_ID), "test-add-files-commit");
        assertThat(receiveCommitRequestStoredInS3Messages())
                .containsExactly(new StateStoreCommitRequestInS3(expectedS3Key));
        assertThat(readAddFilesCommitRequestFromDataBucket(expectedS3Key))
                .isEqualTo(IngestAddFilesCommitRequest.builder()
                        .fileReferences(fileReferences)
                        .tableId(tableProperties.get(TABLE_ID))
                        .build());
    }

    private String createFifoQueueGetUrl() {
        CreateQueueResult result = sqs.createQueue(new CreateQueueRequest()
                .withQueueName(UUID.randomUUID().toString() + ".fifo")
                .withAttributes(Map.of("FifoQueue", "true")));
        return result.getQueueUrl();
    }

    private AddFilesToStateStore bySqs() {
        return bySqs(() -> UUID.randomUUID().toString());
    }

    private AddFilesToStateStore bySqs(Supplier<String> filenameSupplier) {
        return AddFilesToStateStore.bySqs(sqs, s3, instanceProperties, filenameSupplier,
                request -> request.tableId(tableProperties.get(TABLE_ID)));
    }

    private List<StateStoreCommitRequestInS3> receiveCommitRequestStoredInS3Messages() {
        return receiveCommitMessages().stream()
                .map(message -> new StateStoreCommitRequestInS3SerDe().fromJson(message.getBody()))
                .collect(Collectors.toList());
    }

    private List<IngestAddFilesCommitRequest> receiveAddFilesCommitMessages() {
        return receiveCommitMessages().stream()
                .map(message -> new IngestAddFilesCommitRequestSerDe().fromJson(message.getBody()))
                .collect(Collectors.toList());
    }

    private List<Message> receiveCommitMessages() {
        ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest()
                .withQueueUrl(instanceProperties.get(STATESTORE_COMMITTER_QUEUE_URL))
                .withMaxNumberOfMessages(10);
        return sqs.receiveMessage(receiveMessageRequest).getMessages();
    }

    private IngestAddFilesCommitRequest readAddFilesCommitRequestFromDataBucket(String s3Key) {
        String requestJson = s3.getObjectAsString(instanceProperties.get(DATA_BUCKET), s3Key);
        return new IngestAddFilesCommitRequestSerDe().fromJson(requestJson);
    }
}
