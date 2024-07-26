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

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.CreateQueueResult;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.CommonTestConstants;
import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.statestore.StateStoreException;
import sleeper.ingest.job.commit.IngestAddFilesCommitRequest;
import sleeper.ingest.job.commit.IngestAddFilesCommitRequestSerDe;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.configuration.properties.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.STATESTORE_COMMITTER_QUEUE_URL;
import static sleeper.configuration.properties.instance.IngestProperty.INGEST_ADD_FILES_COMMIT_BATCH_SIZE;
import static sleeper.configuration.properties.table.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.configuration.properties.table.TableProperty.TABLE_ID;
import static sleeper.configuration.testutils.LocalStackAwsV1ClientHelper.buildAwsV1Client;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;

@Testcontainers
public class AddFilesToStateStoreIT {
    @Container
    public static LocalStackContainer localStackContainer = new LocalStackContainer(DockerImageName.parse(CommonTestConstants.LOCALSTACK_DOCKER_IMAGE))
            .withServices(LocalStackContainer.Service.SQS);
    private final AmazonSQS sqs = buildAwsV1Client(localStackContainer, LocalStackContainer.Service.SQS, AmazonSQSClient.builder());
    private final Schema schema = schemaWithKey("key");
    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final TableProperties tableProperties = createTestTableProperties(instanceProperties, schema);

    private final PartitionTree partitionTree = new PartitionsBuilder(schema)
            .singlePartition("root")
            .buildTree();
    private final FileReferenceFactory factory = FileReferenceFactory.from(partitionTree);
    private final AddFilesToStateStore addFilesToStateStore = AddFilesToStateStore.bySqs(sqs, instanceProperties,
            request -> request.tableId(tableProperties.get(TABLE_ID)));

    @BeforeEach
    void setUp() {
        instanceProperties.set(STATESTORE_COMMITTER_QUEUE_URL, createFifoQueueGetUrl());
    }

    @Test
    void shouldSplitCommitMessagesIfFilesExceedBatchSize() throws StateStoreException {
        // Given
        instanceProperties.setNumber(INGEST_ADD_FILES_COMMIT_BATCH_SIZE, 1000);
        List<FileReference> files = IntStream.range(0, 1001)
                .mapToObj(i -> factory.rootFile("file" + i + ".parquet", 100L))
                .collect(Collectors.toList());

        // When
        addFilesToStateStore.addFiles(files);

        // Then
        assertThat(getCommitRequestsFromQueue())
                .containsExactly(
                        IngestAddFilesCommitRequest.builder()
                                .fileReferences(files.subList(0, 1000))
                                .tableId(tableProperties.get(TABLE_ID))
                                .build(),
                        IngestAddFilesCommitRequest.builder()
                                .fileReferences(files.subList(1000, 1001))
                                .tableId(tableProperties.get(TABLE_ID))
                                .build());
    }

    private String createFifoQueueGetUrl() {
        CreateQueueResult result = sqs.createQueue(new CreateQueueRequest()
                .withQueueName(UUID.randomUUID().toString() + ".fifo")
                .withAttributes(Map.of("FifoQueue", "true")));
        return result.getQueueUrl();
    }

    private List<IngestAddFilesCommitRequest> getCommitRequestsFromQueue() {
        String commitQueueUrl = instanceProperties.get(STATESTORE_COMMITTER_QUEUE_URL);
        ReceiveMessageResult result = sqs.receiveMessage(new ReceiveMessageRequest(commitQueueUrl)
                .withMaxNumberOfMessages(10));
        IngestAddFilesCommitRequestSerDe serDe = new IngestAddFilesCommitRequestSerDe();
        return result.getMessages().stream()
                .map(Message::getBody)
                .map(serDe::fromJson)
                .collect(Collectors.toList());
    }
}
