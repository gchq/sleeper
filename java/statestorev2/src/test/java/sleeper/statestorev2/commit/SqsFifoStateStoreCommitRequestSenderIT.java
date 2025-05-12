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
package sleeper.statestorev2.commit;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;

import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.statestore.AllReferencesToAFile;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.statestore.commit.StateStoreCommitRequest;
import sleeper.core.statestore.commit.StateStoreCommitRequestSender;
import sleeper.core.statestore.commit.StateStoreCommitRequestSerDe;
import sleeper.core.statestore.testutils.InMemoryTransactionBodyStore;
import sleeper.core.statestore.transactionlog.transaction.PartitionTransaction;
import sleeper.core.statestore.transactionlog.transaction.StateStoreTransaction;
import sleeper.core.statestore.transactionlog.transaction.TransactionSerDeProvider;
import sleeper.core.statestore.transactionlog.transaction.TransactionType;
import sleeper.core.statestore.transactionlog.transaction.impl.AddFilesTransaction;
import sleeper.core.statestore.transactionlog.transaction.impl.InitialisePartitionsTransaction;
import sleeper.localstack.test.LocalStackTestBase;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.STATESTORE_COMMITTER_QUEUE_URL;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.schema.SchemaTestHelper.createSchemaWithKey;

public class SqsFifoStateStoreCommitRequestSenderIT extends LocalStackTestBase {

    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final TableProperties tableProperties = createTestTableProperties(instanceProperties, createSchemaWithKey("key"));
    private final String tableId = tableProperties.get(TABLE_ID);
    private final InMemoryTransactionBodyStore bodyStore = new InMemoryTransactionBodyStore();
    private final StateStoreCommitRequestSerDe serDe = new StateStoreCommitRequestSerDe(tableProperties);

    @BeforeEach
    void setUp() {
        s3Client.createBucket(instanceProperties.get(DATA_BUCKET));
        instanceProperties.set(STATESTORE_COMMITTER_QUEUE_URL, sqsClientV2.createQueue(CreateQueueRequest.builder()
                .queueName(UUID.randomUUID().toString() + ".fifo")
                .attributes(Map.of(QueueAttributeName.FIFO_QUEUE, "true")).build())
                .queueUrl());
    }

    @Test
    void shouldSendCommitToSqs() {
        // Given
        PartitionTransaction transaction = new InitialisePartitionsTransaction(
                new PartitionsBuilder(tableProperties.getSchema()).singlePartition("root").buildList());
        bodyStore.setStoreTransactions(false);

        // When
        sender().send(StateStoreCommitRequest.create(tableId, transaction));

        // Then
        assertThat(receiveCommitRequests())
                .containsExactly(StateStoreCommitRequest.create(tableId, transaction));
    }

    @Test
    void shouldStoreTransactionInS3WhenTooBigForSqsMessage() {
        // Given
        PartitionTransaction transaction = new InitialisePartitionsTransaction(
                new PartitionsBuilder(tableProperties.getSchema()).singlePartition("root").buildList());
        bodyStore.setStoreTransactionsWithObjectKeys(List.of("test/object"));

        // When
        sender().send(StateStoreCommitRequest.create(tableId, transaction));

        // Then
        assertThat(receiveCommitRequests())
                .containsExactly(StateStoreCommitRequest.create(tableId, "test/object", TransactionType.INITIALISE_PARTITIONS));
        assertThat(readTransaction("test/object", TransactionType.INITIALISE_PARTITIONS))
                .isEqualTo(transaction);
    }

    @Test
    void shouldSendCommitWithTooManyFilesForSqs() throws Exception {
        // Given
        instanceProperties.set(DATA_BUCKET, "test-data-bucket-" + UUID.randomUUID().toString());
        s3Client.createBucket(instanceProperties.get(DATA_BUCKET));
        FileReferenceFactory factory = FileReferenceFactory.forSinglePartition("root", tableProperties);
        List<FileReference> fileReferences = IntStream.range(0, 1350)
                .mapToObj(i -> factory.rootFile("s3a://test-data-bucket/test-table/data/partition_root/test-file" + i + ".parquet", 100L))
                .collect(Collectors.toList());
        AddFilesTransaction transaction = AddFilesTransaction.builder()
                .jobId("test-job").taskId("test-task").jobRunId("test-run").writtenTime(Instant.parse("2025-01-23T15:20:00Z"))
                .files(AllReferencesToAFile.newFilesWithReferences(fileReferences))
                .build();
        StateStoreCommitRequest request = StateStoreCommitRequest.create(tableProperties.get(TABLE_ID), transaction);
        bodyStore.setStoreTransactionsWithObjectKeys(List.of("test/object"));

        // When
        sender().send(request);

        // Then
        assertThat(receiveCommitRequests())
                .containsExactly(StateStoreCommitRequest.create(tableId, "test/object", TransactionType.ADD_FILES));
        assertThat(readTransaction("test/object", TransactionType.ADD_FILES))
                .isEqualTo(transaction);
    }

    private StateStoreCommitRequestSender sender() {
        return new SqsFifoStateStoreCommitRequestSender(
                instanceProperties, bodyStore, TransactionSerDeProvider.forOneTable(tableProperties), sqsClientV2);
    }

    private List<StateStoreCommitRequest> receiveCommitRequests() {
        return receiveCommitMessage().messages().stream()
                .map(this::readCommitRequest)
                .collect(Collectors.toList());
    }

    private ReceiveMessageResponse receiveCommitMessage() {
        ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder()
                .queueUrl(instanceProperties.get(STATESTORE_COMMITTER_QUEUE_URL))
                .maxNumberOfMessages(10).build();
        return sqsClientV2.receiveMessage(receiveMessageRequest);
    }

    private StateStoreCommitRequest readCommitRequest(Message message) {
        return serDe.fromJson(message.body());
    }

    private StateStoreTransaction<?> readTransaction(String key, TransactionType transactionType) {
        return bodyStore.getBody(key, tableId, transactionType);
    }

}
