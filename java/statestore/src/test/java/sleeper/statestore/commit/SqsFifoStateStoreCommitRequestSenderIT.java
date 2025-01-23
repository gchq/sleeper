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
package sleeper.statestore.commit;

import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.testutils.FixedTablePropertiesProvider;
import sleeper.core.statestore.commit.StateStoreCommitRequest;
import sleeper.core.statestore.commit.StateStoreCommitRequestSender;
import sleeper.core.statestore.commit.StateStoreCommitRequestSerDe;
import sleeper.core.statestore.transactionlog.PartitionTransaction;
import sleeper.core.statestore.transactionlog.StateStoreTransaction;
import sleeper.core.statestore.transactionlog.TransactionBodyStore;
import sleeper.core.statestore.transactionlog.transactions.InitialisePartitionsTransaction;
import sleeper.core.statestore.transactionlog.transactions.TransactionSerDeProvider;
import sleeper.core.statestore.transactionlog.transactions.TransactionType;
import sleeper.statestore.testutil.LocalStackTestBase;
import sleeper.statestore.transactionlog.S3TransactionBodyStore;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.STATESTORE_COMMITTER_QUEUE_URL;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;
import static sleeper.core.testutils.SupplierTestHelper.fixIds;
import static sleeper.core.testutils.SupplierTestHelper.fixTime;

public class SqsFifoStateStoreCommitRequestSenderIT extends LocalStackTestBase {

    private static final Instant DEFAULT_TRANSACTION_TIME = Instant.parse("2025-01-23T11:36:00Z");
    private static final String DEFAULT_TRANSACTION_ID = "test-transaction";

    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final TableProperties tableProperties = createTestTableProperties(instanceProperties, schemaWithKey("key"));
    private final String tableId = tableProperties.get(TABLE_ID);
    private final StateStoreCommitRequestSerDe serDe = new StateStoreCommitRequestSerDe(tableProperties);
    private Supplier<Instant> timeSupplier = fixTime(DEFAULT_TRANSACTION_TIME);
    private Supplier<String> idSupplier = fixIds(DEFAULT_TRANSACTION_ID);

    @BeforeEach
    void setUp() {
        s3Client.createBucket(instanceProperties.get(DATA_BUCKET));
        instanceProperties.set(STATESTORE_COMMITTER_QUEUE_URL, sqsClient.createQueue(new CreateQueueRequest()
                .withQueueName(UUID.randomUUID().toString() + ".fifo")
                .withAttributes(Map.of("FifoQueue", "true")))
                .getQueueUrl());
    }

    @Test
    void shouldSendStateStoreCommitToSQS() {
        // Given
        PartitionTransaction transaction = new InitialisePartitionsTransaction(
                new PartitionsBuilder(tableProperties.getSchema()).singlePartition("root").buildList());

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

        // When
        senderWithMaxTransactionBytes(10)
                .send(StateStoreCommitRequest.create(tableId, transaction));

        // Then
        String expectedKey = TransactionBodyStore.createObjectKey(tableId, DEFAULT_TRANSACTION_TIME, DEFAULT_TRANSACTION_ID);
        assertThat(receiveCommitRequests())
                .containsExactly(StateStoreCommitRequest.create(tableId, expectedKey, TransactionType.INITIALISE_PARTITIONS));
        assertThat(readTransaction(expectedKey, TransactionType.INITIALISE_PARTITIONS))
                .isEqualTo(transaction);
    }

    private StateStoreCommitRequestSender sender() {
        return senderWithMaxTransactionBytes(SqsFifoStateStoreCommitRequestSender.DEFAULT_MAX_TRANSACTION_BYTES);
    }

    private StateStoreCommitRequestSender senderWithMaxTransactionBytes(int maxBytes) {
        TransactionSerDeProvider serDeProvider = TransactionSerDeProvider.from(new FixedTablePropertiesProvider(tableProperties));
        return new SqsFifoStateStoreCommitRequestSender(instanceProperties,
                new S3TransactionBodyStore(instanceProperties, s3Client, serDeProvider),
                serDeProvider, sqsClient, maxBytes, timeSupplier, idSupplier);
    }

    private List<StateStoreCommitRequest> receiveCommitRequests() {
        return receiveCommitMessage().getMessages().stream()
                .map(this::readCommitRequest)
                .collect(Collectors.toList());
    }

    private ReceiveMessageResult receiveCommitMessage() {
        ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest()
                .withQueueUrl(instanceProperties.get(STATESTORE_COMMITTER_QUEUE_URL))
                .withMaxNumberOfMessages(10);
        return sqsClient.receiveMessage(receiveMessageRequest);
    }

    private StateStoreCommitRequest readCommitRequest(Message message) {
        return serDe.fromJson(message.getBody());
    }

    private StateStoreTransaction<?> readTransaction(String key, TransactionType transactionType) {
        return new S3TransactionBodyStore(instanceProperties, tableProperties, s3Client)
                .getBody(key, tableId, transactionType);
    }

}
