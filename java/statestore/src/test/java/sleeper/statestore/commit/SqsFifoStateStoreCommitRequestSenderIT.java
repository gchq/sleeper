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

import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.testutils.FixedTablePropertiesProvider;
import sleeper.core.statestore.commit.StateStoreCommitRequest;
import sleeper.core.statestore.commit.StateStoreCommitRequestSender;
import sleeper.core.statestore.commit.StateStoreCommitRequestSerDe;
import sleeper.core.statestore.transactionlog.PartitionTransaction;
import sleeper.core.statestore.transactionlog.transactions.InitialisePartitionsTransaction;
import sleeper.statestore.testutil.LocalStackTestBase;
import sleeper.statestore.transactionlog.S3TransactionBodyStore;

import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.STATESTORE_COMMITTER_QUEUE_URL;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;

public class SqsFifoStateStoreCommitRequestSenderIT extends LocalStackTestBase {

    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final TableProperties tableProperties = createTestTableProperties(instanceProperties, schemaWithKey("key"));
    private final String tableId = tableProperties.get(TABLE_ID);
    private final StateStoreCommitRequestSerDe serDe = new StateStoreCommitRequestSerDe(tableProperties);

    @Test
    @Disabled
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

    private StateStoreCommitRequestSender sender() {
        return new SqsFifoStateStoreCommitRequestSender(instanceProperties,
                S3TransactionBodyStore.createProvider(instanceProperties, s3Client)
                        .byTableId(new FixedTablePropertiesProvider(tableProperties)),
                new StateStoreCommitRequestSerDe(tableProperties), sqsClient,
                SqsFifoStateStoreCommitRequestSender.DEFAULT_MAX_LENGTH);
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

}
