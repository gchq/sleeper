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

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.statestore.commit.StateStoreCommitRequest;
import sleeper.core.statestore.commit.StateStoreCommitRequestSender;
import sleeper.core.statestore.commit.StateStoreCommitRequestSerDe;
import sleeper.core.statestore.transactionlog.TransactionBodyStore;
import sleeper.core.statestore.transactionlog.TransactionBodyStoreProviderByTableId;

import java.util.UUID;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.STATESTORE_COMMITTER_QUEUE_URL;

/**
 * Sends state store updates to the state store commit FIFO SQS queue.
 */
public class SqsFifoStateStoreCommitRequestSender implements StateStoreCommitRequestSender {
    public static final Logger LOGGER = LoggerFactory.getLogger(SqsFifoStateStoreCommitRequestSender.class);
    public static final int DEFAULT_MAX_LENGTH = 262144;

    private final InstanceProperties instanceProperties;
    private final TransactionBodyStoreProviderByTableId transactionBodyStore;
    private final StateStoreCommitRequestSerDe serDe;
    private final AmazonSQS sqsClient;
    private final int maxLength;

    public SqsFifoStateStoreCommitRequestSender(
            InstanceProperties instanceProperties, TransactionBodyStoreProviderByTableId transactionBodyStore,
            StateStoreCommitRequestSerDe serDe, AmazonSQS sqsClient, int maxLength) {
        this.instanceProperties = instanceProperties;
        this.transactionBodyStore = transactionBodyStore;
        this.serDe = serDe;
        this.sqsClient = sqsClient;
        this.maxLength = maxLength;
    }

    @Override
    public void send(StateStoreCommitRequest request) {
        LOGGER.debug("Sending asynchronous request to state store committer: {}", request);
        sqsClient.sendMessage(new SendMessageRequest()
                .withQueueUrl(instanceProperties.get(STATESTORE_COMMITTER_QUEUE_URL))
                .withMessageBody(serialiseAndUploadIfTooBig(request))
                .withMessageGroupId(request.getTableId())
                .withMessageDeduplicationId(UUID.randomUUID().toString()));
        LOGGER.debug("Submitted asynchronous request of type {} via state store committer queue", request.getTransactionType());
    }

    private String serialiseAndUploadIfTooBig(StateStoreCommitRequest request) {
        String json = serDe.toJson(request);
        if (json.length() < maxLength) {
            return json;
        } else {
            String key = TransactionBodyStore.createObjectKey(request.getTableId());
            transactionBodyStore.getTransactionBodyStore(request.getTableId()).store(key, request.getTransactionIfHeld().orElseThrow());
            return serDe.toJson(StateStoreCommitRequest.create(request.getTableId(), key, request.getTransactionType()));
        }
    }

}
