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

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.statestore.commit.StateStoreCommitRequest;
import sleeper.core.statestore.commit.StateStoreCommitRequestSender;
import sleeper.core.statestore.commit.StateStoreCommitRequestSerDe;
import sleeper.core.statestore.transactionlog.StateStoreTransaction;
import sleeper.core.statestore.transactionlog.TransactionBodyStore;
import sleeper.core.statestore.transactionlog.transactions.TransactionSerDe;
import sleeper.core.statestore.transactionlog.transactions.TransactionSerDeProvider;
import sleeper.statestore.transactionlog.S3TransactionBodyStore;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Supplier;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.STATESTORE_COMMITTER_QUEUE_URL;

/**
 * Sends state store updates to the state store commit FIFO SQS queue.
 */
public class SqsFifoStateStoreCommitRequestSender implements StateStoreCommitRequestSender {
    public static final Logger LOGGER = LoggerFactory.getLogger(SqsFifoStateStoreCommitRequestSender.class);
    public static final int DEFAULT_MAX_TRANSACTION_BYTES = 262144;

    private final InstanceProperties instanceProperties;
    private final S3TransactionBodyStore transactionBodyStore;
    private final TransactionSerDeProvider transactionSerDeProvider;
    private final StateStoreCommitRequestSerDe requestSerDe;
    private final AmazonSQS sqsClient;
    private final int maxTransactionBytes;
    private final Supplier<Instant> timeSupplier;
    private final Supplier<String> idSupplier;

    public SqsFifoStateStoreCommitRequestSender(
            InstanceProperties instanceProperties, AmazonSQS sqsClient, AmazonS3 s3Client,
            TransactionSerDeProvider transactionSerDeProvider,
            int maxTransactionBytes, Supplier<Instant> timeSupplier, Supplier<String> idSupplier) {
        this.instanceProperties = instanceProperties;
        this.transactionBodyStore = new S3TransactionBodyStore(instanceProperties, s3Client, transactionSerDeProvider);
        this.transactionSerDeProvider = transactionSerDeProvider;
        this.requestSerDe = new StateStoreCommitRequestSerDe(transactionSerDeProvider);
        this.sqsClient = sqsClient;
        this.maxTransactionBytes = maxTransactionBytes;
        this.timeSupplier = timeSupplier;
        this.idSupplier = idSupplier;
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
        return requestSerDe.toJson(request.getTransactionIfHeld()
                .flatMap(transaction -> uploadTransactionIfTooBig(request, transaction))
                .orElse(request));
    }

    private Optional<StateStoreCommitRequest> uploadTransactionIfTooBig(StateStoreCommitRequest request, StateStoreTransaction<?> transaction) {
        TransactionSerDe transactionSerDe = transactionSerDeProvider.getByTableId(request.getTableId());
        String json = transactionSerDe.toJson(transaction);
        if (json.length() < maxTransactionBytes) {
            return Optional.empty();
        } else {
            String key = TransactionBodyStore.createObjectKey(request.getTableId(), timeSupplier.get(), idSupplier.get());
            transactionBodyStore.store(key, json);
            return Optional.of(StateStoreCommitRequest.create(request.getTableId(), key, request.getTransactionType()));
        }
    }

}
