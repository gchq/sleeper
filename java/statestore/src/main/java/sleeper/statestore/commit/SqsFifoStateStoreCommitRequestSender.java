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
package sleeper.statestore.commit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.statestore.commit.StateStoreCommitRequest;
import sleeper.core.statestore.commit.StateStoreCommitRequestSender;
import sleeper.core.statestore.transactionlog.log.TransactionBodyStore;
import sleeper.core.statestore.transactionlog.transaction.TransactionSerDeProvider;
import sleeper.statestore.transactionlog.S3TransactionBodyStore;

import java.util.UUID;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.STATESTORE_COMMITTER_QUEUE_URL;

/**
 * Sends state store updates to the state store commit FIFO SQS queue.
 */
public class SqsFifoStateStoreCommitRequestSender implements StateStoreCommitRequestSender {
    public static final Logger LOGGER = LoggerFactory.getLogger(SqsFifoStateStoreCommitRequestSender.class);

    private final InstanceProperties instanceProperties;
    private final StateStoreCommitRequestUploader requestUploader;
    private final SqsClient sqsClient;

    public SqsFifoStateStoreCommitRequestSender(
            InstanceProperties instanceProperties, SqsClient sqsClient, S3Client s3Client,
            TransactionSerDeProvider transactionSerDeProvider) {
        this(instanceProperties, new S3TransactionBodyStore(instanceProperties, s3Client, transactionSerDeProvider), transactionSerDeProvider, sqsClient);
    }

    public SqsFifoStateStoreCommitRequestSender(
            InstanceProperties instanceProperties,
            TransactionBodyStore transactionBodyStore,
            TransactionSerDeProvider transactionSerDeProvider,
            SqsClient sqsClient) {
        this.instanceProperties = instanceProperties;
        this.requestUploader = new StateStoreCommitRequestUploader(transactionBodyStore, transactionSerDeProvider);
        this.sqsClient = sqsClient;
    }

    @Override
    public void send(StateStoreCommitRequest request) {
        LOGGER.debug("Sending asynchronous request to state store committer of type: {}", request.getTransactionType());
        sqsClient.sendMessage(SendMessageRequest.builder()
                .queueUrl(instanceProperties.get(STATESTORE_COMMITTER_QUEUE_URL))
                .messageBody(requestUploader.serialiseAndUploadIfTooBig(request))
                .messageGroupId(request.getTableId())
                .messageDeduplicationId(UUID.randomUUID().toString())
                .build());
        LOGGER.debug("Submitted asynchronous request of type {} via state store committer queue", request.getTransactionType());
    }

}
