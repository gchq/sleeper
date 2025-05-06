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

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.statestore.commit.StateStoreCommitRequest;
import sleeper.core.statestore.commit.StateStoreCommitRequestSender;
import sleeper.core.statestore.transactionlog.log.TransactionBodyStore;
import sleeper.core.statestore.transactionlog.transaction.TransactionSerDeProvider;
import sleeper.statestorev2.transactionlog.S3TransactionBodyStore;

import java.util.UUID;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.STATESTORE_COMMITTER_QUEUE_URL;

/**
 * Sends state store updates to the state store commit FIFO SQS queue.
 */
public class SqsFifoStateStoreCommitRequestSender implements StateStoreCommitRequestSender {
    public static final Logger LOGGER = LoggerFactory.getLogger(SqsFifoStateStoreCommitRequestSender.class);

    private final InstanceProperties instanceProperties;
    private final StateStoreCommitRequestUploader requestUploader;
    private final AmazonSQS sqsClient;

    public SqsFifoStateStoreCommitRequestSender(
            InstanceProperties instanceProperties, AmazonSQS sqsClient, AmazonS3 s3Client,
            TransactionSerDeProvider transactionSerDeProvider) {
        this(instanceProperties, new S3TransactionBodyStore(instanceProperties, s3Client, transactionSerDeProvider), transactionSerDeProvider, sqsClient);
    }

    public SqsFifoStateStoreCommitRequestSender(
            InstanceProperties instanceProperties,
            TransactionBodyStore transactionBodyStore,
            TransactionSerDeProvider transactionSerDeProvider,
            AmazonSQS sqsClient) {
        this.instanceProperties = instanceProperties;
        this.requestUploader = new StateStoreCommitRequestUploader(transactionBodyStore, transactionSerDeProvider);
        this.sqsClient = sqsClient;
    }

    @Override
    public void send(StateStoreCommitRequest request) {
        LOGGER.debug("Sending asynchronous request to state store committer of type: {}", request.getTransactionType());
        sqsClient.sendMessage(new SendMessageRequest()
                .withQueueUrl(instanceProperties.get(STATESTORE_COMMITTER_QUEUE_URL))
                .withMessageBody(requestUploader.serialiseAndUploadIfTooBig(request))
                .withMessageGroupId(request.getTableId())
                .withMessageDeduplicationId(UUID.randomUUID().toString()));
        LOGGER.debug("Submitted asynchronous request of type {} via state store committer queue", request.getTransactionType());
    }

}
