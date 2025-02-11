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
import sleeper.core.statestore.transactionlog.transaction.TransactionSerDeProvider;

import java.time.Instant;
import java.util.UUID;
import java.util.function.Supplier;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.STATESTORE_COMMITTER_QUEUE_URL;

/**
 * Sends state store updates to the state store commit FIFO SQS queue.
 */
public class SqsFifoStateStoreCommitRequestSender implements StateStoreCommitRequestSender {
    public static final Logger LOGGER = LoggerFactory.getLogger(SqsFifoStateStoreCommitRequestSender.class);

    // Max size of an SQS message is 256KiB. Leave space for request wrapper.
    // https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/quotas-messages.html
    public static final int DEFAULT_MAX_TRANSACTION_BYTES = 250 * 1024;

    private final InstanceProperties instanceProperties;
    private final S3StateStoreCommitRequestUploader requestUploader;
    private final AmazonSQS sqsClient;
    private final int maxTransactionBytes;

    public SqsFifoStateStoreCommitRequestSender(
            InstanceProperties instanceProperties, AmazonSQS sqsClient, AmazonS3 s3Client,
            TransactionSerDeProvider transactionSerDeProvider) {
        this(instanceProperties, sqsClient, s3Client, transactionSerDeProvider,
                DEFAULT_MAX_TRANSACTION_BYTES, Instant::now, () -> UUID.randomUUID().toString());
    }

    public SqsFifoStateStoreCommitRequestSender(
            InstanceProperties instanceProperties, AmazonSQS sqsClient, AmazonS3 s3Client,
            TransactionSerDeProvider transactionSerDeProvider,
            int maxTransactionBytes, Supplier<Instant> timeSupplier, Supplier<String> idSupplier) {
        this.instanceProperties = instanceProperties;
        this.requestUploader = new S3StateStoreCommitRequestUploader(instanceProperties, transactionSerDeProvider, s3Client, timeSupplier, idSupplier);
        this.sqsClient = sqsClient;
        this.maxTransactionBytes = maxTransactionBytes;
    }

    @Override
    public void send(StateStoreCommitRequest request) {
        LOGGER.debug("Sending asynchronous request to state store committer of type: {}", request.getTransactionType());
        sqsClient.sendMessage(new SendMessageRequest()
                .withQueueUrl(instanceProperties.get(STATESTORE_COMMITTER_QUEUE_URL))
                .withMessageBody(requestUploader.serialiseAndUploadIfTooBig(maxTransactionBytes, request))
                .withMessageGroupId(request.getTableId())
                .withMessageDeduplicationId(UUID.randomUUID().toString()));
        LOGGER.debug("Submitted asynchronous request of type {} via state store committer queue", request.getTransactionType());
    }

}
