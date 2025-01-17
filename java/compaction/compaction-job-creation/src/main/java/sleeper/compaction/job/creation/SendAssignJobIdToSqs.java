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
package sleeper.compaction.job.creation;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.compaction.core.job.creation.AssignJobIdQueueSender;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TablePropertiesProvider;
import sleeper.core.statestore.commit.StateStoreCommitRequestByTransaction;
import sleeper.core.statestore.commit.StateStoreCommitRequestByTransactionSerDe;
import sleeper.core.statestore.commit.StateStoreCommitRequestInS3Uploader;

import java.util.UUID;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.STATESTORE_COMMITTER_QUEUE_URL;

public class SendAssignJobIdToSqs implements AssignJobIdQueueSender {
    public static final Logger LOGGER = LoggerFactory.getLogger(SendAssignJobIdToSqs.class);

    private final InstanceProperties instanceProperties;
    private final AmazonSQS sqsClient;
    private final StateStoreCommitRequestInS3Uploader s3Uploader;
    private final StateStoreCommitRequestByTransactionSerDe serDe;

    public SendAssignJobIdToSqs(InstanceProperties instanceProperties, TablePropertiesProvider tablePropertiesProvider, AmazonSQS sqsClient, AmazonS3 s3Client) {
        this.instanceProperties = instanceProperties;
        this.sqsClient = sqsClient;
        this.s3Uploader = new StateStoreCommitRequestInS3Uploader(instanceProperties, s3Client::putObject);
        this.serDe = new StateStoreCommitRequestByTransactionSerDe(tablePropertiesProvider);
    }

    @Override
    public void send(StateStoreCommitRequestByTransaction request) {
        LOGGER.debug("Sending asynchronous request to state store committer: {}", request);
        String json = s3Uploader.uploadAndWrapIfTooBig(request.getTableId(), serDe.toJson(request));
        sqsClient.sendMessage(new SendMessageRequest()
                .withQueueUrl(instanceProperties.get(STATESTORE_COMMITTER_QUEUE_URL))
                .withMessageBody(json)
                .withMessageGroupId(request.getTableId())
                .withMessageDeduplicationId(UUID.randomUUID().toString()));
        LOGGER.debug("Submitted asynchronous request to assign compaction input files via state store committer queue");
    }
}
