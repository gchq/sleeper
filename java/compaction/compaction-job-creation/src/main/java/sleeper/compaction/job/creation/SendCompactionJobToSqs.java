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

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.compaction.core.job.CompactionJob;
import sleeper.compaction.core.job.CompactionJobSerDeNew;
import sleeper.core.properties.instance.InstanceProperties;

import java.io.IOException;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.COMPACTION_JOB_QUEUE_URL;

public class SendCompactionJobToSqs {
    private static final Logger LOGGER = LoggerFactory.getLogger(SendCompactionJobToSqs.class);

    private final InstanceProperties instanceProperties;
    private final AmazonSQS sqsClient;

    public SendCompactionJobToSqs(
            InstanceProperties instanceProperties,
            AmazonSQS sqsClient) {
        this.instanceProperties = instanceProperties;
        this.sqsClient = sqsClient;
    }

    public void send(CompactionJob compactionJob) throws IOException {
        String serialisedJobDefinition = new CompactionJobSerDeNew().toJsonPrettyPrint(compactionJob);
        LOGGER.debug("Sending compaction job with id {} to SQS", compactionJob.getId());
        SendMessageRequest sendMessageRequest = new SendMessageRequest()
                .withQueueUrl(instanceProperties.get(COMPACTION_JOB_QUEUE_URL))
                .withMessageBody(serialisedJobDefinition);
        SendMessageResult sendMessageResult = sqsClient.sendMessage(sendMessageRequest);
        LOGGER.debug("Result of sending message: {}", sendMessageResult);
    }
}
