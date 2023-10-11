/*
 * Copyright 2022-2023 Crown Copyright
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

import sleeper.compaction.job.CompactionJob;
import sleeper.compaction.job.CompactionJobSerDe;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;

import java.io.IOException;

import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.COMPACTION_JOB_QUEUE_URL;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.SPLITTING_COMPACTION_JOB_QUEUE_URL;

public class SendCompactionJobToSqs {
    private static final Logger LOGGER = LoggerFactory.getLogger(SendCompactionJobToSqs.class);

    private final InstanceProperties instanceProperties;
    private final AmazonSQS sqsClient;
    private final CompactionJobSerDe compactionJobSerDe;

    public SendCompactionJobToSqs(
            InstanceProperties instanceProperties,
            TablePropertiesProvider tablePropertiesProvider,
            AmazonSQS sqsClient) {
        this.instanceProperties = instanceProperties;
        this.sqsClient = sqsClient;
        this.compactionJobSerDe = new CompactionJobSerDe(tablePropertiesProvider);
    }

    public void send(CompactionJob compactionJob) throws IOException {
        if (compactionJob.isSplittingJob()) {
            sendToQueue(compactionJob, instanceProperties.get(SPLITTING_COMPACTION_JOB_QUEUE_URL));
        } else {
            sendToQueue(compactionJob, instanceProperties.get(COMPACTION_JOB_QUEUE_URL));
        }
    }

    private void sendToQueue(CompactionJob compactionJob, String queueUrl) throws IOException {
        String serialisedJobDefinition = compactionJobSerDe.serialiseToString(compactionJob);
        LOGGER.debug("Sending compaction job with id {} to SQS", compactionJob.getId());
        SendMessageRequest sendMessageRequest = new SendMessageRequest()
                .withQueueUrl(queueUrl)
                .withMessageBody(serialisedJobDefinition);
        SendMessageResult sendMessageResult = sqsClient.sendMessage(sendMessageRequest);
        LOGGER.debug("Result of sending message: {}", sendMessageResult);
    }
}
