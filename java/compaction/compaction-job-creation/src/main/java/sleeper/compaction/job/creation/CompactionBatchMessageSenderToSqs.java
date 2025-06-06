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
package sleeper.compaction.job.creation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

import sleeper.compaction.core.job.creation.CreateCompactionJobs;
import sleeper.compaction.core.job.dispatch.CompactionJobDispatchRequest;
import sleeper.compaction.core.job.dispatch.CompactionJobDispatchRequestSerDe;
import sleeper.core.properties.instance.InstanceProperties;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.COMPACTION_PENDING_QUEUE_URL;

public class CompactionBatchMessageSenderToSqs implements CreateCompactionJobs.BatchMessageSender {
    public static final Logger LOGGER = LoggerFactory.getLogger(CompactionBatchMessageSenderToSqs.class);

    private final InstanceProperties instanceProperties;
    private final SqsClient sqsClient;
    private final CompactionJobDispatchRequestSerDe serDe = new CompactionJobDispatchRequestSerDe();

    public CompactionBatchMessageSenderToSqs(InstanceProperties instanceProperties, SqsClient sqsClient) {
        this.instanceProperties = instanceProperties;
        this.sqsClient = sqsClient;
    }

    @Override
    public void sendMessage(CompactionJobDispatchRequest dispatchRequest) {
        LOGGER.info("Sending compaction jobs batch: {}", dispatchRequest);
        sqsClient.sendMessage(SendMessageRequest.builder().queueUrl(instanceProperties.get(COMPACTION_PENDING_QUEUE_URL))
                .messageBody(serDe.toJson(dispatchRequest))
                .build());
    }
}
