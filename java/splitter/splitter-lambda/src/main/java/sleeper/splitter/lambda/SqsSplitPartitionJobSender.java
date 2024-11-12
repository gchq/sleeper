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
package sleeper.splitter.lambda;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TablePropertiesProvider;
import sleeper.splitter.core.find.SplitPartitionJobDefinition;
import sleeper.splitter.core.find.SplitPartitionJobDefinitionSerDe;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.PARTITION_SPLITTING_JOB_QUEUE_URL;

/**
 * Sends partition splitting jobs to an SQS queue.
 */
public class SqsSplitPartitionJobSender {
    private final TablePropertiesProvider tablePropertiesProvider;
    private final String sqsUrl;
    private final AmazonSQS sqs;

    private static final Logger LOGGER = LoggerFactory.getLogger(SqsSplitPartitionJobSender.class);

    public SqsSplitPartitionJobSender(
            TablePropertiesProvider tablePropertiesProvider,
            InstanceProperties instanceProperties,
            AmazonSQS sqs) {
        this.tablePropertiesProvider = tablePropertiesProvider;
        this.sqsUrl = instanceProperties.get(PARTITION_SPLITTING_JOB_QUEUE_URL);
        this.sqs = sqs;
    }

    public void send(SplitPartitionJobDefinition job) {

        String serialised = new SplitPartitionJobDefinitionSerDe(tablePropertiesProvider).toJson(job);

        // Send definition to SQS queue
        SendMessageRequest sendMessageRequest = new SendMessageRequest()
                .withQueueUrl(sqsUrl)
                .withMessageBody(serialised);
        SendMessageResult sendMessageResult = sqs.sendMessage(sendMessageRequest);

        LOGGER.info("Sent message for partition {} to SQS queue with url {} with result {}", job.getPartition(), sqsUrl, sendMessageResult);
    }
}
