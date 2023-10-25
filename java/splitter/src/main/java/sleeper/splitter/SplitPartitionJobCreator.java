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
package sleeper.splitter;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.core.partition.Partition;
import sleeper.core.statestore.FileInfo;
import sleeper.core.table.TableId;

import java.util.List;

/**
 * Creates a {@link SplitPartitionJobDefinition} from the provided
 * {@link Partition} and list of {@link FileInfo}s, serialises it to a string
 * and sends that to an SQS queue.
 */
public class SplitPartitionJobCreator {
    private final TableId tableId;
    private final TablePropertiesProvider tablePropertiesProvider;
    private final Partition partition;
    private final List<String> fileNames;
    private final String sqsUrl;
    private final AmazonSQS sqs;

    private static final Logger LOGGER = LoggerFactory.getLogger(SplitPartitionJobCreator.class);

    public SplitPartitionJobCreator(
            TableId tableId,
            TablePropertiesProvider tablePropertiesProvider,
            Partition partition,
            List<String> fileNames,
            String sqsUrl,
            AmazonSQS sqs) {
        this.tableId = tableId;
        this.tablePropertiesProvider = tablePropertiesProvider;
        this.partition = partition;
        this.fileNames = fileNames;
        this.sqsUrl = sqsUrl;
        this.sqs = sqs;
    }

    public void run() {
        // Create definition of partition splitting job to be done
        SplitPartitionJobDefinition partitionSplittingJobDefinition = new SplitPartitionJobDefinition(
                tableId.getTableUniqueId(), partition, fileNames);

        String serialised = new SplitPartitionJobDefinitionSerDe(tablePropertiesProvider)
                .toJson(partitionSplittingJobDefinition);

        // Send definition to SQS queue
        SendMessageRequest sendMessageRequest = new SendMessageRequest()
                .withQueueUrl(sqsUrl)
                .withMessageBody(serialised);
        SendMessageResult sendMessageResult = sqs.sendMessage(sendMessageRequest);

        LOGGER.info("Sent message for partition {} to SQS queue with url {} with result {}", partition, sqsUrl, sendMessageResult);
    }
}
