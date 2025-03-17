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
package sleeper.invoke.tables;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.SendMessageBatchRequest;
import com.amazonaws.services.sqs.model.SendMessageBatchRequestEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.core.table.TableIndex;
import sleeper.core.table.TableNotFoundException;
import sleeper.core.table.TableStatus;
import sleeper.core.util.SplitIntoBatches;

import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toUnmodifiableList;

public class InvokeForTables {
    public static final Logger LOGGER = LoggerFactory.getLogger(InvokeForTables.class);

    private InvokeForTables() {
    }

    public static void sendOneMessagePerTable(AmazonSQS sqsClient, String queueUrl, Stream<TableStatus> tables) {
        // Limit to stay under the maximum size for an SQS sendMessageBatch call.
        SplitIntoBatches.reusingListOfSize(10, tables,
                batch -> sendMessageBatch(sqsClient, queueUrl, batch));
    }

    public static void sendOneMessagePerTableByName(
            AmazonSQS sqsClient, String queueUrl, TableIndex tableIndex, List<String> tableNames) {
        List<TableStatus> tables = tableNames.stream().map(name -> tableIndex.getTableByName(name)
                .orElseThrow(() -> TableNotFoundException.withTableName(name)))
                .collect(toUnmodifiableList());
        sendOneMessagePerTable(sqsClient, queueUrl, tables.stream());
    }

    private static void sendMessageBatch(AmazonSQS sqsClient, String queueUrl, List<TableStatus> tablesBatch) {
        LOGGER.info("Sending table batch of size {} to SQS queue {}: {}", tablesBatch.size(), queueUrl, tablesBatch);
        sqsClient.sendMessageBatch(new SendMessageBatchRequest()
                .withQueueUrl(queueUrl)
                .withEntries(tablesBatch.stream()
                        .map(table -> new SendMessageBatchRequestEntry()
                                .withMessageDeduplicationId(UUID.randomUUID().toString())
                                .withId(table.getTableUniqueId())
                                .withMessageGroupId(table.getTableUniqueId())
                                .withMessageBody(table.getTableUniqueId()))
                        .collect(toUnmodifiableList())));
    }
}
