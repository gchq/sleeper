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
package sleeper.invoke.tablesv2;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequestEntry;

import sleeper.core.table.TableIndex;
import sleeper.core.table.TableNotFoundException;
import sleeper.core.table.TableStatus;
import sleeper.core.util.SplitIntoBatches;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toUnmodifiableList;

/**
 * Class used for controlling sqs messages sent for table to keep within aws design constraints.
 */
public class InvokeForTables {
    public static final Logger LOGGER = LoggerFactory.getLogger(InvokeForTables.class);

    private InvokeForTables() {
    }

    /**
     * Splits steam of tables into batchs to allow messages to be within maximum size for an SQS message.
     *
     * @param sqsClient the sqs client
     * @param queueUrl  the queue url
     * @param tables    stream of table to send messages for
     */
    public static void sendOneMessagePerTable(SqsClient sqsClient, String queueUrl, Stream<TableStatus> tables) {
        // Limit to stay under the maximum size for an SQS sendMessageBatch call.
        SplitIntoBatches.reusingListOfSize(10, tables,
                batch -> sendMessageBatch(sqsClient, queueUrl, batch));
    }

    /**
     * Generates a stream of tables from index to allow for batching.
     *
     * @param sqsClient  the sqs client
     * @param queueUrl   the queue url
     * @param tableIndex index containing table details
     * @param tableNames list of table names
     */
    public static void sendOneMessagePerTableByName(
            SqsClient sqsClient, String queueUrl, TableIndex tableIndex, List<String> tableNames) {
        List<TableStatus> tables = tableNames.stream().map(name -> tableIndex.getTableByName(name)
                .orElseThrow(() -> TableNotFoundException.withTableName(name)))
                .collect(toUnmodifiableList());
        sendOneMessagePerTable(sqsClient, queueUrl, tables.stream());
    }

    private static void sendMessageBatch(SqsClient sqsClient, String queueUrl, List<TableStatus> tablesBatch) {
        LOGGER.info("Sending table batch of size {} to SQS queue {}: {}", tablesBatch.size(), queueUrl, tablesBatch);
        List<SendMessageBatchRequestEntry> list = new ArrayList<SendMessageBatchRequestEntry>();
        tablesBatch.stream().forEach(table -> {
            list.add(SendMessageBatchRequestEntry.builder()
                    .messageDeduplicationId(UUID.randomUUID().toString())
                    .id(table.getTableUniqueId())
                    .messageGroupId(table.getTableUniqueId())
                    .messageBody(table.getTableUniqueId())
                    .build());
        });
        sqsClient.sendMessageBatch(SendMessageBatchRequest.builder()
                .queueUrl(queueUrl)
                .entries(list)
                .build());
    }
}
