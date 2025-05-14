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

import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toUnmodifiableList;

/**
 * Invokes a lambda that is triggered by a FIFO SQS queue where each message is a Sleeper table ID. The Sleeper table ID
 * is also the message group ID. This setup means the lambda can be invoked in parallel for a number of Sleeper tables
 * at the same time, without ever having two invocations for the same Sleeper table at once.
 * <p>
 * Also see this AWS blog: https://aws.amazon.com/blogs/compute/new-for-aws-lambda-sqs-fifo-as-an-event-source/
 */
public class InvokeForTables {
    public static final Logger LOGGER = LoggerFactory.getLogger(InvokeForTables.class);

    private InvokeForTables() {
    }

    /**
     * Invokes a lambda to run for a given set of Sleeper tables.
     *
     * @param sqsClient an SQS client
     * @param queueUrl  the URL of the FIFO queue that triggers the lambda
     * @param tables    the Sleeper tables to invoke the lambda for
     */
    public static void sendOneMessagePerTable(SqsClient sqsClient, String queueUrl, Stream<TableStatus> tables) {
        // Limit to stay under the maximum number of messages for an SQS sendMessageBatch call.
        SplitIntoBatches.reusingListOfSize(10, tables,
                batch -> sendMessageBatch(sqsClient, queueUrl, batch));
    }

    /**
     * Invokes a lambda to run for a given set of Sleeper tables by their names.
     *
     * @param sqsClient  an SQS client
     * @param queueUrl   the URL of the FIFO queue that triggers the lambda
     * @param tableIndex the index of Sleeper tables
     * @param tableNames the names of Sleeper tables to invoke the lambda for
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
        sqsClient.sendMessageBatch(SendMessageBatchRequest.builder()
                .queueUrl(queueUrl)
                .entries(tablesBatch.stream()
                        .map(table -> SendMessageBatchRequestEntry.builder()
                                .messageDeduplicationId(UUID.randomUUID().toString())
                                .id(table.getTableUniqueId())
                                .messageGroupId(table.getTableUniqueId())
                                .messageBody(table.getTableUniqueId())
                                .build())
                        .toList())
                .build());
    }
}
