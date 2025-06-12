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
package sleeper.statestore.lambda.transaction;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SQSBatchResponse;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.amazonaws.services.lambda.runtime.events.SQSEvent.SQSMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.s3.S3Client;

import sleeper.configuration.properties.S3InstanceProperties;
import sleeper.configuration.properties.S3PropertiesReloader;
import sleeper.configuration.properties.S3TableProperties;
import sleeper.core.properties.PropertiesReloader;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertiesProvider;
import sleeper.core.statestore.transactionlog.log.TransactionLogStore;
import sleeper.core.util.LoggedDuration;
import sleeper.statestore.transactionlog.DynamoDBTransactionLogStore;
import sleeper.statestore.transactionlog.TransactionLogTransactionDeleter;
import sleeper.statestore.transactionlog.snapshots.DynamoDBTransactionLogSnapshotMetadataStore;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;

/**
 * A lambda that receives batches of tables from an SQS queue and deletes old transaction log transactions for them.
 */
public class TransactionLogTransactionDeletionLambda implements RequestHandler<SQSEvent, SQSBatchResponse> {
    private static final Logger LOGGER = LoggerFactory.getLogger(TransactionLogTransactionDeletionLambda.class);

    private final S3Client s3Client;
    private final DynamoDbClient dynamoClient;
    private final InstanceProperties instanceProperties;
    private final TablePropertiesProvider tablePropertiesProvider;
    private final PropertiesReloader propertiesReloader;

    public TransactionLogTransactionDeletionLambda() {
        s3Client = S3Client.create();
        dynamoClient = DynamoDbClient.create();
        String configBucketName = System.getenv(CONFIG_BUCKET.toEnvironmentVariable());
        instanceProperties = S3InstanceProperties.loadFromBucket(s3Client, configBucketName);
        tablePropertiesProvider = S3TableProperties.createProvider(instanceProperties, s3Client, dynamoClient);
        propertiesReloader = S3PropertiesReloader.ifConfigured(s3Client, instanceProperties, tablePropertiesProvider);
    }

    @Override
    public SQSBatchResponse handleRequest(SQSEvent event, Context context) {
        Instant startTime = Instant.now();
        LOGGER.info("Lambda started at {}", startTime);
        propertiesReloader.reloadIfNeeded();

        Map<String, List<SQSMessage>> messagesByTableId = event.getRecords().stream()
                .collect(Collectors.groupingBy(SQSEvent.SQSMessage::getBody));
        List<SQSBatchResponse.BatchItemFailure> batchItemFailures = new ArrayList<SQSBatchResponse.BatchItemFailure>();
        List<TableProperties> tables = loadTables(messagesByTableId, batchItemFailures);
        deleteTransactions(tables, messagesByTableId, batchItemFailures);

        Instant finishTime = Instant.now();
        LOGGER.info("Lambda finished at {} (ran for {})", finishTime, LoggedDuration.withFullOutput(startTime, finishTime));
        return new SQSBatchResponse(batchItemFailures);
    }

    private void deleteTransactions(List<TableProperties> tables, Map<String, List<SQSMessage>> messagesByTableId,
            List<SQSBatchResponse.BatchItemFailure> batchItemFailures) {
        for (TableProperties table : tables) {
            LOGGER.info("Deleting old transactions for table {}", table.getStatus());
            try {
                TransactionLogStore filesLogStore = getLogStoreForFiles(table);
                TransactionLogStore partitionsLogStore = getLogStoreForPartitions(table);
                DynamoDBTransactionLogSnapshotMetadataStore metadataStore = new DynamoDBTransactionLogSnapshotMetadataStore(instanceProperties, table, dynamoClient);
                new TransactionLogTransactionDeleter(table, metadataStore::getLatestSnapshotsBefore, filesLogStore, partitionsLogStore, Instant::now)
                        .deleteOldTransactions();
            } catch (RuntimeException e) {
                LOGGER.error("Failed deleting old transactions for table {}", table.getStatus(), e);
                messagesByTableId.get(table.getStatus().getTableUniqueId()).stream()
                        .map(SQSMessage::getMessageId)
                        .map(SQSBatchResponse.BatchItemFailure::new)
                        .forEach(batchItemFailures::add);
            }
        }
    }

    private TransactionLogStore getLogStoreForFiles(TableProperties tableProperties) {
        return DynamoDBTransactionLogStore.forFiles(instanceProperties, tableProperties, dynamoClient, s3Client);
    }

    private TransactionLogStore getLogStoreForPartitions(TableProperties tableProperties) {
        return DynamoDBTransactionLogStore.forPartitions(instanceProperties, tableProperties, dynamoClient, s3Client);
    }

    private List<TableProperties> loadTables(
            Map<String, List<SQSMessage>> messagesByTableId,
            List<SQSBatchResponse.BatchItemFailure> batchItemFailures) {
        List<TableProperties> tables = new ArrayList<>();
        for (Entry<String, List<SQSMessage>> tableAndMessages : messagesByTableId.entrySet()) {
            String tableId = tableAndMessages.getKey();
            List<SQSMessage> tableMessages = tableAndMessages.getValue();
            try {
                TableProperties tableProperties = tablePropertiesProvider.getById(tableId);
                LOGGER.info("Received {} messages for table {}", tableMessages.size(), tableProperties.getStatus());
                tables.add(tableProperties);
            } catch (RuntimeException e) {
                LOGGER.error("Failed loading properties for table {}", tableId, e);
                tableMessages.stream()
                        .map(SQSMessage::getMessageId)
                        .map(SQSBatchResponse.BatchItemFailure::new)
                        .forEach(batchItemFailures::add);
            }
        }
        return tables;
    }
}
