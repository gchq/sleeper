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

package sleeper.ingest.batcher.store;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.ConsumedCapacity;
import software.amazon.awssdk.services.dynamodb.model.Delete;
import software.amazon.awssdk.services.dynamodb.model.DeleteRequest;
import software.amazon.awssdk.services.dynamodb.model.Put;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemResponse;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.ReturnConsumedCapacity;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItem;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItemsRequest;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItemsResponse;
import software.amazon.awssdk.services.dynamodb.model.TransactionCanceledException;
import software.amazon.awssdk.services.dynamodb.model.WriteRequest;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TablePropertiesProvider;
import sleeper.dynamodb.tools.DynamoDBRecordBuilder;
import sleeper.ingest.batcher.core.IngestBatcherStore;
import sleeper.ingest.batcher.core.IngestBatcherTrackedFile;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Comparator.comparing;
import static sleeper.core.properties.instance.CommonProperty.ID;
import static sleeper.dynamodb.tools.DynamoDBUtils.instanceTableName;
import static sleeper.dynamodb.tools.DynamoDBUtils.streamPagedItems;
import static sleeper.ingest.batcher.store.DynamoDBIngestRequestFormat.FILE_PATH;
import static sleeper.ingest.batcher.store.DynamoDBIngestRequestFormat.JOB_ID;
import static sleeper.ingest.batcher.store.DynamoDBIngestRequestFormat.NOT_ASSIGNED_TO_JOB;
import static sleeper.ingest.batcher.store.DynamoDBIngestRequestFormat.createUnassignedKey;

public class DynamoDBIngestBatcherStore implements IngestBatcherStore {
    private static final Logger LOGGER = LoggerFactory.getLogger(DynamoDBIngestBatcherStore.class);
    // Each job assignment takes two write items, so 50 files at a time stays below the transaction limit of 100 items.
    private static final int FILES_IN_ASSIGN_JOB_BATCH = 50;
    private final DynamoDbClient dynamoDB;
    private final String requestsTableName;
    private final TablePropertiesProvider tablePropertiesProvider;
    private final int filesInAssignJobBatch;

    public DynamoDBIngestBatcherStore(
            DynamoDbClient dynamoDB, InstanceProperties instanceProperties,
            TablePropertiesProvider tablePropertiesProvider) {
        this(dynamoDB, instanceProperties, tablePropertiesProvider, FILES_IN_ASSIGN_JOB_BATCH);
    }

    public DynamoDBIngestBatcherStore(
            DynamoDbClient dynamoDB, InstanceProperties instanceProperties,
            TablePropertiesProvider tablePropertiesProvider, int filesInAssignJobBatch) {
        this.dynamoDB = dynamoDB;
        this.requestsTableName = ingestRequestsTableName(instanceProperties.get(ID));
        this.tablePropertiesProvider = tablePropertiesProvider;
        this.filesInAssignJobBatch = filesInAssignJobBatch;
    }

    public static String ingestRequestsTableName(String instanceId) {
        return instanceTableName(instanceId, "ingest-batcher-store");
    }

    @Override
    public void addFile(IngestBatcherTrackedFile fileIngestRequest) {
        PutItemResponse result = dynamoDB.putItem(PutItemRequest.builder()
                .tableName(requestsTableName)
                .returnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
                .item(DynamoDBIngestRequestFormat.createRecord(tablePropertiesProvider, fileIngestRequest))
                .build());
        LOGGER.debug("Put request to ingest file {} to table {}, capacity consumed = {}",
                fileIngestRequest.getFile(), fileIngestRequest.getTableId(), result.consumedCapacity().capacityUnits());
    }

    @Override
    public List<String> assignJobGetAssigned(String jobId, List<IngestBatcherTrackedFile> filesInJob) {
        List<IngestBatcherTrackedFile> assignedFiles = new ArrayList<>();
        for (int i = 0; i < filesInJob.size(); i += filesInAssignJobBatch) {
            List<IngestBatcherTrackedFile> filesInBatch = filesInJob.subList(i, Math.min(i + filesInAssignJobBatch, filesInJob.size()));
            try {
                TransactWriteItemsRequest request = TransactWriteItemsRequest.builder()
                        .returnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
                        .transactItems(filesInBatch.stream()
                                .flatMap(file -> Stream.of(
                                        TransactWriteItem.builder()
                                                .delete(Delete.builder()
                                                        .tableName(requestsTableName)
                                                        .key(DynamoDBIngestRequestFormat.createUnassignedKey(file))
                                                        .conditionExpression("attribute_exists(#filepath)")
                                                        .expressionAttributeNames(Map.of("#filepath", FILE_PATH))
                                                        .build())
                                                .build(),
                                        TransactWriteItem.builder()
                                                .put(Put.builder()
                                                        .tableName(requestsTableName)
                                                        .item(DynamoDBIngestRequestFormat.createRecord(
                                                                tablePropertiesProvider, file.toBuilder().jobId(jobId).build()))
                                                        .conditionExpression("attribute_not_exists(#filepath)")
                                                        .expressionAttributeNames(Map.of("#filepath", FILE_PATH))
                                                        .build())
                                                .build()))
                                .collect(Collectors.toList()))
                        .build();

                TransactWriteItemsResponse result = dynamoDB.transactWriteItems(request);
                List<ConsumedCapacity> consumedCapacity = Optional.ofNullable(result.consumedCapacity()).orElse(List.of());
                double totalConsumed = consumedCapacity.stream().mapToDouble(ConsumedCapacity::capacityUnits).sum();
                LOGGER.debug("Assigned {} files to job {}, capacity consumed = {}",
                        filesInBatch.size(), jobId, totalConsumed);
                assignedFiles.addAll(filesInBatch);
            } catch (TransactionCanceledException e) {
                LOGGER.error("{} files could not be batched, leaving them for next batcher run.", filesInBatch.size());
                long numFailures = e.cancellationReasons().stream()
                        .filter(reason -> !"None".equals(reason.code())).count();
                LOGGER.error("Cancellation reasons ({} failures): {}", numFailures, e.cancellationReasons(), e);
            } catch (RuntimeException e) {
                LOGGER.error("{} files could not be batched, leaving them for next batcher run.", filesInBatch.size(), e);
            }
        }
        return assignedFiles.stream()
                .map(IngestBatcherTrackedFile::getFile)
                .collect(Collectors.toUnmodifiableList());
    }

    @Override
    public List<IngestBatcherTrackedFile> getAllFilesNewestFirst() {
        return streamPagedItems(dynamoDB, ScanRequest.builder()
                .tableName(requestsTableName).build())
                .map(DynamoDBIngestRequestFormat::readRecord)
                .sorted(comparing(IngestBatcherTrackedFile::getReceivedTime).reversed())
                .collect(Collectors.toList());
    }

    @Override
    public List<IngestBatcherTrackedFile> getPendingFilesOldestFirst() {
        return streamPagedItems(dynamoDB, QueryRequest.builder()
                .tableName(requestsTableName)
                .keyConditionExpression("#JobId = :not_assigned")
                .expressionAttributeNames(Map.of("#JobId", JOB_ID))
                .expressionAttributeValues(new DynamoDBRecordBuilder()
                        .string(":not_assigned", NOT_ASSIGNED_TO_JOB)
                        .build())
                .build())
                .map(DynamoDBIngestRequestFormat::readRecord)
                .sorted(comparing(IngestBatcherTrackedFile::getReceivedTime))
                .collect(Collectors.toList());
    }

    @Override
    public void deleteAllPending() {
        List<IngestBatcherTrackedFile> pendingFiles = getPendingFilesOldestFirst();
        if (!pendingFiles.isEmpty()) {
            dynamoDB.batchWriteItem(BatchWriteItemRequest.builder()
                    .requestItems(Map.of(requestsTableName,
                            pendingFiles.stream()
                                    .map(request -> WriteRequest.builder()
                                            .deleteRequest(DeleteRequest.builder()
                                                    .key(createUnassignedKey(request))
                                                    .build())
                                            .build())
                                    .collect(Collectors.toList())))
                    .build());
        }
    }
}
