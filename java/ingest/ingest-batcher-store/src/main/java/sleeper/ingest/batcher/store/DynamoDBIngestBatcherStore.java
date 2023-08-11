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

package sleeper.ingest.batcher.store;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AmazonDynamoDBException;
import com.amazonaws.services.dynamodbv2.model.BatchWriteItemRequest;
import com.amazonaws.services.dynamodbv2.model.Delete;
import com.amazonaws.services.dynamodbv2.model.DeleteRequest;
import com.amazonaws.services.dynamodbv2.model.Put;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.ReturnConsumedCapacity;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.TransactWriteItem;
import com.amazonaws.services.dynamodbv2.model.TransactWriteItemsRequest;
import com.amazonaws.services.dynamodbv2.model.TransactionCanceledException;
import com.amazonaws.services.dynamodbv2.model.WriteRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.dynamodb.tools.DynamoDBRecordBuilder;
import sleeper.ingest.batcher.FileIngestRequest;
import sleeper.ingest.batcher.IngestBatcherStore;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Comparator.comparing;
import static sleeper.configuration.properties.instance.CommonProperty.ID;
import static sleeper.dynamodb.tools.DynamoDBUtils.instanceTableName;
import static sleeper.dynamodb.tools.DynamoDBUtils.streamPagedItems;
import static sleeper.ingest.batcher.store.DynamoDBIngestRequestFormat.FILE_PATH;
import static sleeper.ingest.batcher.store.DynamoDBIngestRequestFormat.JOB_ID;
import static sleeper.ingest.batcher.store.DynamoDBIngestRequestFormat.NOT_ASSIGNED_TO_JOB;
import static sleeper.ingest.batcher.store.DynamoDBIngestRequestFormat.createUnassignedKey;

public class DynamoDBIngestBatcherStore implements IngestBatcherStore {
    private static final Logger LOGGER = LoggerFactory.getLogger(DynamoDBIngestBatcherStore.class);
    private final AmazonDynamoDB dynamoDB;
    private final String requestsTableName;
    private final TablePropertiesProvider tablePropertiesProvider;

    public DynamoDBIngestBatcherStore(AmazonDynamoDB dynamoDB,
                                      InstanceProperties instanceProperties,
                                      TablePropertiesProvider tablePropertiesProvider) {
        this.dynamoDB = dynamoDB;
        this.requestsTableName = ingestRequestsTableName(instanceProperties.get(ID));
        this.tablePropertiesProvider = tablePropertiesProvider;
    }

    public static String ingestRequestsTableName(String instanceId) {
        return instanceTableName(instanceId, "ingest-batcher-store");
    }

    @Override
    public void addFile(FileIngestRequest fileIngestRequest) {
        dynamoDB.putItem(new PutItemRequest()
                .withTableName(requestsTableName)
                .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
                .withItem(DynamoDBIngestRequestFormat.createRecord(tablePropertiesProvider, fileIngestRequest)));
    }

    @Override
    public List<String> assignJobGetAssigned(String jobId, List<FileIngestRequest> filesInJob) {
        List<FileIngestRequest> assignedFiles = new ArrayList<>();
        for (int i = 0; i < filesInJob.size(); i += 50) {
            List<FileIngestRequest> filesInBatch = filesInJob.subList(i, Math.min(i + 50, filesInJob.size()));
            try {
                dynamoDB.transactWriteItems(new TransactWriteItemsRequest()
                        .withTransactItems(filesInBatch.stream()
                                .flatMap(file -> Stream.of(
                                        new TransactWriteItem().withDelete(new Delete()
                                                .withTableName(requestsTableName)
                                                .withKey(DynamoDBIngestRequestFormat.createUnassignedKey(file))
                                                .withConditionExpression("attribute_exists(#filepath)")
                                                .withExpressionAttributeNames(Map.of("#filepath", FILE_PATH))),
                                        new TransactWriteItem().withPut(new Put()
                                                .withTableName(requestsTableName)
                                                .withItem(DynamoDBIngestRequestFormat.createRecord(
                                                        tablePropertiesProvider, file.toBuilder().jobId(jobId).build()))
                                                .withConditionExpression("attribute_not_exists(#filepath)")
                                                .withExpressionAttributeNames(Map.of("#filepath", FILE_PATH))))
                                ).collect(Collectors.toList())));
                assignedFiles.addAll(filesInBatch);
            } catch (TransactionCanceledException e) {
                LOGGER.error("{} files could not be batched, leaving them for next batcher run.", filesInBatch.size());
                LOGGER.error("Cancellation reasons: {}", e.getCancellationReasons(), e);
            } catch (AmazonDynamoDBException e) {
                LOGGER.error("{} files could not be batched, leaving them for next batcher run.", filesInBatch.size(), e);
            }
        }
        return assignedFiles.stream()
                .map(FileIngestRequest::getFile)
                .collect(Collectors.toUnmodifiableList());
    }

    @Override
    public List<FileIngestRequest> getAllFilesNewestFirst() {
        return streamPagedItems(dynamoDB, new ScanRequest()
                .withTableName(requestsTableName))
                .map(DynamoDBIngestRequestFormat::readRecord)
                .sorted(comparing(FileIngestRequest::getReceivedTime).reversed())
                .collect(Collectors.toList());
    }

    @Override
    public List<FileIngestRequest> getPendingFilesOldestFirst() {
        return streamPagedItems(dynamoDB, new QueryRequest()
                .withTableName(requestsTableName)
                .withKeyConditionExpression("#JobId = :not_assigned")
                .withExpressionAttributeNames(Map.of("#JobId", JOB_ID))
                .withExpressionAttributeValues(new DynamoDBRecordBuilder()
                        .string(":not_assigned", NOT_ASSIGNED_TO_JOB)
                        .build()))
                .map(DynamoDBIngestRequestFormat::readRecord)
                .sorted(comparing(FileIngestRequest::getReceivedTime))
                .collect(Collectors.toList());
    }

    @Override
    public void deleteAllPending() {
        List<FileIngestRequest> pendingFiles = getPendingFilesOldestFirst();
        if (!pendingFiles.isEmpty()) {
            dynamoDB.batchWriteItem(new BatchWriteItemRequest()
                    .addRequestItemsEntry(requestsTableName,
                            pendingFiles.stream()
                                    .map(request -> new WriteRequest()
                                            .withDeleteRequest(new DeleteRequest()
                                                    .withKey(createUnassignedKey(request)))
                                    ).collect(Collectors.toList())));
        }
    }
}
