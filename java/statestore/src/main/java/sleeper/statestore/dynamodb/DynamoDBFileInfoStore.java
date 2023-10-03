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
package sleeper.statestore.dynamodb;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;
import com.amazonaws.services.dynamodbv2.model.ConsumedCapacity;
import com.amazonaws.services.dynamodbv2.model.Delete;
import com.amazonaws.services.dynamodbv2.model.DeleteItemRequest;
import com.amazonaws.services.dynamodbv2.model.DeleteItemResult;
import com.amazonaws.services.dynamodbv2.model.IdempotentParameterMismatchException;
import com.amazonaws.services.dynamodbv2.model.InternalServerErrorException;
import com.amazonaws.services.dynamodbv2.model.ItemCollectionSizeLimitExceededException;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughputExceededException;
import com.amazonaws.services.dynamodbv2.model.Put;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import com.amazonaws.services.dynamodbv2.model.RequestLimitExceededException;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.amazonaws.services.dynamodbv2.model.ReturnConsumedCapacity;
import com.amazonaws.services.dynamodbv2.model.TransactWriteItem;
import com.amazonaws.services.dynamodbv2.model.TransactWriteItemsRequest;
import com.amazonaws.services.dynamodbv2.model.TransactWriteItemsResult;
import com.amazonaws.services.dynamodbv2.model.TransactionCanceledException;
import com.amazonaws.services.dynamodbv2.model.TransactionConflictException;
import com.amazonaws.services.dynamodbv2.model.TransactionInProgressException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.core.schema.Schema;
import sleeper.core.statestore.FileInfo;
import sleeper.core.statestore.FileInfoStore;
import sleeper.core.statestore.StateStoreException;
import sleeper.dynamodb.tools.DynamoDBRecordBuilder;

import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static sleeper.core.statestore.FileInfo.FileStatus.ACTIVE;
import static sleeper.core.statestore.FileInfo.FileStatus.READY_FOR_GARBAGE_COLLECTION;
import static sleeper.dynamodb.tools.DynamoDBUtils.deleteAllDynamoTableItems;
import static sleeper.dynamodb.tools.DynamoDBUtils.streamPagedResults;
import static sleeper.statestore.dynamodb.DynamoDBFileInfoFormat.JOB_ID;
import static sleeper.statestore.dynamodb.DynamoDBFileInfoFormat.LAST_UPDATE_TIME;
import static sleeper.statestore.dynamodb.DynamoDBFileInfoFormat.PARTITION;
import static sleeper.statestore.dynamodb.DynamoDBFileInfoFormat.STATUS;
import static sleeper.statestore.dynamodb.DynamoDBFileInfoFormat.TABLE_NAME;
import static sleeper.statestore.dynamodb.DynamoDBStateStore.FILE_NAME;

class DynamoDBFileInfoStore implements FileInfoStore {

    private static final Logger LOGGER = LoggerFactory.getLogger(DynamoDBFileInfoStore.class);

    private final AmazonDynamoDB dynamoDB;
    private final String activeTableName;
    private final String readyForGCTableName;
    private final String sleeperTableName;
    private final boolean stronglyConsistentReads;
    private final int garbageCollectorDelayBeforeDeletionInMinutes;
    private final DynamoDBFileInfoFormat fileInfoFormat;
    private Clock clock = Clock.systemUTC();

    private DynamoDBFileInfoStore(Builder builder) {
        dynamoDB = Objects.requireNonNull(builder.dynamoDB, "dynamoDB must not be null");
        Schema schema = Objects.requireNonNull(builder.schema, "schema must not be null");
        activeTableName = Objects.requireNonNull(builder.activeTableName, "activeTableName must not be null");
        readyForGCTableName = Objects.requireNonNull(builder.readyForGCTableName, "readyForGCTableName must not be null");
        sleeperTableName = Objects.requireNonNull(builder.sleeperTableName, "sleeperTableName must not be null");
        stronglyConsistentReads = builder.stronglyConsistentReads;
        garbageCollectorDelayBeforeDeletionInMinutes = builder.garbageCollectorDelayBeforeDeletionInMinutes;
        fileInfoFormat = new DynamoDBFileInfoFormat(sleeperTableName, schema);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public void addFile(FileInfo fileInfo) throws StateStoreException {
        if (null == fileInfo.getFilename()
                || null == fileInfo.getFileStatus()
                || null == fileInfo.getPartitionId()) {
            throw new IllegalArgumentException("FileInfo needs non-null filename, status and partition: got " + fileInfo);
        }
        Map<String, AttributeValue> itemValues = fileInfoFormat.createRecord(setLastUpdateTime(fileInfo));
        try {
            String tableName = tableName(fileInfo);
            PutItemRequest putItemRequest = new PutItemRequest()
                    .withItem(itemValues)
                    .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
                    .withTableName(tableName);
            PutItemResult putItemResult = dynamoDB.putItem(putItemRequest);
            LOGGER.debug("Put file info for file {} to table {}, capacity consumed = {}",
                    fileInfo.getFilename(), tableName, putItemResult.getConsumedCapacity().getCapacityUnits());
        } catch (ConditionalCheckFailedException | ProvisionedThroughputExceededException | ResourceNotFoundException
                 | ItemCollectionSizeLimitExceededException | TransactionConflictException
                 | RequestLimitExceededException | InternalServerErrorException e) {
            throw new StateStoreException("Exception calling putItem", e);
        }
    }

    private String tableName(FileInfo fileInfo) {
        if (fileInfo.getFileStatus().equals(ACTIVE)) {
            return activeTableName;
        } else {
            return readyForGCTableName;
        }
    }

    @Override
    public void addFiles(List<FileInfo> fileInfos) throws StateStoreException {
        for (FileInfo fileInfo : fileInfos) {
            addFile(fileInfo);
        }
    }

    @Override
    public void atomicallyUpdateFilesToReadyForGCAndCreateNewActiveFile(
            List<FileInfo> filesToBeMarkedReadyForGC,
            FileInfo newActiveFile) throws StateStoreException {
        // Delete record for file for current status
        List<TransactWriteItem> writes = new ArrayList<>();
        setLastUpdateTimes(filesToBeMarkedReadyForGC).forEach(fileInfo -> {
            Delete delete = new Delete()
                    .withTableName(activeTableName)
                    .withKey(fileInfoFormat.createKey(fileInfo))
                    .withExpressionAttributeNames(Map.of("#status", STATUS))
                    .withConditionExpression("attribute_exists(#status)");
            writes.add(new TransactWriteItem().withDelete(delete));
            Put put = new Put()
                    .withTableName(readyForGCTableName)
                    .withItem(fileInfoFormat.createRecordWithStatus(fileInfo, READY_FOR_GARBAGE_COLLECTION));
            writes.add(new TransactWriteItem().withPut(put));
        });
        // Add record for file for new status
        Put put = new Put()
                .withTableName(activeTableName)
                .withItem(fileInfoFormat.createRecordWithStatus(setLastUpdateTime(newActiveFile), ACTIVE));
        writes.add(new TransactWriteItem().withPut(put));
        TransactWriteItemsRequest transactWriteItemsRequest = new TransactWriteItemsRequest()
                .withTransactItems(writes)
                .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL);
        try {
            TransactWriteItemsResult transactWriteItemsResult = dynamoDB.transactWriteItems(transactWriteItemsRequest);
            List<ConsumedCapacity> consumedCapacity = transactWriteItemsResult.getConsumedCapacity();
            double totalConsumed = consumedCapacity.stream().mapToDouble(ConsumedCapacity::getCapacityUnits).sum();
            LOGGER.debug("Updated status of {} files to ready for GC and added active file, capacity consumed = {}",
                    filesToBeMarkedReadyForGC.size(), totalConsumed);
        } catch (TransactionCanceledException | ResourceNotFoundException
                 | TransactionInProgressException | IdempotentParameterMismatchException
                 | ProvisionedThroughputExceededException | InternalServerErrorException e) {
            throw new StateStoreException(e);
        }
    }

    @Override
    public void atomicallyUpdateFilesToReadyForGCAndCreateNewActiveFiles(
            List<FileInfo> filesToBeMarkedReadyForGC, FileInfo leftFileInfo, FileInfo rightFileInfo) throws StateStoreException {
        // Delete record for file for current status
        List<TransactWriteItem> writes = new ArrayList<>();
        setLastUpdateTimes(filesToBeMarkedReadyForGC).forEach(fileInfo -> {
            Delete delete = new Delete()
                    .withTableName(activeTableName)
                    .withKey(fileInfoFormat.createKey(fileInfo))
                    .withExpressionAttributeNames(Map.of("#status", STATUS))
                    .withConditionExpression("attribute_exists(#status)");
            writes.add(new TransactWriteItem().withDelete(delete));
            Put put = new Put()
                    .withTableName(readyForGCTableName)
                    .withItem(fileInfoFormat.createRecordWithStatus(fileInfo, READY_FOR_GARBAGE_COLLECTION));
            writes.add(new TransactWriteItem().withPut(put));
        });
        // Add record for file for new status
        Put put = new Put()
                .withTableName(activeTableName)
                .withItem(fileInfoFormat.createRecordWithStatus(setLastUpdateTime(leftFileInfo), ACTIVE));
        writes.add(new TransactWriteItem().withPut(put));
        Put put2 = new Put()
                .withTableName(activeTableName)
                .withItem(fileInfoFormat.createRecordWithStatus(setLastUpdateTime(rightFileInfo), ACTIVE));
        writes.add(new TransactWriteItem().withPut(put2));
        TransactWriteItemsRequest transactWriteItemsRequest = new TransactWriteItemsRequest()
                .withTransactItems(writes)
                .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL);
        try {
            TransactWriteItemsResult transactWriteItemsResult = dynamoDB.transactWriteItems(transactWriteItemsRequest);
            List<ConsumedCapacity> consumedCapacity = transactWriteItemsResult.getConsumedCapacity();
            double totalConsumed = consumedCapacity.stream().mapToDouble(ConsumedCapacity::getCapacityUnits).sum();
            LOGGER.debug("Updated status of {} files to ready for GC and added 2 active files, capacity consumed = {}",
                    filesToBeMarkedReadyForGC.size(), totalConsumed);
        } catch (TransactionCanceledException | ResourceNotFoundException
                 | TransactionInProgressException | IdempotentParameterMismatchException
                 | ProvisionedThroughputExceededException | InternalServerErrorException e) {
            throw new StateStoreException(e);
        }
    }

    /**
     * Atomically updates the job field of the given files to the given id, as long as
     * the compactionJob field is currently null.
     */
    @Override
    public void atomicallyUpdateJobStatusOfFiles(String jobId, List<FileInfo> files)
            throws StateStoreException {
        List<TransactWriteItem> writes = new ArrayList<>();
        // TODO This should only be done for active files
        // Create Puts for each of the files, conditional on the compactionJob field being not present
        setLastUpdateTimes(files).forEach(fileInfo -> {
            Put put = new Put()
                    .withTableName(activeTableName)
                    .withItem(fileInfoFormat.createRecordWithJobId(fileInfo, jobId))
                    .withExpressionAttributeNames(Map.of(
                            "#filename", FILE_NAME,
                            "#partitionid", PARTITION,
                            "#jobid", JOB_ID))
                    .withExpressionAttributeValues(Map.of(
                            ":filename", new AttributeValue().withS(fileInfo.getFilename()),
                            ":partitionid", new AttributeValue().withS(fileInfo.getPartitionId())))
                    .withConditionExpression("#filename=:filename and #partitionid=:partitionid and attribute_not_exists(#jobid)");
            writes.add(new TransactWriteItem().withPut(put));
        });
        TransactWriteItemsRequest transactWriteItemsRequest = new TransactWriteItemsRequest()
                .withTransactItems(writes)
                .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL);
        try {
            TransactWriteItemsResult transactWriteItemsResult = dynamoDB.transactWriteItems(transactWriteItemsRequest);
            List<ConsumedCapacity> consumedCapacity = transactWriteItemsResult.getConsumedCapacity();
            double totalConsumed = consumedCapacity.stream().mapToDouble(ConsumedCapacity::getCapacityUnits).sum();
            LOGGER.debug("Updated job status of {} files, read capacity consumed = {}",
                    files.size(), totalConsumed);
        } catch (TransactionCanceledException | ResourceNotFoundException
                 | TransactionInProgressException | IdempotentParameterMismatchException
                 | ProvisionedThroughputExceededException | InternalServerErrorException e) {
            throw new StateStoreException(e);
        }
    }

    @Override
    public void deleteReadyForGCFile(FileInfo fileInfo) {
        // Delete record for file for current status
        DeleteItemRequest deleteItemRequest = new DeleteItemRequest()
                .withTableName(readyForGCTableName)
                .withKey(fileInfoFormat.createKey(fileInfo))
                .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL);
        DeleteItemResult deleteItemResult = dynamoDB.deleteItem(deleteItemRequest);
        ConsumedCapacity consumedCapacity = deleteItemResult.getConsumedCapacity();
        LOGGER.debug("Deleted file {}, capacity consumed = {}",
                fileInfo.getFilename(), consumedCapacity.getCapacityUnits());
    }

    @Override
    public List<FileInfo> getActiveFiles() throws StateStoreException {
        try {
            QueryRequest queryRequest = new QueryRequest()
                    .withTableName(activeTableName)
                    .withConsistentRead(stronglyConsistentReads)
                    .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
                    .withKeyConditionExpression("#TableName = :table_name")
                    .withExpressionAttributeNames(Map.of("#TableName", TABLE_NAME))
                    .withExpressionAttributeValues(new DynamoDBRecordBuilder()
                            .string(":table_name", sleeperTableName)
                            .build());

            AtomicReference<Double> totalCapacity = new AtomicReference<>(0.0D);
            List<Map<String, AttributeValue>> results = queryTrackingCapacity(queryRequest, totalCapacity);
            LOGGER.debug("Scanned for all active files, capacity consumed = {}", totalCapacity.get());
            List<FileInfo> fileInfoResults = new ArrayList<>();
            for (Map<String, AttributeValue> map : results) {
                fileInfoResults.add(fileInfoFormat.getFileInfoFromAttributeValues(map));
            }
            return fileInfoResults;
        } catch (ProvisionedThroughputExceededException | ResourceNotFoundException | RequestLimitExceededException
                 | InternalServerErrorException | IOException e) {
            throw new StateStoreException("Exception querying DynamoDB", e);
        }
    }

    @Override
    public Iterator<FileInfo> getReadyForGCFiles() {
        long delayInMilliseconds = 1000L * 60L * garbageCollectorDelayBeforeDeletionInMinutes;
        long deleteTime = clock.millis() - delayInMilliseconds;
        QueryRequest queryRequest = new QueryRequest()
                .withTableName(readyForGCTableName)
                .withConsistentRead(stronglyConsistentReads)
                .withExpressionAttributeNames(Map.of(
                        "#TableName", TABLE_NAME,
                        "#LastUpdateTime", LAST_UPDATE_TIME))
                .withExpressionAttributeValues(new DynamoDBRecordBuilder()
                        .string(":table_name", sleeperTableName)
                        .number(":delete_time", deleteTime)
                        .build())
                .withKeyConditionExpression("#TableName = :table_name")
                .withFilterExpression("#LastUpdateTime < :delete_time")
                .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL);
        AtomicReference<Double> totalCapacity = new AtomicReference<>(0.0D);
        return streamPagedResults(dynamoDB, queryRequest)
                .flatMap(result -> {
                    double newConsumed = totalCapacity.updateAndGet(old ->
                            old + result.getConsumedCapacity().getCapacityUnits());
                    LOGGER.debug("Queried table {} for all ready for GC files, capacity consumed = {}",
                            readyForGCTableName, newConsumed);
                    return result.getItems().stream();
                }).map(item -> {
                    try {
                        return fileInfoFormat.getFileInfoFromAttributeValues(item);
                    } catch (IOException e) {
                        throw new RuntimeException("IOException creating FileInfo from attribute values");
                    }
                }).iterator();
    }

    @Override
    public List<FileInfo> getActiveFilesWithNoJobId() throws StateStoreException {
        try {
            QueryRequest queryRequest = new QueryRequest()
                    .withTableName(activeTableName)
                    .withConsistentRead(stronglyConsistentReads)
                    .withExpressionAttributeNames(Map.of(
                            "#TableName", TABLE_NAME,
                            "#JobId", JOB_ID))
                    .withExpressionAttributeValues(new DynamoDBRecordBuilder()
                            .string(":table_name", sleeperTableName)
                            .build())
                    .withKeyConditionExpression("#TableName = :table_name")
                    .withFilterExpression("attribute_not_exists(#JobId)")
                    .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL);
            AtomicReference<Double> totalCapacity = new AtomicReference<>(0.0D);
            List<Map<String, AttributeValue>> results = queryTrackingCapacity(queryRequest, totalCapacity);
            LOGGER.debug("Scanned for all active files with no job id, capacity consumed = {}", totalCapacity);
            List<FileInfo> fileInfoResults = new ArrayList<>();
            for (Map<String, AttributeValue> map : results) {
                fileInfoResults.add(fileInfoFormat.getFileInfoFromAttributeValues(map));
            }
            return fileInfoResults;
        } catch (ProvisionedThroughputExceededException | ResourceNotFoundException | RequestLimitExceededException
                 | InternalServerErrorException | IOException e) {
            throw new StateStoreException("Exception querying DynamoDB", e);
        }
    }

    @Override
    public Map<String, List<String>> getPartitionToActiveFilesMap() throws StateStoreException {
        List<FileInfo> files = getActiveFiles();
        Map<String, List<String>> partitionToFiles = new HashMap<>();
        for (FileInfo fileInfo : files) {
            String partition = fileInfo.getPartitionId();
            if (!partitionToFiles.containsKey(partition)) {
                partitionToFiles.put(partition, new ArrayList<>());
            }
            partitionToFiles.get(partition).add(fileInfo.getFilename());
        }
        return partitionToFiles;
    }

    private List<Map<String, AttributeValue>> queryTrackingCapacity(
            QueryRequest queryRequest, AtomicReference<Double> totalCapacity) {
        return streamPagedResults(dynamoDB, queryRequest)
                .flatMap(result -> {
                    totalCapacity.updateAndGet(old -> old + result.getConsumedCapacity().getCapacityUnits());
                    return result.getItems().stream();
                }).collect(Collectors.toList());
    }

    @Override
    public void initialise() {
    }

    @Override
    public boolean hasNoFiles() {
        return isTableEmpty(activeTableName);
    }

    private boolean isTableEmpty(String tableName) {
        QueryResult result = dynamoDB.query(new QueryRequest()
                .withTableName(tableName)
                .withExpressionAttributeNames(Map.of("#TableName", TABLE_NAME))
                .withExpressionAttributeValues(new DynamoDBRecordBuilder()
                        .string(":table_name", sleeperTableName)
                        .build())
                .withKeyConditionExpression("#TableName = :table_name")
                .withConsistentRead(stronglyConsistentReads)
                .withLimit(1)
                .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL));
        LOGGER.debug("Scanned for any file in table {}, capacity consumed = {}", tableName, result.getConsumedCapacity().getCapacityUnits());
        return result.getItems().isEmpty();
    }

    @Override
    public void clearTable() {
        clearDynamoTable(activeTableName);
        clearDynamoTable(readyForGCTableName);
    }

    private void clearDynamoTable(String dynamoTableName) {
        deleteAllDynamoTableItems(dynamoDB, new QueryRequest().withTableName(dynamoTableName)
                        .withExpressionAttributeNames(Map.of("#TableName", TABLE_NAME))
                        .withExpressionAttributeValues(new DynamoDBRecordBuilder()
                                .string(":table_name", sleeperTableName)
                                .build())
                        .withKeyConditionExpression("#TableName = :table_name"),
                fileInfoFormat::getKey);
    }

    /**
     * Used to set the current time. Should only be called during tests.
     *
     * @param now Time to set to be the current time
     */
    public void fixTime(Instant now) {
        clock = Clock.fixed(now, ZoneId.of("UTC"));
    }

    private static FileInfo setLastUpdateTime(FileInfo fileInfo) {
        return fileInfo.toBuilder().lastStateStoreUpdateTime(System.currentTimeMillis()).build();
    }

    private static Stream<FileInfo> setLastUpdateTimes(List<FileInfo> fileInfos) {
        return fileInfos.stream().map(DynamoDBFileInfoStore::setLastUpdateTime);
    }

    static final class Builder {
        private AmazonDynamoDB dynamoDB;
        private Schema schema;
        private String activeTableName;
        private String readyForGCTableName;
        private String sleeperTableName;
        private boolean stronglyConsistentReads;
        private int garbageCollectorDelayBeforeDeletionInMinutes;

        private Builder() {
        }

        Builder dynamoDB(AmazonDynamoDB dynamoDB) {
            this.dynamoDB = dynamoDB;
            return this;
        }

        Builder schema(Schema schema) {
            this.schema = schema;
            return this;
        }

        Builder activeTableName(String activeTableName) {
            this.activeTableName = activeTableName;
            return this;
        }

        Builder readyForGCTableName(String readyForGCTableName) {
            this.readyForGCTableName = readyForGCTableName;
            return this;
        }

        Builder sleeperTableName(String sleeperTableName) {
            this.sleeperTableName = sleeperTableName;
            return this;
        }

        Builder stronglyConsistentReads(boolean stronglyConsistentReads) {
            this.stronglyConsistentReads = stronglyConsistentReads;
            return this;
        }

        Builder garbageCollectorDelayBeforeDeletionInMinutes(int garbageCollectorDelayBeforeDeletionInMinutes) {
            this.garbageCollectorDelayBeforeDeletionInMinutes = garbageCollectorDelayBeforeDeletionInMinutes;
            return this;
        }

        DynamoDBFileInfoStore build() {
            return new DynamoDBFileInfoStore(this);
        }
    }
}
