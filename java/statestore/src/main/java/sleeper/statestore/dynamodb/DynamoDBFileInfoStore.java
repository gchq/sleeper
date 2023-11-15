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

import sleeper.core.statestore.FileInfo;
import sleeper.core.statestore.FileInfoStore;
import sleeper.core.statestore.StateStoreException;
import sleeper.dynamodb.tools.DynamoDBRecordBuilder;

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
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static sleeper.core.statestore.FileInfo.FileStatus.ACTIVE;
import static sleeper.dynamodb.tools.DynamoDBUtils.deleteAllDynamoTableItems;
import static sleeper.dynamodb.tools.DynamoDBUtils.streamPagedResults;
import static sleeper.statestore.dynamodb.DynamoDBFileInfoFormat.FILENAME;
import static sleeper.statestore.dynamodb.DynamoDBFileInfoFormat.JOB_ID;
import static sleeper.statestore.dynamodb.DynamoDBFileInfoFormat.LAST_UPDATE_TIME;
import static sleeper.statestore.dynamodb.DynamoDBFileInfoFormat.PARTITION_ID;
import static sleeper.statestore.dynamodb.DynamoDBFileInfoFormat.PARTITION_ID_AND_FILENAME;
import static sleeper.statestore.dynamodb.DynamoDBFileInfoFormat.STATUS;
import static sleeper.statestore.dynamodb.DynamoDBFileInfoFormat.TABLE_ID;
import static sleeper.statestore.dynamodb.DynamoDBFileInfoFormat.getActiveFileSortKey;

class DynamoDBFileInfoStore implements FileInfoStore {

    private static final Logger LOGGER = LoggerFactory.getLogger(DynamoDBFileInfoStore.class);

    private final AmazonDynamoDB dynamoDB;
    private final String activeTableName;
    private final String readyForGCTableName;
    private final String sleeperTableId;
    private final boolean stronglyConsistentReads;
    private final int garbageCollectorDelayBeforeDeletionInMinutes;
    private final DynamoDBFileInfoFormat fileInfoFormat;
    private Clock clock = Clock.systemUTC();
    private final Integer pageLimit;

    private DynamoDBFileInfoStore(Builder builder) {
        dynamoDB = Objects.requireNonNull(builder.dynamoDB, "dynamoDB must not be null");
        activeTableName = Objects.requireNonNull(builder.activeTableName, "activeTableName must not be null");
        readyForGCTableName = Objects.requireNonNull(builder.readyForGCTableName, "readyForGCTableName must not be null");
        sleeperTableId = Objects.requireNonNull(builder.sleeperTableId, "sleeperTableId must not be null");
        stronglyConsistentReads = builder.stronglyConsistentReads;
        garbageCollectorDelayBeforeDeletionInMinutes = builder.garbageCollectorDelayBeforeDeletionInMinutes;
        fileInfoFormat = new DynamoDBFileInfoFormat(sleeperTableId);
        pageLimit = builder.pageLimit;
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public void addFile(FileInfo fileInfo) throws StateStoreException {
        addFile(fileInfo, clock.millis());
    }

    public void addFile(FileInfo fileInfo, long updateTime) throws StateStoreException {
        if (null == fileInfo.getFilename()
                || null == fileInfo.getFileStatus()
                || null == fileInfo.getPartitionId()) {
            throw new IllegalArgumentException("FileInfo needs non-null filename, status and partition: got " + fileInfo);
        }
        Map<String, AttributeValue> itemValues = fileInfoFormat.createRecord(setLastUpdateTime(fileInfo, updateTime));
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
        long updateTime = clock.millis();
        for (FileInfo fileInfo : fileInfos) {
            addFile(fileInfo, updateTime);
        }
    }

    @Override
    public void atomicallyUpdateFilesToReadyForGCAndCreateNewActiveFile(
            List<FileInfo> filesToBeMarkedReadyForGC,
            FileInfo newActiveFile) throws StateStoreException {
        // Delete record for file for current status
        long updateTime = clock.millis();
        List<TransactWriteItem> writes = new ArrayList<>();
        setLastUpdateTimes(filesToBeMarkedReadyForGC, updateTime).forEach(fileInfo -> {
            Delete delete = new Delete()
                    .withTableName(activeTableName)
                    .withKey(fileInfoFormat.createActiveFileKey(fileInfo))
                    .withExpressionAttributeNames(Map.of("#status", STATUS))
                    .withConditionExpression("attribute_exists(#status)");
            writes.add(new TransactWriteItem().withDelete(delete));
            Put put = new Put()
                    .withTableName(readyForGCTableName)
                    .withItem(fileInfoFormat.createReadyForGCRecord(fileInfo));
            writes.add(new TransactWriteItem().withPut(put));
        });
        // Add record for file for new status
        Put put = new Put()
                .withTableName(activeTableName)
                .withItem(fileInfoFormat.createActiveFileRecord(setLastUpdateTime(newActiveFile, updateTime)));
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
        long updateTime = clock.millis();
        List<TransactWriteItem> writes = new ArrayList<>();
        setLastUpdateTimes(filesToBeMarkedReadyForGC, updateTime).forEach(fileInfo -> {
            Delete delete = new Delete()
                    .withTableName(activeTableName)
                    .withKey(fileInfoFormat.createActiveFileKey(fileInfo))
                    .withExpressionAttributeNames(Map.of("#status", STATUS))
                    .withConditionExpression("attribute_exists(#status)");
            writes.add(new TransactWriteItem().withDelete(delete));
            Put put = new Put()
                    .withTableName(readyForGCTableName)
                    .withItem(fileInfoFormat.createReadyForGCRecord(fileInfo));
            writes.add(new TransactWriteItem().withPut(put));
        });
        // Add record for file for new status
        Put put = new Put()
                .withTableName(activeTableName)
                .withItem(fileInfoFormat.createActiveFileRecord(setLastUpdateTime(leftFileInfo, updateTime)));
        writes.add(new TransactWriteItem().withPut(put));
        Put put2 = new Put()
                .withTableName(activeTableName)
                .withItem(fileInfoFormat.createActiveFileRecord(setLastUpdateTime(rightFileInfo, updateTime)));
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
        long updateTime = clock.millis();
        setLastUpdateTimes(files, updateTime).forEach(fileInfo -> {
            Map<String, String> attributeNames;
            Map<String, AttributeValue> attributeValues;
            String conditionExpression;
            if (fileInfo.getFileStatus() == ACTIVE) {
                attributeNames = Map.of(
                        "#partitionidandfilename", PARTITION_ID_AND_FILENAME,
                        "#jobid", JOB_ID);
                attributeValues = Map.of(
                        ":partitionidandfilename", new AttributeValue().withS(getActiveFileSortKey(fileInfo)));
                conditionExpression = "#partitionidandfilename=:partitionidandfilename and attribute_not_exists(#jobid)";
            } else {
                attributeNames = Map.of(
                        "#filename", FILENAME,
                        "#partitionid", PARTITION_ID,
                        "#jobid", JOB_ID);
                attributeValues = Map.of(
                        ":filename", new AttributeValue().withS(fileInfo.getFilename()),
                        ":partitionid", new AttributeValue().withS(fileInfo.getPartitionId()));
                conditionExpression = "#filename=:filename and #partitionid=:partitionid and attribute_not_exists(#jobid)";
            }
            Put put = new Put()
                    .withTableName(activeTableName)
                    .withItem(fileInfoFormat.createRecordWithJobId(fileInfo, jobId))
                    .withExpressionAttributeNames(attributeNames)
                    .withExpressionAttributeValues(attributeValues)
                    .withConditionExpression(conditionExpression);
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
                .withKey(fileInfoFormat.createReadyForGCKey(fileInfo))
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
                    .withKeyConditionExpression("#TableId = :table_id")
                    .withExpressionAttributeNames(Map.of("#TableId", TABLE_ID))
                    .withLimit(pageLimit)
                    .withExpressionAttributeValues(new DynamoDBRecordBuilder()
                            .string(":table_id", sleeperTableId)
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
                 | InternalServerErrorException e) {
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
                        "#TableId", TABLE_ID,
                        "#LastUpdateTime", LAST_UPDATE_TIME))
                .withExpressionAttributeValues(new DynamoDBRecordBuilder()
                        .string(":table_id", sleeperTableId)
                        .number(":delete_time", deleteTime)
                        .build())
                .withKeyConditionExpression("#TableId = :table_id")
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
                }).map(fileInfoFormat::getFileInfoFromAttributeValues).iterator();
    }

    @Override
    public List<FileInfo> getActiveFilesWithNoJobId() throws StateStoreException {
        try {
            QueryRequest queryRequest = new QueryRequest()
                    .withTableName(activeTableName)
                    .withConsistentRead(stronglyConsistentReads)
                    .withExpressionAttributeNames(Map.of(
                            "#TableId", TABLE_ID,
                            "#JobId", JOB_ID))
                    .withExpressionAttributeValues(new DynamoDBRecordBuilder()
                            .string(":table_id", sleeperTableId)
                            .build())
                    .withKeyConditionExpression("#TableId = :table_id")
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
                 | InternalServerErrorException e) {
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
                .withExpressionAttributeNames(Map.of("#TableId", TABLE_ID))
                .withExpressionAttributeValues(new DynamoDBRecordBuilder()
                        .string(":table_id", sleeperTableId)
                        .build())
                .withKeyConditionExpression("#TableId = :table_id")
                .withConsistentRead(stronglyConsistentReads)
                .withLimit(1)
                .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL));
        LOGGER.debug("Scanned for any file in table {}, capacity consumed = {}", tableName, result.getConsumedCapacity().getCapacityUnits());
        return result.getItems().isEmpty();
    }

    @Override
    public void clearTable() {
        clearDynamoTable(activeTableName, fileInfoFormat::getActiveFileKey);
        clearDynamoTable(readyForGCTableName, fileInfoFormat::getReadyForGCKey);
    }

    private void clearDynamoTable(String dynamoTableName, UnaryOperator<Map<String, AttributeValue>> getKey) {
        deleteAllDynamoTableItems(dynamoDB, new QueryRequest().withTableName(dynamoTableName)
                        .withExpressionAttributeNames(Map.of("#TableId", TABLE_ID))
                        .withExpressionAttributeValues(new DynamoDBRecordBuilder()
                                .string(":table_id", sleeperTableId)
                                .build())
                        .withKeyConditionExpression("#TableId = :table_id"),
                getKey);
    }

    /**
     * Used to set the current time. Should only be called during tests.
     *
     * @param now Time to set to be the current time
     */
    public void fixTime(Instant now) {
        clock = Clock.fixed(now, ZoneId.of("UTC"));
    }

    private FileInfo setLastUpdateTime(FileInfo fileInfo, long updateTime) {
        return fileInfo.toBuilder().lastStateStoreUpdateTime(updateTime).build();
    }

    private Stream<FileInfo> setLastUpdateTimes(List<FileInfo> fileInfos, long updateTime) {
        return fileInfos.stream().map(fileInfo -> setLastUpdateTime(fileInfo, updateTime));
    }

    static final class Builder {
        private AmazonDynamoDB dynamoDB;
        private String activeTableName;
        private String readyForGCTableName;
        private String sleeperTableId;
        private boolean stronglyConsistentReads;
        private int garbageCollectorDelayBeforeDeletionInMinutes;
        private Integer pageLimit;

        private Builder() {
        }

        Builder dynamoDB(AmazonDynamoDB dynamoDB) {
            this.dynamoDB = dynamoDB;
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

        Builder sleeperTableId(String sleeperTableId) {
            this.sleeperTableId = sleeperTableId;
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

        Builder pageLimit(Integer pageLimit) {
            this.pageLimit = pageLimit;
            return this;
        }

        DynamoDBFileInfoStore build() {
            return new DynamoDBFileInfoStore(this);
        }
    }
}
