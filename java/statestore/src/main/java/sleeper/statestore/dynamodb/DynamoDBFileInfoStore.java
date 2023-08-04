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
import com.amazonaws.services.dynamodbv2.model.RequestLimitExceededException;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.amazonaws.services.dynamodbv2.model.ReturnConsumedCapacity;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
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

import static sleeper.dynamodb.tools.DynamoDBAttributes.createStringAttribute;
import static sleeper.dynamodb.tools.DynamoDBUtils.streamPagedResults;
import static sleeper.core.statestore.FileInfo.FileStatus.ACTIVE;
import static sleeper.core.statestore.FileInfo.FileStatus.READY_FOR_GARBAGE_COLLECTION;
import static sleeper.statestore.dynamodb.DynamoDBFileInfoFormat.JOB_ID;
import static sleeper.statestore.dynamodb.DynamoDBFileInfoFormat.LAST_UPDATE_TIME;
import static sleeper.statestore.dynamodb.DynamoDBFileInfoFormat.PARTITION;
import static sleeper.statestore.dynamodb.DynamoDBFileInfoFormat.STATUS;
import static sleeper.statestore.dynamodb.DynamoDBStateStore.FILE_NAME;

public class DynamoDBFileInfoStore implements FileInfoStore {

    private static final Logger LOGGER = LoggerFactory.getLogger(DynamoDBFileInfoStore.class);

    private final AmazonDynamoDB dynamoDB;
    private final Schema schema;
    private final String activeTablename;
    private final String readyForGCTablename;
    private final boolean stronglyConsistentReads;
    private final int garbageCollectorDelayBeforeDeletionInMinutes;
    private final DynamoDBFileInfoFormat fileInfoFormat;
    private Clock clock = Clock.systemUTC();

    private DynamoDBFileInfoStore(Builder builder) {
        dynamoDB = Objects.requireNonNull(builder.dynamoDB, "dynamoDB must not be null");
        schema = Objects.requireNonNull(builder.schema, "schema must not be null");
        activeTablename = Objects.requireNonNull(builder.activeTablename, "activeTablename must not be null");
        readyForGCTablename = Objects.requireNonNull(builder.readyForGCTablename, "readyForGCTablename must not be null");
        stronglyConsistentReads = builder.stronglyConsistentReads;
        garbageCollectorDelayBeforeDeletionInMinutes = builder.garbageCollectorDelayBeforeDeletionInMinutes;
        fileInfoFormat = new DynamoDBFileInfoFormat(schema);
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
        Map<String, AttributeValue> itemValues = fileInfoFormat.createRecord(fileInfo);
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
            return activeTablename;
        } else {
            return readyForGCTablename;
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
        for (FileInfo fileInfo : filesToBeMarkedReadyForGC) {
            Map<String, AttributeValue> key = new HashMap<>();
            key.put(FILE_NAME, createStringAttribute(fileInfo.getFilename()));
            Map<String, String> expressionAttributeNames = new HashMap<>();
            expressionAttributeNames.put("#status", STATUS);
            Delete delete = new Delete()
                    .withTableName(activeTablename)
                    .withKey(key)
                    .withExpressionAttributeNames(expressionAttributeNames)
                    .withConditionExpression("attribute_exists(#status)");
            writes.add(new TransactWriteItem().withDelete(delete));
            Map<String, AttributeValue> newItem = fileInfoFormat.createRecordWithStatus(fileInfo, READY_FOR_GARBAGE_COLLECTION);
            Put put = new Put()
                    .withTableName(readyForGCTablename)
                    .withItem(newItem);
            writes.add(new TransactWriteItem().withPut(put));
        }
        // Add record for file for new status
        Map<String, AttributeValue> newItem = fileInfoFormat.createRecordWithStatus(newActiveFile, ACTIVE);
        Put put = new Put()
                .withTableName(activeTablename)
                .withItem(newItem);
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
        for (FileInfo fileInfo : filesToBeMarkedReadyForGC) {
            Map<String, AttributeValue> key = new HashMap<>();
            key.put(FILE_NAME, createStringAttribute(fileInfo.getFilename()));
            Map<String, String> expressionAttributeNames = new HashMap<>();
            expressionAttributeNames.put("#status", STATUS);
            Delete delete = new Delete()
                    .withTableName(activeTablename)
                    .withKey(key)
                    .withExpressionAttributeNames(expressionAttributeNames)
                    .withConditionExpression("attribute_exists(#status)");
            writes.add(new TransactWriteItem().withDelete(delete));
            Map<String, AttributeValue> newItem = fileInfoFormat.createRecordWithStatus(fileInfo, READY_FOR_GARBAGE_COLLECTION);
            Put put = new Put()
                    .withTableName(readyForGCTablename)
                    .withItem(newItem);
            writes.add(new TransactWriteItem().withPut(put));
        }
        // Add record for file for new status
        Map<String, AttributeValue> newKeyLeftFile = fileInfoFormat.createRecordWithStatus(leftFileInfo, ACTIVE);
        Put put = new Put()
                .withTableName(activeTablename)
                .withItem(newKeyLeftFile);
        writes.add(new TransactWriteItem().withPut(put));
        Map<String, AttributeValue> newKeyRightFile = fileInfoFormat.createRecordWithStatus(rightFileInfo, ACTIVE);
        Put put2 = new Put()
                .withTableName(activeTablename)
                .withItem(newKeyRightFile);
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
        for (FileInfo fileInfo : files) {
            Map<String, AttributeValue> fileAttributeValues = fileInfoFormat.createRecordWithJobId(fileInfo, jobId);
            Map<String, String> expressionAttributeNames = new HashMap<>();
            expressionAttributeNames.put("#filename", FILE_NAME);
            expressionAttributeNames.put("#partitionid", PARTITION);
            expressionAttributeNames.put("#jobid", JOB_ID);
            Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();
            expressionAttributeValues.put(":filename", new AttributeValue().withS(fileInfo.getFilename()));
            expressionAttributeValues.put(":partitionid", new AttributeValue().withS(fileInfo.getPartitionId()));
            Put put = new Put()
                    .withTableName(activeTablename)
                    .withItem(fileAttributeValues)
                    .withExpressionAttributeNames(expressionAttributeNames)
                    .withExpressionAttributeValues(expressionAttributeValues)
                    .withConditionExpression("#filename=:filename and #partitionid=:partitionid and attribute_not_exists(#jobid)");
            writes.add(new TransactWriteItem().withPut(put));
        }
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
    public void deleteReadyForGCFile(FileInfo fileInfo) throws StateStoreException {
        // Delete record for file for current status
        Map<String, AttributeValue> key = new HashMap<>();
        key.put(FILE_NAME, new AttributeValue(fileInfo.getFilename()));
        DeleteItemRequest deleteItemRequest = new DeleteItemRequest()
                .withTableName(readyForGCTablename)
                .withKey(key)
                .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL);
        DeleteItemResult deleteItemResult = dynamoDB.deleteItem(deleteItemRequest);
        ConsumedCapacity consumedCapacity = deleteItemResult.getConsumedCapacity();
        LOGGER.debug("Deleted file {}, capacity consumed = {}",
                fileInfo.getFilename(), consumedCapacity.getCapacityUnits());
    }

    @Override
    public List<FileInfo> getActiveFiles() throws StateStoreException {
        try {
            ScanRequest scanRequest = new ScanRequest()
                    .withTableName(activeTablename)
                    .withConsistentRead(stronglyConsistentReads)
                    .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL);

            AtomicReference<Double> totalCapacity = new AtomicReference<>(0.0D);
            List<Map<String, AttributeValue>> results = scanTrackingCapacity(scanRequest, totalCapacity);
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
        ScanRequest scanRequest = new ScanRequest()
                .withTableName(readyForGCTablename)
                .withConsistentRead(stronglyConsistentReads)
                .withExpressionAttributeValues(Map.of(":deletetime", new AttributeValue().withN("" + deleteTime)))
                .withFilterExpression(LAST_UPDATE_TIME + " < :deletetime")
                .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL);
        AtomicReference<Double> totalCapacity = new AtomicReference<>(0.0D);
        return streamPagedResults(dynamoDB, scanRequest)
                .flatMap(result -> {
                    double newConsumed = totalCapacity.updateAndGet(old ->
                            old + result.getConsumedCapacity().getCapacityUnits());
                    LOGGER.debug("Scanned table {} for all ready for GC files, capacity consumed = {}",
                            readyForGCTablename, newConsumed);
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
            ScanRequest scanRequest = new ScanRequest()
                    .withTableName(activeTablename)
                    .withConsistentRead(stronglyConsistentReads)
                    .withFilterExpression("attribute_not_exists(" + JOB_ID + ")")
                    .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL);
            AtomicReference<Double> totalCapacity = new AtomicReference<>(0.0D);
            List<Map<String, AttributeValue>> results = scanTrackingCapacity(scanRequest, totalCapacity);
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

    private List<Map<String, AttributeValue>> scanTrackingCapacity(
            ScanRequest scanRequest, AtomicReference<Double> totalCapacity) {
        return streamPagedResults(dynamoDB, scanRequest)
                .flatMap(result -> {
                    totalCapacity.updateAndGet(old -> old + result.getConsumedCapacity().getCapacityUnits());
                    return result.getItems().stream();
                }).collect(Collectors.toList());
    }

    @Override
    public void initialise() throws StateStoreException {
    }

    /**
     * Used to set the current time. Should only be called during tests.
     *
     * @param now Time to set to be the current time
     */
    public void fixTime(Instant now) {
        clock = Clock.fixed(now, ZoneId.of("UTC"));
    }

    public static final class Builder {
        private AmazonDynamoDB dynamoDB;
        private Schema schema;
        private String activeTablename;
        private String readyForGCTablename;
        private boolean stronglyConsistentReads;
        private int garbageCollectorDelayBeforeDeletionInMinutes;

        private Builder() {
        }

        public Builder dynamoDB(AmazonDynamoDB dynamoDB) {
            this.dynamoDB = dynamoDB;
            return this;
        }

        public Builder schema(Schema schema) {
            this.schema = schema;
            return this;
        }

        public Builder activeTablename(String activeTablename) {
            this.activeTablename = activeTablename;
            return this;
        }

        public Builder readyForGCTablename(String readyForGCTablename) {
            this.readyForGCTablename = readyForGCTablename;
            return this;
        }

        public Builder stronglyConsistentReads(boolean stronglyConsistentReads) {
            this.stronglyConsistentReads = stronglyConsistentReads;
            return this;
        }

        public Builder garbageCollectorDelayBeforeDeletionInMinutes(int garbageCollectorDelayBeforeDeletionInMinutes) {
            this.garbageCollectorDelayBeforeDeletionInMinutes = garbageCollectorDelayBeforeDeletionInMinutes;
            return this;
        }

        public DynamoDBFileInfoStore build() {
            return new DynamoDBFileInfoStore(this);
        }
    }
}
