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
import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.core.schema.Schema;
import sleeper.dynamodb.tools.DynamoDBAttributes;
import sleeper.statestore.FileInfo;
import sleeper.statestore.FileInfoStore;
import sleeper.statestore.StateStoreException;

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
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static sleeper.dynamodb.tools.DynamoDBAttributes.createStringAttribute;
import static sleeper.dynamodb.tools.DynamoDBUtils.streamPagedResults;
import static sleeper.statestore.FileInfo.FileStatus.ACTIVE;
import static sleeper.statestore.FileInfo.FileStatus.FILE_IN_PARTITION;
import static sleeper.statestore.dynamodb.DynamoDBFileInfoFormat.JOB_ID;
import static sleeper.statestore.dynamodb.DynamoDBFileInfoFormat.LAST_UPDATE_TIME;
import static sleeper.statestore.dynamodb.DynamoDBFileInfoFormat.STATUS;
import static sleeper.statestore.dynamodb.DynamoDBStateStore.FILE_NAME;
import static sleeper.statestore.dynamodb.DynamoDBStateStore.PARTITION_ID;

public class DynamoDBFileInfoStore implements FileInfoStore {
    private static final Logger LOGGER = LoggerFactory.getLogger(DynamoDBFileInfoStore.class);

    private final AmazonDynamoDB dynamoDB;
    private final Schema schema;
    private final String fileInPartitionTablename;
    private final String fileLifecycleTablename;
    private final boolean stronglyConsistentReads;
    private final int garbageCollectorDelayBeforeDeletionInMinutes;
    private final DynamoDBFileInfoFormat fileInfoFormat;
    private Clock clock = Clock.systemUTC();

    private DynamoDBFileInfoStore(Builder builder) {
        dynamoDB = Objects.requireNonNull(builder.dynamoDB, "dynamoDB must not be null");
        schema = Objects.requireNonNull(builder.schema, "schema must not be null");
        fileInPartitionTablename = Objects.requireNonNull(builder.fileInPartitionTablename, "fileInPartitionTablename must not be null");
        fileLifecycleTablename = Objects.requireNonNull(builder.fileLifecycleTablename, "fileLifecycleTablename must not be null");
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
                || null == fileInfo.getPartitionId()
                || null == fileInfo.getNumberOfRecords()) {
            throw new IllegalArgumentException("FileInfo needs non-null filename, partition, number of records: got " + fileInfo);
        }

        Map<String, AttributeValue> itemValuesFileInPartition = fileInfoFormat.createRecordWithStatus(fileInfo, FILE_IN_PARTITION);
        Map<String, AttributeValue> itemValuesFileLifecycle = fileInfoFormat.createRecordWithStatus(fileInfo, ACTIVE);
        try {
            List<TransactWriteItem> writes = new ArrayList<>();
            Put fileInPartitionPut = new Put()
                .withItem(itemValuesFileInPartition)
                .withTableName(fileInPartitionTablename);
            writes.add(new TransactWriteItem().withPut(fileInPartitionPut));
            Put fileLifecyclePut = new Put()
                .withItem(itemValuesFileLifecycle)
                .withTableName(fileLifecycleTablename);
            writes.add(new TransactWriteItem().withPut(fileLifecyclePut));
            TransactWriteItemsRequest transactWriteItemsRequest = new TransactWriteItemsRequest()
                .withTransactItems(writes)
                .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL);
            TransactWriteItemsResult transactWriteItemsResult = dynamoDB.transactWriteItems(transactWriteItemsRequest);
            List<ConsumedCapacity> consumedCapacity = transactWriteItemsResult.getConsumedCapacity();
            double totalConsumed = consumedCapacity.stream().mapToDouble(ConsumedCapacity::getCapacityUnits).sum();
            LOGGER.debug("Added FileInfo for file {} to file-in-partition and file-lifecycle tables, capacity consumed = {}",
                        fileInfo.getFilename(), totalConsumed);
        } catch (TransactionCanceledException | ResourceNotFoundException
                    | TransactionInProgressException | IdempotentParameterMismatchException
                    | ProvisionedThroughputExceededException | InternalServerErrorException e) {
            throw new StateStoreException(e);
        }
    }

    @Override
    public void addFiles(List<FileInfo> fileInfos) throws StateStoreException {
        for (FileInfo fileInfo : fileInfos) {
            addFile(fileInfo);
        }
    }

    // TODO Is this method needed?
    // @Override
    // public void setStatusToReadyForGarbageCollection(String filename) throws StateStoreException {
    //     Map<String, AttributeValue> key = new HashMap<>();
    //     key.put(DynamoDBStateStore.FILE_NAME, DynamoDBAttributes.createStringAttribute(filename));
    //     Map<String, String> expressionAttributeNames = new HashMap<>();
    //     expressionAttributeNames.put("#status", DynamoDBFileInfoFormat.STATUS);
    //     expressionAttributeNames.put("#lastupdatetime", DynamoDBFileInfoFormat.LAST_UPDATE_TIME);
    //     Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();
    //     expressionAttributeValues.put(":status", DynamoDBAttributes.createStringAttribute(FileInfo.FileStatus.READY_FOR_GARBAGE_COLLECTION.toString()));
    //     expressionAttributeValues.put(":lastupdatetime", DynamoDBAttributes.createNumberAttribute(Instant.now().toEpochMilli()));
    //     UpdateItemRequest updateItemRequest = new UpdateItemRequest()
    //         .withTableName(fileLifecycleTablename)
    //         .withKey(key)
    //         .withExpressionAttributeNames(expressionAttributeNames)
    //         .withExpressionAttributeValues(expressionAttributeValues)
    //         .withUpdateExpression("SET #status = :status, #lastupdatetime = :lastupdatetime");
    //     try {
    //         dynamoDB.updateItem(updateItemRequest);
    //     } catch (ConditionalCheckFailedException | ProvisionedThroughputExceededException | ResourceNotFoundException
    //         | ItemCollectionSizeLimitExceededException | TransactionConflictException | RequestLimitExceededException
    //         | InternalServerErrorException e) {
    //         throw new StateStoreException(e);
    //     }
    // }

    // @Override
    // public void setStatusToReadyForGarbageCollection(List<String> filenames) throws StateStoreException {
    //     for (String filename : filenames) {
    //         setStatusToReadyForGarbageCollection(filename);
    //     }
    // }

    @Override
    public void atomicallyRemoveFileInPartitionRecordsAndCreateNewActiveFile(
            List<FileInfo> fileInPartitionRecordsToBeDeleted,
            FileInfo newActiveFile) throws StateStoreException {
        // Delete record for file for current status
        List<TransactWriteItem> writes = new ArrayList<>();
        for (FileInfo fileInfo : fileInPartitionRecordsToBeDeleted) {
            Map<String, AttributeValue> key = new HashMap<>();
            key.put(FILE_NAME, createStringAttribute(fileInfo.getFilename()));
            key.put(PARTITION_ID, createStringAttribute(fileInfo.getPartitionId()));
            Map<String, String> expressionAttributeNames = new HashMap<>();
            expressionAttributeNames.put("#status", STATUS);
            Delete delete = new Delete()
                    .withTableName(fileInPartitionTablename)
                    .withKey(key)
                    .withExpressionAttributeNames(expressionAttributeNames)
                    .withConditionExpression("attribute_exists(#status)");
            writes.add(new TransactWriteItem().withDelete(delete));
        }
        // Add record for file for new status
        Map<String, AttributeValue> newItemFileInPartition = fileInfoFormat.createRecordWithStatus(newActiveFile, FILE_IN_PARTITION);
        Map<String, AttributeValue> newItemFileLifecycle = fileInfoFormat.createRecordWithStatus(newActiveFile, ACTIVE);
        Put fileInPartitionPut = new Put()
                .withTableName(fileInPartitionTablename)
                .withItem(newItemFileInPartition);
        Put fileLifecyclePut = new Put()
                .withTableName(fileLifecycleTablename)
                .withItem(newItemFileLifecycle);
        writes.add(new TransactWriteItem().withPut(fileInPartitionPut));
        writes.add(new TransactWriteItem().withPut(fileLifecyclePut));
        TransactWriteItemsRequest transactWriteItemsRequest = new TransactWriteItemsRequest()
                .withTransactItems(writes)
                .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL);
        try {
            TransactWriteItemsResult transactWriteItemsResult = dynamoDB.transactWriteItems(transactWriteItemsRequest);
            List<ConsumedCapacity> consumedCapacity = transactWriteItemsResult.getConsumedCapacity();
            double totalConsumed = consumedCapacity.stream().mapToDouble(ConsumedCapacity::getCapacityUnits).sum();
            LOGGER.debug("Removed file-in-partition records for {} files and added active file to file-in-partition and file lifecycle tables, capacity consumed = {}",
                fileInPartitionRecordsToBeDeleted.size(), totalConsumed);
        } catch (TransactionCanceledException | ResourceNotFoundException
                 | TransactionInProgressException | IdempotentParameterMismatchException
                 | ProvisionedThroughputExceededException | InternalServerErrorException e) {
            throw new StateStoreException(e);
        }
    }

    @Override
    public void atomicallyRemoveFileInPartitionRecordsAndCreateNewActiveFiles(
            List<FileInfo> fileInPartitionRecordsToBeDeleted, FileInfo leftFileInfo, FileInfo rightFileInfo) throws StateStoreException {
        // Delete record for file for current status
        List<TransactWriteItem> writes = new ArrayList<>();
        for (FileInfo fileInfo : fileInPartitionRecordsToBeDeleted) {
            Map<String, AttributeValue> key = new HashMap<>();
            key.put(FILE_NAME, createStringAttribute(fileInfo.getFilename()));
            key.put(PARTITION_ID, createStringAttribute(fileInfo.getPartitionId()));
            Map<String, String> expressionAttributeNames = new HashMap<>();
            expressionAttributeNames.put("#status", STATUS);
            Delete delete = new Delete()
                    .withTableName(fileInPartitionTablename)
                    .withKey(key)
                    .withExpressionAttributeNames(expressionAttributeNames)
                    .withConditionExpression("attribute_exists(#status)");
            writes.add(new TransactWriteItem().withDelete(delete));
        }
        // Add record for file for new status
        Map<String, AttributeValue> newItemFileInPartitionLeftFile = fileInfoFormat.createRecordWithStatus(leftFileInfo, FILE_IN_PARTITION);
        Map<String, AttributeValue> newItemFileLifecycleLeftFile = fileInfoFormat.createRecordWithStatus(leftFileInfo, ACTIVE);
        Put putFileInPartitionLeftFile = new Put()
                .withTableName(fileInPartitionTablename)
                .withItem(newItemFileInPartitionLeftFile);
        Put putFileLifecycleLeftFile = new Put()
                .withTableName(fileLifecycleTablename)
                .withItem(newItemFileLifecycleLeftFile);
        writes.add(new TransactWriteItem().withPut(putFileInPartitionLeftFile));
        writes.add(new TransactWriteItem().withPut(putFileLifecycleLeftFile));

        Map<String, AttributeValue> newItemFileInPartitionRightFile = fileInfoFormat.createRecordWithStatus(rightFileInfo, FILE_IN_PARTITION);
        Map<String, AttributeValue> newItemFileLifecycleRightFile = fileInfoFormat.createRecordWithStatus(rightFileInfo, ACTIVE);
        Put putFileInPartitionRightFile = new Put()
                .withTableName(fileInPartitionTablename)
                .withItem(newItemFileInPartitionRightFile);
        Put putFileLifecycleRightFile = new Put()
                .withTableName(fileLifecycleTablename)
                .withItem(newItemFileLifecycleRightFile);
        writes.add(new TransactWriteItem().withPut(putFileInPartitionRightFile));
        writes.add(new TransactWriteItem().withPut(putFileLifecycleRightFile));
        TransactWriteItemsRequest transactWriteItemsRequest = new TransactWriteItemsRequest()
                .withTransactItems(writes)
                .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL);
        try {
            TransactWriteItemsResult transactWriteItemsResult = dynamoDB.transactWriteItems(transactWriteItemsRequest);
            List<ConsumedCapacity> consumedCapacity = transactWriteItemsResult.getConsumedCapacity();
            double totalConsumed = consumedCapacity.stream().mapToDouble(ConsumedCapacity::getCapacityUnits).sum();
            LOGGER.debug("Removed file-in-partition records for {} files and added active file to file-in-partition and file lifecycle tables, capacity consumed = {}",
                fileInPartitionRecordsToBeDeleted.size(), totalConsumed);
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
        // Create Puts for each of the files, conditional on the compactionJob field being not present
        for (FileInfo fileInfo : files) {
            Map<String, AttributeValue> fileAttributeValues = fileInfoFormat.createRecordWithJobId(fileInfo, jobId);
            Map<String, String> expressionAttributeNames = new HashMap<>();
            expressionAttributeNames.put("#jobid", JOB_ID);
            Put put = new Put()
                    .withTableName(fileInPartitionTablename)
                    .withItem(fileAttributeValues)
                    .withExpressionAttributeNames(expressionAttributeNames)
                    .withConditionExpression("attribute_not_exists(#jobid)");
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
    public void deleteFileLifecycleEntries(List<String> filenames) throws StateStoreException {
        for (String filename : filenames) {
            Map<String, AttributeValue> key = new HashMap<>();
            key.put(FILE_NAME, new AttributeValue(filename));
            DeleteItemRequest deleteItemRequest = new DeleteItemRequest()
                    .withTableName(fileLifecycleTablename)
                    .withKey(key)
                    .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL);
            DeleteItemResult deleteItemResult = dynamoDB.deleteItem(deleteItemRequest);
            ConsumedCapacity consumedCapacity = deleteItemResult.getConsumedCapacity();
            LOGGER.debug("Deleted file {}, capacity consumed = {}",
                    filename, consumedCapacity.getCapacityUnits());
        }
    }

    @Override
    public List<FileInfo> getFileInPartitionList() throws StateStoreException {
        return getFileInfosFromTable(fileInPartitionTablename);
    }

    @Override
    public List<FileInfo> getFileLifecycleList() throws StateStoreException {
        return getFileInfosFromTable(fileLifecycleTablename);
    }

    private List<FileInfo> getFileInfosFromTable(String tablename) throws StateStoreException {
        try {
            ScanRequest scanRequest = new ScanRequest()
                    .withTableName(tablename)
                    .withConsistentRead(stronglyConsistentReads)
                    .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL);
            AtomicReference<Double> totalCapacity = new AtomicReference<>(0.0D);
            List<Map<String, AttributeValue>> results = scanTrackingCapacity(scanRequest, totalCapacity);
            LOGGER.debug("Scanned table {}, capacity consumed = {}", tablename, totalCapacity.get());
            List<FileInfo> fileInfoResults = new ArrayList<>();
            for (Map<String, AttributeValue> map : results) {
                fileInfoResults.add(fileInfoFormat.getFileInfoFromAttributeValues(map));
            }
            return fileInfoResults;
        } catch (ProvisionedThroughputExceededException | ResourceNotFoundException | RequestLimitExceededException
                 | InternalServerErrorException | IOException e) {
            throw new StateStoreException("Exception querying DynamoDB table " + tablename, e);
        }
    }

    @Override
    public List<FileInfo> getActiveFileList() throws StateStoreException {
        Map<String, String> expressionAttributeNames = new HashMap<>();
        expressionAttributeNames.put("#status", STATUS);
        Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();
        expressionAttributeValues.put(":active", new AttributeValue().withS(FileInfo.FileStatus.ACTIVE.toString()));
        ScanRequest scanRequest = new ScanRequest()
                .withTableName(fileLifecycleTablename)
                .withConsistentRead(stronglyConsistentReads)
                .withExpressionAttributeNames(expressionAttributeNames)
                .withExpressionAttributeValues(expressionAttributeValues)
                .withFilterExpression("#status = :active")
                .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL);
        AtomicReference<Double> totalCapacity = new AtomicReference<>(0.0D);
        return streamPagedResults(dynamoDB, scanRequest)
                .flatMap(result -> {
                    double newConsumed = totalCapacity.updateAndGet(old ->
                            old + result.getConsumedCapacity().getCapacityUnits());
                    LOGGER.debug("Scanned table {} for all ready for GC files, capacity consumed = {}",
                            fileLifecycleTablename, newConsumed);
                    return result.getItems().stream();
                }).map(item -> {
                    try {
                        return fileInfoFormat.getFileInfoFromAttributeValues(item);
                    } catch (IOException e) {
                        throw new RuntimeException("IOException creating FileInfo from attribute values");
                    }
                })
                .collect(Collectors.toList());
    }

    @Override
    public Iterator<String> getReadyForGCFiles() {
        return getReadyForGCFileInfosStream().map(FileInfo::getFilename).iterator();
    }

    @Override
    public Iterator<FileInfo> getReadyForGCFileInfos() {
        return getReadyForGCFileInfosStream().iterator();
    }

    private Stream<FileInfo> getReadyForGCFileInfosStream() {
        long delayInMilliseconds = 1000L * 60L * garbageCollectorDelayBeforeDeletionInMinutes;
        long deleteTime = System.currentTimeMillis() - delayInMilliseconds;
        Map<String, String> expressionAttributeNames = new HashMap<>();
        expressionAttributeNames.put("#status", STATUS);
        Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();
        expressionAttributeValues.put(":deletetime", new AttributeValue().withN("" + deleteTime));
        expressionAttributeValues.put(":readyforgc", new AttributeValue().withS(FileInfo.FileStatus.READY_FOR_GARBAGE_COLLECTION.toString()));
        ScanRequest scanRequest = new ScanRequest()
                .withTableName(fileLifecycleTablename)
                .withConsistentRead(stronglyConsistentReads)
                .withExpressionAttributeNames(expressionAttributeNames)
                .withExpressionAttributeValues(expressionAttributeValues)
                .withFilterExpression("#status = :readyforgc AND " + LAST_UPDATE_TIME + " < :deletetime")
                .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL);
        AtomicReference<Double> totalCapacity = new AtomicReference<>(0.0D);
        return streamPagedResults(dynamoDB, scanRequest)
                .flatMap(result -> {
                    double newConsumed = totalCapacity.updateAndGet(old ->
                            old + result.getConsumedCapacity().getCapacityUnits());
                    LOGGER.debug("Scanned table {} for all ready for GC files, capacity consumed = {}",
                            fileLifecycleTablename, newConsumed);
                    return result.getItems().stream();
                }).map(item -> {
                    try {
                        return fileInfoFormat.getFileInfoFromAttributeValues(item);
                    } catch (IOException e) {
                        throw new RuntimeException("IOException creating FileInfo from attribute values");
                    }
                });
    }

    @Override
    public void findFilesThatShouldHaveStatusOfGCPending() throws StateStoreException {
        // List files from file-lifecycle table
        List<FileInfo> fileLifecycleList = getFileLifecycleList();
        Set<String> filenamesFromFileLifecycleList = fileLifecycleList.stream()
            .map(FileInfo::getFilename)
            .collect(Collectors.toSet());
        LOGGER.info("Found {} files in the file-lifecycle table", filenamesFromFileLifecycleList.size());

        // List files from file-in-partition table
        List<FileInfo> fileInPartitionList = getFileInPartitionList();
        Set<String> filenamesFromFileInPartitionList = fileInPartitionList.stream()
            .map(FileInfo::getFilename)
            .collect(Collectors.toSet());
        LOGGER.info("Found {} files in the file-in-partition table", filenamesFromFileInPartitionList.size());

        // Find any files which have a file-lifecycle entry but no file-in-partition entry
        filenamesFromFileLifecycleList.removeAll(filenamesFromFileInPartitionList);
        LOGGER.info("Found {} files which have file-lifecyle entries but no file-in-partition entries", filenamesFromFileLifecycleList.size());

        changeStatusOfFileLifecycleEntriesToGCPending(filenamesFromFileLifecycleList);
        LOGGER.info("Changed status of {} files in file-lifecyle table to GARBAGE_COLLECTION_PENDING", filenamesFromFileLifecycleList.size());
    }

    private void changeStatusOfFileLifecycleEntriesToGCPending(Set<String> filenames) throws StateStoreException {
        if (null == filenames || filenames.isEmpty()) {
            return;
        }

        for (String filename : filenames) {
            changeStatusOfFileLifecycleEntryToGCPending(filename);
        }
    }

    private void changeStatusOfFileLifecycleEntryToGCPending(String filename) throws StateStoreException {
        Map<String, AttributeValue> key = new HashMap<>();
        key.put(DynamoDBStateStore.FILE_NAME, DynamoDBAttributes.createStringAttribute(filename));
        Map<String, String> expressionAttributeNames = new HashMap<>();
        expressionAttributeNames.put("#status", DynamoDBFileInfoFormat.STATUS);
        expressionAttributeNames.put("#lastupdatetime", DynamoDBFileInfoFormat.LAST_UPDATE_TIME);
        Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();
        expressionAttributeValues.put(":status", DynamoDBAttributes.createStringAttribute(FileInfo.FileStatus.GARBAGE_COLLECTION_PENDING.toString()));
        expressionAttributeValues.put(":lastupdatetime", DynamoDBAttributes.createNumberAttribute(Instant.now().toEpochMilli()));
        UpdateItemRequest updateItemRequest = new UpdateItemRequest()
            .withTableName(fileLifecycleTablename)
            .withKey(key)
            .withExpressionAttributeNames(expressionAttributeNames)
            .withExpressionAttributeValues(expressionAttributeValues)
            .withUpdateExpression("SET #status = :status, #lastupdatetime = :lastupdatetime");
        try {
            dynamoDB.updateItem(updateItemRequest);
        } catch (ConditionalCheckFailedException | ProvisionedThroughputExceededException | ResourceNotFoundException
            | ItemCollectionSizeLimitExceededException | TransactionConflictException | RequestLimitExceededException
            | InternalServerErrorException e) {
            throw new StateStoreException(e);
        }
        LOGGER.info("Changed status of file {} to GARBAGE_COLLECTION_PENDING in the file-lifecycle table");
    }

    @Override
    public List<FileInfo> getFileInPartitionInfosWithNoJobId() throws StateStoreException {
        try {
            ScanRequest scanRequest = new ScanRequest()
                    .withTableName(fileInPartitionTablename)
                    .withConsistentRead(stronglyConsistentReads)
                    .withFilterExpression("attribute_not_exists(" + JOB_ID + ")")
                    .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL);
            AtomicReference<Double> totalCapacity = new AtomicReference<>(0.0D);
            List<Map<String, AttributeValue>> results = scanTrackingCapacity(scanRequest, totalCapacity);
            LOGGER.debug("Scanned for file in partition list where files have no job id, capacity consumed = {}", totalCapacity);
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
    public Map<String, List<String>> getPartitionToFileInPartitionMap() throws StateStoreException {
        List<FileInfo> files = getFileInPartitionList();
        Map<String, List<String>> partitionToFiles = new HashMap<>();
        for (FileInfo fileInfo : files) {
            String partition = fileInfo.getPartitionId();
            partitionToFiles.putIfAbsent(partition, new ArrayList<>());
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
        private String fileInPartitionTablename;
        private String fileLifecycleTablename;
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

        public Builder fileInPartitionTablename(String fileInPartitionTablename) {
            this.fileInPartitionTablename = fileInPartitionTablename;
            return this;
        }

        public Builder fileLifecycleTablename(String fileLifecycleTablename) {
            this.fileLifecycleTablename = fileLifecycleTablename;
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
