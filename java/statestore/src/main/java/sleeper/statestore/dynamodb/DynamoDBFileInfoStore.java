/*
 * Copyright 2022 Crown Copyright
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
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.dynamodbv2.model.TransactWriteItem;
import com.amazonaws.services.dynamodbv2.model.TransactWriteItemsRequest;
import com.amazonaws.services.dynamodbv2.model.TransactWriteItemsResult;
import com.amazonaws.services.dynamodbv2.model.TransactionCanceledException;
import com.amazonaws.services.dynamodbv2.model.TransactionConflictException;
import com.amazonaws.services.dynamodbv2.model.TransactionInProgressException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sleeper.core.schema.Schema;
import sleeper.statestore.FileInfo;
import sleeper.statestore.FileInfoStore;
import sleeper.statestore.StateStoreException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static sleeper.dynamodb.tools.DynamoDBAttributes.createStringAttribute;
import static sleeper.statestore.FileInfo.FileStatus.ACTIVE;
import static sleeper.statestore.FileInfo.FileStatus.READY_FOR_GARBAGE_COLLECTION;
import static sleeper.statestore.dynamodb.DynamoDBFileInfoFormat.JOB_ID;
import static sleeper.statestore.dynamodb.DynamoDBFileInfoFormat.LAST_UPDATE_TIME;
import static sleeper.statestore.dynamodb.DynamoDBFileInfoFormat.STATUS;
import static sleeper.statestore.dynamodb.DynamoDBStateStore.FILE_NAME;

public class DynamoDBFileInfoStore implements FileInfoStore {

    private static final Logger LOGGER = LoggerFactory.getLogger(DynamoDBFileInfoStore.class);

    private final AmazonDynamoDB dynamoDB;
    private final Schema schema;
    private final String activeTablename;
    private final String readyForGCTablename;
    private final boolean stronglyConsistentReads;
    private final int garbageCollectorDelayBeforeDeletionInSeconds;
    private final DynamoDBFileInfoFormat fileInfoFormat;

    private DynamoDBFileInfoStore(Builder builder) {
        dynamoDB = Objects.requireNonNull(builder.dynamoDB, "dynamoDB must not be null");
        schema = Objects.requireNonNull(builder.schema, "schema must not be null");
        activeTablename = Objects.requireNonNull(builder.activeTablename, "activeTablename must not be null");
        readyForGCTablename = Objects.requireNonNull(builder.readyForGCTablename, "readyForGCTablename must not be null");
        stronglyConsistentReads = builder.stronglyConsistentReads;
        garbageCollectorDelayBeforeDeletionInSeconds = builder.garbageCollectorDelayBeforeDeletionInSeconds;
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
            expressionAttributeNames.put("#jobid", JOB_ID);
            Put put = new Put()
                    .withTableName(activeTablename)
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
            double totalCapacity = 0.0D;
            ScanRequest queryRequest = new ScanRequest()
                    .withTableName(activeTablename)
                    .withConsistentRead(stronglyConsistentReads)
                    .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL);
            ScanResult queryResult = dynamoDB.scan(queryRequest);
            totalCapacity += queryResult.getConsumedCapacity().getCapacityUnits();
            List<Map<String, AttributeValue>> results = new ArrayList<>();
            results.addAll(queryResult.getItems());
            while (null != queryResult.getLastEvaluatedKey()) {
                queryRequest = new ScanRequest()
                        .withTableName(activeTablename)
                        .withConsistentRead(stronglyConsistentReads)
                        .withExclusiveStartKey(queryResult.getLastEvaluatedKey())
                        .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL);
                queryResult = dynamoDB.scan(queryRequest);
                totalCapacity += queryResult.getConsumedCapacity().getCapacityUnits();
                results.addAll(queryResult.getItems());
            }
            LOGGER.debug("Scanned for all active files, capacity consumed = {}", totalCapacity);
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
        return new FilesReadyForGCIterator(dynamoDB, readyForGCTablename, fileInfoFormat, garbageCollectorDelayBeforeDeletionInSeconds, stronglyConsistentReads);
    }

    private static class FilesReadyForGCIterator implements Iterator<FileInfo> {
        private final AmazonDynamoDB dynamoDB;
        private final String readyForGCFileInfoTablename;
        private final DynamoDBFileInfoFormat fileInfoFormat;
        private final int delayBeforeGarbageCollectionInSeconds;
        private final boolean stronglyConsistentReads;
        private double totalCapacity;
        private ScanResult scanResult;
        private Iterator<Map<String, AttributeValue>> itemsIterator;

        FilesReadyForGCIterator(AmazonDynamoDB dynamoDB,
                                String readyForGCFileInfoTablename,
                                DynamoDBFileInfoFormat fileInfoFormat,
                                int delayBeforeGarbageCollectionInSeconds,
                                boolean stronglyConsistentReads) {
            this.dynamoDB = dynamoDB;
            this.readyForGCFileInfoTablename = readyForGCFileInfoTablename;
            this.fileInfoFormat = fileInfoFormat;
            this.delayBeforeGarbageCollectionInSeconds = delayBeforeGarbageCollectionInSeconds;
            this.stronglyConsistentReads = stronglyConsistentReads;
            this.totalCapacity = 0.0D;
            long delayInMilliseconds = 1000L * delayBeforeGarbageCollectionInSeconds;
            long deleteTime = System.currentTimeMillis() - delayInMilliseconds;
            Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();
            expressionAttributeValues.put(":deletetime", new AttributeValue().withN("" + deleteTime));
            ScanRequest queryRequest = new ScanRequest()
                    .withTableName(readyForGCFileInfoTablename)
                    .withConsistentRead(stronglyConsistentReads)
                    .withExpressionAttributeValues(expressionAttributeValues)
                    .withFilterExpression(LAST_UPDATE_TIME + " < :deletetime")
                    .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL);
            scanResult = dynamoDB.scan(queryRequest);
            totalCapacity += scanResult.getConsumedCapacity().getCapacityUnits();
            itemsIterator = scanResult.getItems().iterator();
        }

        @Override
        public boolean hasNext() {
            if (itemsIterator.hasNext()) {
                return true;
            }
            if (null != scanResult.getLastEvaluatedKey()) {
                long delayInMilliseconds = 1000L * delayBeforeGarbageCollectionInSeconds;
                long deleteTime = System.currentTimeMillis() - delayInMilliseconds;
                Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();
                expressionAttributeValues.put(":deletetime", new AttributeValue().withN("" + deleteTime));
                ScanRequest queryRequest = new ScanRequest()
                        .withTableName(readyForGCFileInfoTablename)
                        .withConsistentRead(stronglyConsistentReads)
                        .withExpressionAttributeValues(expressionAttributeValues)
                        .withExclusiveStartKey(scanResult.getLastEvaluatedKey())
                        .withFilterExpression(LAST_UPDATE_TIME + " < :deletetime")
                        .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL);
                scanResult = dynamoDB.scan(queryRequest);
                totalCapacity += scanResult.getConsumedCapacity().getCapacityUnits();
                List<Map<String, AttributeValue>> items2 = scanResult.getItems();
                this.itemsIterator = items2.iterator();
                return hasNext();
            }
            LOGGER.debug("Scanned table {} for all ready for GC files, capacity consumed = {}", readyForGCFileInfoTablename, totalCapacity);
            return false;
        }

        @Override
        public FileInfo next() {
            try {
                return fileInfoFormat.getFileInfoFromAttributeValues(itemsIterator.next());
            } catch (IOException e) {
                throw new RuntimeException("IOException creating FileInfo from attribute values");
            }
        }
    }

    @Override
    public List<FileInfo> getActiveFilesWithNoJobId() throws StateStoreException {
        try {
            double totalCapacity = 0.0D;
            ScanRequest scanRequest = new ScanRequest()
                    .withTableName(activeTablename)
                    .withConsistentRead(stronglyConsistentReads)
                    .withFilterExpression("attribute_not_exists(" + JOB_ID + ")")
                    .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL);
            ScanResult queryResult = dynamoDB.scan(scanRequest);
            totalCapacity += queryResult.getConsumedCapacity().getCapacityUnits();
            List<Map<String, AttributeValue>> results = new ArrayList<>(queryResult.getItems());
            while (null != queryResult.getLastEvaluatedKey()) {
                scanRequest = new ScanRequest()
                        .withTableName(activeTablename)
                        .withConsistentRead(stronglyConsistentReads)
                        .withFilterExpression("attribute_not_exists(" + JOB_ID + ")")
                        .withExclusiveStartKey(queryResult.getLastEvaluatedKey())
                        .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL);
                queryResult = dynamoDB.scan(scanRequest);
                totalCapacity += queryResult.getConsumedCapacity().getCapacityUnits();
                results.addAll(queryResult.getItems());
            }
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

    @Override
    public void initialise() throws StateStoreException {
    }

    public static final class Builder {
        private AmazonDynamoDB dynamoDB;
        private Schema schema;
        private String activeTablename;
        private String readyForGCTablename;
        private boolean stronglyConsistentReads;
        private int garbageCollectorDelayBeforeDeletionInSeconds;

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

        public Builder garbageCollectorDelayBeforeDeletionInSeconds(int garbageCollectorDelayBeforeDeletionInSeconds) {
            this.garbageCollectorDelayBeforeDeletionInSeconds = garbageCollectorDelayBeforeDeletionInSeconds;
            return this;
        }

        public DynamoDBFileInfoStore build() {
            return new DynamoDBFileInfoStore(this);
        }
    }
}
