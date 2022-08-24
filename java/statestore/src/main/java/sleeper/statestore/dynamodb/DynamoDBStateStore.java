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
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.key.Key;
import sleeper.core.key.KeySerDe;
import sleeper.core.partition.Partition;
import sleeper.core.partition.PartitionsFromSplitPoints;
import sleeper.core.range.RegionSerDe;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.PrimitiveType;
import sleeper.statestore.FileInfo;
import sleeper.statestore.StateStore;
import sleeper.statestore.StateStoreException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static sleeper.configuration.properties.table.TableProperty.ACTIVE_FILEINFO_TABLENAME;
import static sleeper.configuration.properties.table.TableProperty.DYNAMODB_STRONGLY_CONSISTENT_READS;
import static sleeper.configuration.properties.table.TableProperty.GARBAGE_COLLECTOR_DELAY_BEFORE_DELETION;
import static sleeper.configuration.properties.table.TableProperty.PARTITION_TABLENAME;
import static sleeper.configuration.properties.table.TableProperty.READY_FOR_GC_FILEINFO_TABLENAME;
import static sleeper.statestore.FileInfo.FileStatus.ACTIVE;
import static sleeper.statestore.FileInfo.FileStatus.READY_FOR_GARBAGE_COLLECTION;
import static sleeper.statestore.dynamodb.DynamoDBAttributes.createBinaryAttribute;
import static sleeper.statestore.dynamodb.DynamoDBAttributes.createNumberAttribute;
import static sleeper.statestore.dynamodb.DynamoDBAttributes.createStringAttribute;

/**
 * An implementation of {@link StateStore} that uses DynamoDB to store the state.
 */
public class DynamoDBStateStore implements StateStore {
    private static final Logger LOGGER = LoggerFactory.getLogger(DynamoDBStateStore.class);

    public static final String FILE_NAME = "Name";
    public static final String FILE_STATUS = "Status";
    public static final String FILE_PARTITION = "Partition";
    private static final String NUMBER_LINES = "NumLines";
    private static final String MIN_KEY = "MinKey";
    private static final String MAX_KEY = "MaxKey";
    private static final String LAST_UPDATE_TIME = "LastUpdateTime";
    private static final String JOB_ID = "Job_name";
    public static final String PARTITION_ID = "PartitionId";
    private static final String PARTITION_IS_LEAF = "PartitionIsLeaf";
    private static final String PARTITION_PARENT_ID = "PartitionParentId";
    private static final String PARTITION_CHILD_IDS = "PartitionChildIds";
    private static final String PARTITION_SPLIT_DIMENSION = "PartitionSplitDimension";
    private static final String REGION = "Region";

    private final AmazonDynamoDB dynamoDB;
    private final String activeFileInfoTablename;
    private final String readyForGCFileInfoTablename;
    private final String partitionTableName;
    private final Schema schema;
    private final List<PrimitiveType> rowKeyTypes;
    private final int garbageCollectorDelayBeforeDeletionInSeconds;
    private final boolean stronglyConsistentReads;
    private final KeySerDe keySerDe;
    private final RegionSerDe regionSerDe;

    public DynamoDBStateStore(TableProperties tableProperties, AmazonDynamoDB dynamoDB) {
        this(tableProperties.get(ACTIVE_FILEINFO_TABLENAME),
                tableProperties.get(READY_FOR_GC_FILEINFO_TABLENAME),
                tableProperties.get(PARTITION_TABLENAME),
                tableProperties.getSchema(),
                tableProperties.getInt(GARBAGE_COLLECTOR_DELAY_BEFORE_DELETION),
                tableProperties.getBoolean(DYNAMODB_STRONGLY_CONSISTENT_READS),
                dynamoDB);
    }

    public DynamoDBStateStore(String activeFileInfoTablename,
                              String readyForGCFileInfoTablename,
                              String partitionTablename,
                              Schema schema,
                              int garbageCollectorDelayBeforeDeletionInSeconds,
                              boolean stronglyConsistentReads,
                              AmazonDynamoDB dynamoDB) {
        this.activeFileInfoTablename = Objects.requireNonNull(activeFileInfoTablename, "activeFileInfoTablename must not be null");
        this.readyForGCFileInfoTablename = Objects.requireNonNull(readyForGCFileInfoTablename, "readyForGCFileInfoTablename must not be null");
        this.partitionTableName = Objects.requireNonNull(partitionTablename, "partitionTableName must not be null");
        this.schema = Objects.requireNonNull(schema, "schema must not be null");
        this.rowKeyTypes = schema.getRowKeyTypes();
        if (this.rowKeyTypes.isEmpty()) {
            throw new IllegalArgumentException("rowKeyTypes must not be empty");
        }
        this.garbageCollectorDelayBeforeDeletionInSeconds = garbageCollectorDelayBeforeDeletionInSeconds;
        this.stronglyConsistentReads = stronglyConsistentReads;
        this.dynamoDB = Objects.requireNonNull(dynamoDB, "dynamoDB must not be null");
        this.keySerDe = new KeySerDe(rowKeyTypes);
        this.regionSerDe = new RegionSerDe(schema);
    }

    @Override
    public List<PrimitiveType> getRowKeyTypes() {
        return Collections.unmodifiableList(rowKeyTypes);
    }

    @Override
    public void addFile(FileInfo fileInfo) throws StateStoreException {
        if (null == fileInfo.getFilename()
                || null == fileInfo.getFileStatus()
                || null == fileInfo.getPartitionId()) {
            throw new IllegalArgumentException("FileInfo needs non-null filename, status and partition: got " + fileInfo);
        }
        Map<String, AttributeValue> itemValues = createRecord(fileInfo);
        try {
            PutItemRequest putItemRequest = new PutItemRequest()
                    .withItem(itemValues)
                    .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL);
            if (fileInfo.getFileStatus().equals(FileInfo.FileStatus.ACTIVE)) {
                putItemRequest = putItemRequest.withTableName(activeFileInfoTablename);
            } else {
                putItemRequest = putItemRequest.withTableName(readyForGCFileInfoTablename);
            }
            PutItemResult putItemResult = dynamoDB.putItem(putItemRequest);
            LOGGER.debug("Put file info for file {} to table {}, capacity consumed = {}",
                    fileInfo.getFilename(), activeFileInfoTablename, putItemResult.getConsumedCapacity().getCapacityUnits());
        } catch (ConditionalCheckFailedException | ProvisionedThroughputExceededException | ResourceNotFoundException
                 | ItemCollectionSizeLimitExceededException | TransactionConflictException
                 | RequestLimitExceededException | InternalServerErrorException e) {
            throw new StateStoreException("Exception calling putItem", e);
        }
    }

    @Override
    public void addFiles(List<FileInfo> fileInfos) throws StateStoreException {
        for (FileInfo fileInfo : fileInfos) {
            addFile(fileInfo);
        }
    }

    /**
     * Creates a record with a new status
     *
     * @param fileInfo  the File
     * @param newStatus the new status of that file
     * @return A Dynamo record
     * @throws StateStoreException if the Dynamo record fails to be created
     */
    private Map<String, AttributeValue> createRecordWithStatus(FileInfo fileInfo, FileInfo.FileStatus newStatus) throws StateStoreException {
        Map<String, AttributeValue> record = createRecord(fileInfo);
        record.put(FILE_STATUS, createStringAttribute(newStatus.toString()));
        return record;
    }

    private Map<String, AttributeValue> createRecordWithJobId(FileInfo fileInfo, String jobId) throws StateStoreException {
        Map<String, AttributeValue> record = createRecord(fileInfo);
        record.put(JOB_ID, createStringAttribute(jobId));
        return record;
    }

    /**
     * Creates a record for the DynamoDB state store.
     *
     * @param fileInfo the File which the record is about
     * @return A record in DynamoDB
     * @throws StateStoreException if the record fails to create
     */
    private Map<String, AttributeValue> createRecord(FileInfo fileInfo) throws StateStoreException {
        Map<String, AttributeValue> itemValues = new HashMap<>();

        itemValues.put(FILE_NAME, createStringAttribute(fileInfo.getFilename()));
        itemValues.put(FILE_PARTITION, createStringAttribute(fileInfo.getPartitionId()));
        itemValues.put(FILE_STATUS, createStringAttribute(fileInfo.getFileStatus().toString()));
        if (null != fileInfo.getNumberOfRecords()) {
            itemValues.put(NUMBER_LINES, createNumberAttribute(fileInfo.getNumberOfRecords()));
        }
        try {
            if (null != fileInfo.getMinRowKey()) {
                itemValues.put(MIN_KEY, getAttributeValueFromRowKeys(fileInfo.getMinRowKey()));
            }
            if (null != fileInfo.getMaxRowKey()) {
                itemValues.put(MAX_KEY, getAttributeValueFromRowKeys(fileInfo.getMaxRowKey()));
            }
        } catch (IOException e) {
            throw new StateStoreException("IOException serialising row keys", e);
        }
        if (null != fileInfo.getJobId()) {
            itemValues.put(JOB_ID, createStringAttribute(fileInfo.getJobId()));
        }
        if (null != fileInfo.getLastStateStoreUpdateTime()) {
            itemValues.put(LAST_UPDATE_TIME, createNumberAttribute(fileInfo.getLastStateStoreUpdateTime()));
        }

        return itemValues;
    }

    private AttributeValue getAttributeValueFromRowKeys(Key rowKey) throws IOException {
        return createBinaryAttribute(keySerDe.serialise(rowKey));
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
            expressionAttributeNames.put("#status", FILE_STATUS);
            Delete delete = new Delete()
                    .withTableName(activeFileInfoTablename)
                    .withKey(key)
                    .withExpressionAttributeNames(expressionAttributeNames)
                    .withConditionExpression("attribute_exists(#status)");
            writes.add(new TransactWriteItem().withDelete(delete));
            Map<String, AttributeValue> newItem = createRecordWithStatus(fileInfo, READY_FOR_GARBAGE_COLLECTION);
            Put put = new Put()
                    .withTableName(readyForGCFileInfoTablename)
                    .withItem(newItem);
            writes.add(new TransactWriteItem().withPut(put));
        }
        // Add record for file for new status
        Map<String, AttributeValue> newItem = createRecordWithStatus(newActiveFile, ACTIVE);
        Put put = new Put()
                .withTableName(activeFileInfoTablename)
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
    public void atomicallyUpdateFilesToReadyForGCAndCreateNewActiveFiles(List<FileInfo> filesToBeMarkedReadyForGC,
                                                                         FileInfo leftFileInfo,
                                                                         FileInfo rightFileInfo) throws StateStoreException {
        // Delete record for file for current status
        List<TransactWriteItem> writes = new ArrayList<>();
        for (FileInfo fileInfo : filesToBeMarkedReadyForGC) {
            Map<String, AttributeValue> key = new HashMap<>();
            key.put(FILE_NAME, createStringAttribute(fileInfo.getFilename()));
            Map<String, String> expressionAttributeNames = new HashMap<>();
            expressionAttributeNames.put("#status", FILE_STATUS);
            Delete delete = new Delete()
                    .withTableName(activeFileInfoTablename)
                    .withKey(key)
                    .withExpressionAttributeNames(expressionAttributeNames)
                    .withConditionExpression("attribute_exists(#status)");
            writes.add(new TransactWriteItem().withDelete(delete));
            Map<String, AttributeValue> newItem = createRecordWithStatus(fileInfo, READY_FOR_GARBAGE_COLLECTION);
            Put put = new Put()
                    .withTableName(readyForGCFileInfoTablename)
                    .withItem(newItem);
            writes.add(new TransactWriteItem().withPut(put));
        }
        // Add record for file for new status
        Map<String, AttributeValue> newKeyLeftFile = createRecordWithStatus(leftFileInfo, ACTIVE);
        Put put = new Put()
                .withTableName(activeFileInfoTablename)
                .withItem(newKeyLeftFile);
        writes.add(new TransactWriteItem().withPut(put));
        Map<String, AttributeValue> newKeyRightFile = createRecordWithStatus(rightFileInfo, ACTIVE);
        Put put2 = new Put()
                .withTableName(activeFileInfoTablename)
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
            Map<String, AttributeValue> fileAttributeValues = createRecordWithJobId(fileInfo, jobId);
            Map<String, String> expressionAttributeNames = new HashMap<>();
            expressionAttributeNames.put("#jobid", JOB_ID);
            Put put = new Put()
                    .withTableName(activeFileInfoTablename)
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
                .withTableName(readyForGCFileInfoTablename)
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
                    .withTableName(activeFileInfoTablename)
                    .withConsistentRead(stronglyConsistentReads)
                    .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL);
            ScanResult queryResult = dynamoDB.scan(queryRequest);
            totalCapacity += queryResult.getConsumedCapacity().getCapacityUnits();
            List<Map<String, AttributeValue>> results = new ArrayList<>();
            results.addAll(queryResult.getItems());
            while (null != queryResult.getLastEvaluatedKey()) {
                queryRequest = new ScanRequest()
                        .withTableName(activeFileInfoTablename)
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
                fileInfoResults.add(getFileInfoFromAttributeValues(rowKeyTypes, map));
            }
            return fileInfoResults;
        } catch (ProvisionedThroughputExceededException | ResourceNotFoundException | RequestLimitExceededException
                 | InternalServerErrorException | IOException e) {
            throw new StateStoreException("Exception querying DynamoDB", e);
        }
    }

    private static class FilesReadyForGCIterator implements Iterator<FileInfo> {
        private final AmazonDynamoDB dynamoDB;
        private final String readyForGCFileInfoTablename;
        private final List<PrimitiveType> rowKeyTypes;
        private final int delayBeforeGarbageCollectionInSeconds;
        private final boolean stronglyConsistentReads;
        private double totalCapacity;
        private ScanResult scanResult;
        private Iterator<Map<String, AttributeValue>> itemsIterator;

        FilesReadyForGCIterator(AmazonDynamoDB dynamoDB,
                                String readyForGCFileInfoTablename,
                                List<PrimitiveType> rowKeyTypes,
                                int delayBeforeGarbageCollectionInSeconds,
                                boolean stronglyConsistentReads) {
            this.dynamoDB = dynamoDB;
            this.readyForGCFileInfoTablename = readyForGCFileInfoTablename;
            this.rowKeyTypes = rowKeyTypes;
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
                return getFileInfoFromAttributeValues(rowKeyTypes, itemsIterator.next());
            } catch (IOException e) {
                throw new RuntimeException("IOException creating FileInfo from attribute values");
            }
        }
    }

    @Override
    public Iterator<FileInfo> getReadyForGCFiles() {
        return new FilesReadyForGCIterator(dynamoDB, readyForGCFileInfoTablename, rowKeyTypes, garbageCollectorDelayBeforeDeletionInSeconds, stronglyConsistentReads);
    }

    @Override
    public List<FileInfo> getActiveFilesWithNoJobId() throws StateStoreException {
        try {
            double totalCapacity = 0.0D;
            ScanRequest scanRequest = new ScanRequest()
                    .withTableName(activeFileInfoTablename)
                    .withConsistentRead(stronglyConsistentReads)
                    .withFilterExpression("attribute_not_exists(" + JOB_ID + ")")
                    .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL);
            ScanResult queryResult = dynamoDB.scan(scanRequest);
            totalCapacity += queryResult.getConsumedCapacity().getCapacityUnits();
            List<Map<String, AttributeValue>> results = new ArrayList<>(queryResult.getItems());
            while (null != queryResult.getLastEvaluatedKey()) {
                scanRequest = new ScanRequest()
                        .withTableName(activeFileInfoTablename)
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
                fileInfoResults.add(getFileInfoFromAttributeValues(rowKeyTypes, map));
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

    private void addPartition(Partition partition) throws StateStoreException {
        try {
            Map<String, AttributeValue> map = getItemFromPartition(partition);
            PutItemRequest putItemRequest = new PutItemRequest()
                    .withTableName(partitionTableName)
                    .withItem(map)
                    .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL);
            PutItemResult putItemResult = dynamoDB.putItem(putItemRequest);
            LOGGER.debug("Added partition with id {}, capacity consumed = ",
                    partition.getId(), putItemResult.getConsumedCapacity().getCapacityUnits());
        } catch (IOException | ConditionalCheckFailedException | ProvisionedThroughputExceededException
                 | ResourceNotFoundException | ItemCollectionSizeLimitExceededException
                 | TransactionConflictException | RequestLimitExceededException
                 | InternalServerErrorException e) {
            throw new StateStoreException("Exception calling putItem", e);
        }
    }

    private static String childPartitionsToString(List<String> childPartitionIds) {
        if (null == childPartitionIds || childPartitionIds.isEmpty()) {
            return null;
        }
        return String.join("___", childPartitionIds);
    }

    private static List<String> childPartitionsFromString(String childPartitionsString) {
        if (null == childPartitionsString) {
            return new ArrayList<>();
        }
        String[] childPartitions = childPartitionsString.split("___");
        return Arrays.asList(childPartitions);
    }

    @Override
    public void atomicallyUpdatePartitionAndCreateNewOnes(Partition splitPartition,
                                                          Partition newPartition1,
                                                          Partition newPartition2) throws StateStoreException {
        // Validate request: splitPartition should be a non-leaf partition, its children should be newPartition1
        // and newPartition2, their parent should be splitPartition and they should be leaf partitions
        if (splitPartition.isLeafPartition()) {
            throw new StateStoreException("Split partition is a leaf partition (splitPartition = " + splitPartition + ")");
        }
        Set<String> splitPartitionChildrenIds = new HashSet<>(splitPartition.getChildPartitionIds());
        Set<String> newIds = new HashSet<>();
        newIds.add(newPartition1.getId());
        newIds.add(newPartition2.getId());
        if (!splitPartitionChildrenIds.equals(newIds)) {
            throw new StateStoreException("Children of splitPartition do not equal newPartition1 and new Partition2");
        }
        if (!newPartition1.getParentPartitionId().equals(splitPartition.getId())) {
            throw new StateStoreException("Parent of newPartition1 does not equal splitPartition");
        }
        if (!newPartition2.getParentPartitionId().equals(splitPartition.getId())) {
            throw new StateStoreException("Parent of newPartition2 does not equal splitPartition");
        }
        if (!newPartition1.isLeafPartition() || !newPartition2.isLeafPartition()) {
            throw new StateStoreException("newPartition1 and newPartition2 should be leaf partitions");
        }
        List<TransactWriteItem> writes = new ArrayList<>();
        Map<String, AttributeValue> item;
        try {
            item = getItemFromPartition(splitPartition);
        } catch (IOException e) {
            throw new StateStoreException("IOException getting item from partition", e);
        }
        Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();
        expressionAttributeValues.put(":true", new AttributeValue("true"));
        Put put = new Put()
                .withTableName(partitionTableName)
                .withItem(item)
                .withExpressionAttributeValues(expressionAttributeValues)
                .withConditionExpression(PARTITION_IS_LEAF + " = :true");
        writes.add(new TransactWriteItem().withPut(put));
        for (Partition partition : Arrays.asList(newPartition1, newPartition2)) {
            Map<String, AttributeValue> item2;
            try {
                item2 = getItemFromPartition(partition);
            } catch (IOException e) {
                throw new StateStoreException("IOException getting item from partition", e);
            }
            Put put2 = new Put()
                    .withTableName(partitionTableName)
                    .withItem(item2);
            writes.add(new TransactWriteItem().withPut(put2));
        }
        TransactWriteItemsRequest transactWriteItemsRequest = new TransactWriteItemsRequest()
                .withTransactItems(writes)
                .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL);
        try {
            TransactWriteItemsResult transactWriteItemsResult = dynamoDB.transactWriteItems(transactWriteItemsRequest);
            List<ConsumedCapacity> consumedCapacity = transactWriteItemsResult.getConsumedCapacity();
            double totalCapacity = consumedCapacity.stream().mapToDouble(ConsumedCapacity::getCapacityUnits).sum();
            LOGGER.debug("Split partition {}, capacity consumed = {}",
                    splitPartition.getId(), totalCapacity);
        } catch (TransactionCanceledException | ResourceNotFoundException
                 | TransactionInProgressException | IdempotentParameterMismatchException
                 | ProvisionedThroughputExceededException | InternalServerErrorException e) {
            throw new StateStoreException(e);
        }
    }

    private Map<String, AttributeValue> getItemFromPartition(Partition partition) throws IOException {
        Map<String, AttributeValue> map = new HashMap<>();
        map.put(PARTITION_ID, createStringAttribute(partition.getId()));
        map.put(PARTITION_IS_LEAF, createStringAttribute("" + partition.isLeafPartition()));
        if (null != partition.getParentPartitionId()) {
            map.put(PARTITION_PARENT_ID, createStringAttribute(partition.getParentPartitionId()));
        }
        if (null != partition.getChildPartitionIds() && !partition.getChildPartitionIds().isEmpty()) {
            map.put(PARTITION_CHILD_IDS, createStringAttribute(childPartitionsToString(partition.getChildPartitionIds())));
        }
        map.put(PARTITION_SPLIT_DIMENSION, createNumberAttribute(partition.getDimension()));
        map.put(REGION, createStringAttribute(regionSerDe.toJson(partition.getRegion())));
        return map;
    }

    @Override
    public List<Partition> getAllPartitions() throws StateStoreException {
        try {
            double totalCapacity = 0.0D;
            ScanRequest scanRequest = new ScanRequest()
                    .withTableName(partitionTableName)
                    .withConsistentRead(stronglyConsistentReads)
                    .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL);
            ScanResult scanResult = dynamoDB.scan(scanRequest);
            totalCapacity += scanResult.getConsumedCapacity().getCapacityUnits();
            List<Map<String, AttributeValue>> results = new ArrayList<>(scanResult.getItems());
            while (null != scanResult.getLastEvaluatedKey()) {
                scanRequest = new ScanRequest()
                        .withTableName(partitionTableName)
                        .withConsistentRead(stronglyConsistentReads)
                        .withExclusiveStartKey(scanResult.getLastEvaluatedKey())
                        .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL);
                scanResult = dynamoDB.scan(scanRequest);
                totalCapacity += scanResult.getConsumedCapacity().getCapacityUnits();
                results.addAll(scanResult.getItems());
            }
            LOGGER.debug("Scanned for all partitions, capacity consumed = {}", totalCapacity);
            List<Partition> partitionResults = new ArrayList<>();
            for (Map<String, AttributeValue> map : results) {
                partitionResults.add(getPartitionFromAttributeValues(map));
            }
            return partitionResults;
        } catch (ProvisionedThroughputExceededException | ResourceNotFoundException | RequestLimitExceededException
                 | InternalServerErrorException | IOException e) {
            throw new StateStoreException("Exception querying DynamoDB", e);
        }
    }

    @Override
    public List<Partition> getLeafPartitions() throws StateStoreException {
        // TODO optimise by pushing the predicate down to Dynamo
        return getAllPartitions().stream()
                .filter(Partition::isLeafPartition)
                .collect(Collectors.toList());
    }

    private static FileInfo getFileInfoFromAttributeValues(List<PrimitiveType> rowKeyTypes, Map<String, AttributeValue> item) throws IOException {
        FileInfo fileInfo = new FileInfo();
        fileInfo.setRowKeyTypes(rowKeyTypes);
        fileInfo.setFileStatus(FileInfo.FileStatus.valueOf(item.get(FILE_STATUS).getS()));
        fileInfo.setPartitionId(item.get(FILE_PARTITION).getS());
        if (null != item.get(NUMBER_LINES)) {
            fileInfo.setNumberOfRecords(Long.parseLong(item.get(NUMBER_LINES).getN()));
        }
        KeySerDe keySerDe = new KeySerDe(rowKeyTypes);
        if (null != item.get(MIN_KEY)) {
            fileInfo.setMinRowKey(keySerDe.deserialise(item.get(MIN_KEY).getB().array()));
        }
        if (null != item.get(MAX_KEY)) {
            fileInfo.setMaxRowKey(keySerDe.deserialise(item.get(MAX_KEY).getB().array()));
        }
        fileInfo.setFilename(item.get(FILE_NAME).getS());
        if (null != item.get(JOB_ID)) {
            fileInfo.setJobId(item.get(JOB_ID).getS());
        }
        if (null != item.get(LAST_UPDATE_TIME)) {
            fileInfo.setLastStateStoreUpdateTime(Long.parseLong(item.get(LAST_UPDATE_TIME).getN()));
        }
        return fileInfo;
    }

    private Partition getPartitionFromAttributeValues(Map<String, AttributeValue> item) throws IOException {
        Partition partition = new Partition();
        partition.setRowKeyTypes(rowKeyTypes);
        partition.setId(item.get(PARTITION_ID).getS());
        partition.setLeafPartition(Boolean.parseBoolean(item.get(PARTITION_IS_LEAF).getS()));
        if (null != item.get(PARTITION_PARENT_ID)) {
            partition.setParentPartitionId(item.get(PARTITION_PARENT_ID).getS());
        }
        if (null != item.get(PARTITION_CHILD_IDS)) {
            String childPartitionIdsString = item.get(PARTITION_CHILD_IDS).getS();
            partition.setChildPartitionIds(childPartitionsFromString(childPartitionIdsString));
        }
        if (null != item.get(PARTITION_SPLIT_DIMENSION)) {
            partition.setDimension(Integer.parseInt(item.get(PARTITION_SPLIT_DIMENSION).getN()));
        }

        partition.setRegion(regionSerDe.fromJson(item.get(REGION).getS()));

        return partition;
    }

    @Override
    public void initialise() throws StateStoreException {
        initialise(new PartitionsFromSplitPoints(schema, Collections.emptyList()).construct());
    }

    @Override
    public void initialise(List<Partition> initialPartitions) throws StateStoreException {
        setInitialPartitions(initialPartitions);
    }

    private void setInitialPartitions(List<Partition> partitions) throws StateStoreException {
        if (null == partitions || partitions.isEmpty()) {
            throw new StateStoreException("At least one partition must be provided");
        }
        setPartitions(partitions);
    }

    private void setPartitions(List<Partition> partitions) throws StateStoreException {
        for (Partition partition : partitions) {
            addPartition(partition);
            LOGGER.debug("Added partition {}", partition);
        }
    }
}
