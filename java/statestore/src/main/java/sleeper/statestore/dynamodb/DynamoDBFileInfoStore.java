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
import com.amazonaws.services.dynamodbv2.model.IdempotentParameterMismatchException;
import com.amazonaws.services.dynamodbv2.model.InternalServerErrorException;
import com.amazonaws.services.dynamodbv2.model.ItemCollectionSizeLimitExceededException;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughputExceededException;
import com.amazonaws.services.dynamodbv2.model.Put;
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
import com.amazonaws.services.dynamodbv2.model.Update;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TableProperty;
import sleeper.core.statestore.AllFileReferences;
import sleeper.core.statestore.AssignJobToFilesRequest;
import sleeper.core.statestore.FileInfo;
import sleeper.core.statestore.FileInfoStore;
import sleeper.core.statestore.FileReferenceCount;
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

import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.ACTIVE_FILEINFO_TABLENAME;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.FILE_REFERENCE_COUNT_TABLENAME;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.READY_FOR_GC_FILEINFO_TABLENAME;
import static sleeper.configuration.properties.table.TableProperty.DYNAMODB_STRONGLY_CONSISTENT_READS;
import static sleeper.configuration.properties.table.TableProperty.GARBAGE_COLLECTOR_DELAY_BEFORE_DELETION;
import static sleeper.core.statestore.FileInfo.FileStatus.ACTIVE;
import static sleeper.dynamodb.tools.DynamoDBAttributes.createNumberAttribute;
import static sleeper.dynamodb.tools.DynamoDBAttributes.createStringAttribute;
import static sleeper.dynamodb.tools.DynamoDBUtils.deleteAllDynamoTableItems;
import static sleeper.dynamodb.tools.DynamoDBUtils.streamPagedResults;
import static sleeper.statestore.dynamodb.DynamoDBFileInfoFormat.FILENAME;
import static sleeper.statestore.dynamodb.DynamoDBFileInfoFormat.JOB_ID;
import static sleeper.statestore.dynamodb.DynamoDBFileInfoFormat.LAST_UPDATE_TIME;
import static sleeper.statestore.dynamodb.DynamoDBFileInfoFormat.PARTITION_ID_AND_FILENAME;
import static sleeper.statestore.dynamodb.DynamoDBFileInfoFormat.REFERENCES;
import static sleeper.statestore.dynamodb.DynamoDBFileInfoFormat.STATUS;
import static sleeper.statestore.dynamodb.DynamoDBFileInfoFormat.TABLE_ID;

class DynamoDBFileInfoStore implements FileInfoStore {

    private static final Logger LOGGER = LoggerFactory.getLogger(DynamoDBFileInfoStore.class);

    private final AmazonDynamoDB dynamoDB;
    private final String activeTableName;
    private final String readyForGCTableName;
    private final String fileReferenceCountTableName;
    private final String sleeperTableId;
    private final boolean stronglyConsistentReads;
    private final int garbageCollectorDelayBeforeDeletionInMinutes;
    private final DynamoDBFileInfoFormat fileInfoFormat;
    private Clock clock = Clock.systemUTC();

    private DynamoDBFileInfoStore(Builder builder) {
        dynamoDB = Objects.requireNonNull(builder.dynamoDB, "dynamoDB must not be null");
        activeTableName = Objects.requireNonNull(builder.activeTableName, "activeTableName must not be null");
        readyForGCTableName = Objects.requireNonNull(builder.readyForGCTableName, "readyForGCTableName must not be null");
        fileReferenceCountTableName = Objects.requireNonNull(builder.fileReferenceCountTableName, "fileReferenceCountTableName must not be null");
        sleeperTableId = Objects.requireNonNull(builder.sleeperTableId, "sleeperTableId must not be null");
        stronglyConsistentReads = builder.stronglyConsistentReads;
        garbageCollectorDelayBeforeDeletionInMinutes = builder.garbageCollectorDelayBeforeDeletionInMinutes;
        fileInfoFormat = new DynamoDBFileInfoFormat(sleeperTableId);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public void addFile(FileInfo fileInfo) throws StateStoreException {
        addFile(fileInfo, clock.millis());
    }

    public void addFile(FileInfo fileInfo, long updateTime) throws StateStoreException {
        try {
            String tableName = tableName(fileInfo);
            TransactWriteItemsResult transactWriteItemsResult = dynamoDB.transactWriteItems(new TransactWriteItemsRequest()
                    .withTransactItems(
                            new TransactWriteItem().withPut(new Put()
                                    .withTableName(tableName)
                                    .withItem(fileInfoFormat.createRecord(setLastUpdateTime(fileInfo, updateTime)))
                                    .withConditionExpression("attribute_not_exists(#PartitionAndFile)")
                                    .withExpressionAttributeNames(Map.of("#PartitionAndFile", PARTITION_ID_AND_FILENAME))),
                            new TransactWriteItem().withUpdate(fileReferenceCountUpdateAddingFile(fileInfo, updateTime)))
                    .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL));
            List<ConsumedCapacity> consumedCapacity = transactWriteItemsResult.getConsumedCapacity();
            double totalConsumed = consumedCapacity.stream().mapToDouble(ConsumedCapacity::getCapacityUnits).sum();
            LOGGER.debug("Put file info for file {} to table {}, read capacity consumed = {}",
                    fileInfo.getFilename(), tableName, totalConsumed);
        } catch (ConditionalCheckFailedException | ProvisionedThroughputExceededException | ResourceNotFoundException
                 | ItemCollectionSizeLimitExceededException | TransactionConflictException
                 | TransactionCanceledException | RequestLimitExceededException | InternalServerErrorException e) {
            throw new StateStoreException("Exception calling putItem", e);
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
    public void atomicallyUpdateFilesToReadyForGCAndCreateNewActiveFiles(
            List<FileInfo> filesToBeMarkedReadyForGC, List<FileInfo> newFiles) throws StateStoreException {
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
            writes.add(new TransactWriteItem().withUpdate(
                    fileReferenceCountUpdateMarkingFileReadyForGC(fileInfo, updateTime)));
        });
        // Add record for file for new status
        for (FileInfo newFile : newFiles) {
            Put put = new Put()
                    .withTableName(activeTableName)
                    .withItem(fileInfoFormat.createActiveFileRecord(setLastUpdateTime(newFile, updateTime)));
            writes.add(new TransactWriteItem().withPut(put));
        }
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
        String partitionId = files.get(0).getPartitionId();
        atomicallyUpdateJobStatusOfFiles(AssignJobToFilesRequest.builder()
                .tableId(sleeperTableId)
                .partitionId(partitionId)
                .jobId(jobId)
                .files(files.stream().map(FileInfo::getFilename).collect(Collectors.toUnmodifiableList()))
                .build());
    }

    private void atomicallyUpdateJobStatusOfFiles(AssignJobToFilesRequest request)
            throws StateStoreException {
        // Create updates for each of the files, conditional on the compactionJob field being not present
        long updateTime = clock.millis();
        List<TransactWriteItem> writes = request.getFiles().stream().map(filename ->
                        new TransactWriteItem().withUpdate(new Update()
                                .withTableName(activeTableName)
                                .withKey(fileInfoFormat.createActiveFileKeyWithPartitionAndFilename(
                                        request.getPartitionId(), filename))
                                .withUpdateExpression("SET #jobid = :jobid, #time = :time")
                                .withConditionExpression("attribute_exists(#time) and attribute_not_exists(#jobid)")
                                .withExpressionAttributeNames(Map.of(
                                        "#jobid", JOB_ID,
                                        "#time", LAST_UPDATE_TIME))
                                .withExpressionAttributeValues(Map.of(
                                        ":jobid", createStringAttribute(request.getJobId()),
                                        ":time", createNumberAttribute(updateTime)))))
                .collect(Collectors.toUnmodifiableList());
        TransactWriteItemsRequest transactWriteItemsRequest = new TransactWriteItemsRequest()
                .withTransactItems(writes)
                .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL);
        try {
            TransactWriteItemsResult transactWriteItemsResult = dynamoDB.transactWriteItems(transactWriteItemsRequest);
            List<ConsumedCapacity> consumedCapacity = transactWriteItemsResult.getConsumedCapacity();
            double totalConsumed = consumedCapacity.stream().mapToDouble(ConsumedCapacity::getCapacityUnits).sum();
            LOGGER.debug("Updated job status of {} files, read capacity consumed = {}",
                    request.getFiles().size(), totalConsumed);
        } catch (TransactionCanceledException | ResourceNotFoundException
                 | TransactionInProgressException | IdempotentParameterMismatchException
                 | ProvisionedThroughputExceededException | InternalServerErrorException e) {
            throw new StateStoreException(e);
        }
    }

    @Override
    public void deleteReadyForGCFile(FileInfo fileInfo) throws StateStoreException {
        deleteReadyForGCFile(fileInfo.getFilename());
    }

    @Override
    public void deleteReadyForGCFile(String filename) throws StateStoreException {
        // Delete record for file for current status
        TransactWriteItemsRequest transactWriteItemsRequest = new TransactWriteItemsRequest()
                .withTransactItems(
                        new TransactWriteItem().withDelete(new Delete()
                                .withTableName(readyForGCTableName)
                                .withKey(fileInfoFormat.createReadyForGCKey(filename))
                                .withConditionExpression("attribute_exists(#Filename)")
                                .withExpressionAttributeNames(Map.of("#Filename", FILENAME))),
                        new TransactWriteItem().withDelete(new Delete()
                                .withTableName(fileReferenceCountTableName)
                                .withKey(fileInfoFormat.createReferenceCountKey(filename))
                                .withConditionExpression("#References = :refs")
                                .withExpressionAttributeNames(Map.of("#References", REFERENCES))
                                .withExpressionAttributeValues(Map.of(":refs", createNumberAttribute(0)))))
                .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL);
        try {
            TransactWriteItemsResult result = dynamoDB.transactWriteItems(transactWriteItemsRequest);
            List<ConsumedCapacity> consumedCapacity = result.getConsumedCapacity();
            double totalConsumed = consumedCapacity.stream().mapToDouble(ConsumedCapacity::getCapacityUnits).sum();
            LOGGER.debug("Deleted file {}, capacity consumed = {}",
                    filename, totalConsumed);
        } catch (TransactionCanceledException | ResourceNotFoundException
                 | TransactionInProgressException | IdempotentParameterMismatchException
                 | ProvisionedThroughputExceededException | InternalServerErrorException e) {
            throw new StateStoreException(e);
        }
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
    public Stream<String> getReadyForGCFilenamesBefore(Instant maxUpdateTime) {
        QueryRequest queryRequest = new QueryRequest()
                .withTableName(fileReferenceCountTableName)
                .withConsistentRead(stronglyConsistentReads)
                .withKeyConditionExpression("#TableId = :table_id")
                .withFilterExpression("#References < :one_reference AND #UpdatedTime < :maxtime")
                .withExpressionAttributeNames(Map.of(
                        "#TableId", TABLE_ID,
                        "#References", REFERENCES,
                        "#UpdatedTime", LAST_UPDATE_TIME))
                .withExpressionAttributeValues(new DynamoDBRecordBuilder()
                        .string(":table_id", sleeperTableId)
                        .number(":one_reference", 1)
                        .number(":maxtime", maxUpdateTime.toEpochMilli())
                        .build())
                .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL);
        AtomicReference<Double> totalCapacity = new AtomicReference<>(0.0D);
        return streamPagedResults(dynamoDB, queryRequest)
                .flatMap(result -> {
                    double newConsumed = totalCapacity.updateAndGet(old ->
                            old + result.getConsumedCapacity().getCapacityUnits());
                    LOGGER.debug("Queried table {} for all ready for GC files, capacity consumed = {}",
                            readyForGCTableName, newConsumed);
                    return result.getItems().stream();
                }).map(fileInfoFormat::getFilenameFromReferenceCount);
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

    @Override
    public AllFileReferences getAllFileReferences() throws StateStoreException {
        return AllFileReferences.fromActiveFilesAndReferenceCounts(
                getActiveFiles().stream(),
                streamFileReferenceCounts());
    }

    private Stream<FileReferenceCount> streamFileReferenceCounts() {
        QueryRequest queryRequest = new QueryRequest()
                .withTableName(fileReferenceCountTableName)
                .withConsistentRead(stronglyConsistentReads)
                .withKeyConditionExpression("#TableId = :table_id")
                .withExpressionAttributeNames(Map.of("#TableId", TABLE_ID))
                .withExpressionAttributeValues(new DynamoDBRecordBuilder().string(":table_id", sleeperTableId).build())
                .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL);
        AtomicReference<Double> totalCapacity = new AtomicReference<>(0.0D);
        return streamPagedResults(dynamoDB, queryRequest)
                .flatMap(result -> {
                    double newConsumed = totalCapacity.updateAndGet(old ->
                            old + result.getConsumedCapacity().getCapacityUnits());
                    LOGGER.debug("Queried table {} for all file reference counts, capacity consumed = {}",
                            fileReferenceCountTableName, newConsumed);
                    return result.getItems().stream();
                }).map(fileInfoFormat::getFileReferenceCountFromAttributeValues);
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

    private String tableName(FileInfo fileInfo) {
        if (fileInfo.getFileStatus().equals(ACTIVE)) {
            return activeTableName;
        } else {
            return readyForGCTableName;
        }
    }

    private Update fileReferenceCountUpdateAddingFile(FileInfo fileInfo, long updateTime) {
        Update update = new Update().withTableName(fileReferenceCountTableName)
                .withKey(fileInfoFormat.createReferenceCountKey(fileInfo));
        if (fileInfo.getFileStatus() == ACTIVE) {
            return update.withUpdateExpression("SET #UpdateTime = :time, " +
                            "#References = if_not_exists(#References, :init) + :inc")
                    .withExpressionAttributeNames(Map.of(
                            "#UpdateTime", LAST_UPDATE_TIME,
                            "#References", REFERENCES))
                    .withExpressionAttributeValues(new DynamoDBRecordBuilder()
                            .number(":time", updateTime)
                            .number(":init", 0)
                            .number(":inc", 1)
                            .build());
        } else {
            return update.withUpdateExpression("SET #UpdateTime = :time, " +
                            "#References = if_not_exists(#References, :init)")
                    .withExpressionAttributeNames(Map.of(
                            "#UpdateTime", LAST_UPDATE_TIME,
                            "#References", REFERENCES))
                    .withExpressionAttributeValues(new DynamoDBRecordBuilder()
                            .number(":time", updateTime)
                            .number(":init", 0)
                            .build());
        }
    }

    private Update fileReferenceCountUpdateMarkingFileReadyForGC(FileInfo fileInfo, long updateTime) {
        return new Update().withTableName(fileReferenceCountTableName)
                .withKey(fileInfoFormat.createReferenceCountKey(fileInfo))
                .withUpdateExpression("SET #UpdateTime = :time, " +
                        "#References = #References - :dec")
                .withExpressionAttributeNames(Map.of(
                        "#UpdateTime", LAST_UPDATE_TIME,
                        "#References", REFERENCES))
                .withExpressionAttributeValues(new DynamoDBRecordBuilder()
                        .number(":time", updateTime)
                        .number(":dec", 1)
                        .build());
    }

    static final class Builder {
        private AmazonDynamoDB dynamoDB;
        private String activeTableName;
        private String readyForGCTableName;
        private String fileReferenceCountTableName;
        private String sleeperTableId;
        private boolean stronglyConsistentReads;
        private int garbageCollectorDelayBeforeDeletionInMinutes;

        private Builder() {
        }

        Builder instanceProperties(InstanceProperties instanceProperties) {
            return activeTableName(instanceProperties.get(ACTIVE_FILEINFO_TABLENAME))
                    .readyForGCTableName(instanceProperties.get(READY_FOR_GC_FILEINFO_TABLENAME))
                    .fileReferenceCountTableName(instanceProperties.get(FILE_REFERENCE_COUNT_TABLENAME));
        }

        Builder tableProperties(TableProperties tableProperties) {
            return sleeperTableId(tableProperties.get(TableProperty.TABLE_ID))
                    .stronglyConsistentReads(tableProperties.getBoolean(DYNAMODB_STRONGLY_CONSISTENT_READS))
                    .garbageCollectorDelayBeforeDeletionInMinutes(tableProperties.getInt(GARBAGE_COLLECTOR_DELAY_BEFORE_DELETION));
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

        Builder fileReferenceCountTableName(String fileReferenceCountTableName) {
            this.fileReferenceCountTableName = fileReferenceCountTableName;
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

        DynamoDBFileInfoStore build() {
            return new DynamoDBFileInfoStore(this);
        }
    }
}
