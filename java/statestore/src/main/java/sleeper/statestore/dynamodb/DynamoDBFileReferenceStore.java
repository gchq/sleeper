/*
 * Copyright 2022-2024 Crown Copyright
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
import com.amazonaws.services.dynamodbv2.model.AmazonDynamoDBException;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ConsumedCapacity;
import com.amazonaws.services.dynamodbv2.model.Delete;
import com.amazonaws.services.dynamodbv2.model.DeleteItemRequest;
import com.amazonaws.services.dynamodbv2.model.DeleteItemResult;
import com.amazonaws.services.dynamodbv2.model.Put;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import com.amazonaws.services.dynamodbv2.model.ReturnConsumedCapacity;
import com.amazonaws.services.dynamodbv2.model.TransactWriteItem;
import com.amazonaws.services.dynamodbv2.model.TransactWriteItemsRequest;
import com.amazonaws.services.dynamodbv2.model.TransactWriteItemsResult;
import com.amazonaws.services.dynamodbv2.model.Update;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TableProperty;
import sleeper.core.statestore.AllFileReferences;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.FileReferenceCount;
import sleeper.core.statestore.FileReferenceStore;
import sleeper.core.statestore.SplitFileReferenceRequest;
import sleeper.core.statestore.StateStoreException;
import sleeper.dynamodb.tools.DynamoDBRecordBuilder;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.ACTIVE_FILES_TABLELENAME;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.FILE_REFERENCE_COUNT_TABLENAME;
import static sleeper.configuration.properties.table.TableProperty.DYNAMODB_STRONGLY_CONSISTENT_READS;
import static sleeper.dynamodb.tools.DynamoDBAttributes.createNumberAttribute;
import static sleeper.dynamodb.tools.DynamoDBAttributes.createStringAttribute;
import static sleeper.dynamodb.tools.DynamoDBUtils.deleteAllDynamoTableItems;
import static sleeper.dynamodb.tools.DynamoDBUtils.streamPagedResults;
import static sleeper.statestore.dynamodb.DynamoDBFileReferenceFormat.FILENAME;
import static sleeper.statestore.dynamodb.DynamoDBFileReferenceFormat.JOB_ID;
import static sleeper.statestore.dynamodb.DynamoDBFileReferenceFormat.LAST_UPDATE_TIME;
import static sleeper.statestore.dynamodb.DynamoDBFileReferenceFormat.PARTITION_ID_AND_FILENAME;
import static sleeper.statestore.dynamodb.DynamoDBFileReferenceFormat.REFERENCES;
import static sleeper.statestore.dynamodb.DynamoDBFileReferenceFormat.TABLE_ID;

class DynamoDBFileReferenceStore implements FileReferenceStore {

    private static final Logger LOGGER = LoggerFactory.getLogger(DynamoDBFileReferenceStore.class);

    private final AmazonDynamoDB dynamoDB;
    private final String activeTableName;
    private final String fileReferenceCountTableName;
    private final String sleeperTableId;
    private final boolean stronglyConsistentReads;
    private final DynamoDBFileReferenceFormat fileReferenceFormat;
    private Clock clock = Clock.systemUTC();

    private DynamoDBFileReferenceStore(Builder builder) {
        dynamoDB = Objects.requireNonNull(builder.dynamoDB, "dynamoDB must not be null");
        activeTableName = Objects.requireNonNull(builder.activeTableName, "activeTableName must not be null");
        fileReferenceCountTableName = Objects.requireNonNull(builder.fileReferenceCountTableName, "fileReferenceCountTableName must not be null");
        sleeperTableId = Objects.requireNonNull(builder.sleeperTableId, "sleeperTableId must not be null");
        stronglyConsistentReads = builder.stronglyConsistentReads;
        fileReferenceFormat = new DynamoDBFileReferenceFormat(sleeperTableId);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public void addFile(FileReference fileReference) throws StateStoreException {
        addFile(fileReference, clock.millis());
    }

    public void addFile(FileReference fileReference, long updateTime) throws StateStoreException {
        try {
            TransactWriteItemsResult transactWriteItemsResult = dynamoDB.transactWriteItems(new TransactWriteItemsRequest()
                    .withTransactItems(
                            new TransactWriteItem().withPut(putNewFile(fileReference, updateTime)),
                            new TransactWriteItem().withUpdate(fileReferenceCountUpdateAddingFile(fileReference, updateTime)))
                    .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL));
            List<ConsumedCapacity> consumedCapacity = transactWriteItemsResult.getConsumedCapacity();
            double totalConsumed = consumedCapacity.stream().mapToDouble(ConsumedCapacity::getCapacityUnits).sum();
            LOGGER.debug("Put file reference for file {} to table {}, read capacity consumed = {}",
                    fileReference.getFilename(), activeTableName, totalConsumed);
        } catch (AmazonDynamoDBException e) {
            throw new StateStoreException("Failed to add file", e);
        }
    }

    @Override
    public void addFiles(List<FileReference> fileReferences) throws StateStoreException {
        long updateTime = clock.millis();
        for (FileReference fileReference : fileReferences) {
            addFile(fileReference, updateTime);
        }
    }

    @Override
    public void splitFileReferences(List<SplitFileReferenceRequest> splitRequests) throws StateStoreException {
        long updateTime = clock.millis();
        List<SplitFileReferenceRequest> batchRequests = new ArrayList<>();
        List<TransactWriteItem> batchReferenceWrites = new ArrayList<>();
        Map<String, Integer> batchReferenceCountUpdates = new HashMap<>();
        int firstUnappliedRequestIndex = 0;
        try {
            for (int i = 0; i < splitRequests.size(); i++) {
                SplitFileReferenceRequest splitRequest = splitRequests.get(i);
                List<TransactWriteItem> requestWrites = splitFileReferenceWrites(splitRequest, updateTime);
                int newBatchWrites = batchReferenceWrites.size() + requestWrites.size() + batchReferenceCountUpdates.size();
                if (!batchReferenceCountUpdates.containsKey(splitRequest.getFilename())) {
                    // Reference count updates need to be aggregated into one for each file, so this will only result in
                    // a separate write item if this file has not been updated by a previous request
                    newBatchWrites += 1;
                }
                if (newBatchWrites > 100) {
                    applySplitRequestWrites(batchRequests, batchReferenceWrites, batchReferenceCountUpdates, updateTime);
                    firstUnappliedRequestIndex = i;
                    batchReferenceWrites.clear();
                    batchRequests.clear();
                }
                batchReferenceWrites.addAll(requestWrites);
                batchRequests.add(splitRequest);
                int referenceCountDiff = splitRequest.getNewReferences().size() - 1;
                batchReferenceCountUpdates.compute(splitRequest.getFilename(), (file, update) ->
                        update == null ? referenceCountDiff : update + referenceCountDiff);
            }
            if (!batchReferenceWrites.isEmpty()) {
                applySplitRequestWrites(batchRequests, batchReferenceWrites, batchReferenceCountUpdates, updateTime);
            }
        } catch (AmazonDynamoDBException e) {
            throw new SplitRequestsFailedException(
                    splitRequests.subList(0, firstUnappliedRequestIndex),
                    splitRequests.subList(firstUnappliedRequestIndex, splitRequests.size()), e);
        }
    }


    private void applySplitRequestWrites(
            List<SplitFileReferenceRequest> splitRequests, List<TransactWriteItem> referenceWrites,
            Map<String, Integer> referenceCountIncrementByFilename, long updateTime) {
        List<TransactWriteItem> writes = Stream.concat(referenceWrites.stream(),
                        fileReferenceCountWriteItems(referenceCountIncrementByFilename, updateTime))
                .collect(Collectors.toUnmodifiableList());
        applySplitRequestWrites(splitRequests, writes);
    }

    private void applySplitRequestWrites(
            List<SplitFileReferenceRequest> splitRequests, List<TransactWriteItem> writes) {
        TransactWriteItemsRequest transactWriteItemsRequest = new TransactWriteItemsRequest()
                .withTransactItems(writes)
                .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL);
        TransactWriteItemsResult transactWriteItemsResult = dynamoDB.transactWriteItems(transactWriteItemsRequest);
        List<ConsumedCapacity> consumedCapacity = transactWriteItemsResult.getConsumedCapacity();
        double totalConsumed = consumedCapacity.stream().mapToDouble(ConsumedCapacity::getCapacityUnits).sum();
        LOGGER.debug("Removed {} file references and created {} new file references, capacity consumed = {}",
                splitRequests.size(), splitRequests.stream()
                        .mapToLong(splitRequest -> splitRequest.getNewReferences().size())
                        .sum(), totalConsumed);
    }

    private List<TransactWriteItem> splitFileReferenceWrites(SplitFileReferenceRequest splitRequest, long updateTime) {
        FileReference oldReference = splitRequest.getOldReference();
        List<FileReference> newReferences = splitRequest.getNewReferences();
        List<TransactWriteItem> writes = new ArrayList<>();
        writes.add(new TransactWriteItem().withDelete(new Delete()
                .withTableName(activeTableName)
                .withKey(fileReferenceFormat.createActiveFileKey(oldReference.getPartitionId(), oldReference.getFilename()))
                .withExpressionAttributeNames(Map.of(
                        "#PartitionAndFilename", PARTITION_ID_AND_FILENAME))
                .withConditionExpression(
                        "attribute_exists(#PartitionAndFilename)")));
        for (FileReference newReference : newReferences) {
            writes.add(new TransactWriteItem().withPut(putNewFile(newReference, updateTime)));
        }
        return writes;
    }

    @Override
    public void atomicallyUpdateFilesToReadyForGCAndCreateNewActiveFiles(
            String jobId, String partitionId, List<String> filesToBeMarkedReadyForGC, List<FileReference> newFiles) throws StateStoreException {
        // Delete record for file for current status
        long updateTime = clock.millis();
        List<TransactWriteItem> writes = new ArrayList<>();
        Map<String, Integer> updateReferencesByFilename = new HashMap<>();
        filesToBeMarkedReadyForGC.forEach(filename -> {
            Delete delete = new Delete()
                    .withTableName(activeTableName)
                    .withKey(fileReferenceFormat.createActiveFileKey(partitionId, filename))
                    .withExpressionAttributeNames(Map.of(
                            "#PartitionAndFilename", PARTITION_ID_AND_FILENAME,
                            "#JobId", JOB_ID))
                    .withExpressionAttributeValues(Map.of(":jobid", createStringAttribute(jobId)))
                    .withConditionExpression(
                            "attribute_exists(#PartitionAndFilename) and #JobId = :jobid");
            writes.add(new TransactWriteItem().withDelete(delete));
            updateReferencesByFilename.compute(filename,
                    (name, count) -> count == null ? -1 : count - 1);
        });
        // Add record for file for new status
        for (FileReference newFile : newFiles) {
            writes.add(new TransactWriteItem().withPut(putNewFile(newFile, updateTime)));
            updateReferencesByFilename.compute(newFile.getFilename(),
                    (name, count) -> count == null ? 1 : count + 1);
        }
        for (Map.Entry<String, Integer> entry : updateReferencesByFilename.entrySet()) {
            String filename = entry.getKey();
            int increment = entry.getValue();
            if (increment == 0) {
                continue;
            }
            writes.add(new TransactWriteItem().withUpdate(
                    fileReferenceCountUpdate(filename, updateTime, increment)));
        }
        TransactWriteItemsRequest transactWriteItemsRequest = new TransactWriteItemsRequest()
                .withTransactItems(writes)
                .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL);
        try {
            TransactWriteItemsResult transactWriteItemsResult = dynamoDB.transactWriteItems(transactWriteItemsRequest);
            List<ConsumedCapacity> consumedCapacity = transactWriteItemsResult.getConsumedCapacity();
            double totalConsumed = consumedCapacity.stream().mapToDouble(ConsumedCapacity::getCapacityUnits).sum();
            LOGGER.debug("Updated status of {} files to ready for GC and added {} active files, capacity consumed = {}",
                    filesToBeMarkedReadyForGC.size(), newFiles.size(), totalConsumed);
        } catch (AmazonDynamoDBException e) {
            throw new StateStoreException("Failed to mark files ready for GC and add new files", e);
        }
    }

    /**
     * Atomically updates the job field of the given files to the given id, as long as
     * the compactionJob field is currently null.
     */
    @Override
    public void atomicallyUpdateJobStatusOfFiles(String jobId, List<FileReference> files)
            throws StateStoreException {
        // Create Puts for each of the files, conditional on the compactionJob field being not present
        long updateTime = clock.millis();
        List<TransactWriteItem> writes = files.stream().map(file ->
                        new TransactWriteItem().withUpdate(new Update()
                                .withTableName(activeTableName)
                                .withKey(fileReferenceFormat.createActiveFileKey(file))
                                .withUpdateExpression("SET #jobid = :jobid, #time = :time")
                                .withConditionExpression("attribute_exists(#time) and attribute_not_exists(#jobid)")
                                .withExpressionAttributeNames(Map.of(
                                        "#jobid", JOB_ID,
                                        "#time", LAST_UPDATE_TIME))
                                .withExpressionAttributeValues(Map.of(
                                        ":jobid", createStringAttribute(jobId),
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
                    files.size(), totalConsumed);
        } catch (AmazonDynamoDBException e) {
            throw new StateStoreException("Failed to assign files to job", e);
        }
    }

    @Override
    public void deleteReadyForGCFiles(List<String> filenames) throws StateStoreException {
        int i = 0;
        double totalCapacityConsumed = 0;
        double batchCapacityConsumed = 0;
        for (String filename : filenames) {
            DeleteItemRequest delete = new DeleteItemRequest().withTableName(fileReferenceCountTableName)
                    .withKey(fileReferenceFormat.createReferenceCountKey(filename))
                    .withConditionExpression("#References = :refs")
                    .withExpressionAttributeNames(Map.of("#References", REFERENCES))
                    .withExpressionAttributeValues(Map.of(":refs", createNumberAttribute(0)));
            try {
                DeleteItemResult result = dynamoDB.deleteItem(delete);
                if (result.getConsumedCapacity() != null) {
                    batchCapacityConsumed += result.getConsumedCapacity().getCapacityUnits();
                }
                if (i % 100 == 0) {
                    LOGGER.debug("Deleted 100 unreferenced files, capacity consumed = {}", batchCapacityConsumed);
                    totalCapacityConsumed += batchCapacityConsumed;
                    batchCapacityConsumed = 0;
                }
            } catch (AmazonDynamoDBException e) {
                throw new StateStoreException("Failed to delete unreferenced files", e);
            }
            i++;
        }
        LOGGER.debug("Deleted a total of {} unreferenced files, total consumed capacity = {}", filenames.size(), totalCapacityConsumed);
    }

    @Override
    public List<FileReference> getActiveFiles() throws StateStoreException {
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
            List<FileReference> fileReferenceResults = new ArrayList<>();
            for (Map<String, AttributeValue> map : results) {
                fileReferenceResults.add(fileReferenceFormat.getFileReferenceFromAttributeValues(map));
            }
            return fileReferenceResults;
        } catch (AmazonDynamoDBException e) {
            throw new StateStoreException("Failed to load active files", e);
        }
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
                            fileReferenceCountTableName, newConsumed);
                    return result.getItems().stream();
                }).map(fileReferenceFormat::getFilenameFromReferenceCount);
    }

    @Override
    public List<FileReference> getActiveFilesWithNoJobId() throws StateStoreException {
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
            List<FileReference> fileReferenceResults = new ArrayList<>();
            for (Map<String, AttributeValue> map : results) {
                fileReferenceResults.add(fileReferenceFormat.getFileReferenceFromAttributeValues(map));
            }
            return fileReferenceResults;
        } catch (AmazonDynamoDBException e) {
            throw new StateStoreException("Failed to load active files with no job", e);
        }
    }

    @Override
    public Map<String, List<String>> getPartitionToActiveFilesMap() throws StateStoreException {
        List<FileReference> files = getActiveFiles();
        Map<String, List<String>> partitionToFiles = new HashMap<>();
        for (FileReference fileReference : files) {
            String partition = fileReference.getPartitionId();
            if (!partitionToFiles.containsKey(partition)) {
                partitionToFiles.put(partition, new ArrayList<>());
            }
            partitionToFiles.get(partition).add(fileReference.getFilename());
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
        return isTableEmpty(fileReferenceCountTableName);
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
    public void clearFileData() {
        clearDynamoTable(activeTableName, fileReferenceFormat::getActiveFileKey);
        clearDynamoTable(fileReferenceCountTableName, item -> fileReferenceFormat.createReferenceCountKey(item.get(FILENAME).getS()));
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
    public AllFileReferences getAllFileReferencesWithMaxUnreferenced(int maxUnreferencedFiles) throws StateStoreException {
        Set<String> readyForGCFiles = new TreeSet<>();
        int readyForGCFound = 0;
        boolean moreReadyForGC = false;
        try {
            for (QueryResult result : (Iterable<QueryResult>) () -> streamUnreferencedFiles().iterator()) {
                readyForGCFound += result.getItems().size();
                Stream<String> filenames = result.getItems().stream()
                        .map(fileReferenceFormat::getFileReferenceCountFromAttributeValues)
                        .map(FileReferenceCount::getFilename);
                if (readyForGCFound > maxUnreferencedFiles) {
                    moreReadyForGC = true;
                    filenames = filenames.limit(result.getItems().size() - (readyForGCFound - maxUnreferencedFiles));
                }
                filenames.forEach(readyForGCFiles::add);
            }
        } catch (AmazonDynamoDBException e) {
            throw new StateStoreException("Failed to load unreferenced files", e);
        }
        return new AllFileReferences(new LinkedHashSet<>(getActiveFiles()), readyForGCFiles, moreReadyForGC);
    }

    private Stream<QueryResult> streamUnreferencedFiles() {
        QueryRequest queryRequest = new QueryRequest()
                .withTableName(fileReferenceCountTableName)
                .withConsistentRead(stronglyConsistentReads)
                .withKeyConditionExpression("#TableId = :table_id")
                .withFilterExpression("#References = :zero")
                .withExpressionAttributeNames(Map.of("#TableId", TABLE_ID, "#References", REFERENCES))
                .withExpressionAttributeValues(new DynamoDBRecordBuilder()
                        .string(":table_id", sleeperTableId)
                        .number(":zero", 0)
                        .build())
                .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL);
        AtomicReference<Double> totalCapacity = new AtomicReference<>(0.0D);
        return streamPagedResults(dynamoDB, queryRequest)
                .peek(result -> {
                    double newConsumed = totalCapacity.updateAndGet(old ->
                            old + result.getConsumedCapacity().getCapacityUnits());
                    LOGGER.debug("Queried table {} for all file reference counts, capacity consumed = {}",
                            fileReferenceCountTableName, newConsumed);
                });
    }

    /**
     * Used to set the current time. Should only be called during tests.
     *
     * @param now Time to set to be the current time
     */
    public void fixTime(Instant now) {
        clock = Clock.fixed(now, ZoneId.of("UTC"));
    }

    private Update fileReferenceCountUpdateAddingFile(FileReference fileReference, long updateTime) {
        return fileReferenceCountUpdate(fileReference.getFilename(), updateTime, 1);
    }

    private Stream<TransactWriteItem> fileReferenceCountWriteItems(Map<String, Integer> incrementByFilename, long updateTime) {
        return fileReferenceCountUpdates(incrementByFilename, updateTime)
                .map(update -> new TransactWriteItem().withUpdate(update));
    }

    private Stream<Update> fileReferenceCountUpdates(Map<String, Integer> incrementByFilename, long updateTime) {
        return incrementByFilename.entrySet().stream()
                .map(entry -> fileReferenceCountUpdate(entry.getKey(), updateTime, entry.getValue()));
    }

    private Update fileReferenceCountUpdate(String filename, long updateTime, int increment) {
        return new Update().withTableName(fileReferenceCountTableName)
                .withKey(fileReferenceFormat.createReferenceCountKey(filename))
                .withUpdateExpression("SET #UpdateTime = :time, " +
                        "#References = if_not_exists(#References, :init) + :inc")
                .withExpressionAttributeNames(Map.of(
                        "#UpdateTime", LAST_UPDATE_TIME,
                        "#References", REFERENCES))
                .withExpressionAttributeValues(new DynamoDBRecordBuilder()
                        .number(":time", updateTime)
                        .number(":init", 0)
                        .number(":inc", increment)
                        .build());
    }

    private Put putNewFile(FileReference fileReference, long updateTime) {
        return new Put()
                .withTableName(activeTableName)
                .withItem(fileReferenceFormat.createRecord(fileReference.toBuilder().lastStateStoreUpdateTime(updateTime).build()))
                .withConditionExpression("attribute_not_exists(#PartitionAndFile)")
                .withExpressionAttributeNames(Map.of("#PartitionAndFile", PARTITION_ID_AND_FILENAME));
    }

    static final class Builder {
        private AmazonDynamoDB dynamoDB;
        private String activeTableName;
        private String fileReferenceCountTableName;
        private String sleeperTableId;
        private boolean stronglyConsistentReads;

        private Builder() {
        }

        Builder instanceProperties(InstanceProperties instanceProperties) {
            return activeTableName(instanceProperties.get(ACTIVE_FILES_TABLELENAME))
                    .fileReferenceCountTableName(instanceProperties.get(FILE_REFERENCE_COUNT_TABLENAME));
        }

        Builder tableProperties(TableProperties tableProperties) {
            return sleeperTableId(tableProperties.get(TableProperty.TABLE_ID))
                    .stronglyConsistentReads(tableProperties.getBoolean(DYNAMODB_STRONGLY_CONSISTENT_READS));
        }

        Builder dynamoDB(AmazonDynamoDB dynamoDB) {
            this.dynamoDB = dynamoDB;
            return this;
        }

        Builder activeTableName(String activeTableName) {
            this.activeTableName = activeTableName;
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

        DynamoDBFileReferenceStore build() {
            return new DynamoDBFileReferenceStore(this);
        }
    }
}
