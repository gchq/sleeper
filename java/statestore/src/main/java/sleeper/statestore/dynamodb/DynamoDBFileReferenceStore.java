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
import com.amazonaws.services.dynamodbv2.model.CancellationReason;
import com.amazonaws.services.dynamodbv2.model.ConsumedCapacity;
import com.amazonaws.services.dynamodbv2.model.Delete;
import com.amazonaws.services.dynamodbv2.model.Put;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import com.amazonaws.services.dynamodbv2.model.ReturnConsumedCapacity;
import com.amazonaws.services.dynamodbv2.model.ReturnValuesOnConditionCheckFailure;
import com.amazonaws.services.dynamodbv2.model.TransactWriteItem;
import com.amazonaws.services.dynamodbv2.model.TransactWriteItemsRequest;
import com.amazonaws.services.dynamodbv2.model.TransactWriteItemsResult;
import com.amazonaws.services.dynamodbv2.model.TransactionCanceledException;
import com.amazonaws.services.dynamodbv2.model.Update;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TableProperty;
import sleeper.core.statestore.AllReferencesToAFile;
import sleeper.core.statestore.AllReferencesToAllFiles;
import sleeper.core.statestore.AssignJobIdRequest;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.FileReferenceStore;
import sleeper.core.statestore.ReplaceFileReferencesRequest;
import sleeper.core.statestore.SplitFileReferenceRequest;
import sleeper.core.statestore.StateStoreException;
import sleeper.core.statestore.exception.FileAlreadyExistsException;
import sleeper.core.statestore.exception.FileHasReferencesException;
import sleeper.core.statestore.exception.FileNotFoundException;
import sleeper.core.statestore.exception.FileReferenceAlreadyExistsException;
import sleeper.core.statestore.exception.FileReferenceAssignedToJobException;
import sleeper.core.statestore.exception.FileReferenceNotFoundException;
import sleeper.core.statestore.exception.ReplaceRequestsFailedException;
import sleeper.core.statestore.exception.SplitRequestsFailedException;
import sleeper.dynamodb.tools.DynamoDBRecordBuilder;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.ACTIVE_FILES_TABLENAME;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.FILE_REFERENCE_COUNT_TABLENAME;
import static sleeper.core.properties.table.TableProperty.DYNAMODB_STRONGLY_CONSISTENT_READS;
import static sleeper.dynamodb.tools.DynamoDBAttributes.createNumberAttribute;
import static sleeper.dynamodb.tools.DynamoDBAttributes.createStringAttribute;
import static sleeper.dynamodb.tools.DynamoDBAttributes.getIntAttribute;
import static sleeper.dynamodb.tools.DynamoDBAttributes.getStringAttribute;
import static sleeper.dynamodb.tools.DynamoDBUtils.deleteAllDynamoTableItems;
import static sleeper.dynamodb.tools.DynamoDBUtils.hasConditionalCheckFailure;
import static sleeper.dynamodb.tools.DynamoDBUtils.isConditionCheckFailure;
import static sleeper.dynamodb.tools.DynamoDBUtils.streamPagedResults;
import static sleeper.statestore.dynamodb.DynamoDBFileReferenceFormat.FILENAME;
import static sleeper.statestore.dynamodb.DynamoDBFileReferenceFormat.JOB_ID;
import static sleeper.statestore.dynamodb.DynamoDBFileReferenceFormat.LAST_UPDATE_TIME;
import static sleeper.statestore.dynamodb.DynamoDBFileReferenceFormat.PARTITION_ID_AND_FILENAME;
import static sleeper.statestore.dynamodb.DynamoDBFileReferenceFormat.REFERENCES;
import static sleeper.statestore.dynamodb.DynamoDBFileReferenceFormat.TABLE_ID;

/**
 * A Sleeper table file reference store where the state is held in DynamoDB.
 */
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
    public void addFilesWithReferences(List<AllReferencesToAFile> files) throws StateStoreException {
        Instant updateTime = clock.instant();
        for (AllReferencesToAFile file : files) {
            addFile(file, updateTime);
        }
    }

    private void addFile(AllReferencesToAFile file, Instant updateTime) throws StateStoreException {
        addNewFileReferenceCount(file.getFilename(), updateTime);
        for (FileReference reference : file.getReferences()) {
            addFileReference(reference, updateTime);
        }
    }

    private void addFileReference(FileReference fileReference, Instant updateTime) throws StateStoreException {
        try {
            TransactWriteItemsResult transactWriteItemsResult = dynamoDB.transactWriteItems(new TransactWriteItemsRequest()
                    .withTransactItems(
                            new TransactWriteItem().withPut(putNewFileReference(fileReference, updateTime)),
                            new TransactWriteItem().withUpdate(fileReferenceCountUpdate(fileReference.getFilename(), updateTime, 1)))
                    .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL));
            List<ConsumedCapacity> consumedCapacity = transactWriteItemsResult.getConsumedCapacity();
            double totalConsumed = consumedCapacity.stream().mapToDouble(ConsumedCapacity::getCapacityUnits).sum();
            LOGGER.debug("Put file reference for file {} to table {}, read capacity consumed = {}",
                    fileReference.getFilename(), activeTableName, totalConsumed);
        } catch (AmazonDynamoDBException e) {
            if (hasConditionalCheckFailure(e)) {
                throw new FileReferenceAlreadyExistsException(fileReference);
            } else {
                throw new StateStoreException("Failed to add file reference", e);
            }
        }
    }

    private void addNewFileReferenceCount(String filename, Instant updateTime) throws StateStoreException {
        try {
            TransactWriteItemsResult transactWriteItemsResult = dynamoDB.transactWriteItems(new TransactWriteItemsRequest()
                    .withTransactItems(
                            new TransactWriteItem().withPut(putNewFileReferenceCount(filename, 0, updateTime)))
                    .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL));
            List<ConsumedCapacity> consumedCapacity = transactWriteItemsResult.getConsumedCapacity();
            double totalConsumed = consumedCapacity.stream().mapToDouble(ConsumedCapacity::getCapacityUnits).sum();
            LOGGER.debug("Put file reference count for file {} to table {}, read capacity consumed = {}",
                    filename, fileReferenceCountTableName, totalConsumed);
        } catch (AmazonDynamoDBException e) {
            if (hasConditionalCheckFailure(e)) {
                throw new FileAlreadyExistsException(filename);
            } else {
                throw new StateStoreException("Failed to add file reference count", e);
            }
        }
    }

    @Override
    public void splitFileReferences(List<SplitFileReferenceRequest> splitRequests) throws SplitRequestsFailedException {
        Instant updateTime = clock.instant();
        DynamoDBSplitRequestsBatch batch = new DynamoDBSplitRequestsBatch();
        int firstUnappliedRequestIndex = 0;
        try {
            for (int i = 0; i < splitRequests.size(); i++) {
                SplitFileReferenceRequest splitRequest = splitRequests.get(i);
                List<TransactWriteItem> requestReferenceWrites = splitFileReferenceWrites(splitRequest, updateTime);
                if (DynamoDBSplitRequestsBatch.wouldOverflowOneTransaction(requestReferenceWrites)) {
                    throw new SplitRequestsFailedException(
                            "Too many writes in one request to fit in one transaction",
                            splitRequests.subList(0, firstUnappliedRequestIndex),
                            splitRequests.subList(firstUnappliedRequestIndex, splitRequests.size()));
                }
                if (batch.wouldOverflow(splitRequest, requestReferenceWrites)) {
                    LOGGER.debug("Split reference requests did not all fit in one transaction, applying {} requests", batch.getRequests().size());
                    applySplitRequestWrites(batch, updateTime);
                    firstUnappliedRequestIndex = i;
                    batch = new DynamoDBSplitRequestsBatch();
                }
                batch.addRequest(splitRequest, requestReferenceWrites);
            }
            if (!batch.isEmpty()) {
                LOGGER.debug("Applying {} split reference requests", batch.getRequests().size());
                applySplitRequestWrites(batch, updateTime);
            }
        } catch (TransactionCanceledException e) {
            throw FailedDynamoDBSplitRequests.from(e, batch).buildSplitRequestsFailedException(
                    splitRequests.subList(0, firstUnappliedRequestIndex),
                    splitRequests.subList(firstUnappliedRequestIndex, splitRequests.size()),
                    fileReferenceFormat);
        } catch (AmazonDynamoDBException e) {
            throw new SplitRequestsFailedException(
                    splitRequests.subList(0, firstUnappliedRequestIndex),
                    splitRequests.subList(firstUnappliedRequestIndex, splitRequests.size()), e);
        }
    }

    private void applySplitRequestWrites(DynamoDBSplitRequestsBatch batch, Instant updateTime) {
        List<TransactWriteItem> writes = Stream.concat(batch.getReferenceWrites().stream(),
                fileReferenceCountWriteItems(batch.getReferenceCountIncrementByFilename(), updateTime))
                .collect(Collectors.toUnmodifiableList());
        applySplitRequestWrites(batch.getRequests(), writes);
    }

    private void applySplitRequestWrites(
            List<SplitFileReferenceRequest> splitRequests, List<TransactWriteItem> writes) {
        TransactWriteItemsRequest transactWriteItemsRequest = new TransactWriteItemsRequest()
                .withTransactItems(writes)
                .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL);
        TransactWriteItemsResult transactWriteItemsResult = dynamoDB.transactWriteItems(transactWriteItemsRequest);
        List<ConsumedCapacity> consumedCapacity = transactWriteItemsResult.getConsumedCapacity();
        double totalConsumed = consumedCapacity.stream().mapToDouble(ConsumedCapacity::getCapacityUnits).sum();
        LOGGER.debug("Removed {} file references and split to {} new file references, capacity consumed = {}",
                splitRequests.size(), splitRequests.stream()
                        .mapToLong(splitRequest -> splitRequest.getNewReferences().size())
                        .sum(),
                totalConsumed);
    }

    private List<TransactWriteItem> splitFileReferenceWrites(SplitFileReferenceRequest splitRequest, Instant updateTime) {
        FileReference oldReference = splitRequest.getOldReference();
        List<FileReference> newReferences = splitRequest.getNewReferences();
        List<TransactWriteItem> writes = new ArrayList<>();
        writes.add(new TransactWriteItem().withDelete(new Delete()
                .withTableName(activeTableName)
                .withKey(fileReferenceFormat.createActiveFileKey(oldReference.getPartitionId(), oldReference.getFilename()))
                .withExpressionAttributeNames(Map.of(
                        "#PartitionAndFilename", PARTITION_ID_AND_FILENAME,
                        "#JobId", JOB_ID))
                .withConditionExpression(
                        "attribute_exists(#PartitionAndFilename) and attribute_not_exists(#JobId)")
                .withReturnValuesOnConditionCheckFailure(ReturnValuesOnConditionCheckFailure.ALL_OLD)));
        for (FileReference newReference : newReferences) {
            writes.add(new TransactWriteItem().withPut(putNewFileReference(newReference, updateTime)));
        }
        return writes;
    }

    @Override
    public void atomicallyReplaceFileReferencesWithNewOnes(List<ReplaceFileReferencesRequest> requests) throws ReplaceRequestsFailedException {
        List<ReplaceFileReferencesRequest> succeeded = new ArrayList<>();
        List<ReplaceFileReferencesRequest> failed = new ArrayList<>();
        List<Exception> failures = new ArrayList<>();
        for (ReplaceFileReferencesRequest request : requests) {
            try {
                atomicallyReplaceFileReferencesWithNewOne(request);
                succeeded.add(request);
            } catch (StateStoreException | RuntimeException e) {
                LOGGER.error("Failed replacing file references for job {}", request.getJobId(), e);
                failures.add(e);
                failed.add(request);
            }
        }
        if (!failures.isEmpty()) {
            throw new ReplaceRequestsFailedException(succeeded, failed, failures);
        }
    }

    private void atomicallyReplaceFileReferencesWithNewOne(ReplaceFileReferencesRequest request) throws StateStoreException {
        FileReference newReference = request.getNewReference();
        FileReference.validateNewReferenceForJobOutput(request.getInputFiles(), newReference);
        // Delete record for file for current status
        Instant updateTime = clock.instant();
        List<TransactWriteItem> writes = new ArrayList<>();
        List<TransactWriteItem> referenceCountUpdates = new ArrayList<>();
        request.getInputFiles().forEach(filename -> {
            Delete delete = new Delete()
                    .withTableName(activeTableName)
                    .withKey(fileReferenceFormat.createActiveFileKey(request.getPartitionId(), filename))
                    .withExpressionAttributeNames(Map.of(
                            "#PartitionAndFilename", PARTITION_ID_AND_FILENAME,
                            "#JobId", JOB_ID))
                    .withExpressionAttributeValues(Map.of(":jobid", createStringAttribute(request.getJobId())))
                    .withConditionExpression(
                            "attribute_exists(#PartitionAndFilename) and #JobId = :jobid")
                    .withReturnValuesOnConditionCheckFailure(ReturnValuesOnConditionCheckFailure.ALL_OLD);
            writes.add(new TransactWriteItem().withDelete(delete));
            referenceCountUpdates.add(new TransactWriteItem().withUpdate(fileReferenceCountUpdate(filename, updateTime, -1)));
        });
        // Add record for file for new status
        writes.add(new TransactWriteItem().withPut(putNewFileReference(newReference, updateTime)));
        writes.addAll(referenceCountUpdates);
        writes.add(new TransactWriteItem().withPut(putNewFileReferenceCount(newReference.getFilename(), 1, updateTime)));
        TransactWriteItemsRequest transactWriteItemsRequest = new TransactWriteItemsRequest()
                .withTransactItems(writes)
                .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL);
        try {
            TransactWriteItemsResult transactWriteItemsResult = dynamoDB.transactWriteItems(transactWriteItemsRequest);
            List<ConsumedCapacity> consumedCapacity = transactWriteItemsResult.getConsumedCapacity();
            double totalConsumed = consumedCapacity.stream().mapToDouble(ConsumedCapacity::getCapacityUnits).sum();
            LOGGER.debug("Removed {} file references and added 1 new file, capacity consumed = {}",
                    request.getInputFiles().size(), totalConsumed);
        } catch (TransactionCanceledException e) {
            throw FailedDynamoDBReplaceReferences.from(e, request)
                    .buildStateStoreException(fileReferenceFormat);
        } catch (AmazonDynamoDBException e) {
            throw new StateStoreException("Failed to mark files ready for GC and add new files", e);
        }
    }

    @Override
    public void assignJobIds(List<AssignJobIdRequest> requests) throws StateStoreException {
        long updateTime = clock.millis();
        for (AssignJobIdRequest request : requests) {
            assignJobId(request, updateTime);
        }
    }

    private void assignJobId(AssignJobIdRequest request, long updateTime) throws StateStoreException {
        // Create Puts for each of the files, conditional on the compactionJob field being not present
        List<TransactWriteItem> writes = request.getFilenames().stream().map(file -> new TransactWriteItem().withUpdate(new Update()
                .withTableName(activeTableName)
                .withKey(fileReferenceFormat.createActiveFileKey(request.getPartitionId(), file))
                .withUpdateExpression("SET #jobid = :jobid, #time = :time")
                .withConditionExpression("attribute_exists(#time) and attribute_not_exists(#jobid)")
                .withExpressionAttributeNames(Map.of(
                        "#jobid", JOB_ID,
                        "#time", LAST_UPDATE_TIME))
                .withExpressionAttributeValues(Map.of(
                        ":jobid", createStringAttribute(request.getJobId()),
                        ":time", createNumberAttribute(updateTime)))
                .withReturnValuesOnConditionCheckFailure(ReturnValuesOnConditionCheckFailure.ALL_OLD)))
                .collect(Collectors.toUnmodifiableList());
        TransactWriteItemsRequest transactWriteItemsRequest = new TransactWriteItemsRequest()
                .withTransactItems(writes)
                .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL);
        try {
            TransactWriteItemsResult transactWriteItemsResult = dynamoDB.transactWriteItems(transactWriteItemsRequest);
            List<ConsumedCapacity> consumedCapacity = transactWriteItemsResult.getConsumedCapacity();
            double totalConsumed = consumedCapacity.stream().mapToDouble(ConsumedCapacity::getCapacityUnits).sum();
            LOGGER.debug("Updated job status of {} files, read capacity consumed = {}",
                    request.getFilenames().size(), totalConsumed);
        } catch (TransactionCanceledException e) {
            throw buildAssignJobIdStateStoreException(e, request);
        } catch (AmazonDynamoDBException e) {
            throw new StateStoreException("Failed to assign files to job", e);
        }
    }

    private StateStoreException buildAssignJobIdStateStoreException(
            TransactionCanceledException e, AssignJobIdRequest request) {
        List<CancellationReason> reasons = e.getCancellationReasons();
        for (int i = 0; i < request.getFilenames().size(); i++) {
            CancellationReason reason = reasons.get(i);
            String filename = request.getFilenames().get(i);
            if (isConditionCheckFailure(reason)) {
                return getFileReferenceExceptionFromReason(filename, request.getPartitionId(), reason, item -> {
                    FileReference failedUpdate = fileReferenceFormat.getFileReferenceFromAttributeValues(reason.getItem());
                    if (failedUpdate.getJobId() != null) {
                        return new FileReferenceAssignedToJobException(failedUpdate, e);
                    } else {
                        return new FileReferenceNotFoundException(failedUpdate, e);
                    }
                }, e);
            }
        }
        return new StateStoreException("Failed to assign files to job", e);
    }

    @Override
    public void deleteGarbageCollectedFileReferenceCounts(List<String> filenames) throws StateStoreException {
        int i = 0;
        double totalCapacityConsumed = 0;
        double batchCapacityConsumed = 0;
        for (String filename : filenames) {
            TransactWriteItem writeItem = new TransactWriteItem().withDelete(new Delete()
                    .withTableName(fileReferenceCountTableName)
                    .withKey(fileReferenceFormat.createReferenceCountKey(filename))
                    .withConditionExpression("#References = :refs")
                    .withExpressionAttributeNames(Map.of("#References", REFERENCES))
                    .withExpressionAttributeValues(Map.of(":refs", createNumberAttribute(0)))
                    .withReturnValuesOnConditionCheckFailure(ReturnValuesOnConditionCheckFailure.ALL_OLD));
            try {
                TransactWriteItemsResult result = dynamoDB.transactWriteItems(new TransactWriteItemsRequest().withTransactItems(writeItem));
                if (result.getConsumedCapacity() != null) {
                    batchCapacityConsumed += result.getConsumedCapacity().get(0).getCapacityUnits();
                }
                if (i % 100 == 0) {
                    LOGGER.debug("Deleted 100 unreferenced files, capacity consumed = {}", batchCapacityConsumed);
                    totalCapacityConsumed += batchCapacityConsumed;
                    batchCapacityConsumed = 0;
                }
            } catch (TransactionCanceledException e) {
                throw buildDeleteGCFileStateStoreException(e, filename);
            } catch (AmazonDynamoDBException e) {
                throw new StateStoreException("Failed to delete unreferenced files", e);
            }
            i++;
        }
        LOGGER.debug("Deleted a total of {} unreferenced files, total consumed capacity = {}", filenames.size(), totalCapacityConsumed);
    }

    private StateStoreException buildDeleteGCFileStateStoreException(
            TransactionCanceledException e, String filename) {
        List<CancellationReason> reasons = e.getCancellationReasons();
        for (CancellationReason reason : reasons) {
            if (isConditionCheckFailure(reason)) {
                return getFileExceptionFromReason(filename, reason, item -> new FileHasReferencesException(
                        getStringAttribute(reason.getItem(), FILENAME),
                        getIntAttribute(reason.getItem(), REFERENCES, 0), e), e);
            }
        }
        return new StateStoreException("Failed to delete unreferenced files", e);
    }

    private static StateStoreException getFileReferenceExceptionFromReason(String filename, String partitionId, CancellationReason reason, ConditionalFailureCheck itemCheck, Exception cause) {
        if (reason.getItem() != null) {
            return itemCheck.check(reason.getItem());
        } else {
            return new FileReferenceNotFoundException(filename, partitionId, cause);
        }
    }

    private static StateStoreException getFileExceptionFromReason(String filename, CancellationReason reason, ConditionalFailureCheck itemCheck, Exception cause) {
        if (reason.getItem() != null) {
            return itemCheck.check(reason.getItem());
        } else {
            return new FileNotFoundException(filename, cause);
        }
    }

    /**
     * A check to determine why a conditional failure occurred for a given DynamoDB item.
     */
    interface ConditionalFailureCheck {
        StateStoreException check(Map<String, AttributeValue> item);
    }

    @Override
    public List<FileReference> getFileReferences() throws StateStoreException {
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
                    double newConsumed = totalCapacity.updateAndGet(old -> old + result.getConsumedCapacity().getCapacityUnits());
                    LOGGER.debug("Queried table {} for all ready for GC files, capacity consumed = {}",
                            fileReferenceCountTableName, newConsumed);
                    return result.getItems().stream();
                }).map(fileReferenceFormat::getFilenameFromReferenceCount);
    }

    @Override
    public List<FileReference> getFileReferencesWithNoJobId() throws StateStoreException {
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
    public void clearFileData() throws StateStoreException {
        try {
            clearDynamoTable(activeTableName, fileReferenceFormat::getActiveFileKey);
            clearDynamoTable(fileReferenceCountTableName, item -> fileReferenceFormat.createReferenceCountKey(item.get(FILENAME).getS()));
        } catch (RuntimeException e) {
            throw new StateStoreException("Failed clearing files", e);
        }
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
    public AllReferencesToAllFiles getAllFilesWithMaxUnreferenced(int maxUnreferencedFiles) throws StateStoreException {
        Map<String, List<FileReference>> referencesByFilename = getFileReferences().stream()
                .collect(Collectors.groupingBy(FileReference::getFilename));
        List<AllReferencesToAFile> filesWithNoReferences = new ArrayList<>();
        int readyForGCFound = 0;
        boolean moreReadyForGC = false;
        try {
            for (QueryResult result : (Iterable<QueryResult>) () -> streamReferenceCountPagesWithNoReferences().iterator()) {
                readyForGCFound += result.getItems().size();
                Stream<AllReferencesToAFile> filesWithNoReferencesStream = result.getItems().stream()
                        .map(item -> fileReferenceFormat.getReferencedFile(item, referencesByFilename));
                if (readyForGCFound > maxUnreferencedFiles) {
                    moreReadyForGC = true;
                    filesWithNoReferencesStream.limit(result.getItems().size() - (readyForGCFound - maxUnreferencedFiles))
                            .forEach(filesWithNoReferences::add);
                    break;
                } else {
                    filesWithNoReferencesStream.forEach(filesWithNoReferences::add);
                }
            }
        } catch (AmazonDynamoDBException e) {
            throw new StateStoreException("Failed to load unreferenced files", e);
        }
        return new AllReferencesToAllFiles(
                Stream.concat(
                        streamReferenceCountItemsWithReferences()
                                .map(item -> fileReferenceFormat.getReferencedFile(item, referencesByFilename)),
                        filesWithNoReferences.stream()).collect(Collectors.toUnmodifiableList()),
                moreReadyForGC);
    }

    private Stream<Map<String, AttributeValue>> streamReferenceCountItemsWithReferences() {
        QueryRequest queryRequest = new QueryRequest()
                .withTableName(fileReferenceCountTableName)
                .withConsistentRead(stronglyConsistentReads)
                .withKeyConditionExpression("#TableId = :table_id")
                .withFilterExpression("#References > :zero")
                .withExpressionAttributeNames(Map.of("#TableId", TABLE_ID, "#References", REFERENCES))
                .withExpressionAttributeValues(new DynamoDBRecordBuilder()
                        .string(":table_id", sleeperTableId)
                        .number(":zero", 0)
                        .build())
                .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL);
        AtomicReference<Double> totalCapacity = new AtomicReference<>(0.0D);
        return streamPagedResults(dynamoDB, queryRequest)
                .peek(result -> {
                    double newConsumed = totalCapacity.updateAndGet(old -> old + result.getConsumedCapacity().getCapacityUnits());
                    LOGGER.debug("Queried table {} for positive file reference counts, capacity consumed = {}",
                            fileReferenceCountTableName, newConsumed);
                }).flatMap(result -> result.getItems().stream());
    }

    private Stream<QueryResult> streamReferenceCountPagesWithNoReferences() {
        QueryRequest queryRequest = new QueryRequest()
                .withTableName(fileReferenceCountTableName)
                .withConsistentRead(stronglyConsistentReads)
                .withKeyConditionExpression("#TableId = :table_id")
                .withFilterExpression("#References <= :zero")
                .withExpressionAttributeNames(Map.of("#TableId", TABLE_ID, "#References", REFERENCES))
                .withExpressionAttributeValues(new DynamoDBRecordBuilder()
                        .string(":table_id", sleeperTableId)
                        .number(":zero", 0)
                        .build())
                .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL);
        AtomicReference<Double> totalCapacity = new AtomicReference<>(0.0D);
        return streamPagedResults(dynamoDB, queryRequest)
                .peek(result -> {
                    double newConsumed = totalCapacity.updateAndGet(old -> old + result.getConsumedCapacity().getCapacityUnits());
                    LOGGER.debug("Queried table {} for unreferenced files, capacity consumed = {}",
                            fileReferenceCountTableName, newConsumed);
                });
    }

    /**
     * Used to set the current time. Should only be called during tests.
     *
     * @param now Time to set to be the current time
     */
    @Override
    public void fixFileUpdateTime(Instant now) {
        clock = Clock.fixed(now, ZoneId.of("UTC"));
    }

    private Stream<TransactWriteItem> fileReferenceCountWriteItems(Map<String, Integer> incrementByFilename, Instant updateTime) {
        return fileReferenceCountUpdates(incrementByFilename, updateTime)
                .map(update -> new TransactWriteItem().withUpdate(update));
    }

    private Stream<Update> fileReferenceCountUpdates(Map<String, Integer> incrementByFilename, Instant updateTime) {
        return incrementByFilename.entrySet().stream()
                .map(entry -> fileReferenceCountUpdate(entry.getKey(), updateTime, entry.getValue()));
    }

    private Update fileReferenceCountUpdate(String filename, Instant updateTime, int increment) {
        return new Update().withTableName(fileReferenceCountTableName)
                .withKey(fileReferenceFormat.createReferenceCountKey(filename))
                .withUpdateExpression("SET #UpdateTime = :time, " +
                        "#References = #References + :inc")
                .withConditionExpression("attribute_exists(#References)")
                .withExpressionAttributeNames(Map.of(
                        "#UpdateTime", LAST_UPDATE_TIME,
                        "#References", REFERENCES))
                .withExpressionAttributeValues(new DynamoDBRecordBuilder()
                        .number(":time", updateTime.toEpochMilli())
                        .number(":inc", increment)
                        .build());
    }

    private Put putNewFileReferenceCount(String filename, int referenceCount, Instant updateTime) {
        return new Put().withTableName(fileReferenceCountTableName)
                .withItem(fileReferenceFormat.createReferenceCountRecord(filename, updateTime, referenceCount))
                .withConditionExpression("attribute_not_exists(#Filename)")
                .withExpressionAttributeNames(Map.of("#Filename", FILENAME));
    }

    private Put putNewFileReference(FileReference fileReference, Instant updateTime) {
        return new Put()
                .withTableName(activeTableName)
                .withItem(fileReferenceFormat.createRecord(fileReference.toBuilder().lastStateStoreUpdateTime(updateTime).build()))
                .withConditionExpression("attribute_not_exists(#PartitionAndFile)")
                .withExpressionAttributeNames(Map.of("#PartitionAndFile", PARTITION_ID_AND_FILENAME))
                .withReturnValuesOnConditionCheckFailure(ReturnValuesOnConditionCheckFailure.ALL_OLD);
    }

    /**
     * Builder to create a file reference store backed by DynamoDB.
     */
    static final class Builder {
        private AmazonDynamoDB dynamoDB;
        private String activeTableName;
        private String fileReferenceCountTableName;
        private String sleeperTableId;
        private boolean stronglyConsistentReads;

        private Builder() {
        }

        Builder instanceProperties(InstanceProperties instanceProperties) {
            return activeTableName(instanceProperties.get(ACTIVE_FILES_TABLENAME))
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
