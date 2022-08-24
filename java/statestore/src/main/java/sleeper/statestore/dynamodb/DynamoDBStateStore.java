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
import static sleeper.statestore.dynamodb.DynamoDBAttributes.createNumberAttribute;
import static sleeper.statestore.dynamodb.DynamoDBAttributes.createStringAttribute;

/**
 * An implementation of {@link StateStore} that uses DynamoDB to store the state.
 */
public class DynamoDBStateStore implements StateStore {
    private static final Logger LOGGER = LoggerFactory.getLogger(DynamoDBStateStore.class);

    public static final String FILE_NAME = DynamoDBFileInfoFormat.NAME;
    public static final String FILE_STATUS = DynamoDBFileInfoFormat.STATUS;
    public static final String FILE_PARTITION = DynamoDBFileInfoFormat.PARTITION;
    public static final String PARTITION_ID = "PartitionId";
    private static final String PARTITION_IS_LEAF = "PartitionIsLeaf";
    private static final String PARTITION_PARENT_ID = "PartitionParentId";
    private static final String PARTITION_CHILD_IDS = "PartitionChildIds";
    private static final String PARTITION_SPLIT_DIMENSION = "PartitionSplitDimension";
    private static final String REGION = "Region";

    private final AmazonDynamoDB dynamoDB;
    private final String partitionTableName;
    private final Schema schema;
    private final List<PrimitiveType> rowKeyTypes;
    private final boolean stronglyConsistentReads;
    private final RegionSerDe regionSerDe;
    private final DynamoDBFilesStore filesStore;

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
        this.partitionTableName = Objects.requireNonNull(partitionTablename, "partitionTableName must not be null");
        this.schema = Objects.requireNonNull(schema, "schema must not be null");
        this.rowKeyTypes = schema.getRowKeyTypes();
        if (this.rowKeyTypes.isEmpty()) {
            throw new IllegalArgumentException("rowKeyTypes must not be empty");
        }
        this.stronglyConsistentReads = stronglyConsistentReads;
        this.dynamoDB = Objects.requireNonNull(dynamoDB, "dynamoDB must not be null");
        this.regionSerDe = new RegionSerDe(schema);
        this.filesStore = DynamoDBFilesStore.builder()
                .dynamoDB(dynamoDB).schema(schema)
                .activeTablename(activeFileInfoTablename).readyForGCTablename(readyForGCFileInfoTablename)
                .stronglyConsistentReads(stronglyConsistentReads)
                .garbageCollectorDelayBeforeDeletionInSeconds(garbageCollectorDelayBeforeDeletionInSeconds)
                .build();
    }

    @Override
    public List<PrimitiveType> getRowKeyTypes() {
        return Collections.unmodifiableList(rowKeyTypes);
    }

    @Override
    public void addFile(FileInfo fileInfo) throws StateStoreException {
        filesStore.addFile(fileInfo);
    }

    @Override
    public void addFiles(List<FileInfo> fileInfos) throws StateStoreException {
        filesStore.addFiles(fileInfos);
    }

    @Override
    public void atomicallyUpdateFilesToReadyForGCAndCreateNewActiveFile(
            List<FileInfo> filesToBeMarkedReadyForGC,
            FileInfo newActiveFile) throws StateStoreException {
        filesStore.atomicallyUpdateFilesToReadyForGCAndCreateNewActiveFile(filesToBeMarkedReadyForGC, newActiveFile);
    }

    @Override
    public void atomicallyUpdateFilesToReadyForGCAndCreateNewActiveFiles(
            List<FileInfo> filesToBeMarkedReadyForGC, FileInfo leftFileInfo, FileInfo rightFileInfo) throws StateStoreException {
        filesStore.atomicallyUpdateFilesToReadyForGCAndCreateNewActiveFiles(
                filesToBeMarkedReadyForGC, leftFileInfo, rightFileInfo);
    }

    /**
     * Atomically updates the job field of the given files to the given id, as long as
     * the compactionJob field is currently null.
     */
    @Override
    public void atomicallyUpdateJobStatusOfFiles(String jobId, List<FileInfo> files)
            throws StateStoreException {
        filesStore.atomicallyUpdateJobStatusOfFiles(jobId, files);
    }

    @Override
    public void deleteReadyForGCFile(FileInfo fileInfo) throws StateStoreException {
        filesStore.deleteReadyForGCFile(fileInfo);
    }

    @Override
    public List<FileInfo> getActiveFiles() throws StateStoreException {
        return filesStore.getActiveFiles();
    }

    @Override
    public Iterator<FileInfo> getReadyForGCFiles() {
        return filesStore.getReadyForGCFiles();
    }

    @Override
    public List<FileInfo> getActiveFilesWithNoJobId() throws StateStoreException {
        return filesStore.getActiveFilesWithNoJobId();
    }

    @Override
    public Map<String, List<String>> getPartitionToActiveFilesMap() throws StateStoreException {
        return filesStore.getPartitionToActiveFilesMap();
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
