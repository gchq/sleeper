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
import com.amazonaws.services.dynamodbv2.model.DeleteItemRequest;
import com.amazonaws.services.dynamodbv2.model.Put;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.ReturnConsumedCapacity;
import com.amazonaws.services.dynamodbv2.model.TransactWriteItem;
import com.amazonaws.services.dynamodbv2.model.TransactWriteItemsRequest;
import com.amazonaws.services.dynamodbv2.model.TransactWriteItemsResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.core.partition.Partition;
import sleeper.core.partition.PartitionsFromSplitPoints;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TableProperty;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.PartitionStore;
import sleeper.core.statestore.StateStoreException;
import sleeper.dynamodb.tools.DynamoDBRecordBuilder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static sleeper.core.properties.table.TableProperty.DYNAMODB_STRONGLY_CONSISTENT_READS;
import static sleeper.dynamodb.tools.DynamoDBUtils.deleteAllDynamoTableItems;
import static sleeper.dynamodb.tools.DynamoDBUtils.streamPagedResults;
import static sleeper.statestore.dynamodb.DynamoDBPartitionFormat.IS_LEAF;
import static sleeper.statestore.dynamodb.DynamoDBPartitionFormat.TABLE_ID;

/**
 * A Sleeper table partition store where the state is held in DynamoDB.
 */
class DynamoDBPartitionStore implements PartitionStore {

    private static final Logger LOGGER = LoggerFactory.getLogger(DynamoDBPartitionStore.class);

    private final AmazonDynamoDB dynamoDB;
    private final String dynamoTableName;
    private final String sleeperTableId;
    private final Schema schema;
    private final boolean stronglyConsistentReads;
    private final DynamoDBPartitionFormat partitionFormat;

    private DynamoDBPartitionStore(Builder builder) {
        dynamoDB = Objects.requireNonNull(builder.dynamoDB, "dynamoDB must not be null");
        schema = Objects.requireNonNull(builder.schema, "schema must not be null");
        dynamoTableName = Objects.requireNonNull(builder.dynamoTableName, "dynamoTableName must not be null");
        sleeperTableId = Objects.requireNonNull(builder.sleeperTableId, "sleeperTableId must not be null");
        stronglyConsistentReads = builder.stronglyConsistentReads;
        partitionFormat = new DynamoDBPartitionFormat(sleeperTableId, schema);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public void atomicallyUpdatePartitionAndCreateNewOnes(
            Partition splitPartition, Partition newPartition1, Partition newPartition2) throws StateStoreException {
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
        Map<String, AttributeValue> item = partitionFormat.getItemFromPartition(splitPartition);
        Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();
        expressionAttributeValues.put(":true", new AttributeValue("true"));
        Put put = new Put()
                .withTableName(dynamoTableName)
                .withItem(item)
                .withExpressionAttributeValues(expressionAttributeValues)
                .withConditionExpression(IS_LEAF + " = :true");
        writes.add(new TransactWriteItem().withPut(put));
        for (Partition partition : Arrays.asList(newPartition1, newPartition2)) {
            Map<String, AttributeValue> item2 = partitionFormat.getItemFromPartition(partition);
            Put put2 = new Put()
                    .withTableName(dynamoTableName)
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
        } catch (AmazonDynamoDBException e) {
            throw new StateStoreException("Failed to split partition", e);
        }
    }

    @Override
    public List<Partition> getAllPartitions() throws StateStoreException {
        try {
            QueryRequest queryRequest = new QueryRequest()
                    .withTableName(dynamoTableName)
                    .withConsistentRead(stronglyConsistentReads)
                    .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
                    .withKeyConditionExpression("#TableId = :table_id")
                    .withExpressionAttributeNames(Map.of("#TableId", TABLE_ID))
                    .withExpressionAttributeValues(new DynamoDBRecordBuilder()
                            .string(":table_id", sleeperTableId)
                            .build());
            AtomicReference<Double> totalCapacity = new AtomicReference<>(0.0D);
            List<Map<String, AttributeValue>> results = streamPagedResults(dynamoDB, queryRequest)
                    .flatMap(result -> {
                        totalCapacity.updateAndGet(old -> old + result.getConsumedCapacity().getCapacityUnits());
                        return result.getItems().stream();
                    }).collect(Collectors.toList());
            LOGGER.debug("Queried for all partitions, capacity consumed = {}", totalCapacity);
            List<Partition> partitionResults = new ArrayList<>();
            for (Map<String, AttributeValue> map : results) {
                partitionResults.add(partitionFormat.getPartitionFromAttributeValues(map));
            }
            return partitionResults;
        } catch (AmazonDynamoDBException e) {
            throw new StateStoreException("Failed to load partitions", e);
        }
    }

    @Override
    public List<Partition> getLeafPartitions() throws StateStoreException {
        // TODO optimise by pushing the predicate down to Dynamo
        return getAllPartitions().stream()
                .filter(Partition::isLeafPartition)
                .collect(Collectors.toList());
    }

    @Override
    public void initialise() throws StateStoreException {
        initialise(new PartitionsFromSplitPoints(schema, Collections.emptyList()).construct());
    }

    @Override
    public void initialise(List<Partition> partitions) throws StateStoreException {
        if (null == partitions || partitions.isEmpty()) {
            throw new StateStoreException("At least one partition must be provided");
        }
        getAllPartitions().forEach(partition -> dynamoDB.deleteItem(new DeleteItemRequest()
                .withTableName(dynamoTableName)
                .withKey(partitionFormat.getKeyFromPartition(partition))));
        for (Partition partition : partitions) {
            addPartition(partition);
            LOGGER.debug("Added partition {}", partition);
        }
    }

    @Override
    public void clearPartitionData() throws StateStoreException {
        try {
            deleteAllDynamoTableItems(dynamoDB, new QueryRequest().withTableName(dynamoTableName)
                    .withExpressionAttributeNames(Map.of("#TableId", TABLE_ID))
                    .withExpressionAttributeValues(new DynamoDBRecordBuilder()
                            .string(":table_id", sleeperTableId)
                            .build())
                    .withKeyConditionExpression("#TableId = :table_id"),
                    partitionFormat::getKey);
        } catch (RuntimeException e) {
            throw new StateStoreException("Failed deleting partitions", e);
        }
    }

    private void addPartition(Partition partition) throws StateStoreException {
        try {
            Map<String, AttributeValue> map = partitionFormat.getItemFromPartition(partition);
            PutItemRequest putItemRequest = new PutItemRequest()
                    .withTableName(dynamoTableName)
                    .withItem(map)
                    .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL);
            PutItemResult putItemResult = dynamoDB.putItem(putItemRequest);
            LOGGER.debug("Added partition with id {}, capacity consumed = {}",
                    partition.getId(), putItemResult.getConsumedCapacity().getCapacityUnits());
        } catch (AmazonDynamoDBException e) {
            throw new StateStoreException("Failed to add partition", e);
        }
    }

    /**
     * Builder to create a partition store backed by DynamoDB.
     */
    static final class Builder {
        private AmazonDynamoDB dynamoDB;
        private String dynamoTableName;
        private String sleeperTableId;
        private Schema schema;
        private boolean stronglyConsistentReads;

        private Builder() {
        }

        Builder dynamoDB(AmazonDynamoDB dynamoDB) {
            this.dynamoDB = dynamoDB;
            return this;
        }

        Builder dynamoTableName(String dynamoTableName) {
            this.dynamoTableName = dynamoTableName;
            return this;
        }

        Builder tableProperties(TableProperties tableProperties) {
            return schema(tableProperties.getSchema())
                    .sleeperTableId(tableProperties.get(TableProperty.TABLE_ID))
                    .stronglyConsistentReads(tableProperties.getBoolean(DYNAMODB_STRONGLY_CONSISTENT_READS));
        }

        Builder sleeperTableId(String sleeperTableId) {
            this.sleeperTableId = sleeperTableId;
            return this;
        }

        Builder schema(Schema schema) {
            this.schema = schema;
            return this;
        }

        Builder stronglyConsistentReads(boolean stronglyConsistentReads) {
            this.stronglyConsistentReads = stronglyConsistentReads;
            return this;
        }

        DynamoDBPartitionStore build() {
            return new DynamoDBPartitionStore(this);
        }
    }
}
