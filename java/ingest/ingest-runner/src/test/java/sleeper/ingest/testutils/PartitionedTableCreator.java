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
package sleeper.ingest.testutils;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import org.apache.commons.lang3.tuple.Pair;

import sleeper.core.key.Key;
import sleeper.core.partition.Partition;
import sleeper.core.partition.PartitionTree;
import sleeper.core.range.Range;
import sleeper.core.range.Range.RangeFactory;
import sleeper.core.range.Region;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.statestore.StateStore;
import sleeper.statestore.StateStoreException;
import sleeper.statestore.dynamodb.DynamoDBStateStore;
import sleeper.statestore.dynamodb.DynamoDBStateStoreCreator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class PartitionedTableCreator {

    private PartitionedTableCreator() {
    }

    public static StateStore createStateStore(
            AmazonDynamoDB dynamoDbClient,
            Schema sleeperSchema,
            List<Pair<Key, Integer>> keyAndDimensionToSplitOnInOrder) throws StateStoreException {
        String tableNameStub = UUID.randomUUID().toString();
        DynamoDBStateStoreCreator dynamoDBStateStoreCreator =
                new DynamoDBStateStoreCreator(tableNameStub, sleeperSchema, dynamoDbClient);
        DynamoDBStateStore stateStore = dynamoDBStateStoreCreator.create();
        stateStore.initialise();
        PartitionedTableCreator.repeatedlySplitPartitions(
                stateStore,
                sleeperSchema,
                keyAndDimensionToSplitOnInOrder);
        return stateStore;
    }

    public static void repeatedlySplitPartitions(StateStore stateStore,
                                                 Schema sleeperSchema,
                                                 List<Pair<Key, Integer>> keyAndDimensionToSplitOnInOrder) throws StateStoreException {
        PartitionTree partitionTree = new PartitionTree(sleeperSchema, stateStore.getAllPartitions());
        for (Pair<Key, Integer> keyAndDimensionToSplitOn : keyAndDimensionToSplitOnInOrder) {
            splitPartition(stateStore, sleeperSchema, partitionTree, keyAndDimensionToSplitOn.getLeft(), keyAndDimensionToSplitOn.getRight());
        }
    }

    public static void splitPartition(StateStore stateStore,
                                      Schema sleeperSchema,
                                      PartitionTree partitionTree,
                                      Key keyContainingSplitPoint,
                                      int dimensionToSplitOn) throws StateStoreException {
        Partition parentPartition = partitionTree.getLeafPartition(keyContainingSplitPoint);
        // Create child partitions
        Partition leftChild = createSplitPartition(sleeperSchema, parentPartition, keyContainingSplitPoint, dimensionToSplitOn, true);
        Partition rightChild = createSplitPartition(sleeperSchema, parentPartition, keyContainingSplitPoint, dimensionToSplitOn, false);
        // Update parent partition
        parentPartition = parentPartition.toBuilder().leafPartition(false).build();
        parentPartition.setChildPartitionIds(Arrays.asList(leftChild.getId(), rightChild.getId()));
        parentPartition = parentPartition.toBuilder().dimension(dimensionToSplitOn).build();
        // Update state store
        stateStore.atomicallyUpdatePartitionAndCreateNewOnes(parentPartition, leftChild, rightChild);
    }

    private static Partition createSplitPartition(Schema sleeperSchema,
                                                  Partition parentPartition,
                                                  Key keyContainingSplitPoint,
                                                  int dimensionToSplitOn,
                                                  boolean isLeftSplit) {
        Field fieldToSplitOn = sleeperSchema.getRowKeyFields().get(dimensionToSplitOn);
        Object splitPoint = keyContainingSplitPoint.get(dimensionToSplitOn);
        Stream<Range> parentRangesWithoutSplitDimensionStream = parentPartition.getRegion().getRanges().stream()
                .filter(range -> !(range.getFieldName().equals(fieldToSplitOn.getName())));
        RangeFactory rangeFactory = new RangeFactory(sleeperSchema);
        Range parentRangeForSplitDimension = parentPartition.getRegion().getRange(fieldToSplitOn.getName());
        Range childRangeForSplitDimension = rangeFactory.createRange(
                fieldToSplitOn,
                isLeftSplit ? parentRangeForSplitDimension.getMin() : splitPoint,
                isLeftSplit ? splitPoint : parentRangeForSplitDimension.getMax());
        List<Range> childRanges = Stream.of(parentRangesWithoutSplitDimensionStream, Stream.of(childRangeForSplitDimension))
                .flatMap(Function.identity())
                .collect(Collectors.toList());
        return Partition.builder()
                .rowKeyTypes(sleeperSchema.getRowKeyTypes())
                .region(new Region(childRanges))
                .id(UUID.randomUUID().toString())
                .leafPartition(true)
                .parentPartitionId(parentPartition.getId())
                .childPartitionIds(new ArrayList<>())
                .dimension(-1)
                .build();
    }
}
