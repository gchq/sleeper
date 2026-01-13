/*
 * Copyright 2022-2025 Crown Copyright
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
package sleeper.splitter.core.split;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.core.partition.Partition;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.commit.StateStoreCommitRequest;
import sleeper.core.statestore.transactionlog.transaction.impl.SplitPartitionTransaction;
import sleeper.sketches.store.SketchesStore;
import sleeper.splitter.core.sketches.SketchesForSplitting;
import sleeper.splitter.core.sketches.UnionSketchesForSplitting;

import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

import static sleeper.core.properties.table.TableProperty.PARTITION_SPLIT_ASYNC_COMMIT;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;

/**
 * Splits a partition. Identifies the median value of the first dimension. If that leads to a valid split (i.e. one
 * where it is not equal to the minimum value and not equal to the maximum value) then that is used to split the
 * partition. If it doesn't lead to a valid split then the above is repeated for the second dimension. This continues
 * until either a valid split is found or no split is possible.
 * <p>
 * Note that there are two situations in which a partition cannot be split:
 * - If the partition consists of a single point (i.e. the minimum equals the maximum).
 * - If the median equals the minimum then the partition cannot be split.
 * This is because it would have to be split into [min, median) and [median, max), but if the min equals the median then
 * the left one can't have any data in it as a key x in it would have to have min <= x < median = min which is a
 * contradiction.
 */
public class SplitPartition {
    public static final Logger LOGGER = LoggerFactory.getLogger(SplitPartition.class);

    private final StateStore stateStore;
    private final TableProperties tableProperties;
    private final Schema schema;
    private final SketchesStore sketchesStore;
    private final Supplier<String> idSupplier;
    private final SendAsyncCommit sendAsyncCommit;

    public SplitPartition(StateStore stateStore,
            TableProperties tableProperties,
            SketchesStore sketchesStore,
            Supplier<String> idSupplier,
            SendAsyncCommit sendAsyncCommit) {
        this.stateStore = stateStore;
        this.tableProperties = tableProperties;
        this.schema = tableProperties.getSchema();
        this.sketchesStore = sketchesStore;
        this.idSupplier = idSupplier;
        this.sendAsyncCommit = sendAsyncCommit;
    }

    public void splitPartition(Partition partition, List<String> fileNames) {
        getResultIfSplittable(partition, fileNames)
                .ifPresent(this::apply);
    }

    private Optional<SplitPartitionResult> getResultIfSplittable(Partition partition, List<String> fileNames) {
        SketchesForSplitting sketches = new UnionSketchesForSplitting(fileNames.stream()
                .map(filename -> sketchesStore.loadFileSketches(filename, schema))
                .toList());
        return FindPartitionSplitPoint.getResultIfSplittable(schema, partition, sketches, idSupplier);
    }

    private void apply(SplitPartitionResult result) {

        Partition parentPartition = result.getParentPartition();
        Partition leftChild = result.getLeftChild();
        Partition rightChild = result.getRightChild();
        Field splitField = schema.getRowKeyFields().get(parentPartition.getDimension());
        LOGGER.info("Updating StateStore:");
        LOGGER.info("Split partition ({}) is marked as not a leaf partition, split on field {}",
                parentPartition.getId(), splitField.getName());
        LOGGER.info("New partition: {}", leftChild);
        LOGGER.info("New partition: {}", rightChild);

        if (!tableProperties.getBoolean(PARTITION_SPLIT_ASYNC_COMMIT)) {
            new SplitPartitionTransaction(parentPartition, List.of(leftChild, rightChild)).synchronousCommit(stateStore);
        } else {
            sendAsyncCommit.sendCommit(StateStoreCommitRequest.create(tableProperties.get(TABLE_ID),
                    new SplitPartitionTransaction(parentPartition, List.of(leftChild, rightChild))));
        }
    }

    /**
     * Sends commit state store updates to the state store committer.
     */
    @FunctionalInterface
    public interface SendAsyncCommit {
        void sendCommit(StateStoreCommitRequest splitPartitionCommitRequest);
    }
}
