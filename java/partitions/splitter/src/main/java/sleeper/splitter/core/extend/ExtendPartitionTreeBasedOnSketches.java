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
package sleeper.splitter.core.extend;

import sleeper.core.partition.Partition;
import sleeper.core.partition.PartitionTree;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.transactionlog.transaction.impl.ExtendPartitionTreeTransaction;
import sleeper.core.util.PercentageUtil;
import sleeper.sketches.Sketches;
import sleeper.splitter.core.sketches.SketchesForSplitting;
import sleeper.splitter.core.split.FindPartitionSplitPoint;
import sleeper.splitter.core.split.SplitPartitionResult;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Supplier;

import static java.util.stream.Collectors.toMap;
import static sleeper.core.properties.table.TableProperty.BULK_IMPORT_MIN_LEAF_PARTITION_COUNT;
import static sleeper.core.properties.table.TableProperty.PARTITION_SPLIT_MIN_DISTRIBUTION_PERCENT;
import static sleeper.core.properties.table.TableProperty.PARTITION_SPLIT_MIN_ROWS;

/**
 * Creates a transaction to extend the partition tree based on sketches of data in the existing leaf partitions.
 */
public class ExtendPartitionTreeBasedOnSketches {

    private final Schema schema;
    private final int minLeafPartitions;
    private final long minRowsInSketch;
    private final int minExpectedPercentRows;
    private final Supplier<String> idSupplier;

    private ExtendPartitionTreeBasedOnSketches(Schema schema, int minLeafPartitions, long minRowsInSketch, int minExpectedPercentRows, Supplier<String> idSupplier) {
        this.schema = schema;
        this.minLeafPartitions = minLeafPartitions;
        this.minRowsInSketch = minRowsInSketch;
        this.minExpectedPercentRows = minExpectedPercentRows;
        this.idSupplier = idSupplier;
    }

    /**
     * Creates an instance of this class to extend to the minimum required leaf partitions for bulk import.
     *
     * @param  tableProperties the table properties
     * @return                 the object
     */
    public static ExtendPartitionTreeBasedOnSketches forBulkImport(TableProperties tableProperties) {
        return forBulkImport(tableProperties, () -> UUID.randomUUID().toString());
    }

    /**
     * Creates an instance of this class to extend to the minimum required leaf partitions for bulk import.
     *
     * @param  tableProperties the table properties
     * @param  idSupplier      a method to create IDs for any new partitions
     * @return                 the object
     */
    public static ExtendPartitionTreeBasedOnSketches forBulkImport(TableProperties tableProperties, Supplier<String> idSupplier) {
        return new ExtendPartitionTreeBasedOnSketches(
                tableProperties.getSchema(),
                tableProperties.getInt(BULK_IMPORT_MIN_LEAF_PARTITION_COUNT),
                tableProperties.getLong(PARTITION_SPLIT_MIN_ROWS),
                tableProperties.getInt(PARTITION_SPLIT_MIN_DISTRIBUTION_PERCENT),
                idSupplier);
    }

    /**
     * Creates a transaction to extend the partition tree to the required size. This assumes that we've already
     * determined the partition tree needs to be extended.
     *
     * @param  tree                  the existing partition tree
     * @param  partitionIdToSketches a map from partition ID to sketches of the data in that partition
     * @return                       the new transaction
     */
    public ExtendPartitionTreeTransaction createTransaction(PartitionTree tree, Map<String, Sketches> partitionIdToSketches) {
        List<Partition> originalLeafPartitions = tree.streamLeavesInTreeOrder().toList();
        TreeExtensionTracker treeTracker = new TreeExtensionTracker(originalLeafPartitions);
        PartitionSketchIndex sketchIndex = PartitionSketchIndex.from(schema, partitionIdToSketches);
        List<Partition> splittablePartitions = findPartitionsMeetingExpectedMinimumComparedToEvenDistribution(originalLeafPartitions, sketchIndex);
        SplitPriorityTracker priorityTracker = new SplitPriorityTracker(splittablePartitions, sketchIndex);
        while (treeTracker.getNumLeafPartitions() < minLeafPartitions) {
            Partition partition = priorityTracker.nextPartition()
                    .orElseThrow(() -> new InsufficientDataForPartitionSplittingException(minLeafPartitions, treeTracker.getNumLeafPartitions()));
            SketchesForSplitting sketches = sketchIndex.getSketches(partition);
            Optional<SplitPartitionResult> splitResultOpt = FindPartitionSplitPoint.getResultIfSplittable(schema, minRowsInSketch, partition, sketches, idSupplier);
            if (splitResultOpt.isEmpty()) {
                continue;
            }
            SplitPartitionResult splitResult = splitResultOpt.get();
            treeTracker.recordSplit(splitResult);
            sketchIndex.recordSplit(splitResult);
            priorityTracker.recordSplit(splitResult);
        }
        return treeTracker.buildTransaction();
    }

    private List<Partition> findPartitionsMeetingExpectedMinimumComparedToEvenDistribution(List<Partition> leafPartitions, PartitionSketchIndex sketchIndex) {
        Map<String, Long> partitionIdToNumRows = leafPartitions.stream()
                .collect(toMap(Partition::getId, sketchIndex::getNumberOfRecordsSketched));
        long totalRows = partitionIdToNumRows.values().stream().mapToLong(n -> n).sum();
        long expectedPartitionRows = totalRows / leafPartitions.size();
        long minPartitionRows = PercentageUtil.getCeilPercent(expectedPartitionRows, minExpectedPercentRows);
        return leafPartitions.stream()
                .filter(partition -> partitionIdToNumRows.get(partition.getId()) >= minPartitionRows)
                .toList();
    }

}
