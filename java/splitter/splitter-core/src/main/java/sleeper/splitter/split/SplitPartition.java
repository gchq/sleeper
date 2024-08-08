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
package sleeper.splitter.split;

import org.apache.hadoop.conf.Configuration;

import sleeper.core.partition.Partition;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;
import sleeper.splitter.split.FindPartitionSplitPoint.SketchesLoader;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Supplier;

import static sleeper.splitter.split.FindPartitionSplitPoint.loadSketchesFromFile;

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
    private final StateStore stateStore;
    private final Schema schema;
    private final SketchesLoader sketchesLoader;
    private final Supplier<String> idSupplier;

    public SplitPartition(StateStore stateStore,
            Schema schema,
            Configuration conf) {
        this(stateStore, schema, loadSketchesFromFile(schema, conf));
    }

    public SplitPartition(StateStore stateStore,
            Schema schema,
            SketchesLoader sketchesLoader) {
        this(stateStore, schema, sketchesLoader, () -> UUID.randomUUID().toString());
    }

    public SplitPartition(StateStore stateStore,
            Schema schema,
            SketchesLoader sketchesLoader,
            Supplier<String> idSupplier) {
        this.stateStore = stateStore;
        this.schema = schema;
        this.sketchesLoader = sketchesLoader;
        this.idSupplier = idSupplier;
    }

    public void splitPartition(Partition partition, List<String> fileNames) throws StateStoreException, IOException {
        FindPartitionSplitPoint findSplitPoint = new FindPartitionSplitPoint(schema, fileNames, sketchesLoader::load);
        for (int dimension = 0; dimension < schema.getRowKeyFields().size(); dimension++) {
            Optional<Object> splitPointOpt = findSplitPoint.splitPointForDimension(dimension);
            if (splitPointOpt.isPresent()) {
                SplitPartitionResult result = new SplitPartitionResultFactory(schema, idSupplier)
                        .splitPartition(partition, splitPointOpt.get(), dimension);
                stateStore.atomicallyUpdatePartitionAndCreateNewOnes(result.getParentPartition(), result.getLeftChild(), result.getRightChild());
                return;
            }
        }
    }
}
