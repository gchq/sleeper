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
package sleeper.bulkimport.runner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.bulkimport.runner.BulkImportJobDriver.DataSketcher;
import sleeper.core.partition.Partition;
import sleeper.core.partition.PartitionTree;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;
import sleeper.core.statestore.StateStoreProvider;
import sleeper.sketches.Sketches;
import sleeper.splitter.core.extend.ExtendPartitionTreeBasedOnSketches;

import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static sleeper.core.properties.table.TableProperty.BULK_IMPORT_MIN_LEAF_PARTITION_COUNT;
import static sleeper.core.properties.table.TableProperty.BULK_IMPORT_PARTITION_SPLITTING_ATTEMPTS;

public class PartitionPreSplitter<C extends BulkImportContext<C>> {
    private static final Logger LOGGER = LoggerFactory.getLogger(PartitionPreSplitter.class);

    private final DataSketcher<C> dataSketcher;
    private final StateStoreProvider stateStoreProvider;
    private final Supplier<String> partitionIdSupplier;

    public PartitionPreSplitter(DataSketcher<C> dataSketcher, StateStoreProvider stateStoreProvider, Supplier<String> partitionIdSupplier) {
        this.dataSketcher = dataSketcher;
        this.stateStoreProvider = stateStoreProvider;
        this.partitionIdSupplier = partitionIdSupplier;
    }

    public C preSplitPartitionsIfNecessary(TableProperties tableProperties, List<Partition> allPartitions, C context) {
        PartitionTree tree = new PartitionTree(allPartitions);
        List<Partition> leafPartitions = tree.getLeafPartitions();
        int minLeafPartitions = tableProperties.getInt(BULK_IMPORT_MIN_LEAF_PARTITION_COUNT);
        int numberOfAttempts = tableProperties.getInt(BULK_IMPORT_PARTITION_SPLITTING_ATTEMPTS);
        int attempts = 1;

        while (leafPartitions.size() < minLeafPartitions && attempts <= numberOfAttempts) {
            LOGGER.info("Extending partition tree from {} leaf partitions to {}", leafPartitions.size(), minLeafPartitions);
            Map<String, Sketches> partitionIdToSketches = dataSketcher.generatePartitionIdToSketches(context);
            StateStore stateStore = stateStoreProvider.getStateStore(tableProperties);
            try {
                ExtendPartitionTreeBasedOnSketches.forBulkImport(tableProperties, partitionIdSupplier)
                        .createTransaction(tree, partitionIdToSketches)
                        .synchronousCommit(stateStore);
            } catch (StateStoreException sse) {
                LOGGER.error("Failed to update state store, will retry splitting partition tree.", sse);
            }
            allPartitions = stateStore.getAllPartitions();
            tree = new PartitionTree(allPartitions);
            leafPartitions = tree.getLeafPartitions();
            context = context.withPartitions(allPartitions);
            attempts++;
        }

        LOGGER.info("Partition tree meets minimum of {} leaf partitions", minLeafPartitions);
        return context;
    }
}
