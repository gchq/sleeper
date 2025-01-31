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
package sleeper.bulkexport.core.recordretrieval;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.bulkexport.core.model.BulkExportLeafPartitionQuery;
import sleeper.bulkexport.core.model.BulkExportQuery;
import sleeper.core.partition.Partition;
import sleeper.core.partition.PartitionTree;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static sleeper.core.properties.table.TableProperty.QUERY_PROCESSOR_CACHE_TIMEOUT;

/**
 * Splits up an export query into leaf partition export queries.
 */
public class BulkExportQuerySplitter {
    private static final Logger LOGGER = LoggerFactory.getLogger(BulkExportQuerySplitter.class);

    private final StateStore stateStore;
    private final TableProperties tableProperties;
    private final Supplier<String> idSupplier;
    private final Supplier<Instant> timeSupplier;
    private List<Partition> leafPartitions;
    private PartitionTree partitionTree;
    private Map<String, List<String>> partitionToFiles;
    private Instant nextInitialiseTime;

    public BulkExportQuerySplitter(TableProperties tableProperties, StateStore stateStore) {
        this(stateStore, tableProperties, () -> UUID.randomUUID().toString(), Instant::now);
    }

    public BulkExportQuerySplitter(StateStore stateStore, TableProperties tableProperties, Supplier<String> idSupplier, Supplier<Instant> timeSupplier) {
        this.stateStore = stateStore;
        this.tableProperties = tableProperties;
        this.idSupplier = idSupplier;
        this.timeSupplier = timeSupplier;
        init(timeSupplier.get());
    }

    /**
     * Initialises the partitions and the mapping from partitions to active files if
     * needed.
     *
     * @throws StateStoreException if the state store can't be accessed
     */
    public void initIfNeeded() throws StateStoreException {
        Instant now = timeSupplier.get();
        if (nextInitialiseTime.isAfter(now)) {
            LOGGER.debug("Not refreshing state for table {}", tableProperties.getStatus());
            return;
        }
        init(now);
    }

    private void init(Instant now) throws StateStoreException {
        List<Partition> partitions = stateStore.getAllPartitions();
        Map<String, List<String>> partitionToFileMapping = stateStore.getPartitionToReferencedFilesMap();
        init(partitions, partitionToFileMapping, now);
    }

    private void init(List<Partition> partitions, Map<String, List<String>> partitionToFileMapping, Instant now) {
        leafPartitions = partitions.stream()
                .filter(Partition::isLeafPartition)
                .collect(Collectors.toList());
        partitionTree = new PartitionTree(partitions);
        partitionToFiles = partitionToFileMapping;
        nextInitialiseTime = now.plus(tableProperties.getInt(QUERY_PROCESSOR_CACHE_TIMEOUT), ChronoUnit.MINUTES);
        LOGGER.info("Loaded state for table {}. Found {} partitions. Next initialise time: {}",
                tableProperties.getStatus(), partitions.size(), nextInitialiseTime);
    }

    /**
     * Splits up a query into a sub-query per relevant leaf partition. For each leaf partition, it
     * finds the parent partitions in the tree and adds any files still belonging
     * to the parent to the sub query.
     *
     * @param  bulkExportQuery the query to be split up
     * @return                 a list of {@link LeafPartitionQuery}s
     */
    public List<BulkExportLeafPartitionQuery> splitIntoLeafPartitionQueries(BulkExportQuery bulkExportQuery) {
        LOGGER.debug("There are {} relevant leaf partitions", leafPartitions.size());

        List<BulkExportLeafPartitionQuery> leafPartitionQueriesList = new ArrayList<>();
        for (Partition partition : leafPartitions) {
            List<String> files = getFiles(partition);

            if (files.isEmpty()) {
                LOGGER.info("No files for partition {}", partition.getId());
                continue;
            }

            // For each leaf partition, create query with pre-populated list of files that
            // need to be read.
            BulkExportLeafPartitionQuery bulkExportLeafPartitionQuery = BulkExportLeafPartitionQuery
                    .forPartition(bulkExportQuery, tableProperties, partition)
                    .subExportId(idSupplier.get())
                    .files(files)
                    .build();
            LOGGER.debug("Created {}", bulkExportLeafPartitionQuery);
            leafPartitionQueriesList.add(bulkExportLeafPartitionQuery);
        }

        return leafPartitionQueriesList;
    }

    /**
     * Gets a list of file paths for a partition.
     *
     * @param  partition which partition to get the files from
     * @return           a list of files
     */
    protected List<String> getFiles(Partition partition) {
        // Get all partitions up to the root of the tree
        List<String> relevantPartitions = new ArrayList<>();
        relevantPartitions.add(partition.getId());
        relevantPartitions.addAll(partitionTree.getAllAncestorIds(partition.getId()));

        // Get relevant files
        List<String> files = new ArrayList<>();
        for (String partitionId : relevantPartitions) {
            List<String> filesForPartition = partitionToFiles.get(partitionId);
            if (null != filesForPartition) {
                files.addAll(partitionToFiles.get(partitionId));
            }
        }
        return files;
    }
}
