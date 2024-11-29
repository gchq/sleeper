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

import sleeper.bulkexport.model.BulkExportLeafPartitionQuery;
import sleeper.bulkexport.model.BulkExportQuery;
import sleeper.core.iterator.CloseableIterator;
import sleeper.core.iterator.ConcatenatingIterator;
import sleeper.core.partition.Partition;
import sleeper.core.partition.PartitionTree;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.range.Region;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;
import sleeper.core.util.ObjectFactory;

import javax.management.Query;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static sleeper.core.properties.table.TableProperty.QUERY_PROCESSOR_CACHE_TIMEOUT;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;

public class BulkExportQuerySplitter {
    private static final Logger LOGGER = LoggerFactory.getLogger(BulkExportQuerySplitter.class);

    private final ObjectFactory objectFactory;
    private final StateStore stateStore;
    private final TableProperties tableProperties;
    private final LeafPartitionRecordRetriever recordRetriever;
    private List<Partition> leafPartitions;
    private PartitionTree partitionTree;
    private Map<String, List<String>> partitionToFiles;
    private Instant nextInitialiseTime;

    public BulkExportQuerySplitter(
            ObjectFactory objectFactory, TableProperties tableProperties, StateStore stateStore,
            LeafPartitionRecordRetriever recordRetriever) {
        this(objectFactory, stateStore, tableProperties, recordRetriever, Instant.now());
    }

    public BulkExportQuerySplitter(
            ObjectFactory objectFactory, StateStore stateStore, TableProperties tableProperties,
            LeafPartitionRecordRetriever recordRetriever, Instant timeNow) {
        this.objectFactory = objectFactory;
        this.stateStore = stateStore;
        this.tableProperties = tableProperties;
        this.recordRetriever = recordRetriever;
        this.nextInitialiseTime = timeNow;
    }

    /**
     * Initialises the partitions and the mapping from partitions to active files.
     * This method should be called periodically so that this class is aware of
     * new data arriving in the table. How often this method should be called is
     * a balance between having an up-to-date view of the data and the cost of
     * frequently extracting all the information about the files and the partitions
     * from the state store.
     *
     * @throws StateStoreException if the statestore can't be accessed.
     */
    public void init() throws StateStoreException {
        init(Instant.now());
    }

    public void init(List<Partition> partitions, Map<String, List<String>> partitionToFileMapping) {
        init(partitions, partitionToFileMapping, Instant.now());
    }

    public void initIfNeeded(Instant now) throws StateStoreException {
        if (nextInitialiseTime.isAfter(now)) {
            LOGGER.debug("Not refreshing state for table {}", tableProperties.getStatus());
            return;
        }
        init(now);
    }

    public void init(Instant now) throws StateStoreException {
        List<Partition> partitions = stateStore.getAllPartitions();
        Map<String, List<String>> partitionToFileMapping = stateStore.getPartitionToReferencedFilesMap();

        init(partitions, partitionToFileMapping, now);
    }

    public void init(List<Partition> partitions, Map<String, List<String>> partitionToFileMapping, Instant now) {
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
     * Executes a bulk export query. This method first splits up the query into one
     * or more
     * {@link BulkExportLeafPartitionQuery}s.
     *
     * @param bulkExportQuery the bulk export query
     * @returnA list of {@link LeafPartitionQuery}s
     */
    public List<BulkExportLeafPartitionQuery> execute(BulkExportQuery bulkExportQuery) {
        List<BulkExportLeafPartitionQuery> leafPartitionQueries = splitIntoLeafPartitionQueries(bulkExportQuery);
        return leafPartitionQueries;
    }

    /**
     * Splits up a query into a sub-query per relevant leaf partition. Uses the
     * {@link #getRelevantLeafPartitions} method. For each leaf partition, it
     * finds the parent partitions in the tree and adds any files still belonging
     * to the parent to the sub query.
     *
     * @param bulkExportQuery the query to be split up
     * @return A list of {@link LeafPartitionQuery}s
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
            BulkExportLeafPartitionQuery bulkExportLeafPartitionQuery = BulkExportLeafPartitionQuery.builder()
                    .exportId(bulkExportQuery.getExportId())
                    .tableId(tableProperties.get(TABLE_ID))
                    .subExportId((UUID.randomUUID().toString()))
                    .leafPartitionId(partition.getId())
                    .files(files)
                    .build();
            LOGGER.debug("Created {}", bulkExportLeafPartitionQuery);
            leafPartitionQueriesList.add(bulkExportLeafPartitionQuery);
        }

        return leafPartitionQueriesList;
    }

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
