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
package sleeper.query.core.rowretrieval;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.core.partition.Partition;
import sleeper.core.partition.PartitionTree;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.range.Region;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;
import sleeper.query.core.model.LeafPartitionQuery;
import sleeper.query.core.model.Query;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import static sleeper.core.properties.table.TableProperty.QUERY_PROCESSOR_CACHE_TIMEOUT;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;

/**
 * Plans queries against a Sleeper table by querying the state store directly. An instance of this class
 * cannot be used concurrently in multiple threads, due to how partitions are cached.
 */
public class QueryPlanner {
    private static final Logger LOGGER = LoggerFactory.getLogger(QueryPlanner.class);

    private final TableProperties tableProperties;
    private final StateStore stateStore;
    private List<Partition> leafPartitions;
    private PartitionTree partitionTree;
    private Map<String, List<String>> partitionToFiles;
    private Instant nextInitialiseTime;

    public QueryPlanner(TableProperties tableProperties, StateStore stateStore) {
        this(tableProperties, stateStore, Instant.now());
    }

    public QueryPlanner(TableProperties tableProperties, StateStore stateStore, Instant timeNow) {
        this.tableProperties = tableProperties;
        this.stateStore = stateStore;
        this.nextInitialiseTime = timeNow;
    }

    /**
     * Initialises a query executor with partitions and the mapping from partitions to file references.
     * Should be called periodically so that this class is aware of
     * new data arriving in the table. How often this method should be called is
     * a balance between having an up-to-date view of the data and the cost of
     * frequently extracting all the information about the files and the partitions
     * from the state store.
     *
     * @throws StateStoreException if the state store can't be accessed
     */
    public void init() throws StateStoreException {
        init(Instant.now());
    }

    /**
     * Initialises a query executor with the partitions and partition to file mapping,
     * rather than loading them from the state store.
     *
     * @param partitions             the partitions to initialise
     * @param partitionToFileMapping the partition to file mapping information
     */
    public void init(List<Partition> partitions, Map<String, List<String>> partitionToFileMapping) {
        init(partitions, partitionToFileMapping, Instant.now());
    }

    /**
     * Initialises a query executor if the next initialise time has passed.
     *
     * @param  now                 the time now
     * @throws StateStoreException if the state store can't be accessed
     */
    public void initIfNeeded(Instant now) throws StateStoreException {
        if (nextInitialiseTime.isAfter(now)) {
            LOGGER.debug("Not refreshing state for table {}", tableProperties.getStatus());
            return;
        }
        init(now);
    }

    /**
     * Initialises a query executor with given time.
     * The partitions and partition to file mapping are loaded from the state store.
     *
     * @param  now                 the time now
     * @throws StateStoreException if the state store can't be accessed
     */
    public void init(Instant now) throws StateStoreException {
        List<Partition> partitions = stateStore.getAllPartitions();
        Map<String, List<String>> partitionToFileMapping = stateStore.getPartitionToReferencedFilesMap();

        init(partitions, partitionToFileMapping, now);
    }

    /**
     * Initialises a query executor with the partitions, partition to file map and the next initialise time.
     *
     * @param partitions             the partitions to initialise
     * @param partitionToFileMapping the partition to file mapping information
     * @param now                    the time now
     */
    public void init(List<Partition> partitions, Map<String, List<String>> partitionToFileMapping, Instant now) {
        leafPartitions = partitions.stream()
                .filter(Partition::isLeafPartition)
                .collect(Collectors.toList());
        partitionTree = new PartitionTree(partitions);
        partitionToFiles = partitionToFileMapping;
        nextInitialiseTime = now.plus(tableProperties.getInt(QUERY_PROCESSOR_CACHE_TIMEOUT), ChronoUnit.SECONDS);
        LOGGER.info("Loaded state for table {}. Found {} partitions. Next initialise time: {}",
                tableProperties.getStatus(), partitions.size(), nextInitialiseTime);
    }

    /**
     * Splits up a query into a sub-query per relevant leaf partition. Uses the
     * {@link #getRelevantLeafPartitions} method. For each leaf partition, it
     * finds the parent partitions in the tree and adds any files still belonging
     * to the parent to the sub query.
     *
     * @param  query the query to be split up
     * @return       a list of {@link LeafPartitionQuery}s
     */
    public List<LeafPartitionQuery> splitIntoLeafPartitionQueries(Query query) {
        // Get mapping from leaf partitions to ranges from the query that overlap
        // that partition. Only leaf partitions that do overlap one of the ranges
        // from the query are contained in the map.
        Map<Partition, List<Region>> relevantLeafPartitions = getRelevantLeafPartitions(query);
        LOGGER.debug("There are {} relevant leaf partitions", relevantLeafPartitions.size());

        List<LeafPartitionQuery> leafPartitionQueriesList = new ArrayList<>();
        for (Map.Entry<Partition, List<Region>> entry : relevantLeafPartitions.entrySet()) {
            Partition partition = entry.getKey();
            List<Region> regions = entry.getValue();
            List<String> files = getFiles(partition);

            if (files.isEmpty()) {
                LOGGER.info("No files for partition {}", entry.getKey());
                continue;
            }

            // For each leaf partition, create query with pre-populated list of
            // files that need to be read and the filter that needs to be applied
            // (this filter will restrict the results returned to both the range
            // requested and to the range of that leaf partition, this ensures
            // that rows are not returned twice if they are in a non-leaf
            // partition).
            LeafPartitionQuery leafQuery = LeafPartitionQuery.builder()
                    .parentQuery(query)
                    .tableId(tableProperties.get(TABLE_ID))
                    .subQueryId(UUID.randomUUID().toString())
                    .regions(regions)
                    .leafPartitionId(partition.getId())
                    .partitionRegion(partition.getRegion())
                    .files(files)
                    .build();
            LOGGER.debug("Created {}", leafQuery);
            leafPartitionQueriesList.add(leafQuery);
        }

        return leafPartitionQueriesList;
    }

    /**
     * Gets the leaf partitions which are relevant to a query.
     *
     * @param  query the query
     * @return       the relevant leaf partitions
     */
    private Map<Partition, List<Region>> getRelevantLeafPartitions(Query query) {
        Map<Partition, List<Region>> leafPartitionToOverlappingRegions = new HashMap<>();
        leafPartitions.forEach(partition -> {
            leafPartitionToOverlappingRegions.put(partition, new ArrayList<>());
            for (Region region : query.getRegions()) {
                if (partition.doesRegionOverlapPartition(region)) {
                    leafPartitionToOverlappingRegions.get(partition).add(region);
                }
            }
            if (leafPartitionToOverlappingRegions.get(partition).isEmpty()) {
                leafPartitionToOverlappingRegions.remove(partition);
            }
        });
        return leafPartitionToOverlappingRegions;
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
