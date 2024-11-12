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
package sleeper.query.runner.recordretrieval;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.configuration.jars.ObjectFactory;
import sleeper.core.iterator.CloseableIterator;
import sleeper.core.iterator.ConcatenatingIterator;
import sleeper.core.partition.Partition;
import sleeper.core.partition.PartitionTree;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.range.Region;
import sleeper.core.record.Record;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;
import sleeper.query.core.model.LeafPartitionQuery;
import sleeper.query.core.model.Query;
import sleeper.query.core.model.QueryException;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static sleeper.core.properties.table.TableProperty.QUERY_PROCESSOR_CACHE_TIMEOUT;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;

/**
 * Runs queries against a Sleeper table by querying the state store and data files directly. An instance of this class
 * cannot be used concurrently in multiple threads, due to how partitions are cached.
 */
public class QueryExecutor {
    private static final Logger LOGGER = LoggerFactory.getLogger(QueryExecutor.class);

    private final ObjectFactory objectFactory;
    private final StateStore stateStore;
    private final TableProperties tableProperties;
    private final LeafPartitionRecordRetriever recordRetriever;
    private List<Partition> leafPartitions;
    private PartitionTree partitionTree;
    private Map<String, List<String>> partitionToFiles;
    private Instant nextInitialiseTime;

    public QueryExecutor(
            ObjectFactory objectFactory, TableProperties tableProperties, StateStore stateStore,
            Configuration configuration, ExecutorService executorService) {
        this(objectFactory, stateStore, tableProperties,
                new LeafPartitionRecordRetrieverImpl(executorService, configuration),
                Instant.now());
    }

    public QueryExecutor(
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
     * Executes a query. This method first splits up the query into one or more
     * {@link LeafPartitionQuery}s. For each of these a Supplier of CloseableIterator
     * is created. This is done using suppliers to avoid the initialisation of
     * record retrievers until they are needed. In the case of Parquet files,
     * initialisation of the readers requires reading the footers of the file
     * which takes a little time. If a query spanned many leaf partitions and
     * each leaf partition had many active files, then the initialisation time
     * could be high. Using suppliers ensures that only files for a single
     * leaf partition are opened at a time.
     *
     * @param  query          the query
     * @return                An iterator containing the relevant records
     * @throws QueryException if it errors.
     */
    public CloseableIterator<Record> execute(Query query) throws QueryException {
        List<LeafPartitionQuery> leafPartitionQueries = splitIntoLeafPartitionQueries(query);
        List<Supplier<CloseableIterator<Record>>> iteratorSuppliers = createRecordIteratorSuppliers(leafPartitionQueries);
        return new ConcatenatingIterator(iteratorSuppliers);
    }

    public CloseableIterator<Record> execute(LeafPartitionQuery query) throws QueryException {
        return new ConcatenatingIterator(createRecordIteratorSuppliers(List.of(query)));
    }

    /**
     * Splits up a query into a sub-query per relevant leaf partition. Uses the
     * {@link #getRelevantLeafPartitions} method. For each leaf partition, it
     * finds the parent partitions in the tree and adds any files still belonging
     * to the parent to the sub query.
     *
     * @param  query the query to be split up
     * @return       A list of {@link LeafPartitionQuery}s
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
            // that records are not returned twice if they are in a non-leaf
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

    private List<Supplier<CloseableIterator<Record>>> createRecordIteratorSuppliers(List<LeafPartitionQuery> leafPartitionQueries) {
        List<Supplier<CloseableIterator<Record>>> iterators = new ArrayList<>();

        for (LeafPartitionQuery leafPartitionQuery : leafPartitionQueries) {
            iterators.add(() -> {
                try {
                    LeafPartitionQueryExecutor leafPartitionQueryExecutor = new LeafPartitionQueryExecutor(objectFactory, tableProperties, recordRetriever);
                    return leafPartitionQueryExecutor.getRecords(leafPartitionQuery);
                } catch (QueryException e) {
                    throw new RuntimeException("Exception returning records for leaf partition " + leafPartitionQuery, e);
                }
            });
        }
        return iterators;
    }

    /**
     * Gets the leaf partitions which are relevant to a query. This method is
     * called by the default implementation of {@code getPartitionFiles()} If
     * you overwrite getPartitionFiles() then you may make this method a no-op.
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
