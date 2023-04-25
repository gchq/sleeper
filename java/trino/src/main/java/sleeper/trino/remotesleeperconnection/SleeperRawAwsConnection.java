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
package sleeper.trino.remotesleeperconnection;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.UncheckedExecutionException;
import io.airlift.log.Logger;
import io.trino.spi.Page;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.commons.lang3.tuple.Pair;
import software.amazon.awssdk.services.s3.S3AsyncClient;

import sleeper.configuration.jars.ObjectFactory;
import sleeper.configuration.jars.ObjectFactoryException;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.configuration.properties.table.TableProperty;
import sleeper.core.iterator.CloseableIterator;
import sleeper.core.partition.Partition;
import sleeper.core.record.Record;
import sleeper.core.schema.Schema;
import sleeper.ingest.impl.IngestCoordinator;
import sleeper.query.QueryException;
import sleeper.query.executor.QueryExecutor;
import sleeper.query.model.LeafPartitionQuery;
import sleeper.query.model.Query;
import sleeper.statestore.StateStore;
import sleeper.statestore.StateStoreException;
import sleeper.statestore.StateStoreFactory;
import sleeper.statestore.StateStoreProvider;
import sleeper.trino.SleeperConfig;
import sleeper.trino.ingest.BespokeIngestCoordinator;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.util.Objects.requireNonNull;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.CONFIG_BUCKET;

/**
 * This class manages the basic connection to a Sleeper instance, such as retrieving configuration from S3 and
 * performing queries. It uses Sleeper concepts throughout and does not attempt to convert these into Trino concepts.
 * <p>
 * Some of the code in this class has been copied directly from the official Sleeper command-line Client class (@link
 * sleeper.client.utils.Client}. It was necessary to do this because of various method-scope issues in the official
 * client. It may be better to resolve the issues in the client and reduce code duplication.
 * <p>
 * This class loads structural information from Sleeper when it is constructed. This information includes the table
 * names and their column types. This information is never refreshed and so a client will always assume that the Sleeper
 * database has the same structure that it had when it was started.
 * <p>
 * This class is intended to be thread-safe.
 */
public class SleeperRawAwsConnection implements AutoCloseable {
    private static final Logger LOGGER = Logger.get(SleeperRawAwsConnection.class);

    private static final int NO_OF_EXECUTOR_THREADS = 10;
    private static final int PARTITION_CACHE_EXPIRY_VALUE = 60;
    private static final TimeUnit PARTITION_CACHE_EXPIRY_UNITS = TimeUnit.MINUTES;
    public static final int MAX_NO_OF_RECORDS_TO_WRITE_TO_ARROW_FILE_AT_ONCE = 16 * 1024;
    public static final long WORKING_ARROW_BUFFER_ALLOCATOR_BYTES = 64 * 1024 * 1024L;
    public static final long BATCH_ARROW_BUFFER_ALLOCATOR_BYTES_MIN = 64 * 1024 * 1024L;
    public static final String INGEST_COMPRESSION_CODEC = "snappy";
    private static final int INGEST_PARTITION_REFRESH_PERIOD_IN_SECONDS = 120;
    private final BufferAllocator rootBufferAllocator;
    private final SleeperConfig sleeperConfig;
    private final AmazonS3 s3Client;
    private final S3AsyncClient s3AsyncClient;
    private final AmazonDynamoDB dynamoDbClient;
    private final HadoopConfigurationProvider hadoopConfigurationProvider;
    private final InstanceProperties instanceProperties;
    private final StateStoreProvider stateStoreProvider;
    private final StateStoreFactory stateStoreFactory;
    private final Map<String, TableProperties> tableNameToSleeperTablePropertiesMap;
    private final ObjectFactory objectFactory;
    private final ExecutorService executorService;
    private final LoadingCache<Pair<String, Instant>, SleeperTablePartitionStructure> sleeperTablePartitionStructureCache;
    private final String localWorkingDirectory;

    SleeperRawAwsConnection(SleeperConfig sleeperConfig,
                            AmazonS3 s3Client,
                            S3AsyncClient s3AsyncClient,
                            AmazonDynamoDB dynamoDbClient,
                            HadoopConfigurationProvider hadoopConfigurationProvider) throws IOException, ObjectFactoryException {
        requireNonNull(sleeperConfig);
        this.sleeperConfig = sleeperConfig;
        this.s3Client = requireNonNull(s3Client);
        this.s3AsyncClient = requireNonNull(s3AsyncClient);
        this.dynamoDbClient = requireNonNull(dynamoDbClient);
        this.hadoopConfigurationProvider = requireNonNull(hadoopConfigurationProvider);
        this.rootBufferAllocator = new RootAllocator(sleeperConfig.getMaxArrowRootAllocatorBytes());

        String configBucket = sleeperConfig.getConfigBucket();
        this.localWorkingDirectory = sleeperConfig.getLocalWorkingDirectory();

        // Member variables related to the Sleeper service
        // Note that the state-store provider is NOT thread-safe and so occasionally the state-store factory
        // will be used to create a new state store for each thread.
        this.instanceProperties = new InstanceProperties();
        this.instanceProperties.loadFromS3(this.s3Client, configBucket);
        this.stateStoreProvider = new StateStoreProvider(this.dynamoDbClient, this.instanceProperties,
                this.hadoopConfigurationProvider.getHadoopConfiguration(instanceProperties));
        this.stateStoreFactory = new StateStoreFactory(this.dynamoDbClient, this.instanceProperties,
                this.hadoopConfigurationProvider.getHadoopConfiguration(instanceProperties));

        // Member variables related to table properties
        // Note that the table-properties provider is NOT thread-safe.
        List<String> tableNames = pullAllSleeperTableNames();
        TablePropertiesProvider tablePropertiesProvider = new TablePropertiesProvider(this.s3Client, this.instanceProperties);
        LOGGER.info(String.format("Number of Sleeper tables: %d", tableNames.size()));
        this.tableNameToSleeperTablePropertiesMap = tableNames.stream()
                .collect(ImmutableMap.toImmutableMap(
                        Function.identity(),
                        tablePropertiesProvider::getTableProperties));
        this.tableNameToSleeperTablePropertiesMap.forEach(
                (tableName, tableProperties) ->
                        LOGGER.debug("Table %s with schema %s", tableName, tableProperties.getSchema()));

        // Member variables related to queries via direct statestore/S3
        this.objectFactory = new ObjectFactory(this.instanceProperties, this.s3Client, this.localWorkingDirectory);
        this.executorService = Executors.newFixedThreadPool(NO_OF_EXECUTOR_THREADS);

        // We store a time-limited cache for the table partition structure, to support transactions.
        // The cache returns the partition structure for a specified table at a given instant.
        // Each transaction can use a different 'as-of instant' to maintain table consistency, so that files
        // which are added after the transaction has started are ignored.
        // It would be better if we could retrieve a consistent set of tables from the state store 'as-of' the
        // specified time, but at present the partition structure for the table is loaded when it is first
        // required within a transaction and then that structure is reused each time that table is needed.
        // This Guava cache states that it is thread-safe.
        CacheLoader<Pair<String, Instant>, SleeperTablePartitionStructure> cacheLoader = new CacheLoader<>() {
            // The load method is called by the cache whenever it does not already contain the desired value.
            // The key is a pair of (tablename, as-of instant) and the key is the table partition stucture.
            @Override
            public SleeperTablePartitionStructure load(Pair<String, Instant> keyPair) throws StateStoreException {
                String tableName = keyPair.getLeft();
                Instant instant = keyPair.getRight();
                // Check that this transaction is not older than the cache expiry time as this will confuse the cache
                if (instant.isBefore(Instant.now().minus(PARTITION_CACHE_EXPIRY_VALUE, PARTITION_CACHE_EXPIRY_UNITS.toChronoUnit()))) {
                    throw new UnsupportedOperationException(String.format("Transactions longer than %s %s are not supported", PARTITION_CACHE_EXPIRY_UNITS, PARTITION_CACHE_EXPIRY_UNITS));
                }
                return getSleeperTablePartitionStructure(tableName, instant);
            }
        };
        this.sleeperTablePartitionStructureCache = CacheBuilder.newBuilder()
                .expireAfterWrite(PARTITION_CACHE_EXPIRY_VALUE, PARTITION_CACHE_EXPIRY_UNITS)
                .build(cacheLoader);
    }

    /**
     * Close the connection and free all resources.
     */
    @Override
    public void close() {
        LOGGER.info("Closing AWS clients");
        s3Client.shutdown();
        s3AsyncClient.close();
        dynamoDbClient.shutdown();
        rootBufferAllocator.close();
        LOGGER.info("AWS clients closed");
    }

    /**
     * A list of all the Sleeper table names. Note that Sleeper does not separate tables into different parent schemas
     * and so has a flat namespace.
     *
     * @return A set of the names of all of the Sleeper tables.
     */
    public Set<String> getAllSleeperTableNames() {
        return tableNameToSleeperTablePropertiesMap.keySet();
    }

    /**
     * The Sleeper {@link Schema} for a specified table. The Sleeper schema defines the columns of the table.
     *
     * @param tableName The name of the table.
     * @return The Sleeper schema.
     */
    public Schema getSleeperSchema(String tableName) {
        return tableNameToSleeperTablePropertiesMap.get(tableName).getSchema();
    }

    /**
     * Interrogate the S3 Sleeper configuration bucket and retrieve a list of all of the Sleeper table names.
     *
     * @return A list of Sleeper table names.
     */
    private List<String> pullAllSleeperTableNames() {
        ListObjectsV2Result result = s3Client.listObjectsV2(instanceProperties.get(CONFIG_BUCKET), TableProperties.TABLES_PREFIX + "/");
        List<S3ObjectSummary> objectSummaries = result.getObjectSummaries();
        return objectSummaries.stream()
                .map(S3ObjectSummary::getKey)
                .map(s -> s.substring(TableProperties.TABLES_PREFIX.length() + 1))
                .collect(Collectors.toList());
    }

    /**
     * Retrieve the table partition structure for a single table.
     *
     * @param tableName   The name of the table.
     * @param asOfInstant This argument is currently ignored because there is no mechanism to retrieve the structure
     *                    as-of a specified time from the state store.
     * @return The partition structure.
     * @throws StateStoreException When an error occurs accessing the state store.
     */
    public synchronized SleeperTablePartitionStructure getSleeperTablePartitionStructure(String tableName,
                                                                                         Instant asOfInstant)
            throws StateStoreException {
        TableProperties tableProperties = tableNameToSleeperTablePropertiesMap.get(tableName);
        // Use of the state store provider is not thread-safe and this requires the use of a synchronized method.
        // The state store which is returned may not be thread-safe either.
        StateStore stateStore = stateStoreProvider.getStateStore(tableProperties);
        List<Partition> partitions = stateStore.getAllPartitions();
        Map<String, List<String>> partitionToFileMapping = stateStore.getPartitionToActiveFilesMap();
        LOGGER.debug("Retrieved " + partitions.size() + " partitions from StateStore");
        return new SleeperTablePartitionStructure(asOfInstant, partitions, partitionToFileMapping);
    }

    /**
     * Create a stream of {@link Record} objects returned by a single query.
     *
     * @param asOfInstant The instant to use when obtaining the list of files to query from the underlying state store.
     *                    Currently ignored.
     * @param query       The query to run.
     * @return A stream of records containing the results of the query.
     * @throws QueryException     If something goes wrong.
     * @throws ExecutionException If something goes wrong.
     */
    public Stream<Record> createResultRecordStream(Instant asOfInstant, Query query)
            throws QueryException, ExecutionException {
        CloseableIterator<Record> resultRecordIterator = createResultRecordIterator(asOfInstant, query);
        Spliterator<Record> resultRecordSpliterator = Spliterators.spliteratorUnknownSize(
                resultRecordIterator,
                Spliterator.NONNULL | Spliterator.IMMUTABLE);
        Stream<Record> resultRecordStream = StreamSupport.stream(resultRecordSpliterator, false);
        return resultRecordStream.onClose(() -> {
            try {
                resultRecordIterator.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }

    /**
     * Split a {@link Query} into one or more {@link LeafPartitionQuery} objects, each representing a scan of a leaf
     * partition, which combine to cover the entire original query. The leaf partition queries are genersated using the
     * core Sleeper method {@link QueryExecutor#splitIntoLeafPartitionQueries}.
     *
     * @param asOfInstant The instant to use when obtaining the list of files to query from the underlying state store.
     *                    Currently ignored.
     * @param query       The {@link Query} to split into {@link LeafPartitionQuery} objects.
     * @return The list of {@link LeafPartitionQuery} objects.
     * @throws ExecutionException If something goes wrong.
     */
    public List<LeafPartitionQuery> splitIntoLeafPartitionQueries(
            Instant asOfInstant,
            Query query) throws ExecutionException {
        TableProperties tableProperties = this.tableNameToSleeperTablePropertiesMap.get(query.getTableName());
        SleeperTablePartitionStructure sleeperTablePartitionStructure =
                sleeperTablePartitionStructureCache.get(Pair.of(query.getTableName(), asOfInstant));

        // This seems like a lot of effort to go to in order to identify partitions
        QueryExecutor queryExecutor = new QueryExecutor(
                objectFactory,
                tableProperties,
                null,
                this.hadoopConfigurationProvider.getHadoopConfiguration(this.instanceProperties),
                executorService);
        queryExecutor.init(sleeperTablePartitionStructure.getAllPartitions(),
                sleeperTablePartitionStructure.getPartitionToFileMapping());
        return queryExecutor.splitIntoLeafPartitionQueries(query);
    }

    /**
     * Start running a query and return an iterator to use to scroll through the results.
     *
     * @param asOfInstant The instant to use when obtaining the list of files to query from the underlying state store.
     *                    Currently ignored.
     * @param query       The query to run.
     * @return A Closeableterator which iterates through the result records. Make sure that it is closed when it is
     * finished with.
     * @throws QueryException              If something goes wrong.
     * @throws ExecutionException          If something goes wrong.
     * @throws UncheckedExecutionException If something goes wrong.
     */
    private CloseableIterator<Record> createResultRecordIterator(Instant asOfInstant, Query query)
            throws QueryException, ExecutionException, UncheckedExecutionException {
        TableProperties tableProperties = this.tableNameToSleeperTablePropertiesMap.get(query.getTableName());
        StateStore stateStore = this.stateStoreFactory.getStateStore(tableProperties);
        SleeperTablePartitionStructure sleeperTablePartitionStructure = sleeperTablePartitionStructureCache.get(Pair.of(query.getTableName(), asOfInstant));

        LOGGER.debug("Creating result record iterator for query %s", query);
        QueryExecutor queryExecutor = new QueryExecutor(
                this.objectFactory,
                tableProperties,
                stateStore,
                this.hadoopConfigurationProvider.getHadoopConfiguration(this.instanceProperties),
                this.executorService);
        queryExecutor.init(sleeperTablePartitionStructure.getAllPartitions(), sleeperTablePartitionStructure.getPartitionToFileMapping());
        return queryExecutor.execute(query);
    }

    /**
     * Create a new {@link IngestCoordinator} object to add rows to a table.
     * <p>
     * Make sure to initialise the returned object and close it after use.
     *
     * @param tableName The table to add the rows to.
     * @return The new {@link IngestCoordinator} object.
     */
    public IngestCoordinator<Page> createIngestRecordsAsync(String tableName) {
        TableProperties tableProperties = tableNameToSleeperTablePropertiesMap.get(tableName);
        // Use of the state store provider is not thread-safe and this requires the use of a synchronized method.
        // The state store which is returned may not be thread-safe either.
        // To ensure thread safety, a new state store is obtained each time.
        StateStore stateStore = this.stateStoreFactory.getStateStore(tableProperties);
        // Create a new 'ingest records' object
        return BespokeIngestCoordinator.asyncFromPage(
                objectFactory,
                stateStore,
                tableProperties,
                sleeperConfig,
                hadoopConfigurationProvider.getHadoopConfiguration(instanceProperties),
                null,
                null,
                INGEST_PARTITION_REFRESH_PERIOD_IN_SECONDS,
                tableProperties.get(TableProperty.DATA_BUCKET),
                s3AsyncClient,
                rootBufferAllocator);
    }
}
