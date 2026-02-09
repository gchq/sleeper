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
package sleeper.trino.remotesleeperconnection;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.UncheckedExecutionException;
import io.airlift.log.Logger;
import io.trino.spi.Page;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.commons.lang3.tuple.Pair;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;

import sleeper.configuration.jars.S3UserJarsLoader;
import sleeper.configuration.properties.S3InstanceProperties;
import sleeper.configuration.properties.S3TableProperties;
import sleeper.configuration.table.index.DynamoDBTableIndex;
import sleeper.core.iterator.closeable.CloseableIterator;
import sleeper.core.partition.Partition;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertiesProvider;
import sleeper.core.row.Row;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreProvider;
import sleeper.core.table.TableStatus;
import sleeper.core.util.ObjectFactory;
import sleeper.core.util.ObjectFactoryException;
import sleeper.ingest.runner.impl.IngestCoordinator;
import sleeper.query.core.model.LeafPartitionQuery;
import sleeper.query.core.model.Query;
import sleeper.query.core.model.QueryException;
import sleeper.query.core.rowretrieval.LeafPartitionQueryExecutor;
import sleeper.query.core.rowretrieval.LeafPartitionRowRetriever;
import sleeper.query.core.rowretrieval.QueryPlanner;
import sleeper.query.runner.rowretrieval.LeafPartitionRowRetrieverImpl;
import sleeper.statestore.StateStoreFactory;
import sleeper.trino.SleeperConfig;
import sleeper.trino.ingest.BespokeIngestCoordinator;

import java.nio.file.Path;
import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.util.Objects.requireNonNull;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;

/**
 * Manages the basic connection to a Sleeper instance. This includes retrieving configuration from S3 and performing
 * queries. It uses Sleeper concepts throughout and does not attempt to convert these into Trino concepts.
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
    public static final int MAX_NO_OF_ROWS_TO_WRITE_TO_ARROW_FILE_AT_ONCE = 16 * 1024;
    public static final long WORKING_ARROW_BUFFER_ALLOCATOR_BYTES = 64 * 1024 * 1024L;
    public static final long BATCH_ARROW_BUFFER_ALLOCATOR_BYTES_MIN = 64 * 1024 * 1024L;
    private static final int INGEST_PARTITION_REFRESH_PERIOD_IN_SECONDS = 120;
    private final BufferAllocator rootBufferAllocator;
    private final SleeperConfig sleeperConfig;
    private final S3Client s3Client;
    private final S3AsyncClient s3AsyncClient;
    private final DynamoDbClient dynamoDbClient;
    private final HadoopConfigurationProvider hadoopConfigurationProvider;
    private final InstanceProperties instanceProperties;
    private final StateStoreProvider stateStoreProvider;
    private final StateStoreFactory stateStoreFactory;
    private final List<String> tableNames;
    private final TablePropertiesProvider tablePropertiesProvider;
    private final ObjectFactory objectFactory;
    private final ExecutorService executorService;
    private final LoadingCache<Pair<String, Instant>, SleeperTablePartitionStructure> sleeperTablePartitionStructureCache;

    SleeperRawAwsConnection(SleeperConfig sleeperConfig,
            S3Client s3Client,
            S3AsyncClient s3AsyncClient,
            HadoopConfigurationProvider hadoopConfigurationProvider,
            DynamoDbClient dynamoDbClient) throws ObjectFactoryException {
        requireNonNull(sleeperConfig);
        this.sleeperConfig = sleeperConfig;
        this.s3Client = requireNonNull(s3Client);
        this.s3AsyncClient = requireNonNull(s3AsyncClient);
        this.dynamoDbClient = requireNonNull(dynamoDbClient);
        this.hadoopConfigurationProvider = hadoopConfigurationProvider;
        this.rootBufferAllocator = new RootAllocator(sleeperConfig.getMaxArrowRootAllocatorBytes());

        // Member variables related to the Sleeper service
        // Note that the state-store provider is NOT thread-safe and so occasionally the state-store factory
        // will be used to create a new state store for each thread.
        this.instanceProperties = S3InstanceProperties.loadFromBucket(s3Client, sleeperConfig.getConfigBucket());
        this.stateStoreProvider = StateStoreFactory.createProvider(this.instanceProperties, this.s3Client, this.dynamoDbClient);
        this.stateStoreFactory = new StateStoreFactory(this.instanceProperties, this.s3Client, this.dynamoDbClient);

        // Member variables related to table properties
        // Note that the table-properties provider is NOT thread-safe.
        tableNames = new DynamoDBTableIndex(instanceProperties, dynamoDbClient).streamAllTables()
                .map(TableStatus::getTableName).toList();
        tablePropertiesProvider = S3TableProperties.createProvider(instanceProperties, s3Client, dynamoDbClient);
        LOGGER.info(String.format("Number of Sleeper tables: %d", tableNames.size()));
        for (String tableName : tableNames) {
            TableProperties tableProperties = tablePropertiesProvider.getByName(tableName);
            LOGGER.debug("Table %s with schema %s", tableName, tableProperties.getSchema());
        }

        // Member variables related to queries via direct statestore/S3
        this.objectFactory = new S3UserJarsLoader(this.instanceProperties, this.s3Client, Path.of(sleeperConfig.getLocalWorkingDirectory())).buildObjectFactory();
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
            public SleeperTablePartitionStructure load(Pair<String, Instant> keyPair) {
                String tableId = keyPair.getLeft();
                Instant instant = keyPair.getRight();
                // Check that this transaction is not older than the cache expiry time as this will confuse the cache
                if (instant.isBefore(Instant.now().minus(PARTITION_CACHE_EXPIRY_VALUE, PARTITION_CACHE_EXPIRY_UNITS.toChronoUnit()))) {
                    throw new UnsupportedOperationException(String.format("Transactions longer than %s %s are not supported", PARTITION_CACHE_EXPIRY_UNITS, PARTITION_CACHE_EXPIRY_UNITS));
                }
                return getSleeperTablePartitionStructure(tablePropertiesProvider.getById(tableId), instant);
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
        s3Client.close();
        s3AsyncClient.close();
        dynamoDbClient.close();
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
        return new HashSet<>(tableNames);
    }

    /**
     * The Sleeper schema for a specified table. The Sleeper schema defines the columns of the table.
     *
     * @param  tableName The name of the table.
     * @return           The Sleeper schema.
     */
    public Schema getSleeperSchema(String tableName) {
        return tablePropertiesProvider.getByName(tableName).getSchema();
    }

    /**
     * Retrieve the table partition structure for a single table.
     *
     * @param  tableName   the name of the table
     * @param  asOfInstant this argument is currently ignored because there is no mechanism to retrieve the
     *                     structure as-of a specified time from the state store
     * @return             the partition structure
     */
    public SleeperTablePartitionStructure getSleeperTablePartitionStructure(
            String tableName, Instant asOfInstant) {
        return getSleeperTablePartitionStructure(tablePropertiesProvider.getByName(tableName), asOfInstant);
    }

    public synchronized SleeperTablePartitionStructure getSleeperTablePartitionStructure(
            TableProperties tableProperties, Instant asOfInstant) {
        // Use of the state store provider is not thread-safe and this requires the use of a synchronized method.
        // The state store which is returned may not be thread-safe either.
        StateStore stateStore = stateStoreProvider.getStateStore(tableProperties);
        List<Partition> partitions = stateStore.getAllPartitions();
        Map<String, List<String>> partitionToFileMapping = stateStore.getPartitionToReferencedFilesMap();
        LOGGER.debug("Retrieved " + partitions.size() + " partitions from StateStore");
        return new SleeperTablePartitionStructure(asOfInstant, partitions, partitionToFileMapping);
    }

    /**
     * Creates a stream of records returned by a single query.
     *
     * @param  asOfInstant        the instant to use when obtaining the list of files to query from the underlying state
     *                            store (currently ignored)
     * @param  query              the query to run
     * @return                    a stream of records containing the results of the query
     * @throws QueryException     if something goes wrong
     * @throws ExecutionException if something goes wrong
     */
    public Stream<Row> createResultRecordStream(Instant asOfInstant, LeafPartitionQuery query) throws QueryException, ExecutionException {
        CloseableIterator<Row> resultRecordIterator = createResultRecordIterator(asOfInstant, query);
        Spliterator<Row> resultRecordSpliterator = Spliterators.spliteratorUnknownSize(
                resultRecordIterator,
                Spliterator.NONNULL | Spliterator.IMMUTABLE);
        Stream<Row> resultRecordStream = StreamSupport.stream(resultRecordSpliterator, false);
        return resultRecordStream.onClose(() -> {
            try {
                resultRecordIterator.close();
            } catch (Exception e) {
                LOGGER.error(e);
            }
        });
    }

    /**
     * Split a query into one or more sub-queries. Each will represent a scan of a leaf partition, which combine to
     * cover the entire original query. The leaf partition queries are generated using the core Sleeper method
     * {@link QueryPlanner#splitIntoLeafPartitionQueries}.
     *
     * @param  asOfInstant        The instant to use when obtaining the list of files to query from the underlying state
     *                            store.
     *                            Currently ignored.
     * @param  query              The {@link Query} to split into {@link LeafPartitionQuery} objects.
     * @return                    The list of {@link LeafPartitionQuery} objects.
     * @throws ExecutionException If something goes wrong.
     */
    public List<LeafPartitionQuery> splitIntoLeafPartitionQueries(
            Instant asOfInstant,
            Query query) throws ExecutionException {
        TableProperties tableProperties = tablePropertiesProvider.getByName(query.getTableName());
        SleeperTablePartitionStructure sleeperTablePartitionStructure = sleeperTablePartitionStructureCache.get(Pair.of(tableProperties.get(TABLE_ID), asOfInstant));

        QueryPlanner planner = new QueryPlanner(tableProperties, null);
        planner.init(sleeperTablePartitionStructure.getAllPartitions(),
                sleeperTablePartitionStructure.getPartitionToFileMapping());
        return planner.splitIntoLeafPartitionQueries(query);
    }

    /**
     * Start running a query and return an iterator to use to scroll through the results.
     *
     * @param  asOfInstant                 the instant to use when obtaining the list of files to query from the
     *                                     underlying state store (currently ignored)
     * @param  query                       the query to run
     * @return                             an iterator through the result records (make sure this is closed)
     * @throws QueryException              if something goes wrong
     * @throws ExecutionException          if something goes wrong
     * @throws UncheckedExecutionException if something goes wrong
     */
    private CloseableIterator<Row> createResultRecordIterator(Instant asOfInstant, LeafPartitionQuery query) throws QueryException, ExecutionException, UncheckedExecutionException {
        TableProperties tableProperties = tablePropertiesProvider.getById(query.getTableId());

        LOGGER.debug("Creating result record iterator for query %s", query);
        LeafPartitionRowRetriever rowRetriever = new LeafPartitionRowRetrieverImpl(
                executorService, hadoopConfigurationProvider.getHadoopConfiguration(instanceProperties), tableProperties);
        LeafPartitionQueryExecutor executor = new LeafPartitionQueryExecutor(objectFactory, tableProperties, rowRetriever);
        return executor.getRows(query);
    }

    /**
     * Create a new ingest coordinator to add rows to a table. Make sure to close it after use.
     *
     * @param  tableName The table to add the rows to.
     * @return           The new {@link IngestCoordinator} object.
     */
    public IngestCoordinator<Page> createIngestRowsAsync(String tableName) {
        TableProperties tableProperties = tablePropertiesProvider.getByName(tableName);
        // Use of the state store provider is not thread-safe and this requires the use of a synchronized method.
        // The state store which is returned may not be thread-safe either.
        // To ensure thread safety, a new state store is obtained each time.
        StateStore stateStore = this.stateStoreFactory.getStateStore(tableProperties);
        // Create a new 'ingest records' object
        return BespokeIngestCoordinator.asyncFromPage(
                objectFactory,
                stateStore,
                instanceProperties,
                tableProperties,
                sleeperConfig,
                hadoopConfigurationProvider.getHadoopConfiguration(instanceProperties),
                INGEST_PARTITION_REFRESH_PERIOD_IN_SECONDS,
                s3AsyncClient,
                rootBufferAllocator);
    }
}
