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
package sleeper.trino.remotesleeperconnection;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.s3.AmazonS3;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.UncheckedExecutionException;
import io.airlift.log.Logger;
import io.trino.spi.Page;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.commons.lang3.tuple.Pair;
import software.amazon.awssdk.services.s3.S3AsyncClient;

import sleeper.configuration.jars.ObjectFactory;
import sleeper.configuration.jars.ObjectFactoryException;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.configuration.table.index.DynamoDBTableIndex;
import sleeper.core.iterator.CloseableIterator;
import sleeper.core.partition.Partition;
import sleeper.core.record.Record;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;
import sleeper.core.table.TableStatus;
import sleeper.ingest.impl.IngestCoordinator;
import sleeper.query.model.LeafPartitionQuery;
import sleeper.query.model.Query;
import sleeper.query.model.QueryException;
import sleeper.query.runner.recordretrieval.QueryExecutor;
import sleeper.statestore.StateStoreFactory;
import sleeper.statestore.StateStoreProvider;
import sleeper.trino.SleeperConfig;
import sleeper.trino.ingest.BespokeIngestCoordinator;

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
import static sleeper.configuration.properties.table.TableProperty.TABLE_ID;

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
    public static final int MAX_NO_OF_RECORDS_TO_WRITE_TO_ARROW_FILE_AT_ONCE = 16 * 1024;
    public static final long WORKING_ARROW_BUFFER_ALLOCATOR_BYTES = 64 * 1024 * 1024L;
    public static final long BATCH_ARROW_BUFFER_ALLOCATOR_BYTES_MIN = 64 * 1024 * 1024L;
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
    private final List<String> tableNames;
    private final TablePropertiesProvider tablePropertiesProvider;
    private final ObjectFactory objectFactory;
    private final ExecutorService executorService;
    private final LoadingCache<Pair<String, Instant>, SleeperTablePartitionStructure> sleeperTablePartitionStructureCache;

    SleeperRawAwsConnection(SleeperConfig sleeperConfig,
            AmazonS3 s3Client,
            S3AsyncClient s3AsyncClient,
            AmazonDynamoDB dynamoDbClient,
            HadoopConfigurationProvider hadoopConfigurationProvider) throws ObjectFactoryException {
        requireNonNull(sleeperConfig);
        this.sleeperConfig = sleeperConfig;
        this.s3Client = requireNonNull(s3Client);
        this.s3AsyncClient = requireNonNull(s3AsyncClient);
        this.dynamoDbClient = requireNonNull(dynamoDbClient);
        this.hadoopConfigurationProvider = requireNonNull(hadoopConfigurationProvider);
        this.rootBufferAllocator = new RootAllocator(sleeperConfig.getMaxArrowRootAllocatorBytes());

        // Member variables related to the Sleeper service
        // Note that the state-store provider is NOT thread-safe and so occasionally the state-store factory
        // will be used to create a new state store for each thread.
        this.instanceProperties = new InstanceProperties();
        this.instanceProperties.loadFromS3(this.s3Client, sleeperConfig.getConfigBucket());
        this.stateStoreProvider = StateStoreFactory.createProvider(this.instanceProperties, this.s3Client, this.dynamoDbClient,
                this.hadoopConfigurationProvider.getHadoopConfiguration(instanceProperties));
        this.stateStoreFactory = new StateStoreFactory(this.instanceProperties, this.s3Client, this.dynamoDbClient,
                this.hadoopConfigurationProvider.getHadoopConfiguration(instanceProperties));

        // Member variables related to table properties
        // Note that the table-properties provider is NOT thread-safe.
        tableNames = new DynamoDBTableIndex(instanceProperties, dynamoDbClient).streamAllTables()
                .map(TableStatus::getTableName).toList();
        tablePropertiesProvider = new TablePropertiesProvider(instanceProperties, s3Client, dynamoDbClient);
        LOGGER.info(String.format("Number of Sleeper tables: %d", tableNames.size()));
        for (String tableName : tableNames) {
            TableProperties tableProperties = tablePropertiesProvider.getByName(tableName);
            LOGGER.debug("Table %s with schema %s", tableName, tableProperties.getSchema());
        }

        // Member variables related to queries via direct statestore/S3
        this.objectFactory = new ObjectFactory(this.instanceProperties, this.s3Client, sleeperConfig.getLocalWorkingDirectory());
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
     * @param  tableName           the name of the table
     * @param  asOfInstant         this argument is currently ignored because there is no mechanism to retrieve the
     *                             structure as-of a specified time from the state store
     * @return                     the partition structure
     * @throws StateStoreException when an error occurs accessing the state store
     */
    public SleeperTablePartitionStructure getSleeperTablePartitionStructure(
            String tableName, Instant asOfInstant) throws StateStoreException {
        return getSleeperTablePartitionStructure(tablePropertiesProvider.getByName(tableName), asOfInstant);
    }

    public synchronized SleeperTablePartitionStructure getSleeperTablePartitionStructure(
            TableProperties tableProperties, Instant asOfInstant) throws StateStoreException {
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
    public Stream<Record> createResultRecordStream(Instant asOfInstant, LeafPartitionQuery query) throws QueryException, ExecutionException {
        CloseableIterator<Record> resultRecordIterator = createResultRecordIterator(asOfInstant, query);
        Spliterator<Record> resultRecordSpliterator = Spliterators.spliteratorUnknownSize(
                resultRecordIterator,
                Spliterator.NONNULL | Spliterator.IMMUTABLE);
        Stream<Record> resultRecordStream = StreamSupport.stream(resultRecordSpliterator, false);
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
     * {@link QueryExecutor#splitIntoLeafPartitionQueries}.
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
     * @param  asOfInstant                 the instant to use when obtaining the list of files to query from the
     *                                     underlying state store (currently ignored)
     * @param  query                       the query to run
     * @return                             an iterator through the result records (make sure this is closed)
     * @throws QueryException              if something goes wrong
     * @throws ExecutionException          if something goes wrong
     * @throws UncheckedExecutionException if something goes wrong
     */
    private CloseableIterator<Record> createResultRecordIterator(Instant asOfInstant, LeafPartitionQuery query) throws QueryException, ExecutionException, UncheckedExecutionException {
        TableProperties tableProperties = tablePropertiesProvider.getById(query.getTableId());
        StateStore stateStore = this.stateStoreFactory.getStateStore(tableProperties);
        SleeperTablePartitionStructure sleeperTablePartitionStructure = sleeperTablePartitionStructureCache.get(Pair.of(query.getTableId(), asOfInstant));

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
     * Create a new ingest coordinator to add rows to a table. Make sure to close it after use.
     *
     * @param  tableName The table to add the rows to.
     * @return           The new {@link IngestCoordinator} object.
     */
    public IngestCoordinator<Page> createIngestRecordsAsync(String tableName) {
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
                null,
                null,
                INGEST_PARTITION_REFRESH_PERIOD_IN_SECONDS,
                s3AsyncClient,
                rootBufferAllocator);
    }
}
