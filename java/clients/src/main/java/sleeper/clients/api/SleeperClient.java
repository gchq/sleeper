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
package sleeper.clients.api;

import sleeper.bulkexport.core.model.BulkExportQuery;
import sleeper.bulkimport.core.configuration.BulkImportPlatform;
import sleeper.bulkimport.core.job.BulkImportJob;
import sleeper.clients.api.aws.AwsSleeperClientBuilder;
import sleeper.core.properties.SleeperPropertiesInvalidException;
import sleeper.core.properties.instance.CdkDefinedInstanceProperty;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertiesProvider;
import sleeper.core.properties.table.TablePropertiesStore;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;
import sleeper.core.statestore.StateStoreProvider;
import sleeper.core.statestore.transactionlog.transaction.impl.InitialisePartitionsTransaction;
import sleeper.core.table.TableAlreadyExistsException;
import sleeper.core.table.TableIndex;
import sleeper.core.table.TableNotFoundException;
import sleeper.core.table.TableStatus;
import sleeper.core.util.ObjectFactory;
import sleeper.ingest.batcher.core.IngestBatcherSubmitRequest;
import sleeper.ingest.core.job.IngestJob;
import sleeper.query.core.recordretrieval.LeafPartitionRecordRetriever;
import sleeper.query.core.recordretrieval.LeafPartitionRecordRetrieverProvider;
import sleeper.query.core.recordretrieval.QueryExecutor;

import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Stream;

/**
 * A client to interact with an instance of Sleeper. This interacts directly with the underlying AWS resources, and
 * requires permissions against those resources, e.g. the configuration and data buckets in S3, the transaction logs and
 * table index in DynamoDB. There are managed policies and roles deployed with Sleeper that can help with this, e.g.
 * {@link CdkDefinedInstanceProperty#ADMIN_ROLE_ARN}.
 * <p>
 * Note that this class is not thread safe. {@link TablePropertiesProvider} and {@link StateStoreProvider} both cache
 * data in ways that are not thread safe, so this client should be owned by a single thread.
 */
public class SleeperClient implements AutoCloseable {

    private final InstanceProperties instanceProperties;
    private final TableIndex tableIndex;
    private final TablePropertiesStore tablePropertiesStore;
    private final TablePropertiesProvider tablePropertiesProvider;
    private final StateStoreProvider stateStoreProvider;
    private final ObjectFactory objectFactory;
    private final LeafPartitionRecordRetrieverProvider recordRetrieverProvider;
    private final IngestJobSender ingestJobSender;
    private final BulkExportQuerySender bulkExportQuerySender;
    private final BulkImportJobSender bulkImportJobSender;
    private final IngestBatcherSender ingestBatcherSender;
    private final Runnable shutdown;

    private SleeperClient(Builder builder) {
        instanceProperties = Objects.requireNonNull(builder.instanceProperties, "instanceProperties must not be null");
        tableIndex = Objects.requireNonNull(builder.tableIndex, "tableIndex must not be null");
        tablePropertiesStore = Objects.requireNonNull(builder.tablePropertiesStore, "tablePropertiesStore must not be null");
        tablePropertiesProvider = Objects.requireNonNull(builder.tablePropertiesProvider, "tablePropertiesProvider must not be null");
        stateStoreProvider = Objects.requireNonNull(builder.stateStoreProvider, "stateStoreProvider must not be null");
        objectFactory = Objects.requireNonNull(builder.objectFactory, "objectFactory must not be null");
        recordRetrieverProvider = Objects.requireNonNull(builder.recordRetrieverProvider, "recordRetrieverProvider must not be null");
        ingestJobSender = Objects.requireNonNull(builder.ingestJobSender, "ingestJobSender must not be null");
        bulkExportQuerySender = Objects.requireNonNull(builder.bulkExportQuerySender, "bulkExportQuerySender must not be null");
        bulkImportJobSender = Objects.requireNonNull(builder.bulkImportJobSender, "bulkImportJobSender must not be null");
        ingestBatcherSender = Objects.requireNonNull(builder.ingestBatcherSender, "ingestBatcherSender must not be null");
        shutdown = Objects.requireNonNull(builder.shutdown, "shutdown must not be null");
    }

    /**
     * Creates a client to interact with the instance of Sleeper with the given ID.
     * Will use the default AWS configuration.
     *
     * @param  instanceId the instance ID
     * @return            the client
     */
    public static SleeperClient createForInstanceId(String instanceId) {
        return builder().instanceId(instanceId).build();
    }

    /**
     * Creates a builder for a client to interact with AWS. The Sleeper instance to interact with must be set on the
     * builder. Will use the default AWS configuration unless this is overridden.
     *
     * @return the builder
     */
    public static AwsSleeperClientBuilder builder() {
        return new AwsSleeperClientBuilder();
    }

    /**
     * Reads the instance properties.
     *
     * @return the instance properties
     */
    public InstanceProperties getInstanceProperties() {
        return instanceProperties;
    }

    /**
     * Streams through all Sleeper tables in the instance.
     *
     * @return the status of each Sleeper table
     */
    public Stream<TableStatus> streamAllTables() {
        return tableIndex.streamAllTables();
    }

    /**
     * Reads properties of a Sleeper table.
     *
     * @param  tableStatus            the status of the table
     * @return                        the table properties
     * @throws TableNotFoundException if the table with the given name is not found
     */
    public TableProperties getTableProperties(TableStatus tableStatus) {
        return tablePropertiesProvider.get(tableStatus);
    }

    /**
     * Reads properties of a Sleeper table.
     *
     * @param  tableName              the table name
     * @return                        the table properties
     * @throws TableNotFoundException if the table with the given name is not found
     */
    public TableProperties getTableProperties(String tableName) {
        return tablePropertiesProvider.getByName(tableName);
    }

    /**
     * Retrieves the state store for a Sleeper table.
     *
     * @param  tableName the table name
     * @return           the state store
     */
    public StateStore getStateStore(String tableName) {
        return stateStoreProvider.getStateStore(getTableProperties(tableName));
    }

    /**
     * Returns whether a sleeper table for the given name already exists.
     *
     * @param  tableName the table name
     * @return           true if a table with the given name exists
     */
    public boolean doesTableExist(String tableName) {
        return tableIndex.getTableByName(tableName).isPresent();
    }

    /**
     * Adds a Sleeper table to the instance.
     *
     * @param  tableProperties                   the table properties
     * @param  splitPoints                       the split points to initialise the partition tree
     * @throws SleeperPropertiesInvalidException if table properties provided don't validate
     * @throws TableAlreadyExistsException       if the table already exists
     * @throws StateStoreException               if the state store failed to initialise
     */
    public void addTable(TableProperties tableProperties, List<Object> splitPoints) {
        tableProperties.validate();
        tablePropertiesStore.createTable(tableProperties);
        StateStore stateStore = stateStoreProvider.getStateStore(tableProperties);
        InitialisePartitionsTransaction.fromSplitPoints(tableProperties, splitPoints).synchronousCommit(stateStore);
    }

    /**
     * Creates a query executor for a given Sleeper table.
     *
     * @param  tableName              the table name
     * @return                        the query executor
     * @throws TableNotFoundException if the table with the given name is not found
     * @throws StateStoreException    if the state store can't be accessed
     */
    public QueryExecutor getQueryExecutor(String tableName) {
        TableProperties tableProperties = getTableProperties(tableName);
        StateStore stateStore = stateStoreProvider.getStateStore(tableProperties);
        LeafPartitionRecordRetriever recordRetriever = recordRetrieverProvider.getRecordRetriever(tableProperties);
        QueryExecutor executor = new QueryExecutor(objectFactory, tableProperties, stateStore, recordRetriever);
        executor.init();
        return executor;
    }

    /**
     * Ingests the data in some given files to a Sleeper table. This submits the files as a job in a queue. The job will
     * be processed asynchronously. The ID of the job can be used to track its progress.
     * <p>
     * The files must be in S3, either in the Sleeper data bucket or in a source bucket that Sleeper has access to,
     * usually by setting the instance property 'sleeper.ingest.source.bucket'.
     * <p>
     * The files can be either files or directories, specified like 'bucket-name/path/to/file.parquet'
     * or 'bucket-name/path/to/folder'. If they are directories then all Parquet files under the directory will be
     * ingested.
     *
     * @param  tableName the name of the Sleeper table to write to
     * @param  files     a list of files containing the records to ingest
     * @return           the ID of the job for tracking
     */
    public String ingestFromFiles(String tableName, List<String> files) {
        String jobId = UUID.randomUUID().toString();
        ingestFromFiles(IngestJob.builder()
                .tableName(tableName)
                .id(jobId)
                .files(files)
                .build());
        return jobId;
    }

    /**
     * Ingests the data in some given files to a Sleeper table. This submits the files as a job in a queue. The job will
     * be processed asynchronously. The ID of the job can be used to track its progress.
     * <p>
     * The files must be in S3, either in the Sleeper data bucket or in a source bucket that Sleeper has access to,
     * usually by setting the instance property 'sleeper.ingest.source.bucket'.
     * <p>
     * The files can be either files or directories, specified like 'bucket-name/path/to/file.parquet'
     * or 'bucket-name/path/to/folder'. If they are directories then all Parquet files under the directory will be
     * ingested.
     *
     * @param job the job listing files in S3 to ingest
     */
    public void ingestFromFiles(IngestJob job) {
        ingestJobSender.sendFilesToIngest(job);
    }

    /**
     * Ingests the data in some given files to a Sleeper table with the bulk import method. This submits the files as a
     * job in a queue. The job will be processed asynchronously. The ID of the job can be used to track its progress.
     * <p>
     * The files must be in S3, either in the Sleeper data bucket or in a source bucket that Sleeper has access to,
     * usually by setting the instance property 'sleeper.ingest.source.bucket'.
     * <p>
     * The files can be either files or directories, specified like 'bucket-name/path/to/file.parquet'
     * or 'bucket-name/path/to/folder'. If they are directories then all Parquet files under the directory will be
     * ingested.
     * <p>
     * It is vital that the Sleeper table is pre-split first. Bulk import jobs will be refused unless there are a
     * minimum number of partitions defined, set in the table property `sleeper.table.bulk.import.min.leaf.partitions`.
     *
     * @param  tableName the name of the Sleeper table to write to
     * @param  platform  the platform the import should run on
     * @param  files     a list of files containing the records to ingest
     * @return           the ID of the job for tracking
     */
    public String bulkImportFromFiles(String tableName, BulkImportPlatform platform, List<String> files) {
        String jobId = UUID.randomUUID().toString();
        bulkImportFromFiles(platform, BulkImportJob.builder()
                .id(jobId)
                .tableName(tableName)
                .files(files)
                .build());
        return jobId;
    }

    /**
     * Ingests the data in some given files to a Sleeper table with the bulk import method. This submits the files as a
     * job in a queue. The job will be processed asynchronously. The ID of the job can be used to track its progress.
     * <p>
     * The files must be in S3, either in the Sleeper data bucket or in a source bucket that Sleeper has access to,
     * usually by setting the instance property 'sleeper.ingest.source.bucket'.
     * <p>
     * The files can be either files or directories, specified like 'bucket-name/path/to/file.parquet'
     * or 'bucket-name/path/to/folder'. If they are directories then all Parquet files under the directory will be
     * ingested.
     * <p>
     * It is vital that the Sleeper table is pre-split first. Bulk import jobs will be refused unless there are a
     * minimum number of partitions defined, set in the table property `sleeper.table.bulk.import.min.leaf.partitions`.
     *
     * @param platform the platform the import should run on
     * @param job      the job listing files in S3 to ingest
     */
    public void bulkImportFromFiles(BulkImportPlatform platform, BulkImportJob job) {
        bulkImportJobSender.sendFilesToBulkImport(platform, job);
    }

    /**
     * Submits files to the ingest batcher, to be ingested to a Sleeper table. Once the files are in the ingest batcher,
     * they will be added to an ingest or bulk import job at some point in the future, depending on the configuration of
     * the batcher. The files can be tracked individually by their filename in the ingest batcher store, which will
     * track when they are assigned to a job. Any resulting jobs can then be tracked based on that entry.
     * <p>
     * The files must be in S3, either in the Sleeper data bucket or in a source bucket that Sleeper has access to,
     * usually by setting the instance property 'sleeper.ingest.source.bucket'.
     * <p>
     * The files can be either files or directories, specified like 'bucket-name/path/to/file.parquet'
     * or 'bucket-name/path/to/folder'. If they are directories then all Parquet files under the directory will be
     * ingested.
     * <p>
     * The ingest batcher can be configured in table properties in the ingest batcher property group. If it is set to
     * use bulk import, it is vital that the Sleeper table is pre-split first. Bulk import jobs will be refused unless
     * there are a minimum number of partitions defined, set in the table
     * property `sleeper.table.bulk.import.min.leaf.partitions`.
     *
     * @param tableName the name of the Sleeper table to write to
     * @param files     a list of files containing the records to ingest
     */
    public void sendFilesToIngestBatcher(String tableName, List<String> files) {
        ingestBatcherSender.submit(new IngestBatcherSubmitRequest(tableName, files));
    }

    /**
     * Exports the data defined in a bulk export query using the bulk export method. This submits the query as a
     * an export job in a queue. The job will be processed asynchronously. The ID of the export can be used to track its
     * progress.
     * <p>
     * The table must be in S3, either in the Sleeper data bucket or in a source bucket that Sleeper has access to.
     *
     * @param  tableName - the table to export data from
     * @param  tableId   - The id of the table
     * @return           - The id of the export for tracking
     */
    public String bulkExportFromQuery(String tableName, String tableId) {
        String exportId = UUID.randomUUID().toString();
        bulkExportFromQuery(BulkExportQuery.builder()
                .tableName(tableName)
                .tableId(tableId)
                .exportId(exportId)
                .build());
        return exportId;
    }

    /**
     * Exports the data defined in a bulk export query using the bulk export method. This submits the query as a
     * export in a queue. The job will be processed asynchronously. The ID of the export can be used to track its
     * progress.
     * <p>
     * The table must be in S3, either in the Sleeper data bucket or in a source bucket that Sleeper has access to.
     *
     * @param query the bulk export query
     */
    public void bulkExportFromQuery(BulkExportQuery query) {
        bulkExportQuerySender.sendQueryToBulkExport(query);
    }

    @Override
    public void close() {
        shutdown.run();
    }

    public static class Builder {
        private InstanceProperties instanceProperties;
        private TableIndex tableIndex;
        private TablePropertiesStore tablePropertiesStore;
        private TablePropertiesProvider tablePropertiesProvider;
        private StateStoreProvider stateStoreProvider;
        private ObjectFactory objectFactory = ObjectFactory.noUserJars();
        private LeafPartitionRecordRetrieverProvider recordRetrieverProvider;
        private IngestJobSender ingestJobSender;
        private BulkExportQuerySender bulkExportQuerySender;
        private BulkImportJobSender bulkImportJobSender;
        private IngestBatcherSender ingestBatcherSender;
        private Runnable shutdown = () -> {
        };

        /**
         * Sets the instance properties of the instance to interact with.
         *
         * @param  instanceProperties the instance properties
         * @return                    this builder for chaining
         */
        public Builder instanceProperties(InstanceProperties instanceProperties) {
            this.instanceProperties = instanceProperties;
            return this;
        }

        /**
         * Sets the index of Sleeper tables.
         *
         * @param  tableIndex the table index
         * @return            this builder for chaining
         */
        public Builder tableIndex(TableIndex tableIndex) {
            this.tableIndex = tableIndex;
            return this;
        }

        /**
         * Sets the store of table properties.
         *
         * @param  tablePropertiesStore the table properties store
         * @return                      this builder for chaining
         */
        public Builder tablePropertiesStore(TablePropertiesStore tablePropertiesStore) {
            this.tablePropertiesStore = tablePropertiesStore;
            return this;
        }

        /**
         * Sets the provider to cache loaded table properties.
         *
         * @param  tablePropertiesProvider the provider
         * @return                         this builder for chaining
         */
        public Builder tablePropertiesProvider(TablePropertiesProvider tablePropertiesProvider) {
            this.tablePropertiesProvider = tablePropertiesProvider;
            return this;
        }

        /**
         * Sets the provider to interact with and cache table state.
         *
         * @param  stateStoreProvider the provider
         * @return                    this builder for chaining
         */
        public Builder stateStoreProvider(StateStoreProvider stateStoreProvider) {
            this.stateStoreProvider = stateStoreProvider;
            return this;
        }

        /**
         * Sets the object factory to interact with classes loaded from user jars.
         *
         * @param  objectFactory the object factory
         * @return               this builder for chaining
         */
        public Builder objectFactory(ObjectFactory objectFactory) {
            this.objectFactory = objectFactory;
            return this;
        }

        /**
         * Sets the provider for record retrievers to read table data files.
         *
         * @param  recordRetrieverProvider the record retriever
         * @return                         this builder for chaining
         */
        public Builder recordRetrieverProvider(LeafPartitionRecordRetrieverProvider recordRetrieverProvider) {
            this.recordRetrieverProvider = recordRetrieverProvider;
            return this;
        }

        /**
         * Sets the client to send an ingest job.
         *
         * @param  ingestJobSender the client
         * @return                 this builder for chaining
         */
        public Builder ingestJobSender(IngestJobSender ingestJobSender) {
            this.ingestJobSender = ingestJobSender;
            return this;
        }

        /**
         * Sets the client to send a bulk import job.
         *
         * @param  bulkImportJobSender the client
         * @return                     this builder for chaining
         */
        public Builder bulkImportJobSender(BulkImportJobSender bulkImportJobSender) {
            this.bulkImportJobSender = bulkImportJobSender;
            return this;
        }

        /**
         * Sets the client to send files to the ingest batcher.
         *
         * @param  ingestBatcherSender the client
         * @return                     this builder for chaining
         */
        public Builder ingestBatcherSender(IngestBatcherSender ingestBatcherSender) {
            this.ingestBatcherSender = ingestBatcherSender;
            return this;
        }

        /**
         * Sets the client to send a bulk export job.
         *
         * @param  bulkExportQuerySender the client
         * @return                       this builder for chaining
         */
        public Builder bulkExportQuerySender(BulkExportQuerySender bulkExportQuerySender) {
            this.bulkExportQuerySender = bulkExportQuerySender;
            return this;
        }

        /**
         * Sets how to shut down any resources associated with the client.
         *
         * @param  shutdown the shut down behaviour
         * @return          this builder for chaining
         */
        public Builder shutdown(Runnable shutdown) {
            this.shutdown = shutdown;
            return this;
        }

        public SleeperClient build() {
            return new SleeperClient(this);
        }
    }

}
