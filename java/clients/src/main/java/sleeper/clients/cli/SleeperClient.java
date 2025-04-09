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
package sleeper.clients.cli;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.conf.Configuration;

import sleeper.bulkimport.core.job.BulkImportJob;
import sleeper.configuration.properties.S3InstanceProperties;
import sleeper.configuration.properties.S3TableProperties;
import sleeper.configuration.table.index.DynamoDBTableIndex;
import sleeper.core.properties.instance.CdkDefinedInstanceProperty;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertiesProvider;
import sleeper.core.properties.table.TablePropertiesStore;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreProvider;
import sleeper.core.statestore.transactionlog.transaction.impl.InitialisePartitionsTransaction;
import sleeper.core.table.TableIndex;
import sleeper.core.table.TableStatus;
import sleeper.core.util.ObjectFactory;
import sleeper.ingest.core.job.IngestJob;
import sleeper.parquet.utils.HadoopConfigurationProvider;
import sleeper.query.core.recordretrieval.LeafPartitionRecordRetriever;
import sleeper.query.core.recordretrieval.LeafPartitionRecordRetrieverProvider;
import sleeper.query.core.recordretrieval.QueryExecutor;
import sleeper.query.runner.recordretrieval.LeafPartitionRecordRetrieverImpl;
import sleeper.statestore.StateStoreFactory;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Stream;

import static sleeper.configuration.utils.AwsV1ClientHelper.buildAwsV1Client;
import static sleeper.core.properties.table.TableProperty.BULK_IMPORT_EMR_EXECUTOR_X86_INSTANCE_TYPES;

/**
 * A client to interact with an instance of Sleeper. This interacts directly with the underlying AWS resources, and
 * requires permissions against those resources, e.g. the configuration and data buckets in S3, the transaction logs and
 * table index in DynamoDB. There are managed policies and roles deployed with Sleeper that can help with this, e.g.
 * {@link CdkDefinedInstanceProperty#ADMIN_ROLE_ARN}.
 */
public class SleeperClient {

    private final InstanceProperties instanceProperties;
    private final TableIndex tableIndex;
    private final TablePropertiesStore tablePropertiesStore;
    private final TablePropertiesProvider tablePropertiesProvider;
    private final StateStoreProvider stateStoreProvider;
    private final ObjectFactory objectFactory;
    private final LeafPartitionRecordRetrieverProvider recordRetrieverProvider;
    private final SleeperClientIngest sleeperClientIngest;
    private final SleeperClientImport sleeperClientImport;

    private SleeperClient(Builder builder) {
        instanceProperties = builder.instanceProperties;
        tableIndex = builder.tableIndex;
        tablePropertiesStore = builder.tablePropertiesStore;
        tablePropertiesProvider = builder.tablePropertiesProvider;
        stateStoreProvider = builder.stateStoreProvider;
        objectFactory = builder.objectFactory;
        recordRetrieverProvider = builder.recordRetrieverProvider;
        sleeperClientIngest = builder.sleeperClientIngest;
        sleeperClientImport = builder.sleeperClientImport;
    }

    /**
     * Creates a client to interact with the instance of Sleeper with the given ID.
     * Will use the default AWS configuration.
     *
     * @param  instanceId the instance ID
     * @return            the client
     */
    public static SleeperClient createForInstanceId(String instanceId) {
        return createForInstanceId(
                buildAwsV1Client(AmazonS3ClientBuilder.standard()),
                buildAwsV1Client(AmazonDynamoDBClientBuilder.standard()),
                HadoopConfigurationProvider.getConfigurationForClient(),
                instanceId);
    }

    /**
     * Creates a client to interact with the instance of Sleeper with the given ID.
     *
     * @param  s3Client     the AWS S3 client
     * @param  dynamoClient the AWS DynamoDB client
     * @param  hadoopConf   the Hadoop configuration
     * @param  instanceId   the instance ID
     * @return              the client
     */
    public static SleeperClient createForInstanceId(
            AmazonS3 s3Client, AmazonDynamoDB dynamoClient, Configuration hadoopConf, String instanceId) {
        InstanceProperties instanceProperties = S3InstanceProperties.loadGivenInstanceId(s3Client, instanceId);
        TableIndex tableIndex = new DynamoDBTableIndex(instanceProperties, dynamoClient);
        ExecutorService queryExecutorService = Executors.newFixedThreadPool(10);
        return builder()
                .instanceProperties(instanceProperties)
                .tableIndex(tableIndex)
                .tablePropertiesProvider(S3TableProperties.createProvider(instanceProperties, tableIndex, s3Client))
                .stateStoreProvider(
                        StateStoreFactory.createProvider(instanceProperties, s3Client, dynamoClient, hadoopConf))
                .objectFactory(ObjectFactory.noUserJars())
                .recordRetrieverProvider(
                        LeafPartitionRecordRetrieverImpl.createProvider(queryExecutorService, hadoopConf))
                .build();
    }

    public static Builder builder() {
        return new Builder();
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
     * @param  tableStatus the status of the table
     * @return             the table properties
     */
    public TableProperties getTableProperties(TableStatus tableStatus) {
        return tablePropertiesProvider.get(tableStatus);
    }

    /**
     * Reads properties of a Sleeper table.
     *
     * @param  tableName the table name
     * @return           the table properties
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
     * Adds a Sleeper table to the instance.
     *
     * @param tableProperties the table properties
     * @param splitPoints     the split points to initialise the partition tree
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
     * @param  tableName the table name
     * @return           the query executor
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
     * Ingests the data in the given files to the Sleeper table with name table_name. This is done by posting a
     * message containing the list of files to the ingest queue. These files must be in S3. They can be either files or
     * directories. If they are directories then all Parquet files under the directory will be ingested.
     * Files should be specified in the format 'bucket/file'.
     *
     * @param  tableName table name to write to
     * @param  jobId     id of the ingest job, randomly generated if not set
     * @param  files     list of files containing records to ingest
     * @return           id for the job create for the ingest
     */
    public String ingestParquetFilesFromS3(String tableName, String jobId, List<String> files) {
        if (jobId == null) {
            jobId = UUID.randomUUID().toString();
        }
        sleeperClientIngest.sendFilesToIngest(IngestJob.builder()
                .tableName(tableName)
                .id(jobId)
                .files(files)
                .build());

        return jobId;
    }

    /**
     * Ingests the data in the given files to the Sleeper table with name table_name using the bulk
     * import method. This is done by posting a message containing the list of files to the bulk
     * import queue. These files must be in S3. They can be either files or directories. If they
     * are directories then all Parquet files under the directory will be ingested. Files should
     * be specified in the format 'bucket/file'.
     *
     * Instructs Sleeper to bulk import the given files from S3.
     *
     * @param  tableName    the table name to write to
     * @param  jobId        the id of the bulk import job - if one is not provided then a UUID will be assigned
     * @param  files        list of the files containing the records to ingest
     * @param  platformSpec This optional parameter allows you to configure details of the EMR cluster that is created
     *                      to run the bulk import job. This should be a map, containing parameters specifying details
     *                      of the cluster. If this is not provided then sensible defaults are used.
     *
     * @return              number of files imported
     *
     */
    public int bulkImportParquetFilesFromS3(String tableName, String jobId, List<String> files, Map<String, String> platformSpec) {
        if (jobId == null) {
            jobId = UUID.randomUUID().toString();
        }

        if (platformSpec == null || platformSpec.isEmpty()) {
            platformSpec = ImmutableMap.of(BULK_IMPORT_EMR_EXECUTOR_X86_INSTANCE_TYPES.getPropertyName(), "r5.xlarge");
        }

        sleeperClientImport.importFilesFromS3(BulkImportJob.builder()
                .id(jobId)
                .tableName(tableName)
                .tableId(tableName)
                .files(files)
                .className("")
                .platformSpec(platformSpec)
                .build());
        return files.size();
    }

    public static class Builder {
        private InstanceProperties instanceProperties;
        private TableIndex tableIndex;
        private TablePropertiesStore tablePropertiesStore;
        private TablePropertiesProvider tablePropertiesProvider;
        private StateStoreProvider stateStoreProvider;
        private ObjectFactory objectFactory = ObjectFactory.noUserJars();
        private LeafPartitionRecordRetrieverProvider recordRetrieverProvider;
        private SleeperClientIngest sleeperClientIngest;
        private SleeperClientImport sleeperClientImport;

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
         * Provides functionality to send ingest job.
         *
         * @param  sleeperClientIngest the ingest interface for client
         * @return                     this builder for chaining
         */
        public Builder sleeperClientIngest(SleeperClientIngest sleeperClientIngest) {
            this.sleeperClientIngest = sleeperClientIngest;
            return this;
        }

        /**
         * Provides functionality for bulk import of parquet files for client.
         *
         * @param  sleeperClientImport the bulk import interface for client
         * @return                     this builder for chaining
         */
        public Builder sleeperClientImport(SleeperClientImport sleeperClientImport) {
            this.sleeperClientImport = sleeperClientImport;
            return this;
        }

        public SleeperClient build() {
            return new SleeperClient(this);
        }
    }

}
