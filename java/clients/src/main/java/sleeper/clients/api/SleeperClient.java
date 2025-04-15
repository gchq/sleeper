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

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import org.apache.hadoop.conf.Configuration;

import sleeper.bulkimport.core.configuration.BulkImportPlatform;
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
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Stream;

import static sleeper.configuration.utils.AwsV1ClientHelper.buildAwsV1Client;

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
    private final SleeperClientIngest ingestJobSender;
    private final SleeperClientBulkImport bulkImportJobSender;

    private SleeperClient(Builder builder) {
        instanceProperties = Objects.requireNonNull(builder.instanceProperties, "instanceProperties must not be null");
        tableIndex = Objects.requireNonNull(builder.tableIndex, "tableIndex must not be null");
        tablePropertiesStore = Objects.requireNonNull(builder.tablePropertiesStore, "tableProperties must not be null");
        tablePropertiesProvider = Objects.requireNonNull(builder.tablePropertiesProvider, "tablePropertiesProvider must not be null");
        stateStoreProvider = Objects.requireNonNull(builder.stateStoreProvider, "stateStoreProvider must not be null");
        objectFactory = Objects.requireNonNull(builder.objectFactory, "objectFactory must not be null");
        recordRetrieverProvider = Objects.requireNonNull(builder.recordRetrieverProvider, "recordRetrieveProvider must not be null");
        ingestJobSender = Objects.requireNonNull(builder.ingestJobSender, "ingestJobSender must not be null");
        bulkImportJobSender = Objects.requireNonNull(builder.bulkImportJobSender, "bulkImportJobSender must not be null");
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
                buildAwsV1Client(AmazonSQSClientBuilder.standard()),
                HadoopConfigurationProvider.getConfigurationForClient(),
                instanceId);
    }

    private static SleeperClient createForInstanceId(
            AmazonS3 s3Client, AmazonDynamoDB dynamoClient, AmazonSQS sqsClient, Configuration hadoopConf, String instanceId) {
        InstanceProperties instanceProperties = S3InstanceProperties.loadGivenInstanceId(s3Client, instanceId);
        TableIndex tableIndex = new DynamoDBTableIndex(instanceProperties, dynamoClient);
        ExecutorService queryExecutorService = Executors.newFixedThreadPool(10);
        return builder()
                .instanceProperties(instanceProperties)
                .tableIndex(tableIndex)
                .tablePropertiesProvider(S3TableProperties.createProvider(instanceProperties, tableIndex, s3Client))
                .tablePropertiesStore(S3TableProperties.createStore(instanceProperties, s3Client, dynamoClient))
                .stateStoreProvider(
                        StateStoreFactory.createProvider(instanceProperties, s3Client, dynamoClient, hadoopConf))
                .objectFactory(ObjectFactory.noUserJars())
                .recordRetrieverProvider(
                        LeafPartitionRecordRetrieverImpl.createProvider(queryExecutorService, hadoopConf))
                .ingestJobSender(SleeperClientIngest.ingestParquetFilesFromS3(instanceProperties, sqsClient))
                .bulkImportJobSender(SleeperClientBulkImport.bulkImportParquetFilesFromS3(instanceProperties, sqsClient))
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
     * Ingests the data in some given files to a Sleeper table. This is done by sending a message to the ingest queue
     * containing a list of files. These files must be in S3. They can be either files or directories. If they are
     * directories then all Parquet files under the directory will be ingested.
     * <p>
     * Files should be specified in the format 'bucketName/objectKey'.
     *
     * @param  tableName the name of the Sleeper table to write to
     * @param  files     list of files containing records to ingest
     * @return           the ID of the job for tracking
     */
    public String ingestParquetFilesFromS3(String tableName, List<String> files) {
        String jobId = UUID.randomUUID().toString();
        ingestParquetFilesFromS3(IngestJob.builder()
                .tableName(tableName)
                .id(jobId)
                .files(files)
                .build());
        return jobId;
    }

    /**
     * Ingests the data in some given files to a Sleeper table. This is done by sending a message to the ingest queue
     * containing a list of files. These files must be in S3. They can be either files or directories. If they are
     * directories then all Parquet files under the directory will be ingested.
     * <p>
     * Files should be specified in the format 'bucketName/objectKey'.
     *
     * @param job the job listing files in S3 to ingest
     */
    public void ingestParquetFilesFromS3(IngestJob job) {
        ingestJobSender.sendFilesToIngest(job);
    }

    /**
     * Ingests the data in some given files to a Sleeper table with the bulk import method. This is done by sending a
     * message to a bulk import queue containing a list of files. These files must be in S3. They can be either files or
     * directories. If they are directories then all Parquet files under the directory will be ingested.
     * <p>
     * Files should be specified in the format 'bucketName/objectKey'.
     *
     * @param  tableName the table name to write to
     * @param  platform  the platform the import should run on
     * @param  files     list of the files containing the records to ingest
     * @return           the ID of the job for tracking
     */
    public String bulkImportParquetFilesFromS3(String tableName, BulkImportPlatform platform, List<String> files) {
        String jobId = UUID.randomUUID().toString();
        bulkImportParquetFilesFromS3(platform, BulkImportJob.builder()
                .id(jobId)
                .tableName(tableName)
                .files(files)
                .build());
        return jobId;
    }

    /**
     * Ingests the data in some given files to a Sleeper table with the bulk import method. This is done by sending a
     * message to a bulk import queue containing a list of files. These files must be in S3. They can be either files or
     * directories. If they are directories then all Parquet files under the directory will be ingested.
     * <p>
     * Files should be specified in the format 'bucketName/objectKey'.
     *
     * @param platform the platform the import should run on
     * @param job      the job listing files in S3 to ingest
     */
    public void bulkImportParquetFilesFromS3(BulkImportPlatform platform, BulkImportJob job) {
        bulkImportJobSender.sendFilesToBulkImport(platform, job);
    }

    public static class Builder {
        private InstanceProperties instanceProperties;
        private TableIndex tableIndex;
        private TablePropertiesStore tablePropertiesStore;
        private TablePropertiesProvider tablePropertiesProvider;
        private StateStoreProvider stateStoreProvider;
        private ObjectFactory objectFactory = ObjectFactory.noUserJars();
        private LeafPartitionRecordRetrieverProvider recordRetrieverProvider;
        private SleeperClientIngest ingestJobSender;
        private SleeperClientBulkImport bulkImportJobSender;

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
        public Builder ingestJobSender(SleeperClientIngest ingestJobSender) {
            this.ingestJobSender = ingestJobSender;
            return this;
        }

        /**
         * Sets the client to send a bulk import job.
         *
         * @param  bulkImportJobSender the client
         * @return                     this builder for chaining
         */
        public Builder bulkImportJobSender(SleeperClientBulkImport bulkImportJobSender) {
            this.bulkImportJobSender = bulkImportJobSender;
            return this;
        }

        public SleeperClient build() {
            return new SleeperClient(this);
        }
    }

}
