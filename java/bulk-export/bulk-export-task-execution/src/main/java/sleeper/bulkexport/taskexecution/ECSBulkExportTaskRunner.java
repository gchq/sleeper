/*
 * Copyright 2022-2026 Crown Copyright
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
package sleeper.bulkexport.taskexecution;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sqs.SqsClient;

import sleeper.bulkexport.core.model.BulkExportLeafPartitionQuery;
import sleeper.bulkexport.taskexecution.SqsBulkExportQueueHandler.SqsMessageHandle;
import sleeper.compaction.core.job.CompactionJob;
import sleeper.compaction.core.job.CompactionRequest;
import sleeper.compaction.core.job.CompactionRunner;
import sleeper.compaction.job.execution.DefaultCompactionRunnerFactory;
import sleeper.configuration.jars.S3UserJarsLoader;
import sleeper.configuration.properties.S3InstanceProperties;
import sleeper.configuration.properties.S3TableProperties;
import sleeper.core.iterator.IteratorCreationException;
import sleeper.core.properties.instance.CdkDefinedInstanceProperty;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.model.DataEngine;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertiesProvider;
import sleeper.core.schema.Schema;
import sleeper.core.tracker.job.run.RowsProcessed;
import sleeper.core.util.LoggedDuration;
import sleeper.core.util.ObjectFactory;
import sleeper.core.util.ObjectFactoryException;
import sleeper.foreign.bridge.FFIContext;
import sleeper.foreign.datafusion.DataFusionAwsConfig;
import sleeper.parquet.utils.HadoopConfigurationProvider;
import sleeper.query.core.model.LeafPartitionQuery;
import sleeper.query.core.model.QueryProcessingConfig;
import sleeper.query.core.rowretrieval.RowRetrievalException;
import sleeper.query.datafusion.DataFusionLeafPartitionRowRetriever;
import sleeper.query.datafusion.DataFusionQueryFunctions;
import sleeper.sketches.store.NoSketchesStore;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Optional;

import static sleeper.configuration.utils.AwsV2ClientHelper.buildAwsV2Client;
import static sleeper.core.properties.table.TableProperty.DATA_ENGINE;

/**
 * Main class to run the ECS bulk export task.
 */
public class ECSBulkExportTaskRunner {

    private static final Logger LOGGER = LoggerFactory.getLogger(ECSBulkExportTaskRunner.class);

    private ECSBulkExportTaskRunner() {
    }

    /**
     * Main method to run the ECS bulk export task.
     *
     * This method initializes AWS clients for DynamoDB, SQS, and S3, loads instance
     * and table properties, and processes messages from the SQS queue for bulk
     * export tasks.
     *
     * @param  args                      Command line arguments
     * @throws IOException               If there is an error interacting with S3 or
     *                                   other I/O operations.
     * @throws RowRetrievalException     if there is an error retrieving the query results
     * @throws ObjectFactoryException    If there is an error creating objects
     *                                   dynamically.
     * @throws IteratorCreationException If there is an error creating iterators for
     *                                   processing.
     */
    public static void main(String[] args) throws IOException, RowRetrievalException, ObjectFactoryException, IteratorCreationException {

        if (1 != args.length) {
            System.err.println("Error: must have 1 argument (config bucket), got " + args.length + " arguments ("
                    + String.join(",", args) + ")");
            System.exit(1);
        }

        String s3Bucket = args[0];
        Instant startTime = Instant.now();
        try (S3Client s3Client = buildAwsV2Client(S3Client.builder());
                DynamoDbClient dynamoDBClient = buildAwsV2Client(DynamoDbClient.builder());
                SqsClient sqsClient = buildAwsV2Client(SqsClient.builder())) {
            InstanceProperties instanceProperties = S3InstanceProperties.loadFromBucket(s3Client, s3Bucket);
            TablePropertiesProvider tablePropertiesProvider = S3TableProperties.createProvider(instanceProperties, s3Client, dynamoDBClient);
            Configuration hadoopConf = HadoopConfigurationProvider.getConfigurationForECS(instanceProperties);
            runECSBulkExportTaskRunner(instanceProperties, tablePropertiesProvider, sqsClient, s3Client, dynamoDBClient, hadoopConf, DataFusionAwsConfig.getDefault(instanceProperties));
        } finally {
            LOGGER.info("Total run time = {}", LoggedDuration.withFullOutput(startTime, Instant.now()));
        }
    }

    /**
     * Run the ECS bulk export task runner.
     * This method initialises the SQS bulk export queue handler and processes
     * messages from the SQS queue for bulk export tasks.
     *
     * @param  instanceProperties        the instance properties
     * @param  tablePropertiesProvider   a table properties provider
     * @param  sqsClient                 an SQS client
     * @param  s3Client                  an S3 client
     * @param  dynamoDBClient            a DynamoDB client
     * @param  awsConfig                 DataFusion AWS S3 configuration
     * @param  hadoopConf                a Hadoop configuration
     * @throws IOException               if there is an error interacting with S3
     * @throws RowRetrievalException     if there is an error retrieving the query results
     * @throws ObjectFactoryException    if there is an error creating objects
     * @throws IteratorCreationException if there is an error creating iterators
     */
    public static void runECSBulkExportTaskRunner(
            InstanceProperties instanceProperties, TablePropertiesProvider tablePropertiesProvider,
            SqsClient sqsClient, S3Client s3Client, DynamoDbClient dynamoDBClient,
            Configuration hadoopConf, DataFusionAwsConfig awsConfig) throws RuntimeException, IOException, RowRetrievalException, ObjectFactoryException, IteratorCreationException {
        SqsBulkExportQueueHandler exportQueueHandler = new SqsBulkExportQueueHandler(sqsClient,
                tablePropertiesProvider, instanceProperties);
        LOGGER.info("Waiting for leaf partition bulk export job from queue {}",
                instanceProperties.get(CdkDefinedInstanceProperty.LEAF_PARTITION_BULK_EXPORT_QUEUE_URL));
        Optional<SqsMessageHandle> messageHandleOpt;

        messageHandleOpt = exportQueueHandler.receiveMessage();

        if (messageHandleOpt.isPresent()) {
            SqsMessageHandle messageHandle = messageHandleOpt.get();
            try {
                BulkExportLeafPartitionQuery exportTask = messageHandle.getJob();

                LOGGER.info("Received bulk export job for table ID: {}, partition ID: {}", exportTask.getTableId(), exportTask.getLeafPartitionId());
                LOGGER.debug("Bulk Export job details: {}", exportTask);

                runExport(exportTask, instanceProperties, tablePropertiesProvider, s3Client, dynamoDBClient, hadoopConf, awsConfig);
                messageHandle.deleteFromQueue();
                LOGGER.info("Successfully processed and deleted message from queue");
            } catch (RuntimeException | RowRetrievalException e) {
                LOGGER.error("Unexpected error processing bulk export job", e);
                messageHandle.returnToQueue();
                LOGGER.info("Returned message to queue due to unexpected error");
                throw e;
            }
        }
    }

    private static void runExport(BulkExportLeafPartitionQuery bulkExportLeafPartitionQuery,
            InstanceProperties instanceProperties,
            TablePropertiesProvider tablePropertiesProvider,
            S3Client s3Client,
            DynamoDbClient dynamoDBClient,
            Configuration hadoopConf, DataFusionAwsConfig awsConfig) throws RowRetrievalException, IOException, ObjectFactoryException, IteratorCreationException {
        LOGGER.info("Starting compaction for table ID: {}, partition ID: {}",
                bulkExportLeafPartitionQuery.getTableId(), bulkExportLeafPartitionQuery.getLeafPartitionId());

        String outputFile = bulkExportLeafPartitionQuery.getOutputFile(instanceProperties);
        LOGGER.debug("Output file path: {}", outputFile);

        TableProperties tableProperties = tablePropertiesProvider.getById(bulkExportLeafPartitionQuery.getTableId());

        RowsProcessed rowsProcessed = switch (tableProperties.getEnumValue(DATA_ENGINE, DataEngine.class)) {
            case JAVA -> exportViaJavaCompaction(bulkExportLeafPartitionQuery, awsConfig, s3Client, instanceProperties, outputFile, hadoopConf, tableProperties);
            case DATAFUSION, DATAFUSION_EXPERIMENTAL -> exportViaDataFusionQuery(bulkExportLeafPartitionQuery, awsConfig, outputFile, tableProperties);
        };

        LOGGER.info("Bulk export completed for table ID: {}, partition ID: {}. Rows read: {}, rows written: {}",
                bulkExportLeafPartitionQuery.getTableId(),
                bulkExportLeafPartitionQuery.getLeafPartitionId(),
                rowsProcessed.getRowsRead(),
                rowsProcessed.getRowsWritten());
    }

    private static RowsProcessed exportViaJavaCompaction(BulkExportLeafPartitionQuery bulkExportLeafPartitionQuery,
            DataFusionAwsConfig awsConfig, S3Client s3Client, InstanceProperties instanceProperties, String outputFile, Configuration hadoopConf,
            TableProperties tableProperties) throws IOException, IteratorCreationException, ObjectFactoryException {
        ObjectFactory objectFactory = new S3UserJarsLoader(instanceProperties, s3Client, Path.of("/tmp")).buildObjectFactory();
        DefaultCompactionRunnerFactory compactionSelector = new DefaultCompactionRunnerFactory(DataFusionAwsConfig.getDefault(instanceProperties),
                objectFactory, hadoopConf, new NoSketchesStore());

        CompactionJob job = CompactionJob.builder()
                .jobId(bulkExportLeafPartitionQuery.getSubExportId())
                .tableId(bulkExportLeafPartitionQuery.getTableId())
                .partitionId(bulkExportLeafPartitionQuery.getLeafPartitionId())
                .inputFiles(bulkExportLeafPartitionQuery.getFiles())
                .outputFile(outputFile)
                .build();
        LOGGER.debug("Compaction job details: {}", job);

        CompactionRunner compactor = compactionSelector.createCompactor(job, tableProperties);
        return compactor.compact(CompactionRequest.builder()
                .job(job)
                .tableProperties(tableProperties)
                .region(bulkExportLeafPartitionQuery.getPartitionRegion())
                .build());
    }

    private static RowsProcessed exportViaDataFusionQuery(BulkExportLeafPartitionQuery bulkExportLeafPartitionQuery,
            DataFusionAwsConfig awsConfig, String outputFile, TableProperties tableProperties) throws RowRetrievalException {
        Schema schema = tableProperties.getSchema();
        LeafPartitionQuery query = LeafPartitionQuery.builder()
                .files(bulkExportLeafPartitionQuery.getFiles())
                .leafPartitionId(bulkExportLeafPartitionQuery.getLeafPartitionId())
                .partitionRegion(bulkExportLeafPartitionQuery.getPartitionRegion())
                .queryId(bulkExportLeafPartitionQuery.getExportId())
                .regions(bulkExportLeafPartitionQuery.getRegions())
                .subQueryId(bulkExportLeafPartitionQuery.getSubExportId())
                .tableId(bulkExportLeafPartitionQuery.getTableId())
                .processingConfig(QueryProcessingConfig.none())
                .build();
        LOGGER.debug("Query details: {}", query);

        try (BufferAllocator allocator = new RootAllocator();
                FFIContext<DataFusionQueryFunctions> context = FFIContext.getFFIContext(DataFusionQueryFunctions.class)) {
            DataFusionLeafPartitionRowRetriever dataFusion = (DataFusionLeafPartitionRowRetriever) new DataFusionLeafPartitionRowRetriever.Provider(
                    awsConfig, allocator, context).getRowRetriever(tableProperties);
            return dataFusion.queryToFile(query, outputFile, schema, tableProperties);
        }
    }
}
