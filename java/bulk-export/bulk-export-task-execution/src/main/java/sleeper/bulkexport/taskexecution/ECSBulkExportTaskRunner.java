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

    private final BulkExporter javaExporter;
    private final BulkExporter dataFusionExporter;

    /**
     * Creates an instance of ECSBulkExportTaskRunner.
     *
     * @param javaExporter       the exporter to use for the Java data engine
     * @param dataFusionExporter the exporter to use for the DataFusion data engine
     */
    public ECSBulkExportTaskRunner(BulkExporter javaExporter, BulkExporter dataFusionExporter) {
        this.javaExporter = javaExporter;
        this.dataFusionExporter = dataFusionExporter;
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
            DataFusionAwsConfig awsConfig = DataFusionAwsConfig.getDefault(instanceProperties);
            new ECSBulkExportTaskRunner(
                    new JavaCompactionExporter(instanceProperties, s3Client, hadoopConf, awsConfig),
                    new DataFusionQueryExporter(awsConfig))
                    .runECSBulkExportTaskRunner(instanceProperties, tablePropertiesProvider, sqsClient);
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
     * @throws IOException               if there is an error interacting with S3
     * @throws RowRetrievalException     if there is an error retrieving the query results
     * @throws ObjectFactoryException    if there is an error creating objects
     * @throws IteratorCreationException if there is an error creating iterators
     */
    public void runECSBulkExportTaskRunner(
            InstanceProperties instanceProperties, TablePropertiesProvider tablePropertiesProvider,
            SqsClient sqsClient) throws RuntimeException, IOException, RowRetrievalException, ObjectFactoryException, IteratorCreationException {
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

                runExport(exportTask, instanceProperties, tablePropertiesProvider);
                messageHandle.deleteFromQueue();
                LOGGER.info("Successfully processed and deleted message from queue");
            } catch (RuntimeException | RowRetrievalException | IOException | ObjectFactoryException | IteratorCreationException e) {
                LOGGER.error("Unexpected error processing bulk export job", e);
                messageHandle.returnToQueue();
                LOGGER.info("Returned message to queue due to unexpected error");
                throw e;
            }
        }
    }

    private void runExport(BulkExportLeafPartitionQuery bulkExportLeafPartitionQuery,
            InstanceProperties instanceProperties,
            TablePropertiesProvider tablePropertiesProvider) throws RowRetrievalException, IOException, ObjectFactoryException, IteratorCreationException {
        LOGGER.info("Starting compaction for table ID: {}, partition ID: {}",
                bulkExportLeafPartitionQuery.getTableId(), bulkExportLeafPartitionQuery.getLeafPartitionId());

        String outputFile = bulkExportLeafPartitionQuery.getOutputFile(instanceProperties);
        LOGGER.debug("Output file path: {}", outputFile);

        TableProperties tableProperties = tablePropertiesProvider.getById(bulkExportLeafPartitionQuery.getTableId());

        RowsProcessed rowsProcessed = exporterFor(tableProperties, javaExporter, dataFusionExporter)
                .export(bulkExportLeafPartitionQuery, outputFile, tableProperties);

        LOGGER.info("Bulk export completed for table ID: {}, partition ID: {}. Rows read: {}, rows written: {}",
                bulkExportLeafPartitionQuery.getTableId(),
                bulkExportLeafPartitionQuery.getLeafPartitionId(),
                rowsProcessed.getRowsRead(),
                rowsProcessed.getRowsWritten());
    }

    /**
     * Selects the bulk exporter for the given query based on the table's configured data engine.
     *
     * @param  tableProperties    the properties for the table being exported
     * @param  javaExporter       the exporter to use for the Java data engine
     * @param  dataFusionExporter the exporter to use for the DataFusion data engine
     * @return                    the exporter for the table's data engine
     */
    public static BulkExporter exporterFor(
            TableProperties tableProperties,
            BulkExporter javaExporter,
            BulkExporter dataFusionExporter) {
        return switch (tableProperties.getEnumValue(DATA_ENGINE, DataEngine.class)) {
            case JAVA -> javaExporter;
            case DATAFUSION, DATAFUSION_EXPERIMENTAL -> dataFusionExporter;
        };
    }

    /**
     * Functional interface for performing a bulk export and returning the rows processed.
     */
    @FunctionalInterface
    public interface BulkExporter {
        /**
         * Performs the bulk export.
         *
         * @param  query                     the leaf partition query to export
         * @param  outputFile                the output file path
         * @param  tableProperties           the properties for the table being exported
         * @return                           the number of rows read and written
         * @throws RowRetrievalException     if there is an error retrieving rows via DataFusion
         * @throws IOException               if there is an I/O error
         * @throws ObjectFactoryException    if there is an error creating objects dynamically
         * @throws IteratorCreationException if there is an error creating iterators
         */
        RowsProcessed export(BulkExportLeafPartitionQuery query, String outputFile,
                TableProperties tableProperties) throws RowRetrievalException, IOException, ObjectFactoryException, IteratorCreationException;
    }

    /**
     * A bulk exporter that uses the Java compaction code path to perform the export.
     */
    public static class JavaCompactionExporter implements BulkExporter {
        private final InstanceProperties instanceProperties;
        private final S3Client s3Client;
        private final Configuration hadoopConf;
        private final DataFusionAwsConfig awsConfig;

        public JavaCompactionExporter(
                InstanceProperties instanceProperties, S3Client s3Client,
                Configuration hadoopConf, DataFusionAwsConfig awsConfig) {
            this.instanceProperties = instanceProperties;
            this.s3Client = s3Client;
            this.hadoopConf = hadoopConf;
            this.awsConfig = awsConfig;
        }

        @Override
        public RowsProcessed export(BulkExportLeafPartitionQuery query, String outputFile, TableProperties tableProperties) throws IOException, IteratorCreationException, ObjectFactoryException {
            ObjectFactory objectFactory = new S3UserJarsLoader(instanceProperties, s3Client, Path.of("/tmp")).buildObjectFactory();
            DefaultCompactionRunnerFactory compactionSelector = new DefaultCompactionRunnerFactory(awsConfig,
                    objectFactory, hadoopConf, new NoSketchesStore());

            CompactionJob job = CompactionJob.builder()
                    .jobId(query.getSubExportId())
                    .tableId(query.getTableId())
                    .partitionId(query.getLeafPartitionId())
                    .inputFiles(query.getFiles())
                    .outputFile(outputFile)
                    .build();
            LOGGER.debug("Compaction job details: {}", job);

            CompactionRunner compactor = compactionSelector.createCompactor(job, tableProperties);
            return compactor.compact(CompactionRequest.builder()
                    .job(job)
                    .tableProperties(tableProperties)
                    .region(query.getPartitionRegion())
                    .build());
        }
    }

    /**
     * A bulk exporter that uses the DataFusion query code path to perform the export.
     */
    public static class DataFusionQueryExporter implements BulkExporter {
        private final DataFusionAwsConfig awsConfig;

        public DataFusionQueryExporter(DataFusionAwsConfig awsConfig) {
            this.awsConfig = awsConfig;
        }

        @Override
        public RowsProcessed export(BulkExportLeafPartitionQuery query, String outputFile, TableProperties tableProperties) throws RowRetrievalException {
            Schema schema = tableProperties.getSchema();
            LeafPartitionQuery leafPartitionQuery = LeafPartitionQuery.builder()
                    .files(query.getFiles())
                    .leafPartitionId(query.getLeafPartitionId())
                    .partitionRegion(query.getPartitionRegion())
                    .queryId(query.getExportId())
                    .regions(query.getRegions())
                    .subQueryId(query.getSubExportId())
                    .tableId(query.getTableId())
                    .processingConfig(QueryProcessingConfig.none())
                    .build();
            LOGGER.debug("Query details: {}", leafPartitionQuery);

            try (BufferAllocator allocator = new RootAllocator();
                    FFIContext<DataFusionQueryFunctions> context = FFIContext.getFFIContext(DataFusionQueryFunctions.class)) {
                DataFusionLeafPartitionRowRetriever dataFusion = new DataFusionLeafPartitionRowRetriever(awsConfig, allocator, context);
                return dataFusion.queryToFile(leafPartitionQuery, outputFile, schema, tableProperties);
            }
        }
    }
}
