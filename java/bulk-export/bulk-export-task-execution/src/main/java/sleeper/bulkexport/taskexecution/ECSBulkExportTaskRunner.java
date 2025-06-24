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
package sleeper.bulkexport.taskexecution;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sqs.SqsClient;

import sleeper.bulkexport.core.model.BulkExportLeafPartitionQuery;
import sleeper.bulkexport.taskexecution.SqsBulkExportQueueHandler.SqsMessageHandle;
import sleeper.compaction.core.job.CompactionJob;
import sleeper.compaction.core.job.CompactionRunner;
import sleeper.compaction.job.execution.DefaultCompactionRunnerFactory;
import sleeper.configuration.jars.S3UserJarsLoader;
import sleeper.configuration.properties.S3InstanceProperties;
import sleeper.configuration.properties.S3TableProperties;
import sleeper.core.iterator.IteratorCreationException;
import sleeper.core.partition.Partition;
import sleeper.core.properties.instance.CdkDefinedInstanceProperty;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertiesProvider;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreProvider;
import sleeper.core.tracker.job.run.RecordsProcessed;
import sleeper.core.util.LoggedDuration;
import sleeper.core.util.ObjectFactory;
import sleeper.core.util.ObjectFactoryException;
import sleeper.parquet.utils.HadoopConfigurationProvider;
import sleeper.sketches.store.NoSketchesStore;
import sleeper.statestore.StateStoreFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Optional;

import static sleeper.configuration.utils.AwsV2ClientHelper.buildAwsV2Client;

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
     * @throws ObjectFactoryException    If there is an error creating objects
     *                                   dynamically.
     * @throws IteratorCreationException If there is an error creating iterators for
     *                                   processing.
     * @throws IOException               If there is an error interacting with S3 or
     *                                   other I/O operations.
     */
    public static void main(String[] args) throws ObjectFactoryException, IOException, IteratorCreationException {

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
            runECSBulkExportTaskRunner(instanceProperties, tablePropertiesProvider, sqsClient, s3Client, dynamoDBClient, hadoopConf);
        } finally {
            LOGGER.info("Total run time = {}", LoggedDuration.withFullOutput(startTime, Instant.now()));
        }
    }

    /**
     * Run the ECS bulk export task runner.
     * This method initialises the SQS bulk export queue handler and processes
     * messages from the SQS queue for bulk export tasks.
     *
     * @param  instanceProperties      the instance properties
     * @param  tablePropertiesProvider a table properties provider
     * @param  sqsClient               an SQS client
     * @param  s3Client                an S3 client
     * @param  dynamoDBClient          a DynamoDB client
     * @param  hadoopConf              a Hadoop configuration
     * @throws IOException             if there is an error interacting with S3
     * @throws IllegalStateException   if there is an error receiving the message and deseralising it
     */
    public static void runECSBulkExportTaskRunner(
            InstanceProperties instanceProperties, TablePropertiesProvider tablePropertiesProvider,
            SqsClient sqsClient, S3Client s3Client, DynamoDbClient dynamoDBClient,
            Configuration hadoopConf) throws IOException, IllegalStateException {
        SqsBulkExportQueueHandler exportQueueHandler = new SqsBulkExportQueueHandler(sqsClient,
                tablePropertiesProvider, instanceProperties);
        LOGGER.info("Waiting for leaf partition bulk export job from queue {}",
                instanceProperties.get(CdkDefinedInstanceProperty.LEAF_PARTITION_BULK_EXPORT_QUEUE_URL));
        Optional<SqsMessageHandle> messageHandleOpt;

        try {
            messageHandleOpt = exportQueueHandler.receiveMessage();
        } catch (IOException | IllegalStateException e) {
            LOGGER.error("There is a problem getting the message.", e);
            throw e;
        }

        if (messageHandleOpt.isPresent()) {
            SqsMessageHandle messageHandle = messageHandleOpt.get();
            try {
                BulkExportLeafPartitionQuery exportTask;
                if (messageHandle.getJob() == null) {
                    throw new NullPointerException("No job found in bulk export message");
                } else {
                    exportTask = messageHandle.getJob();
                }
                LOGGER.info("Received bulk export job for table ID: {}, partition ID: {}", exportTask.getTableId(), exportTask.getLeafPartitionId());
                LOGGER.debug("Bulk Export job details: {}", exportTask);

                runCompaction(exportTask, instanceProperties, tablePropertiesProvider, s3Client, dynamoDBClient, hadoopConf);
                messageHandle.deleteFromQueue();
                LOGGER.info("Successfully processed and deleted message from queue");
            } catch (RuntimeException | IOException | ObjectFactoryException | IteratorCreationException e) {
                LOGGER.error("Unexpected error processing bulk export job", e);
                messageHandle.returnToQueue();
                LOGGER.info("Returned message to queue due to unexpected error");
            }
        }
    }

    private static void runCompaction(BulkExportLeafPartitionQuery bulkExportLeafPartitionQuery,
            InstanceProperties instanceProperties,
            TablePropertiesProvider tablePropertiesProvider,
            S3Client s3Client,
            DynamoDbClient dynamoDBClient,
            Configuration hadoopConf) throws IOException, IteratorCreationException, ObjectFactoryException {
        LOGGER.info("Starting compaction for table ID: {}, partition ID: {}",
                bulkExportLeafPartitionQuery.getTableId(), bulkExportLeafPartitionQuery.getLeafPartitionId());
        StateStoreProvider stateStoreProvider = StateStoreFactory.createProvider(instanceProperties, s3Client,
                dynamoDBClient);

        String outputFile = bulkExportLeafPartitionQuery.getOutputFile(instanceProperties);
        LOGGER.debug("Output file path: {}", outputFile);

        ObjectFactory objectFactory = new S3UserJarsLoader(instanceProperties, s3Client, Path.of("/tmp")).buildObjectFactory();
        DefaultCompactionRunnerFactory compactionSelector = new DefaultCompactionRunnerFactory(
                objectFactory, hadoopConf, new NoSketchesStore());

        CompactionJob job = CompactionJob.builder()
                .jobId(bulkExportLeafPartitionQuery.getSubExportId())
                .tableId(bulkExportLeafPartitionQuery.getTableId())
                .partitionId(bulkExportLeafPartitionQuery.getLeafPartitionId())
                .inputFiles(bulkExportLeafPartitionQuery.getFiles())
                .outputFile(outputFile)
                .build();
        LOGGER.debug("Compaction job details: {}", job);

        TableProperties tableProperties = tablePropertiesProvider.getById(job.getTableId());
        StateStore stateStore = stateStoreProvider.getStateStore(tableProperties);
        CompactionRunner compactor = compactionSelector.createCompactor(job, tableProperties);
        Partition partition = stateStore.getPartition(bulkExportLeafPartitionQuery.getLeafPartitionId());

        RecordsProcessed recordsProcessed = compactor.compact(job, tableProperties, partition);
        LOGGER.info("Compaction completed for table ID: {}, partition ID: {}. Records read: {}, records written: {}",
                bulkExportLeafPartitionQuery.getTableId(),
                bulkExportLeafPartitionQuery.getLeafPartitionId(),
                recordsProcessed.getRecordsRead(),
                recordsProcessed.getRecordsWritten());
    }
}
