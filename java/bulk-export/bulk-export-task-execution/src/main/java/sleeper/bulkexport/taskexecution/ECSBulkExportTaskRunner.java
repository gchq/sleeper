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

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.bulkexport.core.model.BulkExportLeafPartitionQuery;
import sleeper.configuration.properties.S3InstanceProperties;
import sleeper.configuration.properties.S3TableProperties;
import sleeper.core.iterator.IteratorCreationException;
import sleeper.core.properties.instance.CdkDefinedInstanceProperty;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TablePropertiesProvider;
import sleeper.core.util.LoggedDuration;
import sleeper.core.util.ObjectFactoryException;

import java.io.IOException;
import java.time.Instant;

import static sleeper.configuration.utils.AwsV1ClientHelper.buildAwsV1Client;

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
     * @param args Command line arguments
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
        AmazonDynamoDB dynamoDBClient = buildAwsV1Client(AmazonDynamoDBClientBuilder.standard());
        AmazonSQS sqsClient = buildAwsV1Client(AmazonSQSClientBuilder.standard());
        AmazonS3 s3Client = buildAwsV1Client(AmazonS3ClientBuilder.standard());
        InstanceProperties instanceProperties = S3InstanceProperties.loadFromBucket(s3Client, s3Bucket);
        TablePropertiesProvider tablePropertiesProvider = S3TableProperties.createProvider(instanceProperties, s3Client,
                dynamoDBClient);
        runECSBulkExportTaskRunner(sqsClient, instanceProperties, tablePropertiesProvider);
        LOGGER.info("Total run time = {}", LoggedDuration.withFullOutput(startTime, Instant.now()));
    }

    /**
     * Run the ECS bulk export task runner.
     * This method initialises the SQS bulk export queue handler and processes
     * messages from the SQS queue for bulk export tasks.
     *
     * @param sqsClient               an AmazonSQS client
     * @param instanceProperties      an InstanceProperties object
     * @param tablePropertiesProvider a TablePropertiesProvider object
     * @throws IOException               if there is an error interacting with S3
     * @throws IteratorCreationException if there is an error creating iterators
     * @throws ObjectFactoryException    if there is an error creating objects
     */
    public static void runECSBulkExportTaskRunner(AmazonSQS sqsClient, InstanceProperties instanceProperties,
            TablePropertiesProvider tablePropertiesProvider)
            throws IOException, IteratorCreationException, ObjectFactoryException {
        SqsBulkExportQueueHandler exportQueueHandler = new SqsBulkExportQueueHandler(sqsClient,
                tablePropertiesProvider, instanceProperties);
        LOGGER.info("Waiting for leaf partition bulk export job from queue {}",
                instanceProperties.get(CdkDefinedInstanceProperty.LEAF_PARTITION_BULK_EXPORT_QUEUE_URL));

        exportQueueHandler.receiveMessage().ifPresent(messageHandle -> {
            try {
                BulkExportLeafPartitionQuery exportTask = messageHandle.getJob();
                LOGGER.info("Received leaf partition bulk export job: {}", exportTask);
                exportTask.getFiles().forEach(inputFile -> {
                    LOGGER.info("Input file: {}", inputFile);
                });
                String partitionId = exportTask.getLeafPartitionId();
                LOGGER.info("Partition ID: {}", partitionId);
                String tableId = exportTask.getTableId();
                LOGGER.info("Table ID: {}", tableId);
                messageHandle.deleteFromQueue();
                LOGGER.info("Deleted message from queue");
            } catch (Exception e) {
                LOGGER.error("Error processing compaction job", e);
                messageHandle.returnToQueue();
                LOGGER.info("Returned message to queue");
            }
        });
    }
}
