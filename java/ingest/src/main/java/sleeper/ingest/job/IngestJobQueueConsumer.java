/*
 * Copyright 2022 Crown Copyright
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
package sleeper.ingest.job;

import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.cloudwatch.model.Dimension;
import com.amazonaws.services.cloudwatch.model.MetricDatum;
import com.amazonaws.services.cloudwatch.model.PutMetricDataRequest;
import com.amazonaws.services.cloudwatch.model.StandardUnit;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sleeper.configuration.jars.ObjectFactory;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.UserDefinedInstanceProperty;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.core.iterator.CloseableIterator;
import sleeper.core.iterator.ConcatenatingIterator;
import sleeper.core.iterator.IteratorException;
import sleeper.core.record.Record;
import sleeper.core.schema.Schema;
import sleeper.ingest.IngestRecordsUsingPropertiesSpecifiedMethod;
import sleeper.io.parquet.record.ParquetReaderIterator;
import sleeper.io.parquet.record.ParquetRecordReader;
import sleeper.job.common.action.ActionException;
import sleeper.job.common.action.ChangeMessageVisibilityTimeoutAction;
import sleeper.job.common.action.DeleteMessageAction;
import sleeper.job.common.action.thread.PeriodicActionRunnable;
import sleeper.statestore.FileInfo;
import sleeper.statestore.StateStore;
import sleeper.statestore.StateStoreException;
import sleeper.table.util.StateStoreProvider;
import software.amazon.awssdk.services.s3.S3AsyncClient;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static sleeper.configuration.properties.SystemDefinedInstanceProperty.INGEST_JOB_QUEUE_URL;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.FILE_SYSTEM;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.INGEST_KEEP_ALIVE_PERIOD_IN_SECONDS;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.INGEST_PARTITION_REFRESH_PERIOD_IN_SECONDS;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.MAX_IN_MEMORY_BATCH_SIZE;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.MAX_RECORDS_TO_WRITE_LOCALLY;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.QUEUE_VISIBILITY_TIMEOUT_IN_SECONDS;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.S3A_INPUT_FADVISE;
import static sleeper.configuration.properties.table.TableProperty.ITERATOR_CLASS_NAME;
import static sleeper.configuration.properties.table.TableProperty.ITERATOR_CONFIG;

/**
 * An IngestJobQueueConsumer pulls ingest jobs off an SQS queue and runs them.
 */
public class IngestJobQueueConsumer {
    private static final Logger LOGGER = LoggerFactory.getLogger(IngestJobQueueConsumer.class);

    private final ObjectFactory objectFactory;
    private final AmazonSQS sqsClient;
    private final AmazonCloudWatch cloudWatchClient;
    private final InstanceProperties instanceProperties;
    private final TablePropertiesProvider tablePropertiesProvider;
    private final StateStoreProvider stateStoreProvider;
    private final String sqsJobQueueUrl;
    private final int keepAlivePeriod;
    private final int visibilityTimeoutInSeconds;
    private final String localDir;
    private final long maxLinesInLocalFile;
    private final long maxInMemoryBatchSize;
    private final String fs;
    private final int ingestPartitionRefreshFrequencyInSeconds;
    private final IngestJobSerDe ingestJobSerDe;
    private final String fadvise;
    private final S3AsyncClient s3AsyncClient;
    private final Configuration hadoopConfiguration;

    public IngestJobQueueConsumer(ObjectFactory objectFactory,
                                  AmazonSQS sqsClient,
                                  AmazonCloudWatch cloudWatchClient,
                                  InstanceProperties instanceProperties,
                                  TablePropertiesProvider tablePropertiesProvider,
                                  StateStoreProvider stateStoreProvider,
                                  String localDir) {
        this(objectFactory,
                sqsClient,
                cloudWatchClient,
                instanceProperties,
                tablePropertiesProvider,
                stateStoreProvider,
                localDir,
                S3AsyncClient.create(),
                defaultHadoopConfiguration(instanceProperties.get(S3A_INPUT_FADVISE)));
    }

    public IngestJobQueueConsumer(ObjectFactory objectFactory,
                                  AmazonSQS sqsClient,
                                  AmazonCloudWatch cloudWatchClient,
                                  InstanceProperties instanceProperties,
                                  TablePropertiesProvider tablePropertiesProvider,
                                  StateStoreProvider stateStoreProvider,
                                  String localDir,
                                  S3AsyncClient s3AsyncClient,
                                  Configuration hadoopConfiguration) {
        this.objectFactory = objectFactory;
        this.sqsClient = sqsClient;
        this.cloudWatchClient = cloudWatchClient;
        this.instanceProperties = instanceProperties;
        this.tablePropertiesProvider = tablePropertiesProvider;
        this.stateStoreProvider = stateStoreProvider;
        this.sqsJobQueueUrl = instanceProperties.get(INGEST_JOB_QUEUE_URL);
        this.keepAlivePeriod = instanceProperties.getInt(INGEST_KEEP_ALIVE_PERIOD_IN_SECONDS);
        this.visibilityTimeoutInSeconds = instanceProperties.getInt(QUEUE_VISIBILITY_TIMEOUT_IN_SECONDS);
        this.localDir = localDir;
        this.maxLinesInLocalFile = instanceProperties.getLong(MAX_RECORDS_TO_WRITE_LOCALLY);
        this.maxInMemoryBatchSize = instanceProperties.getLong(MAX_IN_MEMORY_BATCH_SIZE);
        this.fs = instanceProperties.get(FILE_SYSTEM);
        this.fadvise = instanceProperties.get(S3A_INPUT_FADVISE);
        this.ingestPartitionRefreshFrequencyInSeconds = instanceProperties.getInt(INGEST_PARTITION_REFRESH_PERIOD_IN_SECONDS);
        this.ingestJobSerDe = new IngestJobSerDe();
        this.s3AsyncClient = s3AsyncClient;
        this.hadoopConfiguration = hadoopConfiguration;
    }

    private static Configuration defaultHadoopConfiguration(String fadvise) {
        Configuration conf = new Configuration();
        conf.set("fs.s3a.connection.maximum", "10");
        conf.set("fs.s3a.aws.credentials.provider", "com.amazonaws.auth.EC2ContainerCredentialsProviderWrapper");
        conf.set("fs.s3a.experimental.input.fadvise", fadvise);
        return conf;
    }

    public void run() throws InterruptedException, IOException, StateStoreException, IteratorException {
        while (true) {
            ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(sqsJobQueueUrl)
                    .withMaxNumberOfMessages(1)
                    .withWaitTimeSeconds(20); // Must be >= 0 and <= 20
            ReceiveMessageResult receiveMessageResult = sqsClient.receiveMessage(receiveMessageRequest);
            List<Message> messages = receiveMessageResult.getMessages();
            if (messages.isEmpty()) {
                LOGGER.info("Finishing as no jobs have been received");
                return;
            }
            LOGGER.info("Received message {}", messages.get(0).getBody());
            IngestJob ingestJob = ingestJobSerDe.fromJson(messages.get(0).getBody());
            LOGGER.info("Deserialised message to ingest job {}", ingestJob);

            TableProperties tableProperties = tablePropertiesProvider.getTableProperties(ingestJob.getTableName());
            Schema schema = tableProperties.getSchema();

            long recordsWritten = ingest(ingestJob, schema, tableProperties, messages.get(0).getReceiptHandle());
            LOGGER.info("{} records were written", recordsWritten);
        }
    }

    public long ingest(IngestJob job, Schema schema, TableProperties tableProperties, String receiptHandle) throws InterruptedException, IteratorException, StateStoreException, IOException {
        // Create list of all files to be read
        List<Path> paths = IngestJobUtils.getPaths(job.getFiles(), hadoopConfiguration, fs);
        LOGGER.info("There are {} files to ingest", paths.size());
        LOGGER.debug("Files to ingest are: {}", paths);
        LOGGER.info("Max number of records to read into memory is {}", maxInMemoryBatchSize);
        LOGGER.info("Max number of records to write to local disk is {}", maxLinesInLocalFile);

        // Create supplier of iterator of records from each file (using a supplier avoids having multiple files open
        // at the same time)
        List<Supplier<CloseableIterator<Record>>> inputIterators = new ArrayList<>();
        for (Path path : paths) {
            String pathString = path.toString();
            if (pathString.endsWith(".parquet")) {
                inputIterators.add(() -> {
                    try {
                        ParquetReader<Record> reader = new ParquetRecordReader.Builder(path, schema).withConf(hadoopConfiguration).build();
                        return new ParquetReaderIterator(reader);
                    } catch (IOException e) {
                        throw new RuntimeException("Ingest job: " + job.getId() + " IOException creating reader for file "
                                + path + ": " + e.getMessage());
                    }
                });
            } else {
                LOGGER.error("A file with a currently unsupported format has been found on ingest, file path: {}"
                        + ". This file will be ignored and will not be ingested.", pathString);
            }
        }

        // Concatenate iterators into one iterator
        CloseableIterator<Record> concatenatingIterator = new ConcatenatingIterator(inputIterators);

        // Get StateStore for this table
        StateStore stateStore = stateStoreProvider.getStateStore(tableProperties);

        // Create background thread to keep messages alive
        ChangeMessageVisibilityTimeoutAction changeMessageVisibilityAction = new ChangeMessageVisibilityTimeoutAction(sqsClient, sqsJobQueueUrl, "Ingest job " + job.getId(), receiptHandle, visibilityTimeoutInSeconds);
        PeriodicActionRunnable changeTimeoutRunnable = new PeriodicActionRunnable(changeMessageVisibilityAction, keepAlivePeriod);
        changeTimeoutRunnable.start();
        LOGGER.info("Ingest job {}: Created background thread to keep SQS messages alive (period is {} seconds)",
                job.getId(), keepAlivePeriod);

        // Run the ingest
        List<FileInfo> ingestedFileInfoList = IngestRecordsUsingPropertiesSpecifiedMethod.ingestFromRecordIterator(
                objectFactory,
                stateStore,
                instanceProperties,
                tableProperties,
                localDir,
                null,
                s3AsyncClient,
                hadoopConfiguration,
                tableProperties.get(ITERATOR_CLASS_NAME),
                tableProperties.get(ITERATOR_CONFIG),
                concatenatingIterator);
        long numRecordsWritten = ingestedFileInfoList.stream().mapToLong(FileInfo::getNumberOfRecords).sum();
        LOGGER.info("Ingest job {}: Stopping background thread to keep SQS messages alive",
                job.getId());
        changeTimeoutRunnable.stop();
        LOGGER.info("Ingest job {}: Wrote {} records from files {}", job.getId(), numRecordsWritten, paths);

        // Delete messages from SQS queue
        LOGGER.info("Ingest job {}: Deleting messages from queue", job.getId());
        DeleteMessageAction deleteAction = new DeleteMessageAction(sqsClient, sqsJobQueueUrl, job.getId(), receiptHandle);
        try {
            deleteAction.call();
        } catch (ActionException e) {
            LOGGER.error("Ingest job {}: ActionException deleting message with handle {}", job.getId(), receiptHandle);
        }

        // Update metrics
        String metricsNamespace = instanceProperties.get(UserDefinedInstanceProperty.METRICS_NAMESPACE);
        String instanceId = instanceProperties.get(UserDefinedInstanceProperty.ID);
        cloudWatchClient.putMetricData(new PutMetricDataRequest()
                .withNamespace(metricsNamespace)
                .withMetricData(new MetricDatum()
                        .withMetricName("StandardIngestRecordsWritten")
                        .withValue((double) numRecordsWritten)
                        .withUnit(StandardUnit.Count)
                        .withDimensions(
                                new Dimension().withName("instanceId").withValue(instanceId),
                                new Dimension().withName("tableName").withValue(job.getTableName())
                        )));

        return numRecordsWritten;
    }
}
