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
package sleeper.ingest.runner.task;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.record.process.RecordsProcessedSummary;
import sleeper.core.table.TableIndex;
import sleeper.ingest.core.job.IngestJob;
import sleeper.ingest.core.job.IngestJobMessageHandler;
import sleeper.ingest.core.job.status.IngestJobStatusStore;
import sleeper.ingest.core.task.IngestTask.MessageHandle;
import sleeper.ingest.core.task.IngestTask.MessageReceiver;
import sleeper.job.common.action.ActionException;
import sleeper.job.common.action.MessageReference;
import sleeper.job.common.action.thread.PeriodicActionRunnable;
import sleeper.parquet.utils.HadoopPathUtils;

import java.util.List;
import java.util.Optional;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.INGEST_JOB_QUEUE_URL;
import static sleeper.core.properties.instance.CommonProperty.ID;
import static sleeper.core.properties.instance.CommonProperty.QUEUE_VISIBILITY_TIMEOUT_IN_SECONDS;
import static sleeper.core.properties.instance.IngestProperty.INGEST_JOB_QUEUE_WAIT_TIME;
import static sleeper.core.properties.instance.IngestProperty.INGEST_KEEP_ALIVE_PERIOD_IN_SECONDS;
import static sleeper.core.properties.instance.MetricsProperty.METRICS_NAMESPACE;

public class IngestJobQueueConsumer implements MessageReceiver {
    public static final Logger LOGGER = LoggerFactory.getLogger(IngestJobQueueConsumer.class);
    private final AmazonSQS sqsClient;
    private final AmazonCloudWatch cloudWatchClient;
    private final InstanceProperties instanceProperties;
    private final String sqsJobQueueUrl;
    private final int keepAlivePeriod;
    private final int visibilityTimeoutInSeconds;
    private final IngestJobMessageHandler<IngestJob> ingestJobMessageHandler;

    public IngestJobQueueConsumer(AmazonSQS sqsClient,
            AmazonCloudWatch cloudWatchClient,
            InstanceProperties instanceProperties,
            Configuration configuration,
            TableIndex tableIndex,
            IngestJobStatusStore ingestJobStatusStore) {
        this.sqsClient = sqsClient;
        this.cloudWatchClient = cloudWatchClient;
        this.instanceProperties = instanceProperties;
        this.sqsJobQueueUrl = instanceProperties.get(INGEST_JOB_QUEUE_URL);
        this.keepAlivePeriod = instanceProperties.getInt(INGEST_KEEP_ALIVE_PERIOD_IN_SECONDS);
        this.visibilityTimeoutInSeconds = instanceProperties.getInt(QUEUE_VISIBILITY_TIMEOUT_IN_SECONDS);
        this.ingestJobMessageHandler = messageHandler(instanceProperties, configuration, tableIndex, ingestJobStatusStore).build();
    }

    public static IngestJobMessageHandler.Builder<IngestJob> messageHandler(
            InstanceProperties instanceProperties,
            Configuration configuration,
            TableIndex tableIndex,
            IngestJobStatusStore ingestJobStatusStore) {
        return IngestJobMessageHandler.forIngestJob()
                .tableIndex(tableIndex)
                .ingestJobStatusStore(ingestJobStatusStore)
                .expandDirectories(files -> HadoopPathUtils.expandDirectories(files, configuration, instanceProperties));
    }

    @Override
    public Optional<MessageHandle> receiveMessage() {
        while (true) {
            ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(sqsJobQueueUrl)
                    .withMaxNumberOfMessages(1)
                    .withWaitTimeSeconds(instanceProperties.getInt(INGEST_JOB_QUEUE_WAIT_TIME));
            ReceiveMessageResult receiveMessageResult = sqsClient.receiveMessage(receiveMessageRequest);
            List<Message> messages = receiveMessageResult.getMessages();
            if (messages.isEmpty()) {
                LOGGER.info("Finishing as no jobs have been received");
                break;
            }
            Message message = messages.get(0);
            LOGGER.info("Received message {}", message.getBody());

            Optional<IngestJob> ingestJobOpt = ingestJobMessageHandler.deserialiseAndValidate(message.getBody());
            if (ingestJobOpt.isPresent()) {
                IngestJob ingestJob = ingestJobOpt.get();
                MessageReference messageReference = new MessageReference(sqsClient, sqsJobQueueUrl,
                        "Ingest job " + ingestJob.getId(), message.getReceiptHandle());
                // Create background thread to keep messages alive
                PeriodicActionRunnable keepAliveRunnable = new PeriodicActionRunnable(
                        messageReference.changeVisibilityTimeoutAction(visibilityTimeoutInSeconds),
                        keepAlivePeriod);
                keepAliveRunnable.start();
                LOGGER.info("Ingest job {}: Created background thread to keep SQS messages alive (period is {} seconds)",
                        ingestJob.getId(), keepAlivePeriod);
                return Optional.of(new SqsMessageHandle(ingestJob, messageReference, keepAliveRunnable));
            } else {
                LOGGER.info("Could not deserialise ingest job {}, skipping", ingestJobOpt);
            }
        }
        return Optional.empty();
    }

    class SqsMessageHandle implements MessageHandle {
        private final IngestJob job;
        private final MessageReference message;
        private final PeriodicActionRunnable keepAliveRunnable;

        SqsMessageHandle(IngestJob job, MessageReference message, PeriodicActionRunnable keepAliveRunnable) {
            this.job = job;
            this.message = message;
            this.keepAliveRunnable = keepAliveRunnable;
        }

        public IngestJob getJob() {
            return job;
        }

        public void completed(RecordsProcessedSummary summary) {
            // Delete message from queue
            LOGGER.info("Ingest job {}: Deleting message from queue", job.getId());
            try {
                message.deleteAction().call();
            } catch (ActionException e) {
                throw new RuntimeException(e);
            }
            // Update metrics
            String metricsNamespace = instanceProperties.get(METRICS_NAMESPACE);
            String instanceId = instanceProperties.get(ID);
            cloudWatchClient.putMetricData(new PutMetricDataRequest()
                    .withNamespace(metricsNamespace)
                    .withMetricData(new MetricDatum()
                            .withMetricName("StandardIngestRecordsWritten")
                            .withValue((double) summary.getRecordsWritten())
                            .withUnit(StandardUnit.Count)
                            .withDimensions(
                                    new Dimension().withName("instanceId").withValue(instanceId),
                                    new Dimension().withName("tableName").withValue(job.getTableName()))));
        }

        public void failed() {
        }

        public void close() {
            LOGGER.info("Ingest job {}: Stopping background thread to keep SQS messages alive", job.getId());
            if (keepAliveRunnable != null) {
                keepAliveRunnable.stop();
            }
        }
    }
}
