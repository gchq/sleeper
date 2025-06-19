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
package sleeper.ingest.runner.task;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.cloudwatch.CloudWatchClient;
import software.amazon.awssdk.services.cloudwatch.model.Dimension;
import software.amazon.awssdk.services.cloudwatch.model.MetricDatum;
import software.amazon.awssdk.services.cloudwatch.model.PutMetricDataRequest;
import software.amazon.awssdk.services.cloudwatch.model.StandardUnit;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;

import sleeper.common.job.action.ActionException;
import sleeper.common.job.action.MessageReference;
import sleeper.common.job.action.thread.PeriodicActionRunnable;
import sleeper.configuration.utils.S3ExpandDirectories;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.table.TableIndex;
import sleeper.core.tracker.ingest.job.IngestJobTracker;
import sleeper.core.tracker.job.run.JobRunSummary;
import sleeper.ingest.core.IngestTask.MessageHandle;
import sleeper.ingest.core.IngestTask.MessageReceiver;
import sleeper.ingest.core.job.IngestJob;
import sleeper.ingest.core.job.IngestJobMessageHandler;

import java.util.List;
import java.util.Optional;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.INGEST_JOB_QUEUE_URL;
import static sleeper.core.properties.instance.CommonProperty.ID;
import static sleeper.core.properties.instance.IngestProperty.INGEST_JOB_QUEUE_WAIT_TIME;
import static sleeper.core.properties.instance.IngestProperty.INGEST_KEEP_ALIVE_PERIOD_IN_SECONDS;
import static sleeper.core.properties.instance.IngestProperty.INGEST_QUEUE_VISIBILITY_TIMEOUT_IN_SECONDS;
import static sleeper.core.properties.instance.MetricsProperty.METRICS_NAMESPACE;

public class IngestJobQueueConsumer implements MessageReceiver {
    public static final Logger LOGGER = LoggerFactory.getLogger(IngestJobQueueConsumer.class);
    private final SqsClient sqsClient;
    private final CloudWatchClient cloudWatchClient;
    private final InstanceProperties instanceProperties;
    private final String sqsJobQueueUrl;
    private final int keepAlivePeriod;
    private final int visibilityTimeoutInSeconds;
    private final IngestJobMessageHandler<IngestJob> ingestJobMessageHandler;

    public IngestJobQueueConsumer(SqsClient sqsClient,
            S3Client s3Client,
            CloudWatchClient cloudWatchClient,
            InstanceProperties instanceProperties,
            Configuration configuration,
            TableIndex tableIndex,
            IngestJobTracker ingestJobTracker) {
        this.sqsClient = sqsClient;
        this.cloudWatchClient = cloudWatchClient;
        this.instanceProperties = instanceProperties;
        this.sqsJobQueueUrl = instanceProperties.get(INGEST_JOB_QUEUE_URL);
        this.keepAlivePeriod = instanceProperties.getInt(INGEST_KEEP_ALIVE_PERIOD_IN_SECONDS);
        this.visibilityTimeoutInSeconds = instanceProperties.getInt(INGEST_QUEUE_VISIBILITY_TIMEOUT_IN_SECONDS);
        this.ingestJobMessageHandler = messageHandler(instanceProperties, configuration, tableIndex, ingestJobTracker, new S3ExpandDirectories(s3Client)).build();
    }

    public static IngestJobMessageHandler.Builder<IngestJob> messageHandler(
            InstanceProperties instanceProperties,
            Configuration configuration,
            TableIndex tableIndex,
            IngestJobTracker ingestJobTracker,
            S3ExpandDirectories s3PathUtils) {
        return IngestJobMessageHandler.forIngestJob()
                .tableIndex(tableIndex)
                .ingestJobTracker(ingestJobTracker)
                .expandDirectories(files -> s3PathUtils.streamFilenames(files).toList());
    }

    @Override
    public Optional<MessageHandle> receiveMessage() {
        while (true) {
            ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder()
                    .queueUrl(sqsJobQueueUrl)
                    .maxNumberOfMessages(1)
                    .waitTimeSeconds(instanceProperties.getInt(INGEST_JOB_QUEUE_WAIT_TIME))
                    .build();
            ReceiveMessageResponse receiveMessageResult = sqsClient.receiveMessage(receiveMessageRequest);
            List<Message> messages = receiveMessageResult.messages();
            if (messages.isEmpty()) {
                LOGGER.info("Finishing as no jobs have been received");
                break;
            }
            Message message = messages.get(0);
            LOGGER.info("Received message {}", message.body());

            Optional<IngestJob> ingestJobOpt = ingestJobMessageHandler.deserialiseAndValidate(message.body());
            if (ingestJobOpt.isPresent()) {
                IngestJob ingestJob = ingestJobOpt.get();
                MessageReference messageReference = new MessageReference(sqsClient, sqsJobQueueUrl,
                        "Ingest job " + ingestJob.getId(), message.receiptHandle());
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

        public void completed(JobRunSummary summary) {
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
            cloudWatchClient.putMetricData(PutMetricDataRequest.builder()
                    .namespace(metricsNamespace)
                    .metricData(MetricDatum.builder()
                            .metricName("StandardIngestRecordsWritten")
                            .value((double) summary.getRecordsWritten())
                            .unit(StandardUnit.COUNT)
                            .dimensions(
                                    Dimension.builder().name("instanceId").value(instanceId).build(),
                                    Dimension.builder().name("tableName").value(job.getTableName()).build())
                            .build())
                    .build());
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
