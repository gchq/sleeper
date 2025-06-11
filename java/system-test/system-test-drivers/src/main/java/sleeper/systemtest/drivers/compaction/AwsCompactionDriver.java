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

package sleeper.systemtest.drivers.compaction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.autoscaling.AutoScalingClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequestEntry;

import sleeper.common.task.EC2Scaler;
import sleeper.compaction.core.job.CompactionJob;
import sleeper.compaction.core.job.CompactionJobSerDe;
import sleeper.compaction.core.job.commit.CompactionCommitMessage;
import sleeper.compaction.core.job.commit.CompactionCommitMessageSerDe;
import sleeper.compaction.core.job.creation.CreateCompactionJobs;
import sleeper.compaction.core.job.dispatch.CompactionJobDispatchRequest;
import sleeper.compaction.core.job.dispatch.CompactionJobDispatchRequestSerDe;
import sleeper.compaction.job.creation.AwsCreateCompactionJobs;
import sleeper.compaction.job.creation.CompactionBatchJobsWriterToS3;
import sleeper.compaction.job.creation.CompactionBatchMessageSenderToSqs;
import sleeper.compaction.tracker.job.CompactionJobTrackerFactory;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.statestore.StateStoreProvider;
import sleeper.core.tracker.compaction.job.CompactionJobTracker;
import sleeper.core.util.ObjectFactory;
import sleeper.core.util.ObjectFactoryException;
import sleeper.core.util.SplitIntoBatches;
import sleeper.invoke.tablesv2.InvokeForTables;
import sleeper.systemtest.drivers.util.SystemTestClients;
import sleeper.systemtest.drivers.util.sqs.AwsDrainSqsQueue;
import sleeper.systemtest.dsl.compaction.CompactionDriver;
import sleeper.systemtest.dsl.instance.SystemTestInstanceContext;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.COMPACTION_COMMIT_QUEUE_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.COMPACTION_JOB_CREATION_QUEUE_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.COMPACTION_JOB_QUEUE_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.COMPACTION_PENDING_DLQ_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;

public class AwsCompactionDriver implements CompactionDriver {
    private static final Logger LOGGER = LoggerFactory.getLogger(AwsCompactionDriver.class);
    private static final ExecutorService EXECUTOR = createThreadPool();

    private static ExecutorService createThreadPool() {
        ThreadPoolExecutor pool = new ThreadPoolExecutor(
                40, 40,
                60L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>());
        pool.allowCoreThreadTimeOut(true);
        return pool;
    }

    private final SystemTestInstanceContext instance;
    private final DynamoDbClient dynamoClient;
    private final S3Client s3Client;
    private final SqsClient sqsClient;
    private final AutoScalingClient asClient;
    private final Ec2Client ec2Client;
    private final AwsDrainSqsQueue drainQueue;
    private final CompactionJobSerDe serDe = new CompactionJobSerDe();

    public AwsCompactionDriver(SystemTestInstanceContext instance, SystemTestClients clients) {
        this(instance, clients, AwsDrainSqsQueue.forSystemTests(clients.getSqs()));
    }

    public AwsCompactionDriver(SystemTestInstanceContext instance, SystemTestClients clients, AwsDrainSqsQueue drainQueue) {
        this.instance = instance;
        this.dynamoClient = clients.getDynamo();
        this.s3Client = clients.getS3();
        this.sqsClient = clients.getSqs();
        this.asClient = clients.getAutoScaling();
        this.ec2Client = clients.getEc2();
        this.drainQueue = drainQueue;
    }

    @Override
    public CompactionJobTracker getJobTracker() {
        return CompactionJobTrackerFactory
                .getTrackerWithStronglyConsistentReads(dynamoClient, instance.getInstanceProperties());
    }

    @Override
    public void triggerCreateJobs() {
        String queueUrl = instance.getInstanceProperties().get(COMPACTION_JOB_CREATION_QUEUE_URL);
        InvokeForTables.sendOneMessagePerTable(sqsClient, queueUrl, instance.streamTableProperties().map(TableProperties::getStatus));
    }

    @Override
    public void forceCreateJobs() {
        instance.streamTableProperties().parallel().forEach(table -> {
            try {
                CreateCompactionJobs createJobs = AwsCreateCompactionJobs.from(
                        ObjectFactory.noUserJars(), instance.getInstanceProperties(),
                        instance.getTablePropertiesProvider(),
                        new StateStoreProvider(instance.getInstanceProperties(), instance::getStateStore),
                        s3Client, sqsClient);
                createJobs.createJobWithForceAllFiles(table);
            } catch (IOException | ObjectFactoryException e) {
                throw new RuntimeException("Failed creating compaction jobs for table " + table.getStatus(), e);
            }
        });
    }

    @Override
    public void scaleToZero() {
        EC2Scaler.create(instance.getInstanceProperties(), asClient, ec2Client).scaleTo(0);
    }

    @Override
    public List<CompactionJob> drainJobsQueueForWholeInstance(int expectedJobs) {
        String queueUrl = instance.getInstanceProperties().get(COMPACTION_JOB_QUEUE_URL);
        LOGGER.info("Draining compaction jobs queue: {}", queueUrl);
        List<CompactionJob> jobs = drainQueue.drainExpectingMessagesWithRetriesWhenEmpty(expectedJobs, 5, queueUrl)
                .map(Message::body)
                .map(serDe::fromJson)
                .toList();
        LOGGER.info("Found {} jobs", jobs.size());
        return jobs;
    }

    @Override
    public void sendCompactionCommits(Stream<CompactionCommitMessage> commits) {
        SplitIntoBatches.streamBatchesOf(10000, commits).forEach(bigBatch -> {
            LOGGER.info("Processing batch of {} commits", bigBatch.size());
            SplitIntoBatches.streamBatchesOf(10, bigBatch)
                    .map(batch -> {
                        return EXECUTOR.submit(() -> {
                            CompactionCommitMessageSerDe serDe = new CompactionCommitMessageSerDe();
                            sqsClient.sendMessageBatch(builder -> builder
                                    .queueUrl(instance.getInstanceProperties().get(COMPACTION_COMMIT_QUEUE_URL))
                                    .entries(batch.stream()
                                            .map(commit -> SendMessageBatchRequestEntry.builder()
                                                    .id(commit.request().getJobId())
                                                    .messageBody(serDe.toJson(commit))
                                                    .build())
                                            .toList()));
                        });
                    })
                    .forEach(future -> {
                        try {
                            future.get();
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new RuntimeException(e);
                        } catch (ExecutionException e) {
                            throw new RuntimeException(e);
                        }
                    });
            LOGGER.info("Sent batch of {} commits", bigBatch.size());
        });
    }

    @Override
    public String sendCompactionBatchGetKey(List<CompactionJob> jobs) {
        CompactionJobDispatchRequest request = CompactionJobDispatchRequest.forTableWithBatchIdAtTime(
                instance.getTableProperties(), UUID.randomUUID().toString(), Instant.now());
        new CompactionBatchJobsWriterToS3(s3Client)
                .writeJobs(instance.getInstanceProperties().get(DATA_BUCKET), request.getBatchKey(), jobs);
        new CompactionBatchMessageSenderToSqs(instance.getInstanceProperties(), sqsClient)
                .sendMessage(request);
        return request.getBatchKey();
    }

    @Override
    public List<CompactionJobDispatchRequest> drainPendingDeadLetterQueueForWholeInstance() {
        CompactionJobDispatchRequestSerDe serDe = new CompactionJobDispatchRequestSerDe();
        return drainQueue.drain(instance.getInstanceProperties().get(COMPACTION_PENDING_DLQ_URL))
                .map(Message::body)
                .map(serDe::fromJson)
                .toList();
    }
}
