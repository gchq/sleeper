/*
 * Copyright 2022-2023 Crown Copyright
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
package sleeper.systemtest.ingest.batcher;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.sqs.AmazonSQS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.lambda.LambdaClient;

import sleeper.clients.util.PollWithRetries;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.configuration.properties.validation.BatchIngestMode;
import sleeper.ingest.batcher.FileIngestRequest;
import sleeper.ingest.batcher.IngestBatcherStore;
import sleeper.ingest.batcher.store.DynamoDBIngestBatcherStore;
import sleeper.ingest.job.status.IngestJobStatus;
import sleeper.ingest.job.status.IngestJobStatusStore;
import sleeper.ingest.status.store.job.DynamoDBIngestJobStatusStore;
import sleeper.job.common.QueueMessageCount;
import sleeper.systemtest.util.InvokeSystemTestLambda;
import sleeper.systemtest.util.WaitForQueueEstimate;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static sleeper.configuration.properties.SystemDefinedInstanceProperty.INGEST_JOB_QUEUE_URL;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.INGEST_LAMBDA_FUNCTION;
import static sleeper.configuration.properties.table.TableProperty.INGEST_BATCHER_INGEST_MODE;
import static sleeper.configuration.properties.table.TableProperty.INGEST_BATCHER_MAX_JOB_FILES;

public class InvokeIngestBatcher {

    private static final Logger LOGGER = LoggerFactory.getLogger(InvokeIngestBatcher.class);

    private final InstanceProperties instanceProperties;
    private final Consumer<Consumer<TableProperties>> updateTableProperties;
    private final SendFilesToIngestBatcher sendFiles;
    private final IngestBatcherStore batcherStore;
    private final IngestJobStatusStore ingestStatusStore;
    private final InvokeSystemTestLambda.Client lambdaClient;
    private final QueueMessageCount.Client queueClient;

    public InvokeIngestBatcher(
            InstanceProperties instanceProperties, TableProperties tableProperties, String sourceBucketName,
            AmazonS3 s3, AmazonDynamoDB dynamoDB, AmazonSQS sqs, LambdaClient lambda) {
        this(instanceProperties,
                tablePropertiesUpdater(tableProperties, s3),
                new SendFilesToIngestBatcher(instanceProperties, tableProperties, sourceBucketName, s3, sqs, lambda),
                new DynamoDBIngestBatcherStore(dynamoDB, instanceProperties,
                        new TablePropertiesProvider(s3, instanceProperties)),
                new DynamoDBIngestJobStatusStore(dynamoDB, instanceProperties),
                InvokeSystemTestLambda.client(instanceProperties),
                QueueMessageCount.withSqsClient(sqs));
    }

    public InvokeIngestBatcher(InstanceProperties instanceProperties,
                               Consumer<Consumer<TableProperties>> updateTableProperties,
                               SendFilesToIngestBatcher sendFiles, IngestBatcherStore batcherStore,
                               IngestJobStatusStore ingestStatusStore,
                               InvokeSystemTestLambda.Client lambdaClient,
                               QueueMessageCount.Client queueClient) {
        this.instanceProperties = instanceProperties;
        this.updateTableProperties = updateTableProperties;
        this.sendFiles = sendFiles;
        this.batcherStore = batcherStore;
        this.ingestStatusStore = ingestStatusStore;
        this.lambdaClient = lambdaClient;
        this.queueClient = queueClient;
    }

    public static Consumer<Consumer<TableProperties>> tablePropertiesUpdater(
            TableProperties tableProperties, AmazonS3 s3) {
        return update -> {
            update.accept(tableProperties);
            try {
                tableProperties.saveToS3(s3);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        };
    }

    public void runStandardIngest() throws InterruptedException, IOException {
        LOGGER.info("Testing ingest batcher mode: {}", BatchIngestMode.STANDARD_INGEST);
        updateTableProperties.accept(tableProperties ->
                tableProperties.set(INGEST_BATCHER_INGEST_MODE, BatchIngestMode.STANDARD_INGEST.toString()));

        List<String> jobIds = sendFilesGetJobIds(List.of("file-1.parquet", "file-2.parquet", "file-3.parquet", "file-4.parquet"));

        WaitForQueueEstimate waitForNotEmpty = WaitForQueueEstimate.notEmpty(queueClient,
                instanceProperties, INGEST_JOB_QUEUE_URL, PollWithRetries.intervalAndMaxPolls(10000, 10));
        waitForNotEmpty.pollUntilFinished();

        lambdaClient.invokeLambda(INGEST_LAMBDA_FUNCTION);

        waitForJobsToFinish(PollWithRetries.intervalAndMaxPolls(10000, 10), jobIds);
    }

    public void runBulkImportEMR() throws IOException, InterruptedException {
        LOGGER.info("Testing ingest batcher mode: {}", BatchIngestMode.BULK_IMPORT_EMR);
        updateTableProperties.accept(tableProperties -> {
            tableProperties.set(INGEST_BATCHER_INGEST_MODE, BatchIngestMode.BULK_IMPORT_EMR.toString());
            tableProperties.set(INGEST_BATCHER_MAX_JOB_FILES, "10");
        });

        List<String> jobIds = sendFilesGetJobIds(List.of("file-5.parquet", "file-6.parquet", "file-7.parquet", "file-8.parquet"));

        waitForJobsToFinish(PollWithRetries.intervalAndPollingTimeout(
                30000, 1000L * 60L * 10L), jobIds);
    }

    private List<String> sendFilesGetJobIds(List<String> fileNames) throws IOException, InterruptedException {

        List<String> jobIdsBefore = getBatcherJobIds();

        sendFiles.writeFilesAndSendToBatcher(fileNames);

        List<String> jobIdsAfter = getBatcherJobIds();
        List<String> newJobIds = new ArrayList<>(jobIdsAfter);
        newJobIds.removeAll(jobIdsBefore);
        LOGGER.info("Job IDs for files: {}", newJobIds);
        return newJobIds;
    }

    private List<String> getBatcherJobIds() {
        List<FileIngestRequest> allFilesNewestFirst = batcherStore.getAllFilesNewestFirst();
        LOGGER.info("Batcher store contents: {}", allFilesNewestFirst);
        return allFilesNewestFirst.stream()
                .map(FileIngestRequest::getJobId)
                .filter(Objects::nonNull)
                .distinct()
                .collect(Collectors.toList());
    }

    private void waitForJobsToFinish(PollWithRetries poll, List<String> jobIds) throws InterruptedException {
        poll.pollUntil("ingest jobs are finished", () -> {
            List<String> unfinishedJobIds = jobIds.stream().filter(jobId ->
                            ingestStatusStore.getJob(jobId)
                                    .filter(IngestJobStatus::isFinished).isEmpty())
                    .collect(Collectors.toList());
            LOGGER.info("Waiting for {} jobs to finish", unfinishedJobIds.size());
            return unfinishedJobIds.isEmpty();
        });
    }

}
