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
package sleeper.bulkimport.runner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.model.GetCallerIdentityRequest;
import software.amazon.awssdk.services.sts.model.GetCallerIdentityResponse;

import sleeper.bulkimport.core.job.BulkImportJob;
import sleeper.configurationv2.properties.S3InstanceProperties;
import sleeper.configurationv2.properties.S3TableProperties;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertiesProvider;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreProvider;
import sleeper.core.statestore.commit.StateStoreCommitRequest;
import sleeper.core.statestore.commit.StateStoreCommitRequestSender;
import sleeper.core.statestore.transactionlog.transaction.TransactionSerDeProvider;
import sleeper.core.statestore.transactionlog.transaction.impl.AddFilesTransaction;
import sleeper.core.table.TableStatus;
import sleeper.core.tracker.ingest.job.IngestJobTracker;
import sleeper.core.tracker.ingest.job.update.IngestJobFailedEvent;
import sleeper.core.tracker.ingest.job.update.IngestJobFinishedEvent;
import sleeper.core.tracker.ingest.job.update.IngestJobRunIds;
import sleeper.core.tracker.ingest.job.update.IngestJobStartedEvent;
import sleeper.core.tracker.job.run.JobRunSummary;
import sleeper.core.tracker.job.run.RecordsProcessed;
import sleeper.core.util.LoggedDuration;
import sleeper.ingest.trackerv2.job.IngestJobTrackerFactory;
import sleeper.statestorev2.StateStoreFactory;
import sleeper.statestorev2.commit.SqsFifoStateStoreCommitRequestSender;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFileAttributes;
import java.time.Instant;
import java.util.function.Supplier;

import static sleeper.core.properties.table.TableProperty.BULK_IMPORT_FILES_COMMIT_ASYNC;

/**
 * Executes a Spark job that reads input Parquet files and writes to a Sleeper table. This takes a
 * {@link BulkImportJobRunner} implementation, which takes rows from the input files and outputs a file for each Sleeper
 * partition. These will then be added to the {@link StateStore}.
 */
public class BulkImportJobDriver {
    private static final Logger LOGGER = LoggerFactory.getLogger(BulkImportJobDriver.class);

    private final SessionRunner sessionRunner;
    private final TablePropertiesProvider tablePropertiesProvider;
    private final StateStoreProvider stateStoreProvider;
    private final IngestJobTracker tracker;
    private final StateStoreCommitRequestSender asyncSender;
    private final Supplier<Instant> getTime;

    public BulkImportJobDriver(SessionRunner sessionRunner,
            TablePropertiesProvider tablePropertiesProvider,
            StateStoreProvider stateStoreProvider,
            IngestJobTracker tracker,
            StateStoreCommitRequestSender asyncSender,
            Supplier<Instant> getTime) {
        this.sessionRunner = sessionRunner;
        this.tablePropertiesProvider = tablePropertiesProvider;
        this.stateStoreProvider = stateStoreProvider;
        this.tracker = tracker;
        this.asyncSender = asyncSender;
        this.getTime = getTime;
    }

    public void run(BulkImportJob job, String jobRunId, String taskId) throws IOException {
        TableProperties tableProperties = tablePropertiesProvider.getByName(job.getTableName());
        TableStatus table = tableProperties.getStatus();
        Instant startTime = getTime.get();
        IngestJobRunIds runIds = IngestJobRunIds.builder().tableId(table.getTableUniqueId()).jobId(job.getId()).jobRunId(jobRunId).taskId(taskId).build();
        LOGGER.info("Received bulk import job at time {}, {}", startTime, runIds);
        LOGGER.info("Job is for table {}: {}", table, job);
        tracker.jobStarted(IngestJobStartedEvent.builder()
                .jobRunIds(runIds)
                .startTime(startTime)
                .fileCount(job.getFiles().size())
                .build());

        BulkImportJobOutput output;
        try {
            output = sessionRunner.run(job);
        } catch (RuntimeException e) {
            tracker.jobFailed(IngestJobFailedEvent.builder()
                    .jobRunIds(runIds)
                    .failureTime(getTime.get())
                    .failure(e)
                    .build());
            throw e;
        }

        Instant finishTime = getTime.get();
        boolean asyncCommit = tableProperties.getBoolean(BULK_IMPORT_FILES_COMMIT_ASYNC);
        try {
            if (asyncCommit) {
                asyncSender.send(StateStoreCommitRequest.create(table.getTableUniqueId(),
                        AddFilesTransaction.builder()
                                .jobRunIds(runIds)
                                .writtenTime(finishTime)
                                .fileReferences(output.fileReferences())
                                .build()));
                LOGGER.info("Submitted asynchronous request to state store committer to add {} files in table {}, {}", output.numFiles(), table, runIds);
            } else {
                AddFilesTransaction.fromReferences(output.fileReferences())
                        .synchronousCommit(stateStoreProvider.getStateStore(tableProperties));
                LOGGER.info("Added {} files to state store in table {}, {}", output.numFiles(), table, runIds);
            }
        } catch (RuntimeException e) {
            tracker.jobFailed(IngestJobFailedEvent.builder()
                    .jobRunIds(runIds)
                    .failureTime(finishTime)
                    .failure(e)
                    .build());
            throw new RuntimeException("Failed to add files to state store. Ensure this service account has write access. Files may need to "
                    + "be re-imported for clients to access data", e);
        }

        LoggedDuration duration = LoggedDuration.withFullOutput(startTime, finishTime);
        LOGGER.info("Finished bulk import job {} at time {}", job.getId(), finishTime);
        long numRecords = output.numRecords();
        double rate = numRecords / (double) duration.getSeconds();
        LOGGER.info("Bulk import job {} took {} (rate of {} per second)", job.getId(), duration, rate);

        tracker.jobFinished(IngestJobFinishedEvent.builder()
                .jobRunIds(runIds)
                .summary(new JobRunSummary(new RecordsProcessed(numRecords, numRecords), startTime, finishTime))
                .fileReferencesAddedByJob(output.fileReferences())
                .committedBySeparateFileUpdates(asyncCommit)
                .build());

        // Calling this manually stops it potentially timing out after 10 seconds.
        // Note that we stop the Spark context after we've applied the changes in Sleeper.
        output.stopSparkContext();
    }

    @FunctionalInterface
    public interface SessionRunner {
        BulkImportJobOutput run(BulkImportJob job) throws IOException;
    }

    public static void start(String[] args, BulkImportJobRunner runner) throws Exception {
        if (args.length != 5) {
            throw new IllegalArgumentException("Expected 5 arguments:" +
                    " <config bucket name> <bulk import job ID> <bulk import task ID> <bulk import job run ID> <bulk import mode>");
        }
        String configBucket = args[0];
        String jobId = args[1];
        String taskId = args[2];
        String jobRunId = args[3];
        String bulkImportMode = args[4];

        LOGGER.info("Starting bulk import job driver");
        LOGGER.info("Config bucket: {}", configBucket);
        LOGGER.info("Job ID: {}", jobId);
        LOGGER.info("Task ID: {}", taskId);
        LOGGER.info("Job run ID: {}", jobRunId);
        LOGGER.info("Bulk import mode: {}", bulkImportMode);

        try (S3Client s3Client = S3Client.create();
                DynamoDbClient dynamoClient = DynamoDbClient.create();
                SqsClient sqsClient = SqsClient.create()) {
            InstanceProperties instanceProperties;
            try {
                instanceProperties = S3InstanceProperties.loadFromBucket(s3Client, configBucket);
            } catch (Exception e) {
                // This is a good indicator if something is wrong with the permissions
                LOGGER.error("Failed to load instance properties", e);
                logPermissions();
                throw e;
            }

            BulkImportJob bulkImportJob = BulkImportJobLoaderFromS3.loadJob(instanceProperties, jobId, jobRunId, s3Client);

            TablePropertiesProvider tablePropertiesProvider = S3TableProperties.createProvider(instanceProperties, s3Client, dynamoClient);
            StateStoreProvider stateStoreProvider = StateStoreFactory.createProvider(instanceProperties, s3Client, dynamoClient);
            IngestJobTracker tracker = IngestJobTrackerFactory.getTracker(dynamoClient, instanceProperties);
            StateStoreCommitRequestSender commitSender = new SqsFifoStateStoreCommitRequestSender(
                    instanceProperties, sqsClient, s3Client, TransactionSerDeProvider.from(tablePropertiesProvider));
            BulkImportJobDriver driver = new BulkImportJobDriver(new BulkImportSparkSessionRunner(
                    runner, instanceProperties, tablePropertiesProvider, stateStoreProvider),
                    tablePropertiesProvider, stateStoreProvider, tracker, commitSender, Instant::now);
            driver.run(bulkImportJob, jobRunId, taskId);
        }
    }

    private static void logPermissions() throws IOException {
        LOGGER.info("Checking whether token is readable");
        String token = System.getenv("AWS_WEB_IDENTITY_TOKEN_FILE");
        java.nio.file.Path tokenPath = Paths.get(token);
        boolean readable = Files.isReadable(tokenPath);
        LOGGER.info("Token was{} readable", readable ? "" : " not");
        if (!readable) {
            PosixFileAttributes readAttributes = Files.readAttributes(tokenPath, PosixFileAttributes.class);
            LOGGER.info("Token Permissions: {}", readAttributes.permissions());
            LOGGER.info("Token owner: {}", readAttributes.owner());
        }

        // This could error if not logged in correctly
        try (StsClient sts = StsClient.create()) {
            GetCallerIdentityResponse callerIdentity = sts.getCallerIdentity(GetCallerIdentityRequest.builder().build());
            LOGGER.info("Logged in as: {}", callerIdentity.arn());
        }
    }
}
