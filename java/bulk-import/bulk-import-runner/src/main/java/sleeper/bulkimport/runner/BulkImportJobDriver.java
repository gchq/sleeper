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
package sleeper.bulkimport.runner;

import org.apache.logging.log4j.LogManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.SdkSystemSetting;
import software.amazon.awssdk.core.util.VersionInfo;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.model.GetCallerIdentityRequest;
import software.amazon.awssdk.services.sts.model.GetCallerIdentityResponse;

import sleeper.bulkimport.core.job.BulkImportJob;
import sleeper.bulkimport.runner.sketches.GenerateSketchesDriver;
import sleeper.configuration.properties.S3InstanceProperties;
import sleeper.configuration.properties.S3TableProperties;
import sleeper.core.partition.Partition;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertiesProvider;
import sleeper.core.statestore.FileReference;
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
import sleeper.core.tracker.job.run.RowsProcessed;
import sleeper.core.util.LoggedDuration;
import sleeper.ingest.tracker.job.IngestJobTrackerFactory;
import sleeper.sketches.Sketches;
import sleeper.statestore.StateStoreFactory;
import sleeper.statestore.commit.SqsFifoStateStoreCommitRequestSender;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFileAttributes;
import java.security.CodeSource;
import java.security.ProtectionDomain;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Supplier;

import static sleeper.core.properties.table.TableProperty.BULK_IMPORT_FILES_COMMIT_ASYNC;

/**
 * Executes a Spark job that reads input Parquet files and writes to a Sleeper table. This takes a
 * {@link BulkImportJobRunner} implementation, which takes rows from the input files and outputs a file for each Sleeper
 * partition. These will then be added to the {@link StateStore}.
 *
 * @param <C> the type of the Spark context
 */
public class BulkImportJobDriver<C extends BulkImportContext<C>> {
    private static final Logger LOGGER = LoggerFactory.getLogger(BulkImportJobDriver.class);
    private static final org.apache.logging.log4j.Logger LOG4J2 = LogManager.getLogger(BulkImportJobDriver.class);

    private final ContextCreator<C> contextCreator;
    private final PartitionPreSplitter<C> preSplitter;
    private final BulkImporter<C> bulkImporter;
    private final TablePropertiesProvider tablePropertiesProvider;
    private final StateStoreProvider stateStoreProvider;
    private final IngestJobTracker tracker;
    private final StateStoreCommitRequestSender asyncSender;
    private final Supplier<Instant> getTime;

    public BulkImportJobDriver(
            ContextCreator<C> contextCreator,
            DataSketcher<C> dataSketcher,
            BulkImporter<C> bulkImporter,
            TablePropertiesProvider tablePropertiesProvider,
            StateStoreProvider stateStoreProvider,
            IngestJobTracker tracker,
            StateStoreCommitRequestSender asyncSender,
            Supplier<Instant> getTime,
            Supplier<String> partitionIdSupplier) {
        this.contextCreator = contextCreator;
        this.preSplitter = new PartitionPreSplitter<>(dataSketcher, stateStoreProvider, partitionIdSupplier);
        this.bulkImporter = bulkImporter;
        this.tablePropertiesProvider = tablePropertiesProvider;
        this.stateStoreProvider = stateStoreProvider;
        this.tracker = tracker;
        this.asyncSender = asyncSender;
        this.getTime = getTime;
    }

    public void run(BulkImportJob job, String jobRunId, String taskId) throws IOException {
        run(job, jobRunId, taskId, () -> {});
    }

    public void run(BulkImportJob job, String jobRunId, String taskId, Runnable onFailure) throws IOException {
        LOGGER.info("Loading table properties and schema for table {}", job.getTableName());
        TableProperties tableProperties = tablePropertiesProvider.getByName(job.getTableName());
        TableStatus table = tableProperties.getStatus();
        IngestJobRunIds runIds = IngestJobRunIds.builder().tableId(table.getTableUniqueId()).jobId(job.getId()).jobRunId(jobRunId).taskId(taskId).build();
        LOGGER.info("Received bulk import job: {}", runIds);
        LOGGER.info("Job is for table {}: {}", table, job);

        boolean failureAlreadyTracked = false;
        try {
            LOGGER.info("Loading partitions");
            List<Partition> allPartitions = stateStoreProvider.getStateStore(tableProperties).getAllPartitions();

            // Closing the Spark context in a try-with-resources stops it potentially timing out after 10 seconds.
            // Note that we stop the Spark context after we've applied the changes in Sleeper.
            try (C context = contextCreator.createContext(tableProperties, allPartitions, job)) {

                C contextAfterSplit = preSplitter.preSplitPartitionsIfNecessary(tableProperties, allPartitions, context);

                Instant startTime = getTime.get();
                tracker.jobStarted(IngestJobStartedEvent.builder()
                        .jobRunIds(runIds)
                        .startTime(startTime)
                        .fileCount(job.getFiles().size())
                        .build());

                LOGGER.info("Running bulk import job with id {}", job.getId());
                List<FileReference> fileReferences;
                try {
                    fileReferences = bulkImporter.createFileReferences(contextAfterSplit);
                } catch (Exception e) {
                    LOG4J2.error("DIAGNOSTIC: inner catch reached for job {}, exception type: {}, message: {}",
                            runIds.getJobId(), e.getClass().getName(), e.getMessage(), e);
                    // Mark as failed and forcibly halt the JVM before context.close() calls sparkContext.stop().
                    // Spark's internal threads may concurrently call System.exit(0) when a job fails;
                    // Runtime.halt(1) overrides any in-progress System.exit(0) and terminates immediately.
                    failureAlreadyTracked = true;
                    markJobAsFailed(runIds, e);
                    LOG4J2.error("DIAGNOSTIC: job marked as failed, about to call onFailure (halt(1))");
                    onFailure.run();
                    LOG4J2.error("DIAGNOSTIC: onFailure returned without halting - this should never appear");
                    throw e;
                }

                LOG4J2.info("DIAGNOSTIC: createFileReferences completed successfully, committing job");
                commitSuccessfulJob(tableProperties, runIds, startTime, fileReferences);
            }
        } catch (RuntimeException | IOException e) {
            LOG4J2.error("DIAGNOSTIC: outer catch reached for job {}, failureAlreadyTracked={}, exception type: {}, message: {}",
                    runIds.getJobId(), failureAlreadyTracked, e.getClass().getName(), e.getMessage(), e);
            // Handles failures from createContext, preSplit, commitSuccessfulJob
            if (!failureAlreadyTracked) {
                markJobAsFailed(runIds, e);
                onFailure.run();
            }
            throw e;
        }
    }

    private void markJobAsFailed(IngestJobRunIds runIds, Exception e) {
        tracker.jobFailed(IngestJobFailedEvent.builder()
                .jobRunIds(runIds)
                .failureTime(getTime.get())
                .failure(e)
                .build());
    }

    private void commitSuccessfulJob(TableProperties tableProperties, IngestJobRunIds runIds, Instant startTime, List<FileReference> fileReferences) {

        Instant finishTime = getTime.get();
        boolean asyncCommit = tableProperties.getBoolean(BULK_IMPORT_FILES_COMMIT_ASYNC);
        TableStatus table = tableProperties.getStatus();
        try {
            if (asyncCommit) {
                asyncSender.send(StateStoreCommitRequest.create(table.getTableUniqueId(),
                        AddFilesTransaction.builder()
                                .jobRunIds(runIds)
                                .writtenTime(finishTime)
                                .fileReferences(fileReferences)
                                .build()));
                LOGGER.info("Submitted asynchronous request to state store committer to add {} files in table {}, {}", fileReferences.size(), table, runIds);
            } else {
                AddFilesTransaction.fromReferences(fileReferences)
                        .synchronousCommit(stateStoreProvider.getStateStore(tableProperties));
                LOGGER.info("Added {} files to state store in table {}, {}", fileReferences.size(), table, runIds);
            }
        } catch (RuntimeException e) {
            throw new RuntimeException("Failed to add files to state store. " +
                    "Ensure this service account has write access. " +
                    "Files may need to be re-imported for clients to access data.", e);
        }

        trackJobFinished(runIds, startTime, finishTime, fileReferences, asyncCommit);
    }

    private void trackJobFinished(IngestJobRunIds runIds, Instant startTime, Instant finishTime, List<FileReference> fileReferences, boolean asyncCommit) {
        LoggedDuration duration = LoggedDuration.withFullOutput(startTime, finishTime);
        LOGGER.info("Finished bulk import job {} at time {}", runIds.getJobId(), finishTime);
        long numRows = fileReferences.stream()
                .mapToLong(FileReference::getNumberOfRows)
                .sum();
        double rate = numRows / (double) duration.getSeconds();
        LOGGER.info("Bulk import job {} took {} (rate of {} per second)", runIds.getJobId(), duration, rate);

        tracker.jobFinished(IngestJobFinishedEvent.builder()
                .jobRunIds(runIds)
                .summary(new JobRunSummary(new RowsProcessed(numRows, numRows), startTime, finishTime))
                .fileReferencesAddedByJob(fileReferences)
                .committedBySeparateFileUpdates(asyncCommit)
                .build());
    }

    @FunctionalInterface
    public interface ContextCreator<C extends BulkImportContext<C>> {
        C createContext(TableProperties tableProperties, List<Partition> allPartitions, BulkImportJob job);
    }

    @FunctionalInterface
    public interface DataSketcher<C extends BulkImportContext<C>> {
        Map<String, Sketches> generatePartitionIdToSketches(C context);
    }

    @FunctionalInterface
    public interface BulkImporter<C extends BulkImportContext<C>> {
        List<FileReference> createFileReferences(C context) throws IOException;
    }

    public static void start(String[] args, BulkImportJobRunner runner) {
        try {
            startOrThrow(args, runner);
            LOG4J2.info("DIAGNOSTIC: startOrThrow completed successfully");
        } catch (Exception e) {
            LOG4J2.error("DIAGNOSTIC: exception reached start() catch, type: {}, message: {}",
                    e.getClass().getName(), e.getMessage(), e);
            LOGGER.error("Bulk import job driver failed, exiting with code 1", e);
            System.exit(1);
        }
    }

    private static void startOrThrow(String[] args, BulkImportJobRunner runner) throws Exception {
        if (args.length != 5) {
            throw new IllegalArgumentException("Expected 5 arguments:" +
                    " <config bucket name> <bulk import job ID> <bulk import task ID> <bulk import job run ID> <bulk import mode>");
        }
        String configBucket = args[0];
        String jobId = args[1];
        String taskId = args[2];
        String jobRunId = args[3];
        String bulkImportMode = args[4];

        // Shutdown hooks run on System.exit() but NOT on Runtime.halt() — so if this fires it means
        // halt(1) was not called and System.exit() reached us first (likely Spark's own exit(0)).
        // Uses System.err so it reaches CloudWatch even if Logback has already shut down.
        Runtime.getRuntime().addShutdownHook(new Thread(() ->
                System.err.println("DIAGNOSTIC: JVM shutdown hook fired for job " + jobId
                        + " - System.exit() was called (not halt), JVM will exit via normal shutdown sequence")));

        // Log uncaught exceptions from Spark background threads to stderr so they reach CloudWatch
        Thread.setDefaultUncaughtExceptionHandler((thread, throwable) ->
                System.err.println("DIAGNOSTIC: uncaught exception in thread " + thread.getName()
                        + ": " + throwable.getClass().getName() + " - " + throwable.getMessage()));

        LOGGER.info("Starting bulk import job driver");
        LOGGER.info("Config bucket: {}", configBucket);
        LOGGER.info("Job ID: {}", jobId);
        LOGGER.info("Task ID: {}", taskId);
        LOGGER.info("Job run ID: {}", jobRunId);
        LOGGER.info("Bulk import mode: {}", bulkImportMode);
        LOGGER.info("AWS SDK version, from VersionInfo: {}", VersionInfo.SDK_VERSION);
        logCodeSource(VersionInfo.class);
        logCodeSource(SdkSystemSetting.class);

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
            BulkImportJobDriver<BulkImportSparkContext> driver = new BulkImportJobDriver<>(
                    BulkImportSparkContext.creator(instanceProperties), GenerateSketchesDriver::generatePartitionIdToSketches, runner.asImporter(),
                    tablePropertiesProvider, stateStoreProvider, tracker, commitSender,
                    Instant::now, () -> UUID.randomUUID().toString());
            driver.run(bulkImportJob, jobRunId, taskId, () -> Runtime.getRuntime().halt(1));
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

    private static void logCodeSource(Class<?> clazz) {
        ProtectionDomain domain = clazz.getProtectionDomain();
        CodeSource codeSource = domain.getCodeSource();
        if (codeSource != null) {
            LOGGER.info("Code location for class {}: {}", clazz.getName(), codeSource.getLocation());
        } else {
            LOGGER.info("Code source is null for class {}", clazz.getName());
        }
    }
}
