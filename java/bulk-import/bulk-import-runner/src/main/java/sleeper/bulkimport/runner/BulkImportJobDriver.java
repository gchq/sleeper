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
package sleeper.bulkimport.runner;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;
import com.amazonaws.services.securitytoken.model.GetCallerIdentityRequest;
import com.amazonaws.services.securitytoken.model.GetCallerIdentityResult;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.google.gson.JsonSyntaxException;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.bulkimport.core.job.BulkImportJob;
import sleeper.bulkimport.core.job.BulkImportJobSerDe;
import sleeper.configuration.properties.S3InstanceProperties;
import sleeper.configuration.properties.S3TableProperties;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertiesProvider;
import sleeper.core.statestore.AllReferencesToAFile;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreProvider;
import sleeper.core.statestore.commit.StateStoreCommitRequest;
import sleeper.core.statestore.commit.StateStoreCommitRequestSender;
import sleeper.core.statestore.transactionlog.transaction.AddFilesTransaction;
import sleeper.core.statestore.transactionlog.transaction.TransactionSerDeProvider;
import sleeper.core.table.TableStatus;
import sleeper.core.tracker.ingest.job.IngestJobTracker;
import sleeper.core.tracker.job.run.JobRunSummary;
import sleeper.core.tracker.job.run.RecordsProcessed;
import sleeper.core.util.LoggedDuration;
import sleeper.ingest.tracker.job.IngestJobTrackerFactory;
import sleeper.parquet.utils.HadoopConfigurationProvider;
import sleeper.statestore.StateStoreFactory;
import sleeper.statestore.commit.SqsFifoStateStoreCommitRequestSender;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFileAttributes;
import java.time.Instant;
import java.util.function.Supplier;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.BULK_IMPORT_BUCKET;
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
        LOGGER.info("Received bulk import job with id {} at time {}", job.getId(), startTime);
        LOGGER.info("Job is for table {}: {}", table, job);
        tracker.jobStarted(job.toIngestJob()
                .startedAfterValidationEventBuilder(startTime)
                .jobRunId(jobRunId).taskId(taskId).build());

        BulkImportJobOutput output;
        try {
            output = sessionRunner.run(job);
        } catch (RuntimeException e) {
            tracker.jobFailed(job.toIngestJob()
                    .failedEventBuilder(getTime.get())
                    .jobRunId(jobRunId).taskId(taskId).failure(e).build());
            throw e;
        }

        Instant finishTime = getTime.get();
        boolean asyncCommit = tableProperties.getBoolean(BULK_IMPORT_FILES_COMMIT_ASYNC);
        try {
            if (asyncCommit) {
                AddFilesTransaction transaction = AddFilesTransaction.builder()
                        .jobId(job.getId()).taskId(taskId).jobRunId(jobRunId).writtenTime(finishTime)
                        .files(AllReferencesToAFile.newFilesWithReferences(output.fileReferences()))
                        .build();
                asyncSender.send(StateStoreCommitRequest.create(table.getTableUniqueId(), transaction));
                LOGGER.info("Submitted asynchronous request to state store committer to add {} files for job {} in table {}", output.numFiles(), job.getId(), table);
            } else {
                stateStoreProvider.getStateStore(tableProperties)
                        .addFiles(output.fileReferences());
                LOGGER.info("Added {} files to statestore for job {} in table {}", output.numFiles(), job.getId(), table);
            }
        } catch (RuntimeException e) {
            tracker.jobFailed(job.toIngestJob()
                    .failedEventBuilder(finishTime)
                    .jobRunId(jobRunId).taskId(taskId).failure(e).build());
            throw new RuntimeException("Failed to add files to state store. Ensure this service account has write access. Files may need to "
                    + "be re-imported for clients to access data", e);
        }

        LoggedDuration duration = LoggedDuration.withFullOutput(startTime, finishTime);
        LOGGER.info("Finished bulk import job {} at time {}", job.getId(), finishTime);
        long numRecords = output.numRecords();
        double rate = numRecords / (double) duration.getSeconds();
        LOGGER.info("Bulk import job {} took {} (rate of {} per second)", job.getId(), duration, rate);

        tracker.jobFinished(job.toIngestJob()
                .finishedEventBuilder(new JobRunSummary(new RecordsProcessed(numRecords, numRecords), startTime, finishTime))
                .jobRunId(jobRunId).taskId(taskId)
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

        AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();
        AmazonDynamoDB dynamoClient = AmazonDynamoDBClientBuilder.defaultClient();
        AmazonSQS sqsClient = AmazonSQSClientBuilder.defaultClient();
        try {
            InstanceProperties instanceProperties;
            try {
                instanceProperties = S3InstanceProperties.loadFromBucket(s3Client, configBucket);
            } catch (Exception e) {
                // This is a good indicator if something is wrong with the permissions
                LOGGER.error("Failed to load instance properties", e);
                logPermissions();
                throw e;
            }
            Configuration configuration;
            if (bulkImportMode.equals("EKS")) {
                configuration = HadoopConfigurationProvider.getConfigurationForEKS(instanceProperties);
            } else if (bulkImportMode.equals("EMR")) {
                configuration = HadoopConfigurationProvider.getConfigurationForEMR(instanceProperties);
            } else {
                throw new IllegalArgumentException("Unknown bulk import mode: " + bulkImportMode);
            }

            BulkImportJob bulkImportJob = loadJob(instanceProperties, jobId, jobRunId, s3Client);

            TablePropertiesProvider tablePropertiesProvider = S3TableProperties.createProvider(instanceProperties, s3Client, dynamoClient);
            StateStoreProvider stateStoreProvider = StateStoreFactory.createProvider(instanceProperties, s3Client, dynamoClient, configuration);
            IngestJobTracker tracker = IngestJobTrackerFactory.getTracker(dynamoClient, instanceProperties);
            StateStoreCommitRequestSender commitSender = new SqsFifoStateStoreCommitRequestSender(
                    instanceProperties, sqsClient, s3Client, TransactionSerDeProvider.from(tablePropertiesProvider));
            BulkImportJobDriver driver = new BulkImportJobDriver(new BulkImportSparkSessionRunner(
                    runner, instanceProperties, tablePropertiesProvider, stateStoreProvider),
                    tablePropertiesProvider, stateStoreProvider, tracker, commitSender, Instant::now);
            driver.run(bulkImportJob, jobRunId, taskId);
        } finally {
            s3Client.shutdown();
            dynamoClient.shutdown();
            sqsClient.shutdown();
        }
    }

    private static BulkImportJob loadJob(
            InstanceProperties instanceProperties, String jobId, String jobRunId, AmazonS3 s3Client) {
        String bulkImportBucket = instanceProperties.get(BULK_IMPORT_BUCKET);
        if (null == bulkImportBucket) {
            throw new RuntimeException("sleeper.bulk.import.bucket was not set. Has one of the bulk import stacks been deployed?");
        }
        String jsonJobKey = "bulk_import/" + jobId + "-" + jobRunId + ".json";
        LOGGER.info("Loading bulk import job from key {} in bulk import bucket {}", jsonJobKey, bulkImportBucket);
        String jsonJob = s3Client.getObjectAsString(bulkImportBucket, jsonJobKey);
        try {
            return new BulkImportJobSerDe().fromJson(jsonJob);
        } catch (JsonSyntaxException e) {
            LOGGER.error("Json job was malformed");
            throw e;
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
        AWSSecurityTokenService sts = AWSSecurityTokenServiceClientBuilder.defaultClient();
        try {
            GetCallerIdentityResult callerIdentity = sts.getCallerIdentity(new GetCallerIdentityRequest());
            LOGGER.info("Logged in as: {}", callerIdentity.getArn());
        } finally {
            sts.shutdown();
        }
    }
}
