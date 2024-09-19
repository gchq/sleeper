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
package sleeper.bulkimport.job.runner;

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
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.google.gson.JsonSyntaxException;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.bulkimport.job.BulkImportJob;
import sleeper.bulkimport.job.BulkImportJobSerDe;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.configuration.statestore.StateStoreProvider;
import sleeper.core.record.process.ProcessRunTime;
import sleeper.core.record.process.RecordsProcessed;
import sleeper.core.record.process.RecordsProcessedSummary;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;
import sleeper.core.statestore.commit.StateStoreCommitRequestInS3;
import sleeper.core.statestore.commit.StateStoreCommitRequestInS3SerDe;
import sleeper.core.table.TableStatus;
import sleeper.core.util.LoggedDuration;
import sleeper.ingest.job.commit.IngestAddFilesCommitRequest;
import sleeper.ingest.job.commit.IngestAddFilesCommitRequestSerDe;
import sleeper.ingest.job.status.IngestJobStatusStore;
import sleeper.ingest.status.store.job.IngestJobStatusStoreFactory;
import sleeper.io.parquet.utils.HadoopConfigurationProvider;
import sleeper.statestore.StateStoreFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFileAttributes;
import java.time.Instant;
import java.util.UUID;
import java.util.function.Supplier;

import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.BULK_IMPORT_BUCKET;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.STATESTORE_COMMITTER_QUEUE_URL;
import static sleeper.configuration.properties.table.TableProperty.BULK_IMPORT_FILES_COMMIT_ASYNC;
import static sleeper.ingest.job.status.IngestJobFailedEvent.ingestJobFailed;
import static sleeper.ingest.job.status.IngestJobFinishedEvent.ingestJobFinished;
import static sleeper.ingest.job.status.IngestJobStartedEvent.validatedIngestJobStarted;

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
    private final IngestJobStatusStore statusStore;
    private final AddFilesAsynchronously addFilesAsync;
    private final Supplier<Instant> getTime;

    public BulkImportJobDriver(SessionRunner sessionRunner,
            TablePropertiesProvider tablePropertiesProvider,
            StateStoreProvider stateStoreProvider,
            IngestJobStatusStore statusStore,
            AddFilesAsynchronously addFilesAsync,
            Supplier<Instant> getTime) {
        this.sessionRunner = sessionRunner;
        this.tablePropertiesProvider = tablePropertiesProvider;
        this.stateStoreProvider = stateStoreProvider;
        this.statusStore = statusStore;
        this.addFilesAsync = addFilesAsync;
        this.getTime = getTime;
    }

    public void run(BulkImportJob job, String jobRunId, String taskId) throws IOException {
        TableProperties tableProperties = tablePropertiesProvider.getByName(job.getTableName());
        TableStatus table = tableProperties.getStatus();
        Instant startTime = getTime.get();
        LOGGER.info("Received bulk import job with id {} at time {}", job.getId(), startTime);
        LOGGER.info("Job is for table {}: {}", table, job);
        statusStore.jobStarted(validatedIngestJobStarted(job.toIngestJob(), startTime)
                .jobRunId(jobRunId).taskId(taskId).build());

        BulkImportJobOutput output;
        try {
            output = sessionRunner.run(job);
        } catch (RuntimeException e) {
            statusStore.jobFailed(ingestJobFailed(job.toIngestJob(), new ProcessRunTime(startTime, getTime.get()))
                    .jobRunId(jobRunId).taskId(taskId).failure(e).build());
            throw e;
        }

        boolean asyncCommit = tableProperties.getBoolean(BULK_IMPORT_FILES_COMMIT_ASYNC);
        try {
            if (asyncCommit) {
                IngestAddFilesCommitRequest request = IngestAddFilesCommitRequest.builder()
                        .ingestJob(job.toIngestJob())
                        .fileReferences(output.fileReferences())
                        .jobRunId(jobRunId).taskId(taskId)
                        .writtenTime(getTime.get())
                        .build();
                LOGGER.debug("Sending asynchronous request to state store committer: {}", request);
                addFilesAsync.submit(request);
                LOGGER.info("Submitted {} files to statestore committer for job {} in table {}", output.numFiles(), job.getId(), table);
            } else {
                stateStoreProvider.getStateStore(tableProperties)
                        .addFiles(output.fileReferences());
                LOGGER.info("Added {} files to statestore for job {} in table {}", output.numFiles(), job.getId(), table);
            }
        } catch (RuntimeException | StateStoreException e) {
            statusStore.jobFailed(ingestJobFailed(job.toIngestJob(), new ProcessRunTime(startTime, getTime.get()))
                    .jobRunId(jobRunId).taskId(taskId).failure(e).build());
            throw new RuntimeException("Failed to add files to state store. Ensure this service account has write access. Files may need to "
                    + "be re-imported for clients to access data", e);
        }

        Instant finishTime = getTime.get();
        LoggedDuration duration = LoggedDuration.withFullOutput(startTime, finishTime);
        LOGGER.info("Finished bulk import job {} at time {}", job.getId(), finishTime);
        long numRecords = output.numRecords();
        double rate = numRecords / (double) duration.getSeconds();
        LOGGER.info("Bulk import job {} took {} (rate of {} per second)", job.getId(), duration, rate);

        statusStore.jobFinished(ingestJobFinished(job.toIngestJob(),
                new RecordsProcessedSummary(new RecordsProcessed(numRecords, numRecords), startTime, finishTime))
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

        InstanceProperties instanceProperties = new InstanceProperties();
        AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();
        AmazonDynamoDB dynamoClient = AmazonDynamoDBClientBuilder.defaultClient();
        AmazonSQS sqsClient = AmazonSQSClientBuilder.defaultClient();
        Configuration configuration;
        if (bulkImportMode.equals("EKS")) {
            configuration = HadoopConfigurationProvider.getConfigurationForEKS(instanceProperties);
        } else if (bulkImportMode.equals("EMR")) {
            configuration = HadoopConfigurationProvider.getConfigurationForEMR(instanceProperties);
        } else {
            throw new IllegalArgumentException("Unknown bulk import mode: " + bulkImportMode);
        }

        try {
            try {
                instanceProperties.loadFromS3(s3Client, configBucket);
            } catch (Exception e) {
                // This is a good indicator if something is wrong with the permissions
                LOGGER.error("Failed to load instance properties", e);
                logPermissions();
                throw e;
            }

            BulkImportJob bulkImportJob = loadJob(instanceProperties, jobId, jobRunId, s3Client);

            TablePropertiesProvider tablePropertiesProvider = new TablePropertiesProvider(instanceProperties, s3Client, dynamoClient);
            StateStoreProvider stateStoreProvider = StateStoreFactory.createProvider(instanceProperties, s3Client, dynamoClient, configuration);
            IngestJobStatusStore statusStore = IngestJobStatusStoreFactory.getStatusStore(dynamoClient, instanceProperties);
            AddFilesAsynchronously addFilesAsync = submitFilesToCommitQueue(sqsClient, s3Client, instanceProperties);
            BulkImportJobDriver driver = new BulkImportJobDriver(new BulkImportSparkSessionRunner(
                    runner, instanceProperties, tablePropertiesProvider, stateStoreProvider),
                    tablePropertiesProvider, stateStoreProvider, statusStore, addFilesAsync, Instant::now);
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

    public interface AddFilesAsynchronously {
        void submit(IngestAddFilesCommitRequest request);
    }

    public static AddFilesAsynchronously submitFilesToCommitQueue(
            AmazonSQS sqsClient, AmazonS3 s3Client, InstanceProperties instanceProperties) {
        return submitFilesToCommitQueue(sqsClient, s3Client, instanceProperties, () -> UUID.randomUUID().toString());
    }

    public static AddFilesAsynchronously submitFilesToCommitQueue(
            AmazonSQS sqsClient, AmazonS3 s3Client, InstanceProperties instanceProperties, Supplier<String> s3FilenameSupplier) {
        IngestAddFilesCommitRequestSerDe serDe = new IngestAddFilesCommitRequestSerDe();
        return request -> {
            String json = serDe.toJson(request);
            // Store in S3 if the request will not fit in an SQS message
            if (json.length() > 262144) {
                String s3Key = StateStoreCommitRequestInS3.createFileS3Key(request.getTableId(), s3FilenameSupplier.get());
                s3Client.putObject(instanceProperties.get(DATA_BUCKET), s3Key, json);
                json = new StateStoreCommitRequestInS3SerDe().toJson(new StateStoreCommitRequestInS3(s3Key));
                LOGGER.info("Request to add files was too big for an SQS message. Will submit a reference to file in data bucket: {}", s3Key);
            }
            sqsClient.sendMessage(new SendMessageRequest()
                    .withQueueUrl(instanceProperties.get(STATESTORE_COMMITTER_QUEUE_URL))
                    .withMessageBody(json)
                    .withMessageGroupId(request.getTableId())
                    .withMessageDeduplicationId(UUID.randomUUID().toString()));
        };
    }
}
