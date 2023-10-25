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
package sleeper.bulkimport.job.runner;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;
import com.amazonaws.services.securitytoken.model.GetCallerIdentityRequest;
import com.amazonaws.services.securitytoken.model.GetCallerIdentityResult;
import com.google.gson.JsonSyntaxException;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.bulkimport.job.BulkImportJob;
import sleeper.bulkimport.job.BulkImportJobSerDe;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.core.record.process.RecordsProcessed;
import sleeper.core.record.process.RecordsProcessedSummary;
import sleeper.core.statestore.StateStore;
import sleeper.ingest.job.status.IngestJobStatusStore;
import sleeper.ingest.status.store.job.IngestJobStatusStoreFactory;
import sleeper.statestore.StateStoreProvider;
import sleeper.utils.HadoopConfigurationProvider;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFileAttributes;
import java.time.Duration;
import java.time.Instant;
import java.util.function.Supplier;

import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.BULK_IMPORT_BUCKET;
import static sleeper.ingest.job.status.IngestJobFinishedEvent.ingestJobFinished;
import static sleeper.ingest.job.status.IngestJobStartedEvent.validatedIngestJobStarted;

/**
 * This class executes a Spark job that reads in input Parquet files and writes
 * out files of {@link sleeper.core.record.Record}s. This takes a {@link BulkImportJobRunner} implementation,
 * which takes rows from the input files and outputs a file for each Sleeper partition.
 * These will then be used to update the {@link StateStore}.
 */
public class BulkImportJobDriver {
    private static final Logger LOGGER = LoggerFactory.getLogger(BulkImportJobDriver.class);

    private final SessionRunner sessionRunner;
    private final TablePropertiesProvider tablePropertiesProvider;
    private final StateStoreProvider stateStoreProvider;
    private final IngestJobStatusStore statusStore;
    private final Supplier<Instant> getTime;

    public BulkImportJobDriver(SessionRunner sessionRunner,
                               TablePropertiesProvider tablePropertiesProvider,
                               StateStoreProvider stateStoreProvider,
                               IngestJobStatusStore statusStore,
                               Supplier<Instant> getTime) {
        this.sessionRunner = sessionRunner;
        this.tablePropertiesProvider = tablePropertiesProvider;
        this.stateStoreProvider = stateStoreProvider;
        this.statusStore = statusStore;
        this.getTime = getTime;
    }

    public static BulkImportJobDriver from(BulkImportJobRunner jobRunner, InstanceProperties instanceProperties,
                                           AmazonS3 s3Client, AmazonDynamoDB dynamoClient, Configuration conf) {
        return from(jobRunner, instanceProperties, s3Client, dynamoClient, conf,
                IngestJobStatusStoreFactory.getStatusStore(dynamoClient, instanceProperties), Instant::now);
    }

    public static BulkImportJobDriver from(BulkImportJobRunner jobRunner, InstanceProperties instanceProperties,
                                           AmazonS3 s3Client, AmazonDynamoDB dynamoClient, Configuration conf,
                                           IngestJobStatusStore statusStore,
                                           Supplier<Instant> getTime) {
        TablePropertiesProvider tablePropertiesProvider = new TablePropertiesProvider(instanceProperties, s3Client, dynamoClient);
        StateStoreProvider stateStoreProvider = new StateStoreProvider(dynamoClient, instanceProperties, conf);
        return new BulkImportJobDriver(new BulkImportSparkSessionRunner(
                jobRunner, instanceProperties, tablePropertiesProvider, stateStoreProvider),
                tablePropertiesProvider, stateStoreProvider, statusStore, getTime);
    }

    public void run(BulkImportJob job, String jobRunId, String taskId) throws IOException {
        Instant startTime = getTime.get();
        LOGGER.info("Received bulk import job with id {} at time {}", job.getId(), startTime);
        LOGGER.info("Job is {}", job);
        statusStore.jobStarted(validatedIngestJobStarted(job.toIngestJob(), startTime)
                .jobRunId(jobRunId).taskId(taskId).build());

        BulkImportJobOutput output;
        try {
            output = sessionRunner.run(job);
        } catch (RuntimeException e) {
            statusStore.jobFinished(ingestJobFinished(job.toIngestJob(), new RecordsProcessedSummary(
                    new RecordsProcessed(0, 0), startTime, getTime.get()))
                    .jobRunId(jobRunId).taskId(taskId).build());
            throw e;
        }

        try {
            stateStoreProvider.getStateStore(job.getTableName(), tablePropertiesProvider)
                    .addFiles(output.fileInfos());
            LOGGER.info("Added {} files to statestore for job {}", output.numFiles(), job.getId());
        } catch (Exception e) {
            statusStore.jobFinished(ingestJobFinished(job.toIngestJob(), new RecordsProcessedSummary(
                    new RecordsProcessed(0, 0), startTime, getTime.get()))
                    .jobRunId(jobRunId).taskId(taskId).build());
            throw new RuntimeException("Failed to add files to state store. Ensure this service account has write access. Files may need to "
                    + "be re-imported for clients to access data", e);
        }

        Instant finishTime = getTime.get();
        LOGGER.info("Finished bulk import job {} at time {}", job.getId(), finishTime);
        long durationInSeconds = Duration.between(startTime, finishTime).getSeconds();
        long numRecords = output.numRecords();
        double rate = numRecords / (double) durationInSeconds;
        LOGGER.info("Bulk import job {} took {} seconds (rate of {} per second)", job.getId(), durationInSeconds, rate);
        statusStore.jobFinished(ingestJobFinished(job.toIngestJob(), new RecordsProcessedSummary(
                new RecordsProcessed(numRecords, numRecords), startTime, finishTime))
                .jobRunId(jobRunId).taskId(taskId).build());

        // Calling this manually stops it potentially timing out after 10 seconds.
        // Note that we stop the Spark context after we've applied the changes in Sleeper.
        output.stopSparkContext();
    }

    @FunctionalInterface
    public interface SessionRunner {
        BulkImportJobOutput run(BulkImportJob job) throws IOException;
    }

    public static void start(String[] args, BulkImportJobRunner runner) throws Exception {
        if (args.length != 4) {
            throw new IllegalArgumentException("Expected 4 arguments:" +
                    " <config bucket name> <bulk import job ID> <bulk import task ID> <bulk import job run ID>");
        }
        String configBucket = args[0];
        String jobId = args[1];
        String taskId = args[2];
        String jobRunId = args[3];

        InstanceProperties instanceProperties = new InstanceProperties();
        AmazonS3 amazonS3 = AmazonS3ClientBuilder.defaultClient();

        try {
            instanceProperties.loadFromS3(amazonS3, configBucket);
        } catch (Exception e) {
            // This is a good indicator if something is wrong with the permissions
            LOGGER.error("Failed to load instance properties", e);
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
            GetCallerIdentityResult callerIdentity = sts.getCallerIdentity(new GetCallerIdentityRequest());
            LOGGER.info("Logged in as: {}", callerIdentity.getArn());

            throw e;
        }

        String bulkImportBucket = instanceProperties.get(BULK_IMPORT_BUCKET);
        if (null == bulkImportBucket) {
            throw new RuntimeException("sleeper.bulk.import.bucket was not set. Has one of the bulk import stacks been deployed?");
        }
        String jsonJobKey = "bulk_import/" + jobId + "-" + jobRunId + ".json";
        LOGGER.info("Loading bulk import job from key {} in bulk import bucket {}", jsonJobKey, bulkImportBucket);
        String jsonJob = amazonS3.getObjectAsString(bulkImportBucket, jsonJobKey);
        BulkImportJob bulkImportJob;
        try {
            bulkImportJob = new BulkImportJobSerDe().fromJson(jsonJob);
        } catch (JsonSyntaxException e) {
            LOGGER.error("Json job was malformed: {}", jobId);
            throw e;
        }

        BulkImportJobDriver driver = BulkImportJobDriver.from(runner, instanceProperties,
                amazonS3, AmazonDynamoDBClientBuilder.defaultClient(),
                HadoopConfigurationProvider.getConfigurationForEMR(instanceProperties));
        driver.run(bulkImportJob, jobRunId, taskId);
    }
}
