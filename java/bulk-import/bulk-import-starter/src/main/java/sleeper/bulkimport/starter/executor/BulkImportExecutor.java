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
package sleeper.bulkimport.starter.executor;

import com.amazonaws.services.s3.AmazonS3;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.bulkimport.CheckLeafPartitionCount;
import sleeper.bulkimport.job.BulkImportJob;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.core.statestore.StateStore;
import sleeper.ingest.job.status.IngestJobStatusStore;
import sleeper.statestore.StateStoreProvider;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.regex.Pattern;

import static sleeper.ingest.job.status.IngestJobValidatedEvent.ingestJobAccepted;
import static sleeper.ingest.job.status.IngestJobValidatedEvent.ingestJobRejected;

public class BulkImportExecutor {
    private static final Logger LOGGER = LoggerFactory.getLogger(BulkImportExecutor.class);
    private static final Predicate<String> LOWER_ALPHANUMERICS_AND_DASHES = Pattern.compile("^[a-z0-9-]+$").asPredicate();

    protected final InstanceProperties instanceProperties;
    protected final TablePropertiesProvider tablePropertiesProvider;
    protected final StateStoreProvider stateStoreProvider;
    protected final IngestJobStatusStore ingestJobStatusStore;
    protected final WriteJobToBucket writeJobToBucket;
    protected final PlatformExecutor platformExecutor;
    protected final Supplier<Instant> validationTimeSupplier;

    public BulkImportExecutor(
            InstanceProperties instanceProperties, TablePropertiesProvider tablePropertiesProvider,
            StateStoreProvider stateStoreProvider, IngestJobStatusStore ingestJobStatusStore, AmazonS3 s3Client,
            PlatformExecutor platformExecutor, Supplier<Instant> validationTimeSupplier) {
        this(instanceProperties, tablePropertiesProvider, stateStoreProvider, ingestJobStatusStore,
                new BulkImportJobWriterToS3(instanceProperties, s3Client),
                platformExecutor, validationTimeSupplier);
    }

    public BulkImportExecutor(
            InstanceProperties instanceProperties, TablePropertiesProvider tablePropertiesProvider,
            StateStoreProvider stateStoreProvider, IngestJobStatusStore ingestJobStatusStore,
            WriteJobToBucket writeJobToBucket, PlatformExecutor platformExecutor, Supplier<Instant> validationTimeSupplier) {
        this.instanceProperties = instanceProperties;
        this.tablePropertiesProvider = tablePropertiesProvider;
        this.stateStoreProvider = stateStoreProvider;
        this.ingestJobStatusStore = ingestJobStatusStore;
        this.writeJobToBucket = writeJobToBucket;
        this.platformExecutor = platformExecutor;
        this.validationTimeSupplier = validationTimeSupplier;
    }

    public void runJob(BulkImportJob bulkImportJob) {
        runJob(bulkImportJob, UUID.randomUUID().toString());
    }

    public void runJob(BulkImportJob bulkImportJob, String jobRunId) {
        if (null == bulkImportJob) {
            LOGGER.warn("Received null job, exiting early.");
            return;
        }
        LOGGER.info("Validating job: {}", bulkImportJob);
        if (!validateJob(bulkImportJob)) {
            return;
        }
        ingestJobStatusStore.jobValidated(ingestJobAccepted(
                bulkImportJob.toIngestJob(), validationTimeSupplier.get())
                .jobRunId(jobRunId).build());
        LOGGER.info("Writing job with id {} to JSON file", bulkImportJob.getId());
        writeJobToBucket.writeJobToBulkImportBucket(bulkImportJob, jobRunId);
        LOGGER.info("Submitting job with id {}", bulkImportJob.getId());
        platformExecutor.runJobOnPlatform(BulkImportArguments.builder()
                .instanceProperties(instanceProperties)
                .bulkImportJob(bulkImportJob).jobRunId(jobRunId)
                .build());
        LOGGER.info("Successfully submitted job");
    }

    private boolean validateJob(BulkImportJob bulkImportJob) {
        List<String> failedChecks = new ArrayList<>();
        String id = bulkImportJob.getId();
        if (!LOWER_ALPHANUMERICS_AND_DASHES.test(id)) {
            failedChecks.add("Job Ids must only contain lowercase alphanumerics and dashes.");
        }
        if (id.length() > 63) {
            failedChecks.add("Job IDs are only allowed to be up to 63 characters long.");
        }

        if (!hasMinimumPartitions(bulkImportJob)) {
            failedChecks.add("The minimum partition count was not reached");
        }

        if (null == bulkImportJob.getFiles() || bulkImportJob.getFiles().isEmpty()) {
            failedChecks.add("The input files must be set to a non-null and non-empty value.");
        }

        if (!failedChecks.isEmpty()) {
            String errorMessage = "The bulk import job failed validation with the following checks failing: \n"
                    + String.join("\n", failedChecks);
            LOGGER.warn(errorMessage);
            ingestJobStatusStore.jobValidated(ingestJobRejected(
                    bulkImportJob.toIngestJob(), validationTimeSupplier.get(), failedChecks));
            return false;
        } else {
            return true;
        }
    }

    private boolean hasMinimumPartitions(BulkImportJob bulkImportJob) {
        TableProperties tableProperties = tablePropertiesProvider.getByName(bulkImportJob.getTableName());
        StateStore stateStore = stateStoreProvider.getStateStore(tableProperties);
        return CheckLeafPartitionCount.hasMinimumPartitions(tableProperties, stateStore, bulkImportJob);
    }

    /**
     * Writes a bulk import job to the bulk import bucket, to be read by a Spark driver.
     */
    @FunctionalInterface
    public interface WriteJobToBucket {

        void writeJobToBulkImportBucket(BulkImportJob bulkImportJob, String jobRunID);
    }
}
