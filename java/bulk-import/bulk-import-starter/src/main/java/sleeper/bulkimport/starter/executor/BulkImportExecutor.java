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
package sleeper.bulkimport.starter.executor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.bulkimport.core.job.BulkImportJob;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TablePropertiesProvider;
import sleeper.core.statestore.StateStoreProvider;
import sleeper.core.tracker.ingest.job.IngestJobTracker;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.regex.Pattern;

public class BulkImportExecutor {
    private static final Logger LOGGER = LoggerFactory.getLogger(BulkImportExecutor.class);
    private static final Predicate<String> LOWER_ALPHANUMERICS_AND_DASHES = Pattern.compile("^[a-z0-9-]+$").asPredicate();

    protected final InstanceProperties instanceProperties;
    protected final TablePropertiesProvider tablePropertiesProvider;
    protected final StateStoreProvider stateStoreProvider;
    protected final IngestJobTracker ingestJobTracker;
    protected final WriteJobToBucket writeJobToBucket;
    protected final PlatformExecutor platformExecutor;
    protected final Supplier<Instant> validationTimeSupplier;

    public BulkImportExecutor(
            InstanceProperties instanceProperties, TablePropertiesProvider tablePropertiesProvider,
            StateStoreProvider stateStoreProvider, IngestJobTracker ingestJobTracker,
            WriteJobToBucket writeJobToBucket, PlatformExecutor platformExecutor, Supplier<Instant> validationTimeSupplier) {
        this.instanceProperties = instanceProperties;
        this.tablePropertiesProvider = tablePropertiesProvider;
        this.stateStoreProvider = stateStoreProvider;
        this.ingestJobTracker = ingestJobTracker;
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
        ingestJobTracker.jobValidated(bulkImportJob.toIngestJob()
                .acceptedEventBuilder(validationTimeSupplier.get())
                .jobRunId(jobRunId).build());
        try {
            LOGGER.info("Writing job with id {} to JSON file", bulkImportJob.getId());
            writeJobToBucket.writeJobToBulkImportBucket(bulkImportJob, jobRunId);
            LOGGER.info("Submitting job with id {}", bulkImportJob.getId());
            platformExecutor.runJobOnPlatform(BulkImportArguments.builder()
                    .instanceProperties(instanceProperties)
                    .bulkImportJob(bulkImportJob).jobRunId(jobRunId)
                    .build());
            LOGGER.info("Successfully submitted job");
        } catch (RuntimeException e) {
            LOGGER.error("Failed submitting job with id {} for table {}",
                    bulkImportJob.getId(), bulkImportJob.getTableId(), e);
            ingestJobTracker.jobFailed(bulkImportJob.toIngestJob()
                    .failedEventBuilder(validationTimeSupplier.get())
                    .jobRunId(jobRunId).failure(e)
                    .build());
            throw e;
        }
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

        if (null == bulkImportJob.getFiles() || bulkImportJob.getFiles().isEmpty()) {
            failedChecks.add("The input files must be set to a non-null and non-empty value.");
        }

        if (!failedChecks.isEmpty()) {
            String errorMessage = "The bulk import job failed validation with the following checks failing: \n"
                    + String.join("\n", failedChecks);
            LOGGER.warn(errorMessage);
            ingestJobTracker.jobValidated(bulkImportJob.toIngestJob()
                    .createRejectedEvent(validationTimeSupplier.get(), failedChecks));
            return false;
        } else {
            return true;
        }
    }

    /**
     * Writes a bulk import job to the bulk import bucket, to be read by a Spark driver.
     */
    @FunctionalInterface
    public interface WriteJobToBucket {

        void writeJobToBulkImportBucket(BulkImportJob bulkImportJob, String jobRunID);
    }
}
