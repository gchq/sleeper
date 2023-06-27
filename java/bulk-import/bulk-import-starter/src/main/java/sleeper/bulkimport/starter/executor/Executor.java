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
package sleeper.bulkimport.starter.executor;

import com.amazonaws.services.s3.AmazonS3;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.bulkimport.job.BulkImportJob;
import sleeper.bulkimport.job.BulkImportJobSerDe;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.ingest.job.status.IngestJobStatusStore;
import sleeper.statestore.StateStoreProvider;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.regex.Pattern;

import static sleeper.bulkimport.CheckLeafPartitionCount.hasMinimumPartitions;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.BULK_IMPORT_BUCKET;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.BULK_IMPORT_CLASS_NAME;
import static sleeper.ingest.job.status.IngestJobValidatedEvent.ingestJobAccepted;
import static sleeper.ingest.job.status.IngestJobValidatedEvent.ingestJobRejected;

public abstract class Executor {
    private static final Logger LOGGER = LoggerFactory.getLogger(Executor.class);
    private static final Predicate<String> LOWER_ALPHANUMERICS_AND_DASHES = Pattern.compile("^[a-z0-9-]+$").asPredicate();

    protected final InstanceProperties instanceProperties;
    protected final TablePropertiesProvider tablePropertiesProvider;
    protected final StateStoreProvider stateStoreProvider;
    protected final IngestJobStatusStore ingestJobStatusStore;
    protected final AmazonS3 s3Client;
    protected final Supplier<Instant> validationTimeSupplier;

    public Executor(InstanceProperties instanceProperties, TablePropertiesProvider tablePropertiesProvider,
                    StateStoreProvider stateStoreProvider, IngestJobStatusStore ingestJobStatusStore, AmazonS3 s3Client,
                    Supplier<Instant> validationTimeSupplier) {
        this.instanceProperties = instanceProperties;
        this.tablePropertiesProvider = tablePropertiesProvider;
        this.stateStoreProvider = stateStoreProvider;
        this.ingestJobStatusStore = ingestJobStatusStore;
        this.s3Client = s3Client;
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
        writeJobToJSONFile(bulkImportJob);
        LOGGER.info("Submitting job with id {}", bulkImportJob.getId());
        runJobOnPlatform(bulkImportJob, jobRunId);
        LOGGER.info("Successfully submitted job");
    }

    protected abstract void runJobOnPlatform(BulkImportJob bulkImportJob, String jobRunId);

    protected abstract String getJarLocation();

    protected List<String> constructArgs(BulkImportJob bulkImportJob, String jobRunId, String taskId) {
        Map<String, String> userConfig = bulkImportJob.getSparkConf();
        LOGGER.info("Using Spark config {}", userConfig);

        String className = bulkImportJob.getClassName() != null ? bulkImportJob.getClassName() : instanceProperties.get(BULK_IMPORT_CLASS_NAME);

        List<String> args = Lists.newArrayList("spark-submit", "--deploy-mode", "cluster", "--class", className);

        if (null != userConfig) {
            for (Map.Entry<String, String> configurationItem : userConfig.entrySet()) {
                args.add("--conf");
                args.add(configurationItem.getKey() + "=" + configurationItem.getValue());
            }
        }

        args.add(getJarLocation());

        args.add(instanceProperties.get(CONFIG_BUCKET));
        args.add(bulkImportJob.getId());
        args.add(taskId);
        args.add(jobRunId);
        return args;
    }

    private boolean validateJob(BulkImportJob bulkImportJob) {
        List<String> failedChecks = new ArrayList<>();
        String id = bulkImportJob.getId();
        if (null == id) {
            String newId = UUID.randomUUID().toString();
            LOGGER.info("Bulk import job has null id, setting id to {}", newId);
            bulkImportJob.setId(newId);
        } else {
            if (!LOWER_ALPHANUMERICS_AND_DASHES.test(id)) {
                failedChecks.add("Job Ids must only contain lowercase alphanumerics and dashes.");
            }
            if (id.length() > 63) {
                failedChecks.add("Job IDs are only allowed to be up to 63 characters long.");
            }
        }

        if (null == bulkImportJob.getTableName()) {
            failedChecks.add("The table name must be set to a non-null value.");
        } else if (!doesTableExist(bulkImportJob.getTableName())) {
            failedChecks.add("Table does not exist.");
        } else if (!hasMinimumPartitions(stateStoreProvider, tablePropertiesProvider, bulkImportJob)) {
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

    private boolean doesTableExist(String tableName) {
        try {
            if (tablePropertiesProvider.getTableProperties(tableName) != null) {
                return true;
            }
        } catch (RuntimeException ep) {
            LOGGER.warn("Could not find properties for table");
        }
        return false;
    }

    private void writeJobToJSONFile(BulkImportJob bulkImportJob) {
        String bulkImportBucket = instanceProperties.get(BULK_IMPORT_BUCKET);
        if (null == bulkImportBucket) {
            throw new RuntimeException("sleeper.bulk.import.bucket was not set. Has one of the bulk import stacks been deployed?");
        }
        String key = "bulk_import/" + bulkImportJob.getId() + ".json";
        String bulkImportJobJSON = new BulkImportJobSerDe().toJson(bulkImportJob);
        s3Client.putObject(bulkImportBucket, key, bulkImportJobJSON);
        LOGGER.info("Put object for job {} to key {} in bucket {}", bulkImportJob.getId(), key, bulkImportBucket);
    }
}
