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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Predicate;
import java.util.regex.Pattern;

import static sleeper.configuration.properties.SystemDefinedInstanceProperty.BULK_IMPORT_BUCKET;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.BULK_IMPORT_CLASS_NAME;

public abstract class Executor {
    private static final Logger LOGGER = LoggerFactory.getLogger(Executor.class);
    private static final Predicate<String> LOWER_ALPHANUMERICS_AND_DASHES = Pattern.compile("^[a-z0-9-]+$").asPredicate();

    protected final InstanceProperties instanceProperties;
    protected final TablePropertiesProvider tablePropertiesProvider;
    protected final AmazonS3 s3Client;

    protected Executor(InstanceProperties instanceProperties, TablePropertiesProvider tablePropertiesProvider, AmazonS3 amazonS3Client) {
        this.instanceProperties = instanceProperties;
        this.tablePropertiesProvider = tablePropertiesProvider;
        this.s3Client = amazonS3Client;
    }

    public void runJob(BulkImportJob bulkImportJob) {
        if (null == bulkImportJob) {
            LOGGER.warn("Received null job, exiting early.");
            return;
        }
        LOGGER.info("Validating job: {}", bulkImportJob);
        validateJob(bulkImportJob);
        LOGGER.info("Writing job with id {} to JSON file", bulkImportJob.getId());
        writeJobToJSONFile(bulkImportJob);
        LOGGER.info("Submitting job with id {}", bulkImportJob.getId());
        boolean jobSubmitted = runJobOnPlatform(bulkImportJob);
        if (jobSubmitted) {
            LOGGER.info("Successfully submitted job");
        } else {
            LOGGER.info("Job was not submitted");
        }
    }

    protected abstract boolean runJobOnPlatform(BulkImportJob bulkImportJob);

    protected abstract String getJarLocation();

    protected List<String> constructArgs(BulkImportJob bulkImportJob, String taskId) {
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

        return args;
    }

    private void validateJob(BulkImportJob bulkImportJob) {
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
        }

        if (null == bulkImportJob.getFiles() || bulkImportJob.getFiles().isEmpty()) {
            failedChecks.add("The input files must be set to a non-null and non-empty value.");
        }

        if (!failedChecks.isEmpty()) {
            String errorMessage = "The bulk import job failed validation with the following checks failing: \n"
                    + String.join("\n", failedChecks);

            throw new IllegalArgumentException(errorMessage);
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
