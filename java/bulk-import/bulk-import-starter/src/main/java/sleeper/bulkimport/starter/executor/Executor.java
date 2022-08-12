/*
 * Copyright 2022 Crown Copyright
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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sleeper.bulkimport.job.BulkImportJob;
import sleeper.configuration.properties.InstanceProperties;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.*;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.configuration.properties.table.TableProperty;

public abstract class Executor {
    private static final Logger LOGGER = LoggerFactory.getLogger(Executor.class);
    private static final Predicate<String> LOWER_ALPHANUMERICS_AND_DASHES = Pattern.compile("^[a-z0-9-]+$").asPredicate();

    protected final InstanceProperties instanceProperties;
    protected final TablePropertiesProvider tablePropertiesProvider;
    protected final AmazonS3 s3Client;

    public Executor(InstanceProperties instanceProperties, TablePropertiesProvider tablePropertiesProvider, AmazonS3 amazonS3Client) {
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
        LOGGER.info("Submitting job");
        runJobOnPlatform(bulkImportJob);
        LOGGER.info("Successfully submitted job");
    }

    protected abstract void runJobOnPlatform(BulkImportJob bulkImportJob);

    protected abstract Map<String, String> getDefaultSparkConfig(BulkImportJob bulkImportJob, Map<String, String> platformSpec, TableProperties tableProperties, InstanceProperties instanceProperties);

    protected abstract String getJarLocation();

    protected List<String> constructArgs(BulkImportJob bulkImportJob) {
        Map<String, String> config = getDefaultSparkConfig(bulkImportJob,
            bulkImportJob.getPlatformSpec(), tablePropertiesProvider.getTableProperties(bulkImportJob.getTableName()), instanceProperties);
        Map<String, String> userConfig = bulkImportJob.getSparkConf();
        if (null != userConfig) {
            config.putAll(userConfig);
        }
        LOGGER.info("Using Spark config {}", config);

        String className = bulkImportJob.getClassName() != null ? bulkImportJob.getClassName() : instanceProperties.get(BULK_IMPORT_CLASS_NAME);

        List<String> args = Lists.newArrayList("spark-submit", "--deploy-mode", "cluster", "--class", className);

        // Specifying these explicitly rather than relying on the default config
        // seems to be the only way to get YARN to know the number of cores being
        // used by each Spark executor.
        args.add("--executor-cores");
        args.add("" + instanceProperties.get(BULK_IMPORT_PERSISTENT_EMR_SPARK_EXECUTOR_CORES));
        args.add("--driver-cores");
        args.add("" + instanceProperties.get(BULK_IMPORT_PERSISTENT_EMR_SPARK_DRIVER_CORES));
        args.add("--executor-memory");
        args.add("" + instanceProperties.get(BULK_IMPORT_PERSISTENT_EMR_SPARK_EXECUTOR_MEMORY));
        args.add("--driver-memory");
        args.add("" + instanceProperties.get(BULK_IMPORT_PERSISTENT_EMR_SPARK_DRIVER_MEMORY));
        args.add("--num-executors");
        args.add("" + instanceProperties.get(BULK_IMPORT_PERSISTENT_EMR_SPARK_EXECUTOR_INSTANCES));

        for (Map.Entry<String, String> configurationItem : config.entrySet()) {
            args.add("--conf");
            args.add(configurationItem.getKey() + "=" + configurationItem.getValue());
        }

        args.add(getJarLocation());

        return args;
    }

    protected String getFromPlatformSpec(TableProperty tableProperty, Map<String, String> platformSpec, TableProperties tableProperties) {
        if (null == platformSpec) {
            return tableProperties.get(tableProperty);
        }
        return platformSpec.getOrDefault(tableProperty.getPropertyName(), tableProperties.get(tableProperty));
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

        if (failedChecks.size() > 0) {
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
}
