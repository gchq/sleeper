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
import sleeper.bulkimport.job.BulkImportJob;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.UserDefinedInstanceProperty;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.configuration.properties.table.TableProperty;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static sleeper.configuration.properties.SystemDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.BULK_IMPORT_PERSISTENT_EMR_SPARK_DEFAULT_PARALLELISM;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.BULK_IMPORT_PERSISTENT_EMR_SPARK_DRIVER_EXTRA_JAVA_OPTIONS;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.BULK_IMPORT_PERSISTENT_EMR_SPARK_DYNAMIC_ALLOCATION_ENABLED;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.BULK_IMPORT_PERSISTENT_EMR_SPARK_EXECUTOR_EXTRA_JAVA_OPTIONS;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.BULK_IMPORT_PERSISTENT_EMR_SPARK_EXECUTOR_HEARTBEAT_INTERVAL;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.BULK_IMPORT_PERSISTENT_EMR_SPARK_MEMORY_FRACTION;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.BULK_IMPORT_PERSISTENT_EMR_SPARK_MEMORY_STORAGE_FRACTION;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.BULK_IMPORT_PERSISTENT_EMR_SPARK_NETWORK_TIMEOUT;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.BULK_IMPORT_PERSISTENT_EMR_SPARK_RDD_COMPRESS;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.BULK_IMPORT_PERSISTENT_EMR_SPARK_SHUFFLE_COMPRESS;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.BULK_IMPORT_PERSISTENT_EMR_SPARK_SHUFFLE_SPILL_COMPRESS;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.BULK_IMPORT_PERSISTENT_EMR_SPARK_STORAGE_LEVEL;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.BULK_IMPORT_PERSISTENT_EMR_SPARK_YARN_DRIVER_MEMORY_OVERHEAD;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.BULK_IMPORT_PERSISTENT_EMR_SPARK_YARN_EXECUTOR_MEMORY_OVERHEAD;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.BULK_IMPORT_PERSISTENT_EMR_SPARK_YARN_SCHEDULER_REPORTER_THREAD_MAX_FAILURES;

public abstract class AbstractEmrExecutor extends Executor {

    public AbstractEmrExecutor(
            InstanceProperties instanceProperties,
            TablePropertiesProvider tablePropertiesProvider,
            AmazonS3 amazonS3Client) {
        super(instanceProperties, tablePropertiesProvider, amazonS3Client);
    }

    @Override
    protected Map<String, String> getDefaultSparkConfig(BulkImportJob bulkImportJob, Map<String, String> platformSpec, TableProperties tableProperties, InstanceProperties instanceProperties) {
        Map<String, String> defaultConfig = new HashMap<>();
        defaultConfig.put("spark.yarn.executor.memoryOverhead", instanceProperties.get(BULK_IMPORT_PERSISTENT_EMR_SPARK_YARN_EXECUTOR_MEMORY_OVERHEAD));
        defaultConfig.put("spark.yarn.driver.memoryOverhead", instanceProperties.get(BULK_IMPORT_PERSISTENT_EMR_SPARK_YARN_DRIVER_MEMORY_OVERHEAD));
        defaultConfig.put("spark.default.parallelism", instanceProperties.get(BULK_IMPORT_PERSISTENT_EMR_SPARK_DEFAULT_PARALLELISM));
        defaultConfig.put("spark.network.timeout", instanceProperties.get(BULK_IMPORT_PERSISTENT_EMR_SPARK_NETWORK_TIMEOUT));
        defaultConfig.put("spark.executor.heartbeatInterval", instanceProperties.get(BULK_IMPORT_PERSISTENT_EMR_SPARK_EXECUTOR_HEARTBEAT_INTERVAL));
        defaultConfig.put("spark.dynamicAllocation.enabled", instanceProperties.get(BULK_IMPORT_PERSISTENT_EMR_SPARK_DYNAMIC_ALLOCATION_ENABLED));
        defaultConfig.put("spark.memory.fraction", instanceProperties.get(BULK_IMPORT_PERSISTENT_EMR_SPARK_MEMORY_FRACTION));
        defaultConfig.put("spark.memory.storageFraction", instanceProperties.get(BULK_IMPORT_PERSISTENT_EMR_SPARK_MEMORY_STORAGE_FRACTION));
        defaultConfig.put("spark.executor.extraJavaOptions", instanceProperties.get(BULK_IMPORT_PERSISTENT_EMR_SPARK_EXECUTOR_EXTRA_JAVA_OPTIONS));
        defaultConfig.put("spark.driver.extraJavaOptions", instanceProperties.get(BULK_IMPORT_PERSISTENT_EMR_SPARK_DRIVER_EXTRA_JAVA_OPTIONS));
        defaultConfig.put("spark.yarn.scheduler.reporterThread.maxFailures", instanceProperties.get(BULK_IMPORT_PERSISTENT_EMR_SPARK_YARN_SCHEDULER_REPORTER_THREAD_MAX_FAILURES));
        defaultConfig.put("spark.storage.level", instanceProperties.get(BULK_IMPORT_PERSISTENT_EMR_SPARK_STORAGE_LEVEL));
        defaultConfig.put("spark.rdd.compress", instanceProperties.get(BULK_IMPORT_PERSISTENT_EMR_SPARK_RDD_COMPRESS));
        defaultConfig.put("spark.shuffle.compress", instanceProperties.get(BULK_IMPORT_PERSISTENT_EMR_SPARK_SHUFFLE_COMPRESS));
        defaultConfig.put("spark.shuffle.spill.compress", instanceProperties.get(BULK_IMPORT_PERSISTENT_EMR_SPARK_SHUFFLE_SPILL_COMPRESS));
        defaultConfig.put("spark.shuffle.mapStatus.compression.codec", getFromPlatformSpec(TableProperty.BULK_IMPORT_SPARK_SHUFFLE_MAPSTATUS_COMPRESSION_CODEC, platformSpec, tableProperties));
        defaultConfig.put("spark.speculation", getFromPlatformSpec(TableProperty.BULK_IMPORT_SPARK_SPECULATION, platformSpec, tableProperties));
        defaultConfig.put("spark.speculation.quantile", getFromPlatformSpec(TableProperty.BULK_IMPORT_SPARK_SPECULATION_QUANTILE, platformSpec, tableProperties));
        defaultConfig.put("spark.hadoop.fs.s3a.connection.maximum", instanceProperties.get(UserDefinedInstanceProperty.MAXIMUM_CONNECTIONS_TO_S3));
        return defaultConfig;
    }

    @Override
    protected List<String> constructArgs(BulkImportJob bulkImportJob) {
        List<String> args = super.constructArgs(bulkImportJob);
        args.add(bulkImportJob.getId());
        args.add(instanceProperties.get(CONFIG_BUCKET));
        return args;
    }

    @Override
    protected String getJarLocation() {
        return "s3a://"
                + instanceProperties.get(UserDefinedInstanceProperty.JARS_BUCKET)
                + "/bulk-import-runner-"
                + instanceProperties.get(UserDefinedInstanceProperty.VERSION) + ".jar";
    }
}
