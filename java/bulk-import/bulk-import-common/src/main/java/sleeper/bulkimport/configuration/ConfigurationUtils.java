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
package sleeper.bulkimport.configuration;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.validation.EmrInstanceArchitecture;

import java.util.HashMap;
import java.util.Map;

import static sleeper.core.properties.instance.BulkImportProperty.BULK_IMPORT_SPARK_SHUFFLE_MAPSTATUS_COMPRESSION_CODEC;
import static sleeper.core.properties.instance.BulkImportProperty.BULK_IMPORT_SPARK_SPECULATION;
import static sleeper.core.properties.instance.BulkImportProperty.BULK_IMPORT_SPARK_SPECULATION_QUANTILE;
import static sleeper.core.properties.instance.CommonProperty.MAXIMUM_CONNECTIONS_TO_S3;
import static sleeper.core.properties.instance.EMRProperty.BULK_IMPORT_EMR_SPARK_DEFAULT_PARALLELISM;
import static sleeper.core.properties.instance.EMRProperty.BULK_IMPORT_EMR_SPARK_DRIVER_CORES;
import static sleeper.core.properties.instance.EMRProperty.BULK_IMPORT_EMR_SPARK_DRIVER_EXTRA_JAVA_OPTIONS;
import static sleeper.core.properties.instance.EMRProperty.BULK_IMPORT_EMR_SPARK_DRIVER_MEMORY;
import static sleeper.core.properties.instance.EMRProperty.BULK_IMPORT_EMR_SPARK_DRIVER_MEMORY_OVERHEAD;
import static sleeper.core.properties.instance.EMRProperty.BULK_IMPORT_EMR_SPARK_DYNAMIC_ALLOCATION_ENABLED;
import static sleeper.core.properties.instance.EMRProperty.BULK_IMPORT_EMR_SPARK_EXECUTOR_CORES;
import static sleeper.core.properties.instance.EMRProperty.BULK_IMPORT_EMR_SPARK_EXECUTOR_EXTRA_JAVA_OPTIONS;
import static sleeper.core.properties.instance.EMRProperty.BULK_IMPORT_EMR_SPARK_EXECUTOR_HEARTBEAT_INTERVAL;
import static sleeper.core.properties.instance.EMRProperty.BULK_IMPORT_EMR_SPARK_EXECUTOR_INSTANCES;
import static sleeper.core.properties.instance.EMRProperty.BULK_IMPORT_EMR_SPARK_EXECUTOR_MEMORY;
import static sleeper.core.properties.instance.EMRProperty.BULK_IMPORT_EMR_SPARK_EXECUTOR_MEMORY_OVERHEAD;
import static sleeper.core.properties.instance.EMRProperty.BULK_IMPORT_EMR_SPARK_MEMORY_FRACTION;
import static sleeper.core.properties.instance.EMRProperty.BULK_IMPORT_EMR_SPARK_MEMORY_STORAGE_FRACTION;
import static sleeper.core.properties.instance.EMRProperty.BULK_IMPORT_EMR_SPARK_NETWORK_TIMEOUT;
import static sleeper.core.properties.instance.EMRProperty.BULK_IMPORT_EMR_SPARK_RDD_COMPRESS;
import static sleeper.core.properties.instance.EMRProperty.BULK_IMPORT_EMR_SPARK_SHUFFLE_COMPRESS;
import static sleeper.core.properties.instance.EMRProperty.BULK_IMPORT_EMR_SPARK_SHUFFLE_SPILL_COMPRESS;
import static sleeper.core.properties.instance.EMRProperty.BULK_IMPORT_EMR_SPARK_SQL_SHUFFLE_PARTITIONS;
import static sleeper.core.properties.instance.EMRProperty.BULK_IMPORT_EMR_SPARK_STORAGE_LEVEL;
import static sleeper.core.properties.instance.EMRProperty.BULK_IMPORT_EMR_SPARK_YARN_SCHEDULER_REPORTER_THREAD_MAX_FAILURES;
import static sleeper.core.properties.instance.EMRServerlessProperty.BULK_IMPORT_EMR_SERVERLESS_DRIVER_CORES;
import static sleeper.core.properties.instance.EMRServerlessProperty.BULK_IMPORT_EMR_SERVERLESS_DRIVER_MEMORY;
import static sleeper.core.properties.instance.EMRServerlessProperty.BULK_IMPORT_EMR_SERVERLESS_DYNAMIC_ALLOCATION;
import static sleeper.core.properties.instance.EMRServerlessProperty.BULK_IMPORT_EMR_SERVERLESS_EXECUTOR_CORES;
import static sleeper.core.properties.instance.EMRServerlessProperty.BULK_IMPORT_EMR_SERVERLESS_EXECUTOR_DISK;
import static sleeper.core.properties.instance.EMRServerlessProperty.BULK_IMPORT_EMR_SERVERLESS_EXECUTOR_INSTANCES;
import static sleeper.core.properties.instance.EMRServerlessProperty.BULK_IMPORT_EMR_SERVERLESS_EXECUTOR_MEMORY;
import static sleeper.core.properties.instance.EMRServerlessProperty.BULK_IMPORT_EMR_SERVERLESS_SPARK_DEFAULT_PARALLELISM;
import static sleeper.core.properties.instance.EMRServerlessProperty.BULK_IMPORT_EMR_SERVERLESS_SPARK_EXECUTOR_HEARTBEAT_INTERVAL;
import static sleeper.core.properties.instance.EMRServerlessProperty.BULK_IMPORT_EMR_SERVERLESS_SPARK_MEMORY_FRACTION;
import static sleeper.core.properties.instance.EMRServerlessProperty.BULK_IMPORT_EMR_SERVERLESS_SPARK_MEMORY_STORAGE_FRACTION;
import static sleeper.core.properties.instance.EMRServerlessProperty.BULK_IMPORT_EMR_SERVERLESS_SPARK_NETWORK_TIMEOUT;
import static sleeper.core.properties.instance.EMRServerlessProperty.BULK_IMPORT_EMR_SERVERLESS_SPARK_RDD_COMPRESS;
import static sleeper.core.properties.instance.EMRServerlessProperty.BULK_IMPORT_EMR_SERVERLESS_SPARK_SHUFFLE_COMPRESS;
import static sleeper.core.properties.instance.EMRServerlessProperty.BULK_IMPORT_EMR_SERVERLESS_SPARK_SHUFFLE_MAPSTATUS_COMPRESSION_CODEC;
import static sleeper.core.properties.instance.EMRServerlessProperty.BULK_IMPORT_EMR_SERVERLESS_SPARK_SHUFFLE_SPILL_COMPRESS;
import static sleeper.core.properties.instance.EMRServerlessProperty.BULK_IMPORT_EMR_SERVERLESS_SPARK_SPECULATION;
import static sleeper.core.properties.instance.EMRServerlessProperty.BULK_IMPORT_EMR_SERVERLESS_SPARK_SPECULATION_QUANTILE;
import static sleeper.core.properties.instance.EMRServerlessProperty.BULK_IMPORT_EMR_SERVERLESS_SPARK_SQL_SHUFFLE_PARTITIONS;
import static sleeper.core.properties.validation.EmrInstanceArchitecture.ARM64;
import static sleeper.core.properties.validation.EmrInstanceArchitecture.X86_64;

/**
 * Properties in this class are based on AWS recommended values. See this blog for details:
 * https://aws.amazon.com/blogs/big-data/best-practices-for-successfully-managing-memory-for-apache-spark-applications-on-amazon-emr/
 */
public class ConfigurationUtils {

    private static final String JAVA_HOME = "/usr/lib/jvm/java-17-amazon-corretto.%s";

    private ConfigurationUtils() {
    }

    public static Map<String, String> getSparkConfigurationFromInstanceProperties(
            InstanceProperties instanceProperties, EmrInstanceArchitecture arch) {
        Map<String, String> sparkConf = new HashMap<>();

        // spark.driver properties
        sparkConf.put("spark.driver.cores", instanceProperties.get(BULK_IMPORT_EMR_SPARK_DRIVER_CORES));
        sparkConf.put("spark.driver.extraJavaOptions", instanceProperties.get(BULK_IMPORT_EMR_SPARK_DRIVER_EXTRA_JAVA_OPTIONS));
        sparkConf.put("spark.driver.memory", instanceProperties.get(BULK_IMPORT_EMR_SPARK_DRIVER_MEMORY));

        // spark.executor properties
        sparkConf.put("spark.executor.cores", instanceProperties.get(BULK_IMPORT_EMR_SPARK_EXECUTOR_CORES));
        sparkConf.put("spark.executor.extraJavaOptions", instanceProperties.get(BULK_IMPORT_EMR_SPARK_EXECUTOR_EXTRA_JAVA_OPTIONS));
        sparkConf.put("spark.executor.heartbeatInterval", instanceProperties.get(BULK_IMPORT_EMR_SPARK_EXECUTOR_HEARTBEAT_INTERVAL));
        sparkConf.put("spark.executor.instances", instanceProperties.get(BULK_IMPORT_EMR_SPARK_EXECUTOR_INSTANCES));
        sparkConf.put("spark.executor.memory", instanceProperties.get(BULK_IMPORT_EMR_SPARK_EXECUTOR_MEMORY));

        // spark.yarn properties
        sparkConf.put("spark.driver.memoryOverhead", instanceProperties.get(BULK_IMPORT_EMR_SPARK_DRIVER_MEMORY_OVERHEAD));
        sparkConf.put("spark.executor.memoryOverhead", instanceProperties.get(BULK_IMPORT_EMR_SPARK_EXECUTOR_MEMORY_OVERHEAD));
        sparkConf.put("spark.yarn.scheduler.reporterThread.maxFailures", instanceProperties.get(BULK_IMPORT_EMR_SPARK_YARN_SCHEDULER_REPORTER_THREAD_MAX_FAILURES));

        // spark.default properties
        sparkConf.put("spark.default.parallelism", instanceProperties.get(BULK_IMPORT_EMR_SPARK_DEFAULT_PARALLELISM));

        // spark.network properties
        sparkConf.put("spark.network.timeout", instanceProperties.get(BULK_IMPORT_EMR_SPARK_NETWORK_TIMEOUT));

        // spark.dynamicAllocation properties
        sparkConf.put("spark.dynamicAllocation.enabled", instanceProperties.get(BULK_IMPORT_EMR_SPARK_DYNAMIC_ALLOCATION_ENABLED));

        // spark.memory properties
        sparkConf.put("spark.memory.fraction", instanceProperties.get(BULK_IMPORT_EMR_SPARK_MEMORY_FRACTION));
        sparkConf.put("spark.memory.storageFraction", instanceProperties.get(BULK_IMPORT_EMR_SPARK_MEMORY_STORAGE_FRACTION));

        // spark.storage properties
        sparkConf.put("spark.storage.level", instanceProperties.get(BULK_IMPORT_EMR_SPARK_STORAGE_LEVEL));

        // spark.rdd properties
        sparkConf.put("spark.rdd.compress", instanceProperties.get(BULK_IMPORT_EMR_SPARK_RDD_COMPRESS));

        // spark.shuffle properties
        sparkConf.put("spark.shuffle.compress", instanceProperties.get(BULK_IMPORT_EMR_SPARK_SHUFFLE_COMPRESS));
        sparkConf.put("spark.shuffle.spill.compress", instanceProperties.get(BULK_IMPORT_EMR_SPARK_SHUFFLE_SPILL_COMPRESS));
        // The following value is not mentioned in the blog linked above, but setting this explicitly
        // was found necessary to stop "Decompression error: Version not supported" errors -
        // only a value of "lz4" has been tested.
        sparkConf.put("spark.shuffle.mapStatus.compression.codec", instanceProperties.get(BULK_IMPORT_SPARK_SHUFFLE_MAPSTATUS_COMPRESSION_CODEC));

        // spark.speculation properties (not referenced in the blog linked above)
        sparkConf.put("spark.speculation", instanceProperties.get(BULK_IMPORT_SPARK_SPECULATION));
        sparkConf.put("spark.speculation.quantile", instanceProperties.get(BULK_IMPORT_SPARK_SPECULATION_QUANTILE));

        // spark.hadoop properties (not referenced in the blog linked above)
        sparkConf.put("spark.hadoop.fs.s3a.connection.maximum", instanceProperties.get(MAXIMUM_CONNECTIONS_TO_S3));

        // spark.sql properties
        sparkConf.put("spark.sql.shuffle.partitions", instanceProperties.get(BULK_IMPORT_EMR_SPARK_SQL_SHUFFLE_PARTITIONS));

        // Set JAVA_HOME explicitly
        sparkConf.put("spark.executorEnv.JAVA_HOME", getJavaHome(arch));

        return sparkConf;
    }

    public static Map<String, String> getSparkServerlessConfigurationFromInstanceProperties(
            InstanceProperties instanceProperties, EmrInstanceArchitecture arch) {
        Map<String, String> sparkConf = new HashMap<>();
        // spark.driver properties
        sparkConf.put("spark.driver.cores", instanceProperties.get(BULK_IMPORT_EMR_SERVERLESS_DRIVER_CORES));
        sparkConf.put("spark.driver.memory", instanceProperties.get(BULK_IMPORT_EMR_SERVERLESS_DRIVER_MEMORY));

        // spark.executor properties
        sparkConf.put("spark.executor.cores", instanceProperties.get(BULK_IMPORT_EMR_SERVERLESS_EXECUTOR_CORES));
        sparkConf.put("spark.emr-serverless.executor.disk", instanceProperties.get(BULK_IMPORT_EMR_SERVERLESS_EXECUTOR_DISK));
        sparkConf.put("spark.executor.heartbeatInterval", instanceProperties.get(BULK_IMPORT_EMR_SERVERLESS_SPARK_EXECUTOR_HEARTBEAT_INTERVAL));
        sparkConf.put("spark.executor.instances", instanceProperties.get(BULK_IMPORT_EMR_SERVERLESS_EXECUTOR_INSTANCES));
        sparkConf.put("spark.executor.memory", instanceProperties.get(BULK_IMPORT_EMR_SERVERLESS_EXECUTOR_MEMORY));

        // spark.default properties
        sparkConf.put("spark.default.parallelism", instanceProperties.get(BULK_IMPORT_EMR_SERVERLESS_SPARK_DEFAULT_PARALLELISM));

        // spark.network properties
        sparkConf.put("spark.network.timeout", instanceProperties.get(BULK_IMPORT_EMR_SERVERLESS_SPARK_NETWORK_TIMEOUT));

        // spark.dynamicAllocation properties
        sparkConf.put("spark.dynamicAllocation.enabled", instanceProperties.get(BULK_IMPORT_EMR_SERVERLESS_DYNAMIC_ALLOCATION));

        // spark.memory properties
        sparkConf.put("spark.memory.fraction", instanceProperties.get(BULK_IMPORT_EMR_SERVERLESS_SPARK_MEMORY_FRACTION));
        sparkConf.put("spark.memory.storageFraction", instanceProperties.get(BULK_IMPORT_EMR_SERVERLESS_SPARK_MEMORY_STORAGE_FRACTION));

        // spark.rdd properties
        sparkConf.put("spark.rdd.compress", instanceProperties.get(BULK_IMPORT_EMR_SERVERLESS_SPARK_RDD_COMPRESS));

        // spark.shuffle properties
        sparkConf.put("spark.shuffle.compress", instanceProperties.get(BULK_IMPORT_EMR_SERVERLESS_SPARK_SHUFFLE_COMPRESS));
        sparkConf.put("spark.shuffle.spill.compress", instanceProperties.get(BULK_IMPORT_EMR_SERVERLESS_SPARK_SHUFFLE_SPILL_COMPRESS));
        // The following value is not mentioned in the blog linked above, but setting this explicitly
        // was found necessary to stop "Decompression error: Version not supported" errors -
        // only a value of "lz4" has been tested.
        sparkConf.put("spark.shuffle.mapStatus.compression.codec", instanceProperties.get(BULK_IMPORT_EMR_SERVERLESS_SPARK_SHUFFLE_MAPSTATUS_COMPRESSION_CODEC));

        // spark.speculation properties (not referenced in the blog linked above)
        sparkConf.put("spark.speculation", instanceProperties.get(BULK_IMPORT_EMR_SERVERLESS_SPARK_SPECULATION));
        sparkConf.put("spark.speculation.quantile", instanceProperties.get(BULK_IMPORT_EMR_SERVERLESS_SPARK_SPECULATION_QUANTILE));

        // spark.sql properties
        sparkConf.put("spark.sql.shuffle.partitions", instanceProperties.get(BULK_IMPORT_EMR_SERVERLESS_SPARK_SQL_SHUFFLE_PARTITIONS));

        // Set JAVA_HOME explicitly
        sparkConf.put("spark.executorEnv.JAVA_HOME", getJavaHome(arch));
        sparkConf.put("spark.emr-serverless.driverEnv.JAVA_HOME", getJavaHome(arch));

        return sparkConf;
    }

    public static Map<String, String> getSparkEMRConfiguration() {
        Map<String, String> sparkEmrConf = new HashMap<>();
        sparkEmrConf.put("maximizeResourceAllocation", "false");

        return sparkEmrConf;
    }

    public static Map<String, String> getYarnConfiguration() {
        Map<String, String> yarnConf = new HashMap<>();
        yarnConf.put("yarn.nodemanager.vmem-check-enabled", "false");
        yarnConf.put("yarn.nodemanager.pmem-check-enabled", "false");

        return yarnConf;
    }

    public static Map<String, String> getMapRedSiteConfiguration() {
        Map<String, String> mapRedSiteConf = new HashMap<>();
        mapRedSiteConf.put("mapreduce.map.output.compress", "true");

        return mapRedSiteConf;
    }

    public static Map<String, String> getJavaHomeConfiguration(EmrInstanceArchitecture arch) {
        Map<String, String> javaHomeConf = new HashMap<>();
        javaHomeConf.put("JAVA_HOME", getJavaHome(arch));
        return javaHomeConf;
    }

    public static String getJavaHome(EmrInstanceArchitecture arch) {
        if (arch == X86_64) {
            return String.format(JAVA_HOME, "x86_64");
        } else if (arch == ARM64) {
            return String.format(JAVA_HOME, "aarch64");
        } else {
            throw new IllegalArgumentException("Unrecognised architecture: " + arch);
        }
    }
}
