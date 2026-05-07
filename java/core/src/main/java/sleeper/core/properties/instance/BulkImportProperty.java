/*
 * Copyright 2022-2026 Crown Copyright
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

package sleeper.core.properties.instance;

import sleeper.core.properties.SleeperPropertyIndex;
import sleeper.core.properties.model.SleeperPropertyValueUtils;

import java.util.List;

import static sleeper.core.properties.instance.TableStateProperty.DEFAULT_TABLE_STATE_LAMBDA_MEMORY;

/**
 * Definitions of instance properties relating to bulk import. Also see {@link EMRProperty},
 * {@link EMRServerlessProperty}, {@link PersistentEMRProperty}, {@link EKSProperty}.
 */
public interface BulkImportProperty {
    UserDefinedInstanceProperty BULK_IMPORT_CLASS_NAME = Index.propertyBuilder("sleeper.bulk.import.class.name")
            .description("The class to use to perform the bulk import. The default value below uses Spark Dataframes. There is an " +
                    "alternative option that uses RDDs (sleeper.bulkimport.runner.rdd.BulkImportJobRDDDriver).")
            .defaultValue("sleeper.bulkimport.runner.dataframelocalsort.BulkImportDataframeLocalSortDriver")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT).build();
    UserDefinedInstanceProperty BULK_IMPORT_SPARK_SHUFFLE_MAPSTATUS_COMPRESSION_CODEC = Index.propertyBuilder("sleeper.bulk.import.emr.spark.shuffle.mapStatus.compression.codec")
            .description("The compression codec for map status results. Used to set spark.shuffle.mapStatus.compression.codec.\n" +
                    "Stops \"Decompression error: Version not supported\" errors - only a value of \"lz4\" has been tested.")
            .defaultValue("lz4")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty BULK_IMPORT_SPARK_SPECULATION = Index.propertyBuilder("sleeper.bulk.import.emr.spark.speculation")
            .description("If true then speculative execution of tasks will be performed. Used to set spark.speculation.\n" +
                    "See https://spark.apache.org/docs/latest/configuration.html.")
            .defaultValue("false")
            .validationPredicate(SleeperPropertyValueUtils::isTrueOrFalse)
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty BULK_IMPORT_SPARK_SPECULATION_QUANTILE = Index.propertyBuilder("sleeper.bulk.import.spark.speculation.quantile")
            .description("Fraction of tasks which must be complete before speculation is enabled for a particular stage. Used to set spark.speculation.quantile.\n" +
                    "See https://spark.apache.org/docs/latest/configuration.html.")
            .defaultValue("0.75")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty BULK_IMPORT_SPARK_EXECUTOR_EXTRA_JAVA_OPTIONS = Index.propertyBuilder("sleeper.bulk.import.spark.executor.extra.java.options")
            .description("JVM options passed to the executors. Used to set spark.executor.extraJavaOptions.\n" +
                    "See https://spark.apache.org/docs/latest/configuration.html.")
            .defaultValue(
                    "-XX:+UseG1GC -XX:+UnlockDiagnosticVMOptions -XX:+G1SummarizeConcMark -XX:InitiatingHeapOccupancyPercent=35 -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:OnOutOfMemoryError='kill -9 %p'")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT).build();
    UserDefinedInstanceProperty BULK_IMPORT_SPARK_DRIVER_EXTRA_JAVA_OPTIONS = Index.propertyBuilder("sleeper.bulk.import.spark.driver.extra.java.options")
            .description("JVM options passed to the driver. Used to set spark.driver.extraJavaOptions.\n" +
                    "See https://spark.apache.org/docs/latest/configuration.html.")
            .defaultValue(BULK_IMPORT_SPARK_EXECUTOR_EXTRA_JAVA_OPTIONS.getDefaultValue())
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT).build();
    UserDefinedInstanceProperty BULK_IMPORT_SPARK_EXECUTOR_HEARTBEAT_INTERVAL = Index.propertyBuilder("sleeper.bulk.import.spark.executor.heartbeat.interval")
            .description("The interval between heartbeats from executors to the driver. Used to set spark.executor.heartbeatInterval.\n" +
                    "See https://spark.apache.org/docs/latest/configuration.html.")
            .defaultValue("60s")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT).build();
    UserDefinedInstanceProperty BULK_IMPORT_SPARK_NETWORK_TIMEOUT = Index.propertyBuilder("sleeper.bulk.import.spark.network.timeout")
            .description("The default timeout for network interactions in Spark. Used to set spark.network.timeout.\n" +
                    "See https://spark.apache.org/docs/latest/configuration.html.")
            .defaultValue("800s")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT).build();
    UserDefinedInstanceProperty BULK_IMPORT_SPARK_MEMORY_FRACTION = Index.propertyBuilder("sleeper.bulk.import.spark.memory.fraction")
            .description("The fraction of heap space used for execution and storage. Used to set spark.memory.fraction.\n" +
                    "See https://spark.apache.org/docs/latest/configuration.html.")
            .defaultValue("0.80")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT).build();
    UserDefinedInstanceProperty BULK_IMPORT_SPARK_MEMORY_STORAGE_FRACTION = Index.propertyBuilder("sleeper.bulk.import.spark.memory.storage.fraction")
            .description("The amount of storage memory immune to eviction, expressed as a fraction of the heap space used for execution and storage. " +
                    "Used to set spark.memory.storageFraction.\n" +
                    "See https://spark.apache.org/docs/latest/configuration.html.")
            .defaultValue("0.30")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT).build();
    UserDefinedInstanceProperty BULK_IMPORT_SPARK_STORAGE_LEVEL = Index.propertyBuilder("sleeper.bulk.import.spark.storage.level")
            .description("The storage to use for temporary caching. Used to set spark.storage.level.\n" +
                    "See https://aws.amazon.com/blogs/big-data/best-practices-for-successfully-managing-memory-for-apache-spark-applications-on-amazon-emr/.")
            .defaultValue("MEMORY_AND_DISK_SER")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT).build();
    UserDefinedInstanceProperty BULK_IMPORT_SPARK_RDD_COMPRESS = Index.propertyBuilder("sleeper.bulk.import.spark.rdd.compress")
            .description("Whether to compress serialized RDD partitions. Used to set spark.rdd.compress.\n" +
                    "See https://spark.apache.org/docs/latest/configuration.html.")
            .defaultValue("true")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT).build();
    UserDefinedInstanceProperty BULK_IMPORT_SPARK_SHUFFLE_COMPRESS = Index.propertyBuilder("sleeper.bulk.import.spark.shuffle.compress")
            .description("Whether to compress map output files. Used to set spark.shuffle.compress.\n" +
                    "See https://spark.apache.org/docs/latest/configuration.html.")
            .defaultValue("true")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT).build();
    UserDefinedInstanceProperty BULK_IMPORT_SPARK_SHUFFLE_SPILL_COMPRESS = Index.propertyBuilder("sleeper.bulk.import.spark.shuffle.spill.compress")
            .description("Whether to compress data spilled during shuffles. Used to set spark.shuffle.spill.compress.\n" +
                    "See https://spark.apache.org/docs/latest/configuration.html.")
            .defaultValue("true")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT).build();
    UserDefinedInstanceProperty BULK_IMPORT_STARTER_LAMBDA_MEMORY = Index.propertyBuilder("sleeper.bulk.import.starter.memory.mb")
            .description("The amount of memory in MB for lambda functions that start bulk import jobs.")
            .defaultProperty(DEFAULT_TABLE_STATE_LAMBDA_MEMORY)
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .runCdkDeployWhenChanged(true).build();

    static List<UserDefinedInstanceProperty> getAll() {
        return Index.INSTANCE.getAll();
    }

    /**
     * An index of property definitions in this file.
     */
    class Index {
        private Index() {
        }

        private static final SleeperPropertyIndex<UserDefinedInstanceProperty> INSTANCE = new SleeperPropertyIndex<>();

        private static UserDefinedInstancePropertyImpl.Builder propertyBuilder(String propertyName) {
            return UserDefinedInstancePropertyImpl.named(propertyName)
                    .addToIndex(INSTANCE::add);
        }
    }
}
