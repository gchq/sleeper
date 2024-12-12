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

package sleeper.core.properties.instance;

import sleeper.core.properties.SleeperPropertyIndex;
import sleeper.core.properties.validation.EmrInstanceArchitecture;
import sleeper.core.properties.validation.SleeperPropertyValueUtils;

import java.util.List;
import java.util.function.Predicate;

import static sleeper.core.properties.instance.NonPersistentEMRProperty.DEFAULT_BULK_IMPORT_EMR_RELEASE_LABEL;

/**
 * Definitions of instance properties relating to bulk import on AWS EMR Serverless.
 */
public interface EMRServerlessProperty {

    UserDefinedInstanceProperty BULK_IMPORT_EMR_SERVERLESS_ARCHITECTURE = Index
            .propertyBuilder("sleeper.bulk.import.emr.serverless.architecture")
            .description("The architecture for EMR Serverless to use. X86_64 or ARM64 (Coming soon)")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT).defaultValue(EmrInstanceArchitecture.X86_64.toString())
            .validationPredicate(Predicate.isEqual(EmrInstanceArchitecture.X86_64.toString()))
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_SERVERLESS_RELEASE = Index
            .propertyBuilder("sleeper.bulk.import.emr.serverless.release")
            .description("The version of EMR Serverless to use.")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .defaultValue(DEFAULT_BULK_IMPORT_EMR_RELEASE_LABEL.getDefaultValue())
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_SERVERLESS_CUSTOM_IMAGE_REPO = Index
            .propertyBuilder("sleeper.bulk.import.emr.serverless.repo")
            .description("The name of the repository for the EMR serverless container. "
                    + "The Docker image from the bulk-import module "
                    + "should have been uploaded to an ECR repository of this name in this account.")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_SERVERLESS_AUTOSTART = Index
            .propertyBuilder("sleeper.bulk.import.emr.serverless.autostart.enabled")
            .description("Set to true to allow an EMR Serverless Application to start "
                    + "automatically when a job is submitted.")
            .defaultValue("true")
            .validationPredicate(SleeperPropertyValueUtils::isTrueOrFalse)
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_SERVERLESS_AUTOSTOP = Index
            .propertyBuilder("sleeper.bulk.import.emr.serverless.autostop.enabled")
            .description("Set to true to allow an EMR Serverless Application to stop "
                    + "automatically when there are no jobs to process.\n"
                    + "Turning this off with pre-initialised capacity turned off is not recommended.")
            .defaultValue("true")
            .validationPredicate(SleeperPropertyValueUtils::isTrueOrFalse)
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_SERVERLESS_AUTOSTOP_TIMEOUT_MINUTES = Index
            .propertyBuilder("sleeper.bulk.import.emr.serverless.autostop.timeout")
            .description("The number of minutes of inactivity before EMR Serverless stops the application.")
            .defaultValue("15")
            .validationPredicate(SleeperPropertyValueUtils::isInteger)
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_SERVERLESS_EXECUTOR_CORES = Index
            .propertyBuilder("sleeper.bulk.import.emr.serverless.spark.executor.cores")
            .description("The number of cores used by a Serverless executor. Used to set spark.executor.cores.\n"
                    + "See https://spark.apache.org/docs/latest/configuration.html.")
            .defaultValue("4")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT).build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_SERVERLESS_EXECUTOR_MEMORY = Index
            .propertyBuilder("sleeper.bulk.import.emr.serverless.spark.executor.memory")
            .description("The amount of memory allocated to a Serverless executor. Used to set spark.executor.memory.\n"
                    + "See https://spark.apache.org/docs/latest/configuration.html.")
            .defaultValue("16G")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT).build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_SERVERLESS_EXECUTOR_DISK = Index
            .propertyBuilder("sleeper.bulk.import.emr.serverless.spark.emr-serverless.executor.disk")
            .description("The amount of storage allocated to a Serverless executor.\n"
                    + "See https://spark.apache.org/docs/latest/configuration.html.")
            .defaultValue("200G")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT).build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_SERVERLESS_EXECUTOR_INSTANCES = Index
            .propertyBuilder("sleeper.bulk.import.emr.serverless.spark.executor.instances")
            .description("The number of executors to be used with Serverless. Used to set spark.executor.instances.\n"
                    + "See https://spark.apache.org/docs/latest/configuration.html.")
            .defaultValue("36")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT).build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_SERVERLESS_DRIVER_CORES = Index
            .propertyBuilder("sleeper.bulk.import.emr.serverless.spark.driver.cores")
            .description("The number of cores used by the Serverless Spark driver. Used to set spark.driver.cores.\n"
                    + "See https://spark.apache.org/docs/latest/configuration.html.")
            .defaultValue(BULK_IMPORT_EMR_SERVERLESS_EXECUTOR_CORES.getDefaultValue())
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT).build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_SERVERLESS_DRIVER_MEMORY = Index
            .propertyBuilder("sleeper.bulk.import.emr.serverless.spark.driver.memory")
            .description("The amount of memory allocated to the Serverless Spark driver. Used to set spark.driver.memory.\n"
                    + "See https://spark.apache.org/docs/latest/configuration.html.")
            .defaultValue(BULK_IMPORT_EMR_SERVERLESS_EXECUTOR_MEMORY.getDefaultValue())
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT).build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_SERVERLESS_JAVA_HOME = Index
            .propertyBuilder("sleeper.bulk.import.emr.serverless.spark.executorEnv.JAVA_HOME")
            .description("The path to JAVA_HOME to be used by the custom image for bulk import.")
            .defaultValue("/usr/lib/jvm/jre-11")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT).build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_SERVERLESS_DYNAMIC_ALLOCATION = Index
            .propertyBuilder("sleeper.bulk.import.emr.serverless.spark.dynamic.allocation.enabled")
            .description("Whether Spark should use dynamic allocation to scale resources up and down. "
                    + "Used to set spark.dynamicAllocation.enabled. See https://spark.apache.org/docs/latest/configuration.html.")
            .defaultValue("false")
            .validationPredicate(SleeperPropertyValueUtils::isTrueOrFalse)
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT).build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_SERVERLESS_SPARK_RDD_COMPRESS = Index.propertyBuilder("sleeper.bulk.import.emr.serverless.spark.rdd.compress")
            .description("Whether to compress serialized RDD partitions. Used to set spark.rdd.compress.\n" +
                    "See https://spark.apache.org/docs/latest/configuration.html.")
            .defaultValue("true")
            .validationPredicate(SleeperPropertyValueUtils::isTrueOrFalse)
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT).build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_SERVERLESS_SPARK_SHUFFLE_COMPRESS = Index.propertyBuilder("sleeper.bulk.import.emr.serverless.spark.shuffle.compress")
            .description("Whether to compress map output files. Used to set spark.shuffle.compress.\n" +
                    "See https://spark.apache.org/docs/latest/configuration.html.")
            .defaultValue("true")
            .validationPredicate(SleeperPropertyValueUtils::isTrueOrFalse)
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT).build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_SERVERLESS_SPARK_SHUFFLE_SPILL_COMPRESS = Index.propertyBuilder("sleeper.bulk.import.emr.serverless.spark.shuffle.spill.compress")
            .description("Whether to compress data spilled during shuffles. Used to set spark.shuffle.spill.compress.\n" +
                    "See https://spark.apache.org/docs/latest/configuration.html.")
            .defaultValue("true")
            .validationPredicate(SleeperPropertyValueUtils::isTrueOrFalse)
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT).build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_SERVERLESS_SPARK_DEFAULT_PARALLELISM = Index.propertyBuilder("sleeper.bulk.import.emr.serverless.spark.default.parallelism")
            .description("The default parallelism for Spark job. Used to set spark.default.parallelism.\n" +
                    "See https://spark.apache.org/docs/latest/configuration.html.")
            .defaultValue("288")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT).build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_SERVERLESS_SPARK_SQL_SHUFFLE_PARTITIONS = Index.propertyBuilder("sleeper.bulk.import.emr.serverless.spark.sql.shuffle.partitions")
            .description("The number of partitions used in a Spark SQL/dataframe shuffle operation. Used to set spark.sql.shuffle.partitions.\n" +
                    "See https://spark.apache.org/docs/latest/configuration.html.")
            .defaultValue(BULK_IMPORT_EMR_SERVERLESS_SPARK_DEFAULT_PARALLELISM.getDefaultValue())
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT).build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_SERVERLESS_SPARK_NETWORK_TIMEOUT = Index.propertyBuilder("sleeper.bulk.import.emr.serverless.spark.network.timeout")
            .description("The default timeout for network interactions in Spark. Used to set spark.network.timeout.\n" +
                    "See https://spark.apache.org/docs/latest/configuration.html.")
            .defaultValue("800s")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT).build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_SERVERLESS_SPARK_EXECUTOR_HEARTBEAT_INTERVAL = Index.propertyBuilder("sleeper.bulk.import.emr.serverless.spark.executor.heartbeat.interval")
            .description("(The interval between heartbeats from executors to the driver. Used to set spark.executor.heartbeatInterval.\n" +
                    "See https://spark.apache.org/docs/latest/configuration.html.")
            .defaultValue("60s")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT).build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_SERVERLESS_SPARK_MEMORY_FRACTION = Index.propertyBuilder("sleeper.bulk.import.emr.serverless.spark.memory.fraction")
            .description("The fraction of heap space used for execution and storage. Used to set spark.memory.fraction.\n" +
                    "See https://spark.apache.org/docs/latest/configuration.html.")
            .defaultValue("0.80")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT).build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_SERVERLESS_SPARK_MEMORY_STORAGE_FRACTION = Index.propertyBuilder("sleeper.bulk.import.emr.serverless.spark.memory.storage.fraction")
            .description("The amount of storage memory immune to eviction, expressed as a fraction of the heap space used for execution and storage. " +
                    "Used to set spark.memory.storageFraction.\n" +
                    "See https://spark.apache.org/docs/latest/configuration.html.")
            .defaultValue("0.30")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT).build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_SERVERLESS_SPARK_SPECULATION = Index.propertyBuilder("sleeper.bulk.import.emr.serverless.spark.speculation")
            .description("If true then speculative execution of tasks will be performed. Used to set spark.speculation.\n" +
                    "See https://spark.apache.org/docs/latest/configuration.html.")
            .defaultValue("false")
            .validationPredicate(SleeperPropertyValueUtils::isTrueOrFalse)
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT).build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_SERVERLESS_SPARK_SPECULATION_QUANTILE = Index.propertyBuilder("sleeper.bulk.import.emr.serverless.spark.speculation.quantile")
            .description("Fraction of tasks which must be complete before speculation is enabled for a particular stage. Used to set spark.speculation.quantile.\n" +
                    "See https://spark.apache.org/docs/latest/configuration.html.")
            .defaultValue("0.75")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT).build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_SERVERLESS_SPARK_SHUFFLE_MAPSTATUS_COMPRESSION_CODEC = Index
            .propertyBuilder("sleeper.bulk.import.emr.serverless.spark.shuffle.mapStatus.compression.codec")
            .description("The compression codec for map status results. Used to set spark.shuffle.mapStatus.compression.codec.\n" +
                    "Stops \"Decompression error: Version not supported\" errors - only a value of \"lz4\" has been tested.")
            .defaultValue("lz4")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT).build();

    UserDefinedInstanceProperty BULK_IMPORT_EMR_SERVERLESS_INITIAL_CAPACITY_ENABLED = Index.propertyBuilder("sleeper.bulk.import.emr.serverless.initial.capacity.enabled")
            .description("Set to enable the pre-initialise capacity option for EMR Serverless application.\n" +
                    "See: https://docs.aws.amazon.com/emr/latest/EMR-Serverless-UserGuide/pre-init-capacity.html")
            .defaultValue("false")
            .validationPredicate(SleeperPropertyValueUtils::isTrueOrFalse)
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_SERVERLESS_INITIAL_CAPACITY_EXECUTOR_COUNT = Index.propertyBuilder("sleeper.bulk.import.emr.serverless.initial.capacity.executor.count")
            .description("The number of executors to pre-initialise.\n" +
                    "See: https://docs.aws.amazon.com/emr/latest/EMR-Serverless-UserGuide/pre-init-capacity.html")
            .defaultValue("72")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_SERVERLESS_INITIAL_CAPACITY_EXECUTOR_CORES = Index.propertyBuilder("sleeper.bulk.import.emr.serverless.initial.capacity.executor.cores")
            .description("The amount of CPUs per executor for the pre-initialise capacity.\n" +
                    "See: https://docs.aws.amazon.com/emr/latest/EMR-Serverless-UserGuide/pre-init-capacity.html")
            .defaultValue("4vCPU")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_SERVERLESS_INITIAL_CAPACITY_EXECUTOR_MEMORY = Index.propertyBuilder("sleeper.bulk.import.emr.serverless.initial.capacity.executor.memory")
            .description("The amount of memory per executor for the pre-initialise capacity.\n" +
                    "See: https://docs.aws.amazon.com/emr/latest/EMR-Serverless-UserGuide/pre-init-capacity.html")
            .defaultValue("18GB")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_SERVERLESS_INITIAL_CAPACITY_EXECUTOR_DISK = Index.propertyBuilder("sleeper.bulk.import.emr.serverless.initial.capacity.executor.disk")
            .description("The amount of storage per executor for the pre-initialise capacity.\n" +
                    "See: https://docs.aws.amazon.com/emr/latest/EMR-Serverless-UserGuide/pre-init-capacity.html")
            .defaultValue("200GB")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .runCdkDeployWhenChanged(true).build();

    UserDefinedInstanceProperty BULK_IMPORT_EMR_SERVERLESS_INITIAL_CAPACITY_DRIVER_COUNT = Index.propertyBuilder("sleeper.bulk.import.emr.serverless.initial.capacity.driver.count")
            .description("The number of drivers to pre-initialise.\n" +
                    "See: https://docs.aws.amazon.com/emr/latest/EMR-Serverless-UserGuide/pre-init-capacity.html")
            .defaultValue("5")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_SERVERLESS_INITIAL_CAPACITY_DRIVER_CORES = Index.propertyBuilder("sleeper.bulk.import.emr.serverless.initial.capacity.driver.cores")
            .description("The amount of CPUs per driver for the pre-initialise capacity.\n" +
                    "See: https://docs.aws.amazon.com/emr/latest/EMR-Serverless-UserGuide/pre-init-capacity.html")
            .defaultValue("4vCPU")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_SERVERLESS_INITIAL_CAPACITY_DRIVER_MEMORY = Index.propertyBuilder("sleeper.bulk.import.emr.serverless.initial.capacity.driver.memory")
            .description("The amount of memory per driver for the pre-initialise capacity.\n" +
                    "See: https://docs.aws.amazon.com/emr/latest/EMR-Serverless-UserGuide/pre-init-capacity.html")
            .defaultValue("18GB")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_SERVERLESS_INITIAL_CAPACITY_DRIVER_DISK = Index.propertyBuilder("sleeper.bulk.import.emr.serverless.initial.capacity.driver.disk")
            .description("The amount of storage per driver for the pre-initialise capacity.\n" +
                    "See: https://docs.aws.amazon.com/emr/latest/EMR-Serverless-UserGuide/pre-init-capacity.html")
            .defaultValue("20GB")
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
            return UserDefinedInstancePropertyImpl.named(propertyName).addToIndex(INSTANCE::add);
        }
    }
}
