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

package sleeper.configuration.properties.instance;

import sleeper.core.properties.SleeperPropertyIndex;
import sleeper.core.properties.instance.InstancePropertyGroup;
import sleeper.core.properties.validation.SleeperPropertyValueUtils;

import java.util.List;

/**
 * Definitions of instance properties relating to bulk import on AWS EMR.
 */
public interface EMRProperty {
    UserDefinedInstanceProperty BULK_IMPORT_EMR_SPARK_EXECUTOR_MEMORY = Index.propertyBuilder("sleeper.bulk.import.emr.spark.executor.memory")
            .description("The amount of memory allocated to a Spark executor. Used to set spark.executor.memory.\n" +
                    "See https://spark.apache.org/docs/latest/configuration.html.")
            .defaultValue("16g")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_SPARK_DRIVER_MEMORY = Index.propertyBuilder("sleeper.bulk.import.emr.spark.driver.memory")
            .description("The amount of memory allocated to the Spark driver. Used to set spark.driver.memory.\n" +
                    "See https://spark.apache.org/docs/latest/configuration.html.")
            .defaultValue(BULK_IMPORT_EMR_SPARK_EXECUTOR_MEMORY.getDefaultValue())
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_SPARK_EXECUTOR_INSTANCES = Index.propertyBuilder("sleeper.bulk.import.emr.spark.executor.instances")
            .description("The number of executors. Used to set spark.executor.instances.\n" +
                    "See https://spark.apache.org/docs/latest/configuration.html.")
            .defaultValue("29")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_SPARK_EXECUTOR_MEMORY_OVERHEAD = Index.propertyBuilder("sleeper.bulk.import.emr.spark.executor.memory.overhead")
            .description("The memory overhead for an executor. Used to set spark.executor.memoryOverhead.\n" +
                    "See https://spark.apache.org/docs/latest/configuration.html.")
            .defaultValue("2g")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_SPARK_DRIVER_MEMORY_OVERHEAD = Index.propertyBuilder("sleeper.bulk.import.emr.spark.driver.memory.overhead")
            .description("The memory overhead for the driver. Used to set spark.driver.memoryOverhead.\n" +
                    "See https://spark.apache.org/docs/latest/configuration.html.")
            .defaultValue(BULK_IMPORT_EMR_SPARK_EXECUTOR_MEMORY_OVERHEAD.getDefaultValue())
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_SPARK_DEFAULT_PARALLELISM = Index.propertyBuilder("sleeper.bulk.import.emr.spark.default.parallelism")
            .description("The default parallelism for Spark job. Used to set spark.default.parallelism.\n" +
                    "See https://spark.apache.org/docs/latest/configuration.html.")
            .defaultValue("290")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_SPARK_SQL_SHUFFLE_PARTITIONS = Index.propertyBuilder("sleeper.bulk.import.emr.spark.sql.shuffle.partitions")
            .description("The number of partitions used in a Spark SQL/dataframe shuffle operation. Used to set spark.sql.shuffle.partitions.\n" +
                    "See https://spark.apache.org/docs/latest/configuration.html.")
            .defaultValue(BULK_IMPORT_EMR_SPARK_DEFAULT_PARALLELISM.getDefaultValue())
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .runCdkDeployWhenChanged(true).build();
    //  - Properties that are independent of the instance type and number of instances:
    UserDefinedInstanceProperty BULK_IMPORT_EMR_EC2_KEYPAIR_NAME = Index.propertyBuilder("sleeper.bulk.import.emr.keypair.name")
            .description("(Non-persistent or persistent EMR mode only) An EC2 keypair to use for the EC2 instances. Specifying this will allow you to SSH to the nodes " +
                    "in the cluster while it's running.")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_MASTER_ADDITIONAL_SECURITY_GROUP = Index.propertyBuilder("sleeper.bulk.import.emr.master.additional.security.group")
            .description("(Non-persistent or persistent EMR mode only) Specifying this security group causes the group " +
                    "to be added to the EMR master's list of security groups.")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_SPARK_EXECUTOR_CORES = Index.propertyBuilder("sleeper.bulk.import.emr.spark.executor.cores")
            .description("(Non-persistent or persistent EMR mode only) The number of cores used by an executor. Used to set spark.executor.cores.\n" +
                    "See https://spark.apache.org/docs/latest/configuration.html.")
            .defaultValue("5")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_SPARK_DRIVER_CORES = Index.propertyBuilder("sleeper.bulk.import.emr.spark.driver.cores")
            .description("(Non-persistent or persistent EMR mode only) The number of cores used by the driver. Used to set spark.driver.cores.\n" +
                    "See https://spark.apache.org/docs/latest/configuration.html.")
            .defaultValue(BULK_IMPORT_EMR_SPARK_EXECUTOR_CORES.getDefaultValue())
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_SPARK_NETWORK_TIMEOUT = Index.propertyBuilder("sleeper.bulk.import.emr.spark.network.timeout")
            .description("(Non-persistent or persistent EMR mode only) The default timeout for network interactions in Spark. " +
                    "Used to set spark.network.timeout.\n" +
                    "See https://spark.apache.org/docs/latest/configuration.html.")
            .defaultValue("800s")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_SPARK_EXECUTOR_HEARTBEAT_INTERVAL = Index.propertyBuilder("sleeper.bulk.import.emr.spark.executor.heartbeat.interval")
            .description("(Non-persistent or persistent EMR mode only) The interval between heartbeats from executors to the driver. " +
                    "Used to set spark.executor.heartbeatInterval.\n" +
                    "See https://spark.apache.org/docs/latest/configuration.html.")
            .defaultValue("60s")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_SPARK_DYNAMIC_ALLOCATION_ENABLED = Index.propertyBuilder("sleeper.bulk.import.emr.spark.dynamic.allocation.enabled")
            .description("(Non-persistent or persistent EMR mode only) Whether Spark should use dynamic allocation to scale resources up and down. " +
                    "Used to set spark.dynamicAllocation.enabled.\n" +
                    "See https://spark.apache.org/docs/latest/configuration.html.")
            .defaultValue("false")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_SPARK_MEMORY_FRACTION = Index.propertyBuilder("sleeper.bulk.import.emr.spark.memory.fraction")
            .description("(Non-persistent or persistent EMR mode only) The fraction of heap space used for execution and storage. " +
                    "Used to set spark.memory.fraction.\n" +
                    "See https://spark.apache.org/docs/latest/configuration.html.")
            .defaultValue("0.80")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_SPARK_MEMORY_STORAGE_FRACTION = Index.propertyBuilder("sleeper.bulk.import.emr.spark.memory.storage.fraction")
            .description("(Non-persistent or persistent EMR mode only) The amount of storage memory immune to eviction, " +
                    "expressed as a fraction of the heap space used for execution and storage. " +
                    "Used to set spark.memory.storageFraction.\n" +
                    "See https://spark.apache.org/docs/latest/configuration.html.")
            .defaultValue("0.30")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_SPARK_EXECUTOR_EXTRA_JAVA_OPTIONS = Index.propertyBuilder("sleeper.bulk.import.emr.spark.executor.extra.java.options")
            .description("(Non-persistent or persistent EMR mode only) JVM options passed to the executors. " +
                    "Used to set spark.executor.extraJavaOptions.\n" +
                    "See https://spark.apache.org/docs/latest/configuration.html.")
            .defaultValue(
                    "-XX:+UseG1GC -XX:+UnlockDiagnosticVMOptions -XX:+G1SummarizeConcMark -XX:InitiatingHeapOccupancyPercent=35 -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:OnOutOfMemoryError='kill -9 %p'")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_SPARK_DRIVER_EXTRA_JAVA_OPTIONS = Index.propertyBuilder("sleeper.bulk.import.emr.spark.driver.extra.java.options")
            .description("(Non-persistent or persistent EMR mode only) JVM options passed to the driver. " +
                    "Used to set spark.driver.extraJavaOptions.\n" +
                    "See https://spark.apache.org/docs/latest/configuration.html.")
            .defaultValue(BULK_IMPORT_EMR_SPARK_EXECUTOR_EXTRA_JAVA_OPTIONS.getDefaultValue())
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_SPARK_YARN_SCHEDULER_REPORTER_THREAD_MAX_FAILURES = Index.propertyBuilder("sleeper.bulk.import.emr.spark.yarn.scheduler.reporter.thread.max.failures")
            .description("(Non-persistent or persistent EMR mode only) The maximum number of executor failures before YARN can fail the application. " +
                    "Used to set spark.yarn.scheduler.reporterThread.maxFailures.\n" +
                    "See https://aws.amazon.com/blogs/big-data/best-practices-for-successfully-managing-memory-for-apache-spark-applications-on-amazon-emr/.")
            .defaultValue("5")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_SPARK_STORAGE_LEVEL = Index.propertyBuilder("sleeper.bulk.import.emr.spark.storage.level")
            .description("(Non-persistent or persistent EMR mode only) The storage to use for temporary caching. " +
                    "Used to set spark.storage.level.\n" +
                    "See https://aws.amazon.com/blogs/big-data/best-practices-for-successfully-managing-memory-for-apache-spark-applications-on-amazon-emr/.")
            .defaultValue("MEMORY_AND_DISK_SER")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_SPARK_RDD_COMPRESS = Index.propertyBuilder("sleeper.bulk.import.emr.spark.rdd.compress")
            .description("(Non-persistent or persistent EMR mode only) Whether to compress serialized RDD partitions. " +
                    "Used to set spark.rdd.compress.\n" +
                    "See https://spark.apache.org/docs/latest/configuration.html.")
            .defaultValue("true")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_SPARK_SHUFFLE_COMPRESS = Index.propertyBuilder("sleeper.bulk.import.emr.spark.shuffle.compress")
            .description("(Non-persistent or persistent EMR mode only) Whether to compress map output files. " +
                    "Used to set spark.shuffle.compress.\n" +
                    "See https://spark.apache.org/docs/latest/configuration.html.")
            .defaultValue("true")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_SPARK_SHUFFLE_SPILL_COMPRESS = Index.propertyBuilder("sleeper.bulk.import.emr.spark.shuffle.spill.compress")
            .description("(Non-persistent or persistent EMR mode only) Whether to compress data spilled during shuffles. " +
                    "Used to set spark.shuffle.spill.compress.\n" +
                    "See https://spark.apache.org/docs/latest/configuration.html.")
            .defaultValue("true")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_EBS_VOLUME_SIZE_IN_GB = Index.propertyBuilder("sleeper.bulk.import.emr.ebs.volume.size.gb")
            .description("(Non-persistent or persistent EMR mode only) The size of the EBS volume in gibibytes (GiB).\n" +
                    "This can be a number from 10 to 1024.")
            .defaultValue("256")
            .validationPredicate(SleeperPropertyValueUtils::isValidEbsSize)
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_EBS_VOLUME_TYPE = Index.propertyBuilder("sleeper.bulk.import.emr.ebs.volume.type")
            .description("(Non-persistent or persistent EMR mode only) The type of the EBS volume.\n" +
                    "Valid values are 'gp2', 'gp3', 'io1', 'io2'.")
            .defaultValue("gp2")
            .validationPredicate(SleeperPropertyValueUtils::isValidEbsVolumeType)
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_EBS_VOLUMES_PER_INSTANCE = Index.propertyBuilder("sleeper.bulk.import.emr.ebs.volumes.per.instance")
            .description("(Non-persistent or persistent EMR mode only) The number of EBS volumes per instance.\n" +
                    "This can be a number from 1 to 25.")
            .defaultValue("4").validationPredicate(s -> SleeperPropertyValueUtils.isPositiveIntLtEqValue(s, 25))
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

        static UserDefinedInstancePropertyImpl.Builder propertyBuilder(String propertyName) {
            return UserDefinedInstancePropertyImpl.named(propertyName)
                    .addToIndex(INSTANCE::add);
        }
    }
}
