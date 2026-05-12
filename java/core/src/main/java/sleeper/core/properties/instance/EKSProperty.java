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

import java.util.List;

/**
 * Definitions of instance properties relating to bulk import on AWS EKS.
 */
public interface EKSProperty {
    UserDefinedInstanceProperty EKS_CLUSTER_ADMIN_ROLES = Index.propertyBuilder("sleeper.bulk.import.eks.cluster.admin.roles")
            .description("(EKS mode only) Names of AWS IAM roles which should have access to administer the EKS cluster.")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty EKS_IS_NATIVE_LIBS_IMAGE = Index.propertyBuilder("sleeper.bulk.import.eks.is.native.libs.image")
            .description("(EKS mode only) Set to true if sleeper.bulk.import.eks.repo contains the image built with " +
                    "native Hadoop libraries. By default when deploying with the EKS stack enabled, an image will be " +
                    "built based on the official Spark Docker image, so this should be false.")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .defaultValue("false")
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty BULK_IMPORT_EKS_SPARK_EXECUTOR_INSTANCES = Index.propertyBuilder("sleeper.bulk.import.eks.spark.executor.instances")
            .description("(EKS mode only) The number of Spark executors. Used to set spark.executor.instances.\n" +
                    "See https://spark.apache.org/docs/latest/configuration.html.")
            .defaultValue("29")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT).build();
    UserDefinedInstanceProperty BULK_IMPORT_EKS_SPARK_DRIVER_MEMORY = Index.propertyBuilder("sleeper.bulk.import.eks.spark.driver.memory")
            .description("(EKS mode only) The amount of memory allocated to the Spark driver. Used to set " +
                    "spark.driver.memory. Default values are overridden because Fargate doesn't work with Spark's " +
                    "default values.\n" +
                    "See https://spark.apache.org/docs/latest/configuration.html.")
            .defaultValue("12g")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT).build();
    UserDefinedInstanceProperty BULK_IMPORT_EKS_SPARK_EXECUTOR_MEMORY = Index.propertyBuilder("sleeper.bulk.import.eks.spark.executor.memory")
            .description("(EKS mode only) The amount of memory allocated to a Spark executor. Used to set " +
                    "spark.executor.memory. Default values are overridden because Fargate doesn't work with Spark's " +
                    "default values.\n" +
                    "See https://spark.apache.org/docs/latest/configuration.html.")
            .defaultValue("12g")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT).build();
    UserDefinedInstanceProperty BULK_IMPORT_EKS_SPARK_DRIVER_MEMORY_OVERHEAD = Index.propertyBuilder("sleeper.bulk.import.eks.spark.driver.memory.overhead")
            .description("(EKS mode only) The memory overhead for the Spark driver. Used to set " +
                    "spark.driver.memoryOverhead. Fargate provides extra memory so no need to include extra which " +
                    "also messes up the scheduler.\n" +
                    "See https://spark.apache.org/docs/latest/configuration.html.")
            .defaultValue("1g")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT).build();
    UserDefinedInstanceProperty BULK_IMPORT_EKS_SPARK_EXECUTOR_MEMORY_OVERHEAD = Index.propertyBuilder("sleeper.bulk.import.eks.spark.executor.memory.overhead")
            .description("(EKS mode only) The memory overhead for a Spark executor. Used to set " +
                    "spark.executor.memoryOverhead. Fargate provides extra memory so no need to include extra which " +
                    "also messes up the scheduler.\n" +
                    "See https://spark.apache.org/docs/latest/configuration.html.")
            .defaultValue("1g")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT).build();
    UserDefinedInstanceProperty BULK_IMPORT_EKS_SPARK_DRIVER_SERVICE_ACCOUNT_NAME = Index.propertyBuilder("sleeper.bulk.import.eks.spark.kubernetes.driver.service.account.name")
            .description("(EKS mode only) The Kubernetes service account used by the Spark driver pod. Used to set " +
                    "spark.kubernetes.authenticate.driver.serviceAccountName.\n" +
                    "See https://spark.apache.org/docs/latest/running-on-kubernetes.html.")
            .defaultValue("spark")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT).build();
    UserDefinedInstanceProperty BULK_IMPORT_EKS_SPARK_HADOOP_S3A_CREDENTIALS_PROVIDER = Index.propertyBuilder("sleeper.bulk.import.eks.spark.hadoop.fs.s3a.aws.credentials.provider")
            .description("(EKS mode only) The AWS credentials provider class used by the S3A Hadoop filesystem. " +
                    "Used to set spark.hadoop.fs.s3a.aws.credentials.provider.")
            .defaultValue("software.amazon.awssdk.auth.credentials.WebIdentityTokenFileCredentialsProvider")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT).build();
    UserDefinedInstanceProperty BULK_IMPORT_EKS_SPARK_HADOOP_S3A_INPUT_FADVISE = Index.propertyBuilder("sleeper.bulk.import.eks.spark.hadoop.fs.s3a.experimental.input.fadvise")
            .description("(EKS mode only) The S3A input read policy. Used to set " +
                    "spark.hadoop.fs.s3a.experimental.input.fadvise.")
            .defaultValue("sequential")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT).build();
    UserDefinedInstanceProperty BULK_IMPORT_EKS_SPARK_EXECUTOR_CORES = Index.propertyBuilder("sleeper.bulk.import.eks.spark.executor.cores")
            .description("(EKS mode only) The number of cores used by a Spark executor. Used to set spark.executor.cores. " +
                    "Should reflect the Fargate task shape rather than the EMR EC2 instance type.\n" +
                    "See https://spark.apache.org/docs/latest/configuration.html.")
            .defaultValue("5")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT).build();
    UserDefinedInstanceProperty BULK_IMPORT_EKS_SPARK_DRIVER_CORES = Index.propertyBuilder("sleeper.bulk.import.eks.spark.driver.cores")
            .description("(EKS mode only) The number of cores used by the Spark driver. Used to set spark.driver.cores. " +
                    "Should reflect the Fargate task shape rather than the EMR EC2 instance type.\n" +
                    "See https://spark.apache.org/docs/latest/configuration.html.")
            .defaultValue(BULK_IMPORT_EKS_SPARK_EXECUTOR_CORES.getDefaultValue())
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT).build();
    UserDefinedInstanceProperty BULK_IMPORT_EKS_SPARK_DEFAULT_PARALLELISM = Index.propertyBuilder("sleeper.bulk.import.eks.spark.default.parallelism")
            .description("(EKS mode only) The default parallelism for the Spark job. Used to set spark.default.parallelism. " +
                    "Should scale with the total cores across the EKS cluster, which may differ from EMR.\n" +
                    "See https://spark.apache.org/docs/latest/configuration.html.")
            .defaultValue("290")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT).build();
    UserDefinedInstanceProperty BULK_IMPORT_EKS_SPARK_SQL_SHUFFLE_PARTITIONS = Index.propertyBuilder("sleeper.bulk.import.eks.spark.sql.shuffle.partitions")
            .description("(EKS mode only) The number of partitions used in a Spark SQL/dataframe shuffle operation. " +
                    "Used to set spark.sql.shuffle.partitions.\n" +
                    "See https://spark.apache.org/docs/latest/configuration.html.")
            .defaultValue(BULK_IMPORT_EKS_SPARK_DEFAULT_PARALLELISM.getDefaultValue())
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT).build();
    UserDefinedInstanceProperty BULK_IMPORT_EKS_SPARK_DYNAMIC_ALLOCATION_ENABLED = Index.propertyBuilder("sleeper.bulk.import.eks.spark.dynamic.allocation.enabled")
            .description("(EKS mode only) Whether Spark should use dynamic allocation to scale resources up and down. " +
                    "Used to set spark.dynamicAllocation.enabled. Kubernetes support for dynamic allocation is more " +
                    "limited than YARN's; consider leaving this disabled on EKS.\n" +
                    "See https://spark.apache.org/docs/latest/configuration.html.")
            .defaultValue("false")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT).build();
    UserDefinedInstanceProperty BULK_IMPORT_EKS_SPARK_EXECUTOR_EXTRA_JAVA_OPTIONS = Index.propertyBuilder("sleeper.bulk.import.eks.spark.executor.extra.java.options")
            .description("JVM options passed to the executors. Used to set spark.executor.extraJavaOptions.\n" +
                    "See https://spark.apache.org/docs/latest/configuration.html.")
            .defaultValue(
                    "-XX:+UseG1GC -XX:+UnlockDiagnosticVMOptions -XX:+G1SummarizeConcMark -XX:InitiatingHeapOccupancyPercent=35 -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:OnOutOfMemoryError='kill -9 %p'")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT).build();
    UserDefinedInstanceProperty BULK_IMPORT_EKS_SPARK_DRIVER_EXTRA_JAVA_OPTIONS = Index.propertyBuilder("sleeper.bulk.import.eks.spark.driver.extra.java.options")
            .description("JVM options passed to the driver. Used to set spark.driver.extraJavaOptions.\n" +
                    "See https://spark.apache.org/docs/latest/configuration.html.")
            .defaultValue(BULK_IMPORT_EKS_SPARK_EXECUTOR_EXTRA_JAVA_OPTIONS.getDefaultValue())
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT).build();
    UserDefinedInstanceProperty BULK_IMPORT_EKS_SPARK_EXECUTOR_HEARTBEAT_INTERVAL = Index.propertyBuilder("sleeper.bulk.import.eks.spark.executor.heartbeat.interval")
            .description("The interval between heartbeats from executors to the driver. Used to set spark.executor.heartbeatInterval.\n" +
                    "See https://spark.apache.org/docs/latest/configuration.html.")
            .defaultValue("60s")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT).build();
    UserDefinedInstanceProperty BULK_IMPORT_EKS_SPARK_NETWORK_TIMEOUT = Index.propertyBuilder("sleeper.bulk.import.eks.spark.network.timeout")
            .description("The default timeout for network interactions in Spark. Used to set spark.network.timeout.\n" +
                    "See https://spark.apache.org/docs/latest/configuration.html.")
            .defaultValue("800s")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT).build();
    UserDefinedInstanceProperty BULK_IMPORT_EKS_SPARK_MEMORY_FRACTION = Index.propertyBuilder("sleeper.bulk.import.eks.spark.memory.fraction")
            .description("The fraction of heap space used for execution and storage. Used to set spark.memory.fraction.\n" +
                    "See https://spark.apache.org/docs/latest/configuration.html.")
            .defaultValue("0.80")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT).build();
    UserDefinedInstanceProperty BULK_IMPORT_EKS_SPARK_MEMORY_STORAGE_FRACTION = Index.propertyBuilder("sleeper.bulk.import.eks.spark.memory.storage.fraction")
            .description("The amount of storage memory immune to eviction, expressed as a fraction of the heap space used for execution and storage. " +
                    "Used to set spark.memory.storageFraction.\n" +
                    "See https://spark.apache.org/docs/latest/configuration.html.")
            .defaultValue("0.30")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT).build();
    UserDefinedInstanceProperty BULK_IMPORT_EKS_SPARK_STORAGE_LEVEL = Index.propertyBuilder("sleeper.bulk.import.eks.spark.storage.level")
            .description("The storage to use for temporary caching. Used to set spark.storage.level.\n" +
                    "See https://aws.amazon.com/blogs/containers/best-practices-for-running-spark-on-amazon-eks/")
            .defaultValue("MEMORY_AND_DISK_SER")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT).build();
    UserDefinedInstanceProperty BULK_IMPORT_EKS_SPARK_RDD_COMPRESS = Index.propertyBuilder("sleeper.bulk.import.eks.spark.rdd.compress")
            .description("Whether to compress serialized RDD partitions. Used to set spark.rdd.compress.\n" +
                    "See https://spark.apache.org/docs/latest/configuration.html.")
            .defaultValue("true")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT).build();
    UserDefinedInstanceProperty BULK_IMPORT_EKS_SPARK_SHUFFLE_COMPRESS = Index.propertyBuilder("sleeper.bulk.import.eks.spark.shuffle.compress")
            .description("Whether to compress map output files. Used to set spark.shuffle.compress.\n" +
                    "See https://spark.apache.org/docs/latest/configuration.html.")
            .defaultValue("true")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT).build();
    UserDefinedInstanceProperty BULK_IMPORT_EKS_SPARK_SHUFFLE_SPILL_COMPRESS = Index.propertyBuilder("sleeper.bulk.import.eks.spark.shuffle.spill.compress")
            .description("Whether to compress data spilled during shuffles. Used to set spark.shuffle.spill.compress.\n" +
                    "See https://spark.apache.org/docs/latest/configuration.html.")
            .defaultValue("true")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT).build();

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
