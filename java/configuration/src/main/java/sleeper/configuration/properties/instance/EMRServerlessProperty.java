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

package sleeper.configuration.properties.instance;

import sleeper.configuration.Utils;
import sleeper.configuration.properties.SleeperPropertyIndex;

import java.util.List;
import java.util.function.Predicate;

public interface EMRServerlessProperty {

    UserDefinedInstanceProperty BULK_IMPORT_EMR_SERVERLESS_ARCHITECTURE = Index
            .propertyBuilder("sleeper.bulk.import.emr.serverless.architecture")
            .description("The architecture for EMR Serverless to use. X86_64 or ARM (Coming soon)")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT).defaultValue("X86_64")
            .validationPredicate(Predicate.isEqual("X86_64"))
            .runCDKDeployWhenChanged(true).build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_SERVERLESS_RELEASE = Index
            .propertyBuilder("sleeper.bulk.import.emr.serverless.release")
            .description("The version of EMR Serverless to use.")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT).defaultValue("emr-6.10.0")
            .runCDKDeployWhenChanged(true).build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_SERVERLESS_CUSTOM_IMAGE_REPO = Index
            .propertyBuilder("sleeper.bulk.import.emr.serverless.repo")
            .description("The name of the repository for the EMR serverless container. "
                            + "The Docker image from the bulk-import module "
                            + "should have been uploaded to an ECR repository of this name in this account.")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .runCDKDeployWhenChanged(true).build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_SERVERLESS_EXECUTOR_CORES = Index
            .propertyBuilder("sleeper.bulk.import.emr.serverless.executor.cores")
            .description(
                    "The number of cores used by a Serverless executor. Used to set spark.executor.cores.\n"
                            + "See https://spark.apache.org/docs/latest/configuration.html.")
            .defaultValue("4")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .runCDKDeployWhenChanged(true).build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_SERVERLESS_EXECUTOR_MEMORY = Index
            .propertyBuilder("sleeper.bulk.import.emr.serverless.executor.memory")
            .description(
                    "The amount of memory allocated to a Serverless executor. Used to set spark.executor.memory.\n"
                            + "See https://spark.apache.org/docs/latest/configuration.html.")
            .defaultValue("16g")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .runCDKDeployWhenChanged(true).build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_SERVERLESS_EXECUTOR_DISK = Index
            .propertyBuilder("sleeper.bulk.import.emr.serverless.executor.disk")
            .description("The amount of storage allocated to a Serverless executor.")
            .defaultValue("200g").
            propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .runCDKDeployWhenChanged(true).build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_SERVERLESS_EXECUTOR_INSTANCES = Index
            .propertyBuilder("sleeper.bulk.import.emr.serverless.executor.instances")
            .description(
                    "The number of executors to be used with Serverless. Used to set spark.executor.instances.\n"
                            + "See https://spark.apache.org/docs/latest/configuration.html.")
            .defaultValue("36")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .runCDKDeployWhenChanged(true).build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_SERVERLESS_DRIVER_CORES = Index
            .propertyBuilder("sleeper.bulk.import.emr.serverless.driver.cores")
            .description(
                    "The number of cores used by the Serverless Spark driver. Used to set spark.driver.cores.\n"
                            + "See https://spark.apache.org/docs/latest/configuration.html.")
            .defaultValue(BULK_IMPORT_EMR_SERVERLESS_EXECUTOR_CORES.getDefaultValue())
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .runCDKDeployWhenChanged(true).build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_SERVERLESS_DRIVER_MEMORY = Index
            .propertyBuilder("sleeper.bulk.import.emr.serverless.driver.memory")
            .description(
                    "The amount of memory allocated to the Serverless Spark driver. Used to set spark.driver.memory.\n"
                            + "See https://spark.apache.org/docs/latest/configuration.html.")
            .defaultValue(BULK_IMPORT_EMR_SERVERLESS_EXECUTOR_MEMORY.getDefaultValue())
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .runCDKDeployWhenChanged(true).build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_SERVERLESS_JAVA_HOME = Index
            .propertyBuilder("sleeper.bulk.import.emr.serverless.java.home")
            .description("The path to JAVA_HOME to be used by the custom image for bulk import.")
            .defaultValue("/usr/lib/jvm/jre-11")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .runCDKDeployWhenChanged(true).build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_SERVERLESS_DYNAMIC_ALLOCATION = Index
            .propertyBuilder("sleeper.bulk.import.emr.serverless.dynamic.allocation.enabled")
            .description("Whether Spark should use dynamic allocation to scale resources up and down. "
                            + "Used to set spark.dynamicAllocation.enabled. See https://spark.apache.org/docs/latest/configuration.html.")
            .defaultValue("false")
            .validationPredicate(Utils::isTrueOrFalse)
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .runCDKDeployWhenChanged(true).build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_SERVERLESS_SPARK_RDD_COMPRESS = Index.propertyBuilder("sleeper.bulk.import.emr.serverless.spark.rdd.compress")
            .description("Whether to compress serialized RDD partitions. Used to set spark.rdd.compress.\n" +
                    "See https://spark.apache.org/docs/latest/configuration.html.")
            .defaultValue("true")
            .validationPredicate(Utils::isTrueOrFalse)
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .runCDKDeployWhenChanged(true).build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_SERVERLESS_SPARK_SHUFFLE_COMPRESS = Index.propertyBuilder("sleeper.bulk.import.emr.serverless.spark.shuffle.compress")
            .description("Whether to compress map output files. Used to set spark.shuffle.compress.\n" +
                    "See https://spark.apache.org/docs/latest/configuration.html.")
            .defaultValue("true")
            .validationPredicate(Utils::isTrueOrFalse)
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .runCDKDeployWhenChanged(true).build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_SERVERLESS_SPARK_SHUFFLE_SPILL_COMPRESS = Index.propertyBuilder("sleeper.bulk.import.emr.serverless.spark.shuffle.spill.compress")
            .description("Whether to compress data spilled during shuffles. Used to set spark.shuffle.spill.compress.\n" +
                    "See https://spark.apache.org/docs/latest/configuration.html.")
            .defaultValue("true")
            .validationPredicate(Utils::isTrueOrFalse)
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .runCDKDeployWhenChanged(true).build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_SERVERLESS_SPARK_DEFAULT_PARALLELISM = Index.propertyBuilder("sleeper.bulk.import.emr.serverless.spark.default.parallelism")
            .description("The default parallelism for Spark job. Used to set spark.default.parallelism.\n" +
                    "See https://spark.apache.org/docs/latest/configuration.html.")
            .defaultValue("288")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .runCDKDeployWhenChanged(true).build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_SERVERLESS_SPARK_SQL_SHUFFLE_PARTITIONS = Index.propertyBuilder("sleeper.bulk.import.emr.serverless.spark.sql.shuffle.partitions")
            .description("The number of partitions used in a Spark SQL/dataframe shuffle operation. Used to set spark.sql.shuffle.partitions.\n" +
                    "See https://spark.apache.org/docs/latest/configuration.html.")
            .defaultValue(BULK_IMPORT_EMR_SERVERLESS_SPARK_DEFAULT_PARALLELISM.getDefaultValue())
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .runCDKDeployWhenChanged(true).build();

            UserDefinedInstanceProperty BULK_IMPORT_EMR_SERVERLESS_SPARK_NETWORK_TIMEOUT = Index.propertyBuilder("sleeper.bulk.import.emr.serverless.spark.network.timeout")
            .description("The default timeout for network interactions in Spark. Used to set spark.network.timeout.\n" +
                    "See https://spark.apache.org/docs/latest/configuration.html.")
            .defaultValue("800s")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .runCDKDeployWhenChanged(true).build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_SERVERLESS_SPARK_EXECUTOR_HEARTBEAT_INTERVAL = Index.propertyBuilder("sleeper.bulk.import.emr.serverless.spark.executor.heartbeat.interval")
            .description("(The interval between heartbeats from executors to the driver. Used to set spark.executor.heartbeatInterval.\n" +
                    "See https://spark.apache.org/docs/latest/configuration.html.")
            .defaultValue("60s")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .runCDKDeployWhenChanged(true).build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_SERVERLESS_SPARK_MEMORY_FRACTION = Index.propertyBuilder("sleeper.bulk.import.emr.serverless.spark.memory.fraction")
            .description("The fraction of heap space used for execution and storage. Used to set spark.memory.fraction.\n" +
                    "See https://spark.apache.org/docs/latest/configuration.html.")
            .defaultValue("0.80")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .runCDKDeployWhenChanged(true).build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_SERVERLESS_SPARK_MEMORY_STORAGE_FRACTION = Index.propertyBuilder("sleeper.bulk.import.emr.serverless.spark.memory.storage.fraction")
            .description("The amount of storage memory immune to eviction, expressed as a fraction of the heap space used for execution and storage. " +
                    "Used to set spark.memory.storageFraction.\n" +
                    "See https://spark.apache.org/docs/latest/configuration.html.")
            .defaultValue("0.30")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .runCDKDeployWhenChanged(true).build();

    static List<UserDefinedInstanceProperty> getAll() {
        return Index.INSTANCE.getAll();
    }

    static boolean has(String propertyName) {
        return Index.INSTANCE.getByName(propertyName).isPresent();
    }

    class Index {
        private Index() {
        }

        private static final SleeperPropertyIndex<UserDefinedInstanceProperty> INSTANCE = new SleeperPropertyIndex<>();

        static UserDefinedInstancePropertyImpl.Builder propertyBuilder(String propertyName) {
            return UserDefinedInstancePropertyImpl.named(propertyName).addToIndex(INSTANCE::add);
        }
    }
}
