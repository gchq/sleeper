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

import sleeper.configuration.properties.SleeperPropertyIndex;

import java.util.List;

public interface EMRServerlessProperty {

    UserDefinedInstanceProperty BULK_IMPORT_EMR_SERVERLESS_CLASS_NAME = Index
            .propertyBuilder("sleeper.bulk.import.emr.serverless.class.name")
            .description("The class to use to perform the bulk import.")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .defaultValue(
                    "sleeper.bulkimport.job.runner.dataframelocalsort.BulkImportDataframeLocalSortDriver")
            .runCDKDeployWhenChanged(true).build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_SERVERLESS_ARCHITECTURE = Index
            .propertyBuilder("sleeper.bulk.import.emr.serverless.architecture")
            .description("The architecture for EMR Serverless to use. X86_64 or ARM (Coming soon)")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT).defaultValue("X86_64")
            .runCDKDeployWhenChanged(true).build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_SERVERLESS_ENABLED = Index
            .propertyBuilder("sleeper.bulk.import.emr.serverless.enabled")
            .description("The switch to enable EMR Serverless over persistent EMR.")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT).defaultValue("true")
            .runCDKDeployWhenChanged(true).build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_SERVERLESS_RELEASE = Index
            .propertyBuilder("sleeper.bulk.import.emr.serverless.release")
            .description("The version of EMR Serverless to use.")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT).defaultValue("emr-6.10.0")
            .runCDKDeployWhenChanged(true).build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_SERVERLESS_CUSTOM_IMAGE_REPO = Index
            .propertyBuilder("sleeper.bulk.import.emr.serverless.repo")
            .description(
                    "The name of the repository for the EMR serverless container. The Docker image from the bulk-import module "
                            + "should have been uploaded to an ECR repository of this name in this account.")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT).runCDKDeployWhenChanged(true).build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_SERVERLESS_TYPE = Index
            .propertyBuilder("sleeper.bulk.import.emr.serverless.type")
            .description("The type of EMR Serverless to use. Spark or Hive")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT).defaultValue("Spark")
            .runCDKDeployWhenChanged(true).build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_SERVERLESS_EXECUTOR_CORES = Index
            .propertyBuilder("sleeper.bulk.import.emr.serverless.executor.cores")
            .description(
                    "The number of cores used by an Serverless executor. Used to set spark.executor.cores.\n"
                            + "See https://spark.apache.org/docs/latest/configuration.html.")
            .defaultValue("4").propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .runCDKDeployWhenChanged(true).build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_SERVERLESS_EXECUTOR_MEMORY = Index
            .propertyBuilder("sleeper.bulk.import.emr.serverless.executor.memory")
            .description(
                    "The amount of memory allocated to a Serverless executor. Used to set spark.executor.memory.\n"
                            + "See https://spark.apache.org/docs/latest/configuration.html.")
            .defaultValue("16g").propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .runCDKDeployWhenChanged(true).build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_SERVERLESS_EXECUTOR_INSTANCES = Index
            .propertyBuilder("sleeper.bulk.import.emr.serverless.executor.instances")
            .description(
                    "The number of executors to be used with Serverless. Used to set spark.executor.instances.\n"
                            + "See https://spark.apache.org/docs/latest/configuration.html.")
            .defaultValue("28").propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .runCDKDeployWhenChanged(true).build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_SERVERLESS_DRIVER_CORES = Index
            .propertyBuilder("sleeper.bulk.import.emr.serverless.driver.cores")
            .description(
                    "The number of cores used by the Serverless Spark driver. Used to set spark.driver.cores.\n"
                            + "See https://spark.apache.org/docs/latest/configuration.html.")
            .defaultValue(BULK_IMPORT_EMR_SERVERLESS_EXECUTOR_CORES.getDefaultValue())
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT).runCDKDeployWhenChanged(true).build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_SERVERLESS_DRIVER_MEMORY = Index
            .propertyBuilder("sleeper.bulk.import.emr.serverless.driver.memory")
            .description(
                    "The amount of memory allocated to the Serverless Spark driver. Used to set spark.driver.memory.\n"
                            + "See https://spark.apache.org/docs/latest/configuration.html.")
            .defaultValue(BULK_IMPORT_EMR_SERVERLESS_EXECUTOR_MEMORY.getDefaultValue())
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT).runCDKDeployWhenChanged(true).build();
    UserDefinedInstanceProperty BULK_IMPORT_EMR_SERVERLESS_JAVA_HOME = Index
            .propertyBuilder("sleeper.bulk.import.emr.serverless.java.home")
            .description("The path to JAVA_HOME to be used by the custom image for bulk import.")
            .defaultValue("/usr/lib/jvm/jre-11")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT).runCDKDeployWhenChanged(true).build();

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
