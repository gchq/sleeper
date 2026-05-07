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
            .defaultValue("3")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT).build();
    UserDefinedInstanceProperty BULK_IMPORT_EKS_SPARK_DRIVER_MEMORY = Index.propertyBuilder("sleeper.bulk.import.eks.spark.driver.memory")
            .description("(EKS mode only) The amount of memory allocated to the Spark driver. Used to set " +
                    "spark.driver.memory. Default values are overridden because Fargate doesn't work with Spark's " +
                    "default values.\n" +
                    "See https://spark.apache.org/docs/latest/configuration.html.")
            .defaultValue("7g")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT).build();
    UserDefinedInstanceProperty BULK_IMPORT_EKS_SPARK_EXECUTOR_MEMORY = Index.propertyBuilder("sleeper.bulk.import.eks.spark.executor.memory")
            .description("(EKS mode only) The amount of memory allocated to a Spark executor. Used to set " +
                    "spark.executor.memory. Default values are overridden because Fargate doesn't work with Spark's " +
                    "default values.\n" +
                    "See https://spark.apache.org/docs/latest/configuration.html.")
            .defaultValue("7g")
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
