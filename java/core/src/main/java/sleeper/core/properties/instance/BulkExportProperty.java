/*
 * Copyright 2022-2025 Crown Copyright
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
import sleeper.core.properties.validation.SleeperPropertyValueUtils;

import java.util.List;

import static sleeper.core.properties.instance.TableStateProperty.DEFAULT_TABLE_STATE_LAMBDA_MEMORY;

/**
 * Properties for bulk export processing.
 */
public interface BulkExportProperty {
    UserDefinedInstanceProperty BULK_EXPORT_LAMBDA_MEMORY_IN_MB = Index
            .propertyBuilder("sleeper.bulk.export.memory.mb")
            .description("The amount of memory in MB for lambda functions that start bulk export jobs.")
            .defaultProperty(DEFAULT_TABLE_STATE_LAMBDA_MEMORY)
            .propertyGroup(InstancePropertyGroup.BULK_EXPORT)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty BULK_EXPORT_LAMBDA_TIMEOUT_IN_SECONDS = Index
            .propertyBuilder("sleeper.bulk.export.timeout.seconds")
            .description("The default timeout in seconds for the bulk export lambda.")
            .defaultValue("800")
            .propertyGroup(InstancePropertyGroup.BULK_EXPORT)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty BULK_EXPORT_QUEUE_VISIBILITY_TIMEOUT_IN_SECONDS = Index
            .propertyBuilder("sleeper.bulk.export.queue.visibility.timeout.seconds")
            .description("The visibility timeout in seconds for the bulk export queue.")
            .defaultValue("800")
            .propertyGroup(InstancePropertyGroup.BULK_EXPORT)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty BULK_EXPORT_LEAF_PARTITION_LANGUAGE = Index
            .propertyBuilder("sleeper.bulk.export.leaf.partition.language")
            .description("The language to use for the leaf partition bulk export processor.")
            .defaultValue("java")
            .validationPredicate(SleeperPropertyValueUtils::isValidBulkExportLanguage)
            .propertyGroup(InstancePropertyGroup.BULK_EXPORT)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty BULK_EXPORT_TASK_CPU_ARCHITECTURE = Index
            .propertyBuilder("sleeper.bulk.export.task.cpu.architecture")
            .description("The CPU architecture to run bulk export tasks on. Valid values are X86_64 and ARM64.\n" +
                    "See Task CPU architecture at https://docs.aws.amazon.com/AmazonECS/latest/developerguide/AWS_Fargate.html")
            .defaultValue("X86_64")
            .propertyGroup(InstancePropertyGroup.BULK_EXPORT)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty BULK_EXPORT_TASK_ARM_CPU = Index.propertyBuilder("sleeper.bulk.export.task.arm.cpu")
            .description("The CPU for a bulk. export task using an ARM64 architecture.\n" +
                    "See https://docs.aws.amazon.com/AmazonECS/latest/developerguide/task-cpu-memory-error.html for valid options.")
            .defaultValue("1024")
            .propertyGroup(InstancePropertyGroup.BULK_EXPORT)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty BULK_EXPORT_TASK_ARM_MEMORY = Index
            .propertyBuilder("sleeper.bulk.export.task.arm.memory.mb")
            .description("The amount of memory in MB for a bulk export task using an ARM64 architecture.\n" +
                    "See https://docs.aws.amazon.com/AmazonECS/latest/developerguide/task-cpu-memory-error.html for valid options.")
            .defaultValue("4096")
            .propertyGroup(InstancePropertyGroup.BULK_EXPORT)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty BULK_EXPORT_TASK_X86_CPU = Index.propertyBuilder("sleeper.bulk.export.task.x86.cpu")
            .description("The CPU for a bulk export task using an x86_64 architecture.\n" +
                    "See https://docs.aws.amazon.com/AmazonECS/latest/developerguide/task-cpu-memory-error.html for valid options.")
            .defaultValue("1024")
            .propertyGroup(InstancePropertyGroup.BULK_EXPORT)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty BULK_EXPORT_TASK_X86_MEMORY = Index
            .propertyBuilder("sleeper.bulk.export.task.x86.memory.mb")
            .description("The amount of memory in MB for a bulk export task using an x86_64 architecture.\n" +
                    "See https://docs.aws.amazon.com/AmazonECS/latest/developerguide/task-cpu-memory-error.html for valid options.")
            .defaultValue("4096")
            .propertyGroup(InstancePropertyGroup.BULK_EXPORT)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty BULK_EXPORT_ECR_REPO = Index.propertyBuilder("sleeper.bulk.export.ecr.repo")
            .description("The name of the repository for the bulk export container. The Docker image should have been" +
                    "uploaded to an ECR repository of this name in this account.")
            .propertyGroup(InstancePropertyGroup.BULK_EXPORT)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty BULK_EXPORT_TASK_CREATION_PERIOD_IN_MINUTES = Index
            .propertyBuilder("sleeper.bulk.export.task.creation.period.minutes")
            .description("The rate at which a check to see if bulk export ECS tasks need to be created is made (in minutes, must be >= 1).")
            .defaultValue("1")
            .validationPredicate(SleeperPropertyValueUtils::isPositiveInteger)
            .propertyGroup(InstancePropertyGroup.BULK_EXPORT)
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
