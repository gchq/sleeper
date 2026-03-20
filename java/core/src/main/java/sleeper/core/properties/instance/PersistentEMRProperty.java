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
import sleeper.core.properties.model.EmrInstanceArchitecture;
import sleeper.core.properties.model.EmrInstanceTypeConfig;
import sleeper.core.properties.model.SleeperPropertyValueUtils;

import java.util.List;

import static sleeper.core.properties.instance.NonPersistentEMRProperty.DEFAULT_BULK_IMPORT_EMR_EXECUTOR_ARM_INSTANCE_TYPES;
import static sleeper.core.properties.instance.NonPersistentEMRProperty.DEFAULT_BULK_IMPORT_EMR_EXECUTOR_X86_INSTANCE_TYPES;
import static sleeper.core.properties.instance.NonPersistentEMRProperty.DEFAULT_BULK_IMPORT_EMR_MASTER_ARM_INSTANCE_TYPES;
import static sleeper.core.properties.instance.NonPersistentEMRProperty.DEFAULT_BULK_IMPORT_EMR_MASTER_X86_INSTANCE_TYPES;
import static sleeper.core.properties.instance.NonPersistentEMRProperty.DEFAULT_BULK_IMPORT_EMR_RELEASE_LABEL;

/**
 * Definitions of instance properties relating to bulk import on AWS EMR with a persistent cluster.
 */
public interface PersistentEMRProperty {
    UserDefinedInstanceProperty BULK_IMPORT_PERSISTENT_EMR_RELEASE_LABEL = Index.propertyBuilder("sleeper.bulk.import.persistent.emr.release.label")
            .description("(Persistent EMR mode only) The EMR release used to create the persistent EMR cluster.")
            .defaultValue(DEFAULT_BULK_IMPORT_EMR_RELEASE_LABEL.getDefaultValue())
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty BULK_IMPORT_PERSISTENT_EMR_INSTANCE_ARCHITECTURE = Index.propertyBuilder("sleeper.bulk.import.persistent.emr.instance.architecture")
            .description("(Persistent EMR mode only) Which architecture to be used for EC2 instance types " +
                    "in the EMR cluster. Must be either \"x86_64\" \"arm64\" or \"x86_64,arm64\". " +
                    "For more information, see the Bulk import using EMR - Instance types section in docs/usage/bulk-import.md")
            .defaultValue("arm64")
            .validationPredicate(EmrInstanceArchitecture::isValid)
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT).build();
    UserDefinedInstanceProperty BULK_IMPORT_PERSISTENT_EMR_MASTER_X86_INSTANCE_TYPES = Index.propertyBuilder("sleeper.bulk.import.persistent.emr.master.x86.instance.types")
            .description("(Persistent EMR mode only) The EC2 x86_64 instance types and weights used for the master " +
                    "node of the persistent EMR cluster.\n" +
                    "For more information, see the Bulk import using EMR - Instance types section in docs/usage/bulk-import.md")
            .defaultValue(DEFAULT_BULK_IMPORT_EMR_MASTER_X86_INSTANCE_TYPES.getDefaultValue())
            .validationPredicate(EmrInstanceTypeConfig::isValidInstanceTypes)
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty BULK_IMPORT_PERSISTENT_EMR_EXECUTOR_X86_INSTANCE_TYPES = Index.propertyBuilder("sleeper.bulk.import.persistent.emr.executor.x86.instance.types")
            .description("(Persistent EMR mode only) The EC2 x86_64 instance types and weights used for the executor " +
                    "nodes of the persistent EMR cluster.\n" +
                    "For more information, see the Bulk import using EMR - Instance types section in docs/usage/bulk-import.md")
            .defaultValue(DEFAULT_BULK_IMPORT_EMR_EXECUTOR_X86_INSTANCE_TYPES.getDefaultValue())
            .validationPredicate(EmrInstanceTypeConfig::isValidInstanceTypes)
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty BULK_IMPORT_PERSISTENT_EMR_MASTER_ARM_INSTANCE_TYPES = Index.propertyBuilder("sleeper.bulk.import.persistent.emr.master.arm.instance.types")
            .description("(Persistent EMR mode only) The EC2 ARM64 instance types and weights used for the master " +
                    "node of the persistent EMR cluster.\n" +
                    "For more information, see the Bulk import using EMR - Instance types section in docs/usage/bulk-import.md")
            .defaultValue(DEFAULT_BULK_IMPORT_EMR_MASTER_ARM_INSTANCE_TYPES.getDefaultValue())
            .validationPredicate(EmrInstanceTypeConfig::isValidInstanceTypes)
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty BULK_IMPORT_PERSISTENT_EMR_EXECUTOR_ARM_INSTANCE_TYPES = Index.propertyBuilder("sleeper.bulk.import.persistent.emr.executor.arm.instance.types")
            .description("(Persistent EMR mode only) The EC2 ARM64 instance types and weights used for the executor " +
                    "nodes of the persistent EMR cluster.\n" +
                    "For more information, see the Bulk import using EMR - Instance types section in docs/usage/bulk-import.md")
            .defaultValue(DEFAULT_BULK_IMPORT_EMR_EXECUTOR_ARM_INSTANCE_TYPES.getDefaultValue())
            .validationPredicate(EmrInstanceTypeConfig::isValidInstanceTypes)
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty BULK_IMPORT_PERSISTENT_EMR_USE_MANAGED_SCALING = Index.propertyBuilder("sleeper.bulk.import.persistent.emr.use.managed.scaling")
            .description("(Persistent EMR mode only) Whether the persistent EMR cluster should use managed scaling or not.")
            .defaultValue("true")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty BULK_IMPORT_PERSISTENT_EMR_MIN_CAPACITY = Index.propertyBuilder("sleeper.bulk.import.persistent.emr.min.capacity")
            .description("(Persistent EMR mode only) The minimum number of capacity units to provision as EC2 " +
                    "instances for executors in the persistent EMR cluster.\n" +
                    "This is measured in instance fleet capacity units. These are declared alongside the requested " +
                    "instance types, as each type will count for a certain number of units. By default the units are " +
                    "the number of instances.\n" +
                    "If managed scaling is not used then the cluster will be of fixed size, with a number of " +
                    "instances equal to this value. This value must be in the range [1, 2000].")
            .defaultValue("1")
            .validationPredicate(v -> SleeperPropertyValueUtils.isNonNegativeIntLtEqValue(v, 2000))
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty BULK_IMPORT_PERSISTENT_EMR_MAX_CAPACITY = Index.propertyBuilder("sleeper.bulk.import.persistent.emr.max.capacity")
            .description("(Persistent EMR mode only) The maximum number of capacity units to provision as EC2 " +
                    "instances for executors in the persistent EMR cluster.\n" +
                    "This is measured in instance fleet capacity units. These are declared alongside the requested " +
                    "instance types, as each type will count for a certain number of units. By default the units are " +
                    "the number of instances.\n" +
                    "This value is only used if managed scaling is used. This value must be in the range [1, 2000] and greater than\n" +
                    "the minimum capacity.")
            .defaultValue("10")
            .validationPredicate(v -> SleeperPropertyValueUtils.isNonNegativeIntLtEqValue(v, 2000))
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty BULK_IMPORT_PERSISTENT_EMR_STEP_CONCURRENCY_LEVEL = Index.propertyBuilder("sleeper.bulk.import.persistent.emr.step.concurrency.level")
            .description("(Persistent EMR mode only) This controls the number of EMR steps that can run concurrently.")
            .defaultValue("2")
            .validationPredicate(SleeperPropertyValueUtils::isPositiveInteger)
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty BULK_IMPORT_PERSISTENT_EMR_REQUEUE_DELAY_SECONDS = Index.propertyBuilder("sleeper.bulk.import.persistent.emr.cluster.full.requeue.delay")
            .description("(Persistent EMR mode only) The number of seconds to wait before requeueing a bulk import job because the persistent EMR cluster is full.")
            .defaultValue("60")
            .validationPredicate(SleeperPropertyValueUtils::isPositiveInteger)
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
