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

import sleeper.configuration.properties.SleeperPropertyIndex;
import sleeper.configuration.properties.validation.EmrInstanceArchitecture;
import sleeper.configuration.properties.validation.EmrInstanceTypeConfig;
import sleeper.configuration.properties.validation.SleeperPropertyValueUtils;

import java.util.List;

import static sleeper.configuration.properties.instance.NonPersistentEMRProperty.DEFAULT_BULK_IMPORT_EMR_EXECUTOR_ARM_INSTANCE_TYPES;
import static sleeper.configuration.properties.instance.NonPersistentEMRProperty.DEFAULT_BULK_IMPORT_EMR_EXECUTOR_X86_INSTANCE_TYPES;
import static sleeper.configuration.properties.instance.NonPersistentEMRProperty.DEFAULT_BULK_IMPORT_EMR_MASTER_ARM_INSTANCE_TYPES;
import static sleeper.configuration.properties.instance.NonPersistentEMRProperty.DEFAULT_BULK_IMPORT_EMR_MASTER_X86_INSTANCE_TYPES;
import static sleeper.configuration.properties.instance.NonPersistentEMRProperty.DEFAULT_BULK_IMPORT_EMR_RELEASE_LABEL;

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
                    "For more information, see the Bulk import using EMR - Instance types section in docs/05-ingest.md")
            .defaultValue("x86_64")
            .validationPredicate(EmrInstanceArchitecture::isValid)
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT).build();
    UserDefinedInstanceProperty BULK_IMPORT_PERSISTENT_EMR_MASTER_X86_INSTANCE_TYPES = Index.propertyBuilder("sleeper.bulk.import.persistent.emr.master.x86.instance.types")
            .description("(Persistent EMR mode only) The EC2 x86_64 instance types and weights used for the master " +
                    "node of the persistent EMR cluster.\n" +
                    "For more information, see the Bulk import using EMR - Instance types section in docs/05-ingest.md")
            .defaultValue(DEFAULT_BULK_IMPORT_EMR_MASTER_X86_INSTANCE_TYPES.getDefaultValue())
            .validationPredicate(EmrInstanceTypeConfig::isValidInstanceTypes)
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty BULK_IMPORT_PERSISTENT_EMR_EXECUTOR_X86_INSTANCE_TYPES = Index.propertyBuilder("sleeper.bulk.import.persistent.emr.executor.x86.instance.types")
            .description("(Persistent EMR mode only) The EC2 x86_64 instance types and weights used for the executor " +
                    "nodes of the persistent EMR cluster.\n" +
                    "For more information, see the Bulk import using EMR - Instance types section in docs/05-ingest.md")
            .defaultValue(DEFAULT_BULK_IMPORT_EMR_EXECUTOR_X86_INSTANCE_TYPES.getDefaultValue())
            .validationPredicate(EmrInstanceTypeConfig::isValidInstanceTypes)
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty BULK_IMPORT_PERSISTENT_EMR_MASTER_ARM_INSTANCE_TYPES = Index.propertyBuilder("sleeper.bulk.import.persistent.emr.master.arm.instance.types")
            .description("(Persistent EMR mode only) The EC2 ARM64 instance types and weights used for the master " +
                    "node of the persistent EMR cluster.\n" +
                    "For more information, see the Bulk import using EMR - Instance types section in docs/05-ingest.md")
            .defaultValue(DEFAULT_BULK_IMPORT_EMR_MASTER_ARM_INSTANCE_TYPES.getDefaultValue())
            .validationPredicate(EmrInstanceTypeConfig::isValidInstanceTypes)
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty BULK_IMPORT_PERSISTENT_EMR_EXECUTOR_ARM_INSTANCE_TYPES = Index.propertyBuilder("sleeper.bulk.import.persistent.emr.executor.arm.instance.types")
            .description("(Persistent EMR mode only) The EC2 ARM64 instance types and weights used for the executor " +
                    "nodes of the persistent EMR cluster.\n" +
                    "For more information, see the Bulk import using EMR - Instance types section in docs/05-ingest.md")
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
                    "instances equal to this value.")
            .defaultValue("1")
            .validationPredicate(SleeperPropertyValueUtils::isNonNegativeInteger)
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty BULK_IMPORT_PERSISTENT_EMR_MAX_CAPACITY = Index.propertyBuilder("sleeper.bulk.import.persistent.emr.max.capacity")
            .description("(Persistent EMR mode only) The maximum number of capacity units to provision as EC2 " +
                    "instances for executors in the persistent EMR cluster.\n" +
                    "This is measured in instance fleet capacity units. These are declared alongside the requested " +
                    "instance types, as each type will count for a certain number of units. By default the units are " +
                    "the number of instances.\n" +
                    "This value is only used if managed scaling is used.")
            .defaultValue("10")
            .validationPredicate(SleeperPropertyValueUtils::isPositiveInteger)
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .runCdkDeployWhenChanged(true).build();
    UserDefinedInstanceProperty BULK_IMPORT_PERSISTENT_EMR_STEP_CONCURRENCY_LEVEL = Index.propertyBuilder("sleeper.bulk.import.persistent.emr.step.concurrency.level")
            .description("(Persistent EMR mode only) This controls the number of EMR steps that can run concurrently.")
            .defaultValue("2")
            .validationPredicate(SleeperPropertyValueUtils::isPositiveInteger)
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
