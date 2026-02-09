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

/**
 * Definitions of instance properties relating to bulk import on AWS EMR with a separate cluster created for each job.
 */
public interface NonPersistentEMRProperty {
    UserDefinedInstanceProperty DEFAULT_BULK_IMPORT_EMR_RELEASE_LABEL = Index.propertyBuilder("sleeper.default.table.bulk.import.emr.release.label")
            .description("(Non-persistent EMR mode only) The default EMR release label to be used when creating an EMR cluster for bulk importing data " +
                    "using Spark running on EMR.\n" +
                    "This property is a default which can be overridden by a table property or by a property in the " +
                    "bulk import job specification.")
            .defaultValue("emr-7.12.0")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT).build();
    UserDefinedInstanceProperty DEFAULT_BULK_IMPORT_EMR_INSTANCE_ARCHITECTURE = Index.propertyBuilder("sleeper.default.table.bulk.import.emr.instance.architecture")
            .description("(Non-persistent EMR mode only) Which architecture to be used for EC2 instance types " +
                    "in the EMR cluster. Must be either \"x86_64\" \"arm64\" or \"x86_64,arm64\". " +
                    "For more information, see the Bulk import using EMR - Instance types section in docs/usage/bulk-import.md")
            .defaultValue("arm64")
            .validationPredicate(EmrInstanceArchitecture::isValid)
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT).build();
    UserDefinedInstanceProperty DEFAULT_BULK_IMPORT_EMR_MASTER_X86_INSTANCE_TYPES = Index.propertyBuilder("sleeper.default.table.bulk.import.emr.master.x86.instance.types")
            .description("(Non-persistent EMR mode only) The default EC2 x86_64 instance types and weights to be " +
                    "used for the master node of the EMR cluster.\n" +
                    "For more information, see the Bulk import using EMR - Instance types section in docs/usage/bulk-import.md")
            .defaultValue("m7i.xlarge")
            .validationPredicate(EmrInstanceTypeConfig::isValidInstanceTypes)
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT).build();
    UserDefinedInstanceProperty DEFAULT_BULK_IMPORT_EMR_EXECUTOR_X86_INSTANCE_TYPES = Index.propertyBuilder("sleeper.default.table.bulk.import.emr.executor.x86.instance.types")
            .description("(Non-persistent EMR mode only) The default EC2 x86_64 instance types and weights to be " +
                    "used for the executor nodes of the EMR cluster.\n" +
                    "For more information, see the Bulk import using EMR - Instance types section in docs/usage/bulk-import.md")
            .defaultValue("m7i.4xlarge")
            .validationPredicate(EmrInstanceTypeConfig::isValidInstanceTypes)
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT).build();
    UserDefinedInstanceProperty DEFAULT_BULK_IMPORT_EMR_MASTER_ARM_INSTANCE_TYPES = Index.propertyBuilder("sleeper.default.table.bulk.import.emr.master.arm.instance.types")
            .description("(Non-persistent EMR mode only) The default EC2 ARM64 instance types and weights to be used " +
                    "for the master node of the EMR cluster.\n" +
                    "For more information, see the Bulk import using EMR - Instance types section in docs/usage/bulk-import.md")
            .defaultValue("m7g.xlarge")
            .validationPredicate(EmrInstanceTypeConfig::isValidInstanceTypes)
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT).build();
    UserDefinedInstanceProperty DEFAULT_BULK_IMPORT_EMR_EXECUTOR_ARM_INSTANCE_TYPES = Index.propertyBuilder("sleeper.default.table.bulk.import.emr.executor.arm.instance.types")
            .description("(Non-persistent EMR mode only) The default EC2 ARM64 instance types and weights to be used " +
                    "for the executor nodes of the EMR cluster.\n" +
                    "For more information, see the Bulk import using EMR - Instance types section in docs/usage/bulk-import.md")
            .defaultValue("m7g.4xlarge")
            .validationPredicate(EmrInstanceTypeConfig::isValidInstanceTypes)
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT).build();
    UserDefinedInstanceProperty DEFAULT_BULK_IMPORT_EMR_EXECUTOR_MARKET_TYPE = Index.propertyBuilder("sleeper.default.table.bulk.import.emr.executor.market.type")
            .description("(Non-persistent EMR mode only) The default purchasing option to be used for the executor " +
                    "nodes of the EMR cluster.\n" +
                    "Valid values are ON_DEMAND or SPOT.\n" +
                    "This property is a default which can be overridden by a table property or by a property in the " +
                    "bulk import job specification.")
            .defaultValue("SPOT").validationPredicate(s -> "SPOT".equals(s) || "ON_DEMAND".equals(s))
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT).build();
    UserDefinedInstanceProperty DEFAULT_BULK_IMPORT_EMR_INITIAL_EXECUTOR_CAPACITY = Index.propertyBuilder("sleeper.default.table.bulk.import.emr.executor.initial.capacity")
            .description("(Non-persistent EMR mode only) The default initial number of capacity units to provision as EC2 " +
                    "instances for executors in the EMR cluster.\n" +
                    "This is measured in instance fleet capacity units. These are declared alongside the requested " +
                    "instance types, as each type will count for a certain number of units. By default the units are " +
                    "the number of instances.\n" +
                    "This property is a default which can be overridden by a table property or by a property in the " +
                    "bulk import job specification.")
            .defaultValue("2")
            .validationPredicate(SleeperPropertyValueUtils::isNonNegativeInteger)
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT).build();
    UserDefinedInstanceProperty DEFAULT_BULK_IMPORT_EMR_MAX_EXECUTOR_CAPACITY = Index.propertyBuilder("sleeper.default.table.bulk.import.emr.executor.max.capacity")
            .description("(Non-persistent EMR mode only) The default maximum number of capacity units to provision as EC2 " +
                    "instances for executors in the EMR cluster.\n" +
                    "This is measured in instance fleet capacity units. These are declared alongside the requested " +
                    "instance types, as each type will count for a certain number of units. By default the units are " +
                    "the number of instances.\n" +
                    "This property is a default which can be overridden by a table property or by a property in the " +
                    "bulk import job specification.")
            .defaultValue("10")
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
