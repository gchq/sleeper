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

package sleeper.configuration.properties;


import sleeper.configuration.Utils;

import java.util.List;

public interface NonPersistentEMRProperty {
    UserDefinedInstanceProperty DEFAULT_BULK_IMPORT_EMR_RELEASE_LABEL = Index.propertyBuilder("sleeper.default.bulk.import.emr.release.label")
            .description("(Non-persistent EMR mode only) The default EMR release label to be used when creating an EMR cluster for bulk importing data " +
                    "using Spark running on EMR.\n" +
                    "This property is a default which can be overridden by a table property or by a property in the " +
                    "bulk import job specification.")
            .defaultValue("emr-6.10.0")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT).build();
    UserDefinedInstanceProperty DEFAULT_BULK_IMPORT_EMR_MASTER_X86_INSTANCE_TYPES = Index.propertyBuilder("sleeper.default.bulk.import.emr.master.x86.instance.types")
            .description("(Non-persistent EMR mode only) The default EC2 x86 instance types to be used for the master " +
                    "node of the EMR cluster. " +
                    "For more information, see the Bulk import using EMR - Instance types section in docs/05-ingest.md")
            .defaultValue("m6i.xlarge")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT).build();
    UserDefinedInstanceProperty DEFAULT_BULK_IMPORT_EMR_EXECUTOR_X86_INSTANCE_TYPES = Index.propertyBuilder("sleeper.default.bulk.import.emr.executor.x86.instance.types")
            .description("(Non-persistent EMR mode only) The default EC2 x86_64 instance types to be used for the executor " +
                    "nodes of the EMR cluster. " +
                    "For more information, see the Bulk import using EMR - Instance types section in docs/05-ingest.md")
            .defaultValue("m6i.4xlarge")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT).build();
    UserDefinedInstanceProperty DEFAULT_BULK_IMPORT_EMR_MASTER_ARM_INSTANCE_TYPES = Index.propertyBuilder("sleeper.default.bulk.import.emr.master.arm.instance.types")
            .description("(Non-persistent EMR mode only) The default EC2 ARM64 instance types to be used for the master " +
                    "node of the EMR cluster. " +
                    "For more information, see the Bulk import using EMR - Instance types section in docs/05-ingest.md")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT).build();
    UserDefinedInstanceProperty DEFAULT_BULK_IMPORT_EMR_EXECUTOR_ARM_INSTANCE_TYPES = Index.propertyBuilder("sleeper.default.bulk.import.emr.executor.arm.instance.types")
            .description("(Non-persistent EMR mode only) The default EC2 ARM64 instance types to be used for the executor " +
                    "nodes of the EMR cluster. " +
                    "For more information, see the Bulk import using EMR - Instance types section in docs/05-ingest.md")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT).build();
    UserDefinedInstanceProperty DEFAULT_BULK_IMPORT_EMR_EXECUTOR_MARKET_TYPE = Index.propertyBuilder("sleeper.default.bulk.import.emr.executor.market.type")
            .description("(Non-persistent EMR mode only) The default purchasing option to be used for the executor " +
                    "nodes of the EMR cluster.\n" +
                    "Valid values are ON_DEMAND or SPOT.\n" +
                    "This property is a default which can be overridden by a table property or by a property in the " +
                    "bulk import job specification.")
            .defaultValue("SPOT").validationPredicate(s -> ("SPOT".equals(s) || "ON_DEMAND".equals(s)))
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT).build();
    UserDefinedInstanceProperty DEFAULT_BULK_IMPORT_EMR_INITIAL_EXECUTOR_CAPACITY = Index.propertyBuilder("sleeper.default.bulk.import.emr.executor.initial.instances")
            .description("(Non-persistent EMR mode only) The default initial number of capacity units to provision as EC2 " +
                    "instances for executors in the EMR cluster.\n" +
                    "This is measured in instance fleet capacity units. These are declared alongside the requested " +
                    "instance types, as each type will count for a certain number of units. By default the units are " +
                    "the number of instances.\n" +
                    "This property is a default which can be overridden by a table property or by a property in the " +
                    "bulk import job specification.")
            .defaultValue("2")
            .validationPredicate(Utils::isNonNegativeInteger)
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT).build();
    UserDefinedInstanceProperty DEFAULT_BULK_IMPORT_EMR_MAX_EXECUTOR_CAPACITY = Index.propertyBuilder("sleeper.default.bulk.import.emr.executor.max.instances")
            .description("(Non-persistent EMR mode only) The default maximum number of capacity units to provision as EC2 " +
                    "instances for executors in the EMR cluster.\n" +
                    "This is measured in instance fleet capacity units. These are declared alongside the requested " +
                    "instance types, as each type will count for a certain number of units. By default the units are " +
                    "the number of instances.\n" +
                    "This property is a default which can be overridden by a table property or by a property in the " +
                    "bulk import job specification.")
            .defaultValue("10")
            .validationPredicate(Utils::isPositiveInteger)
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT).build();


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
            return UserDefinedInstancePropertyImpl.named(propertyName)
                    .addToIndex(INSTANCE::add);
        }
    }
}
