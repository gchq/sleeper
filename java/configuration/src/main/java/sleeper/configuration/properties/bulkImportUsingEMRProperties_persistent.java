package sleeper.configuration.properties;

import sleeper.configuration.Utils;

import java.util.List;

import static sleeper.configuration.properties.UserDefinedInstanceProperty.*;
import static sleeper.configuration.properties.bulkImportUsingEMRProperties_non_persistant.*;

public interface bulkImportUsingEMRProperties_persistent {
    UserDefinedInstanceProperty BULK_IMPORT_PERSISTENT_EMR_RELEASE_LABEL = UserDefinedInstanceProperty.Index.propertyBuilder("sleeper.bulk.import.persistent.emr.release.label")
            .description("(Persistent EMR mode only) The EMR release used to create the persistent EMR cluster.")
            .defaultValue(DEFAULT_BULK_IMPORT_EMR_RELEASE_LABEL.getDefaultValue())
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .runCDKDeployWhenChanged(true).build();
    UserDefinedInstanceProperty BULK_IMPORT_PERSISTENT_EMR_MASTER_X86_INSTANCE_TYPES = UserDefinedInstanceProperty.Index.propertyBuilder("sleeper.bulk.import.persistent.emr.master.x86.instance.types")
            .description("(Persistent EMR mode only) The EC2 x86 instance types used for the master node of the " +
                    "persistent EMR cluster. " +
                    "For more information, see the Bulk import using EMR - Instance types section in docs/05-ingest.md")
            .defaultValue(DEFAULT_BULK_IMPORT_EMR_MASTER_X86_INSTANCE_TYPES.getDefaultValue())
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .runCDKDeployWhenChanged(true).build();
    UserDefinedInstanceProperty BULK_IMPORT_PERSISTENT_EMR_EXECUTOR_X86_INSTANCE_TYPES = UserDefinedInstanceProperty.Index.propertyBuilder("sleeper.bulk.import.persistent.emr.executor.x86.instance.types")
            .description("(Persistent EMR mode only) The EC2 x86 instance types used for the executor nodes of the " +
                    "persistent EMR cluster. " +
                    "For more information, see the Bulk import using EMR - Instance types section in docs/05-ingest.md")
            .defaultValue(DEFAULT_BULK_IMPORT_EMR_EXECUTOR_X86_INSTANCE_TYPES.getDefaultValue())
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .runCDKDeployWhenChanged(true).build();
    UserDefinedInstanceProperty BULK_IMPORT_PERSISTENT_EMR_MASTER_ARM_INSTANCE_TYPES = UserDefinedInstanceProperty.Index.propertyBuilder("sleeper.bulk.import.persistent.emr.master.arm.instance.types")
            .description("(Persistent EMR mode only) The EC2 ARM64 instance types used for the master node of the " +
                    "persistent EMR cluster. " +
                    "For more information, see the Bulk import using EMR - Instance types section in docs/05-ingest.md")
            .defaultValue(DEFAULT_BULK_IMPORT_EMR_MASTER_ARM_INSTANCE_TYPES.getDefaultValue())
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .runCDKDeployWhenChanged(true).build();
    UserDefinedInstanceProperty BULK_IMPORT_PERSISTENT_EMR_EXECUTOR_ARM_INSTANCE_TYPES = UserDefinedInstanceProperty.Index.propertyBuilder("sleeper.bulk.import.persistent.emr.executor.arm.instance.types")
            .description("(Persistent EMR mode only) The EC2 ARM64 instance types used for the executor nodes of the " +
                    "persistent EMR cluster. " +
                    "For more information, see the Bulk import using EMR - Instance types section in docs/05-ingest.md")
            .defaultValue(DEFAULT_BULK_IMPORT_EMR_EXECUTOR_ARM_INSTANCE_TYPES.getDefaultValue())
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .runCDKDeployWhenChanged(true).build();
    UserDefinedInstanceProperty BULK_IMPORT_PERSISTENT_EMR_USE_MANAGED_SCALING = UserDefinedInstanceProperty.Index.propertyBuilder("sleeper.bulk.import.persistent.emr.use.managed.scaling")
            .description("(Persistent EMR mode only) Whether the persistent EMR cluster should use managed scaling or not.")
            .defaultValue("true")
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .runCDKDeployWhenChanged(true).build();
    UserDefinedInstanceProperty BULK_IMPORT_PERSISTENT_EMR_MIN_CAPACITY = UserDefinedInstanceProperty.Index.propertyBuilder("sleeper.bulk.import.persistent.emr.min.capacity")
            .description("(Persistent EMR mode only) The minimum number of capacity units to provision as EC2 " +
                    "instances for executors in the persistent EMR cluster.\n" +
                    "This is measured in instance fleet capacity units. These are declared alongside the requested " +
                    "instance types, as each type will count for a certain number of units. By default the units are " +
                    "the number of instances.\n" +
                    "If managed scaling is not used then the cluster will be of fixed size, with a number of " +
                    "instances equal to this value.")
            .defaultValue("1")
            .validationPredicate(Utils::isNonNegativeInteger)
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .runCDKDeployWhenChanged(true).build();
    UserDefinedInstanceProperty BULK_IMPORT_PERSISTENT_EMR_MAX_CAPACITY = UserDefinedInstanceProperty.Index.propertyBuilder("sleeper.bulk.import.persistent.emr.max.capacity")
            .description("(Persistent EMR mode only) The maximum number of capacity units to provision as EC2 " +
                    "instances for executors in the persistent EMR cluster.\n" +
                    "This is measured in instance fleet capacity units. These are declared alongside the requested " +
                    "instance types, as each type will count for a certain number of units. By default the units are " +
                    "the number of instances.\n" +
                    "This value is only used if managed scaling is used.")
            .defaultValue("10")
            .validationPredicate(Utils::isPositiveInteger)
            .propertyGroup(InstancePropertyGroup.BULK_IMPORT)
            .runCDKDeployWhenChanged(true).build();
    UserDefinedInstanceProperty BULK_IMPORT_PERSISTENT_EMR_STEP_CONCURRENCY_LEVEL = UserDefinedInstanceProperty.Index.propertyBuilder("sleeper.bulk.import.persistent.emr.step.concurrency.level")
            .description("(Persistent EMR mode only) This controls the number of EMR steps that can run concurrently.")
            .defaultValue("2")
            .validationPredicate(Utils::isPositiveInteger)
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
            return UserDefinedInstancePropertyImpl.named(propertyName)
                    .addToIndex(INSTANCE::add);
        }
    }
}
