package sleeper.configuration.properties;

import sleeper.configuration.Utils;

import java.util.List;

public interface athenaProperties {
    UserDefinedInstanceProperty SPILL_BUCKET_AGE_OFF_IN_DAYS = Index.propertyBuilder("sleeper.athena.spill.bucket.ageoff.days")
            .description("The number of days before objects in the spill bucket are deleted.")
            .defaultValue("1")
            .propertyGroup(InstancePropertyGroup.ATHENA)
            .runCDKDeployWhenChanged(true).build();
    UserDefinedInstanceProperty ATHENA_COMPOSITE_HANDLER_CLASSES = Index.propertyBuilder("sleeper.athena.handler.classes")
            .description("The fully qualified composite classes to deploy. These are the classes that interact with Athena. " +
                    "You can choose to remove one if you don't need them. Both are deployed by default.")
            .defaultValue("sleeper.athena.composite.SimpleCompositeHandler,sleeper.athena.composite.IteratorApplyingCompositeHandler")
            .propertyGroup(InstancePropertyGroup.ATHENA)
            .runCDKDeployWhenChanged(true).build();
    UserDefinedInstanceProperty ATHENA_COMPOSITE_HANDLER_MEMORY = Index.propertyBuilder("sleeper.athena.handler.memory")
            .description("The amount of memory (GB) the athena composite handler has.")
            .defaultValue("4096")
            .propertyGroup(InstancePropertyGroup.ATHENA)
            .runCDKDeployWhenChanged(true).build();
    UserDefinedInstanceProperty ATHENA_COMPOSITE_HANDLER_TIMEOUT_IN_SECONDS = Index.propertyBuilder("sleeper.athena.handler.timeout.seconds")
            .description("The timeout in seconds for the athena composite handler.")
            .defaultValue("900")
            .validationPredicate(Utils::isValidLambdaTimeout)
            .propertyGroup(InstancePropertyGroup.ATHENA)
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
