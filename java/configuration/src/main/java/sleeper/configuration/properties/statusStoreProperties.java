package sleeper.configuration.properties;

import sleeper.configuration.Utils;

import java.util.List;

public interface statusStoreProperties {
    UserDefinedInstanceProperty INGEST_STATUS_STORE_ENABLED = UserDefinedInstanceProperty.Index.propertyBuilder("sleeper.ingest.status.store.enabled")
            .description("Flag to enable/disable storage of tracking information for ingest jobs and tasks.")
            .defaultValue("true")
            .propertyGroup(InstancePropertyGroup.INGEST)
            .runCDKDeployWhenChanged(true).build();
    UserDefinedInstanceProperty INGEST_JOB_STATUS_TTL_IN_SECONDS = UserDefinedInstanceProperty.Index.propertyBuilder("sleeper.ingest.job.status.ttl")
            .description("The time to live in seconds for ingest job updates in the status store. Default is 1 week.\n" +
                    "The expiry time is fixed when an update is saved to the store, so changing this will only affect new data.")
            .defaultValue("604800") // Default is 1 week
            .validationPredicate(Utils::isPositiveInteger)
            .propertyGroup(InstancePropertyGroup.INGEST).build();
    UserDefinedInstanceProperty INGEST_TASK_STATUS_TTL_IN_SECONDS = UserDefinedInstanceProperty.Index.propertyBuilder("sleeper.ingest.task.status.ttl")
            .description("The time to live in seconds for ingest task updates in the status store. Default is 1 week.\n" +
                    "The expiry time is fixed when an update is saved to the store, so changing this will only affect new data.")
            .defaultValue("604800") // Default is 1 week
            .validationPredicate(Utils::isPositiveInteger)
            .propertyGroup(InstancePropertyGroup.INGEST).build();

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
