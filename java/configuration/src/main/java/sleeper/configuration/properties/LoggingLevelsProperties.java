package sleeper.configuration.properties;

import java.util.List;

public interface LoggingLevelsProperties {
    UserDefinedInstanceProperty LOGGING_LEVEL = Index.propertyBuilder("sleeper.logging.level")
            .description("The logging level for logging Sleeper classes. This does not apply to the MetricsLogger which is always set to INFO.")
            .propertyGroup(InstancePropertyGroup.LOGGING)
            .runCDKDeployWhenChanged(true).build();
    UserDefinedInstanceProperty APACHE_LOGGING_LEVEL = Index.propertyBuilder("sleeper.logging.apache.level")
            .description("The logging level for Apache logs that are not Parquet.")
            .propertyGroup(InstancePropertyGroup.LOGGING)
            .runCDKDeployWhenChanged(true).build();
    UserDefinedInstanceProperty PARQUET_LOGGING_LEVEL = Index.propertyBuilder("sleeper.logging.parquet.level")
            .description("The logging level for Parquet logs.")
            .propertyGroup(InstancePropertyGroup.LOGGING)
            .runCDKDeployWhenChanged(true).build();
    UserDefinedInstanceProperty AWS_LOGGING_LEVEL = Index.propertyBuilder("sleeper.logging.aws.level")
            .description("The logging level for AWS logs.")
            .propertyGroup(InstancePropertyGroup.LOGGING)
            .runCDKDeployWhenChanged(true).build();
    UserDefinedInstanceProperty ROOT_LOGGING_LEVEL = Index.propertyBuilder("sleeper.logging.root.level")
            .description("The logging level for everything else.")
            .propertyGroup(InstancePropertyGroup.LOGGING)
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
