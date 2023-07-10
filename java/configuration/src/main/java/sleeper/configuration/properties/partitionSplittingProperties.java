package sleeper.configuration.properties;

import java.util.List;

public interface partitionSplittingProperties {
    UserDefinedInstanceProperty PARTITION_SPLITTING_PERIOD_IN_MINUTES = UserDefinedInstanceProperty.Index.propertyBuilder("sleeper.partition.splitting.period.minutes")
            .description("The frequency in minutes with which the lambda that finds partitions that need splitting runs.")
            .defaultValue("30")
            .propertyGroup(InstancePropertyGroup.PARTITION_SPLITTING)
            .runCDKDeployWhenChanged(true).build();
    UserDefinedInstanceProperty MAX_NUMBER_FILES_IN_PARTITION_SPLITTING_JOB = UserDefinedInstanceProperty.Index.propertyBuilder("sleeper.partition.splitting.files.maximum")
            .description("When a partition needs splitting, a partition splitting job is created. This reads in the sketch files " +
                    "associated to the files in the partition in order to identify the median. This parameter controls the " +
                    "maximum number of files that are read in.")
            .defaultValue("50")
            .propertyGroup(InstancePropertyGroup.PARTITION_SPLITTING).build();
    UserDefinedInstanceProperty FIND_PARTITIONS_TO_SPLIT_LAMBDA_MEMORY_IN_MB = UserDefinedInstanceProperty.Index.propertyBuilder("sleeper.partition.splitting.finder.memory")
            .description("The amount of memory in MB for the lambda function used to identify partitions that need to be split.")
            .defaultValue("2048")
            .propertyGroup(InstancePropertyGroup.PARTITION_SPLITTING)
            .runCDKDeployWhenChanged(true).build();
    UserDefinedInstanceProperty FIND_PARTITIONS_TO_SPLIT_TIMEOUT_IN_SECONDS = UserDefinedInstanceProperty.Index.propertyBuilder("sleeper.partition.splitting.finder.timeout.seconds")
            .description("The timeout in seconds for the lambda function used to identify partitions that need to be split.")
            .defaultValue("900")
            .propertyGroup(InstancePropertyGroup.PARTITION_SPLITTING)
            .runCDKDeployWhenChanged(true).build();
    UserDefinedInstanceProperty SPLIT_PARTITIONS_LAMBDA_MEMORY_IN_MB = UserDefinedInstanceProperty.Index.propertyBuilder("sleeper.partition.splitting.memory")
            .description("The memory for the lambda function used to split partitions.")
            .defaultValue("2048")
            .propertyGroup(InstancePropertyGroup.PARTITION_SPLITTING)
            .runCDKDeployWhenChanged(true).build();
    UserDefinedInstanceProperty SPLIT_PARTITIONS_TIMEOUT_IN_SECONDS = UserDefinedInstanceProperty.Index.propertyBuilder("sleeper.partition.splitting.timeout.seconds")
            .description("The timeout in seconds for the lambda function used to split partitions.")
            .defaultValue("900")
            .propertyGroup(InstancePropertyGroup.PARTITION_SPLITTING)
            .runCDKDeployWhenChanged(true).build();
    UserDefinedInstanceProperty DEFAULT_PARTITION_SPLIT_THRESHOLD = UserDefinedInstanceProperty.Index.propertyBuilder("sleeper.default.partition.splitting.threshold")
            .description("This is the default value of the partition splitting threshold. Partitions with more than the following " +
                    "number of records in will be split. This value can be overridden on a per-table basis.")
            .defaultValue("1000000000")
            .propertyGroup(InstancePropertyGroup.PARTITION_SPLITTING).build();

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
