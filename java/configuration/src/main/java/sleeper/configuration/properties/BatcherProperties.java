package sleeper.configuration.properties;

import sleeper.configuration.Utils;

import java.util.List;

public interface BatcherProperties {
    UserDefinedInstanceProperty INGEST_BATCHER_SUBMITTER_MEMORY_IN_MB = Index.propertyBuilder("sleeper.ingest.batcher.submitter.memory.mb")
            .description("The amount of memory in MB for the lambda that receives submitted requests to ingest files.")
            .defaultValue("1024")
            .propertyGroup(InstancePropertyGroup.INGEST)
            .runCDKDeployWhenChanged(true).build();
    UserDefinedInstanceProperty INGEST_BATCHER_SUBMITTER_TIMEOUT_IN_SECONDS = Index.propertyBuilder("sleeper.ingest.batcher.submitter.timeout.seconds")
            .description("The timeout in seconds for the lambda that receives submitted requests to ingest files.")
            .defaultValue("20")
            .propertyGroup(InstancePropertyGroup.INGEST)
            .runCDKDeployWhenChanged(true).build();
    UserDefinedInstanceProperty INGEST_BATCHER_JOB_CREATION_MEMORY_IN_MB = Index.propertyBuilder("sleeper.ingest.batcher.job.creation.memory.mb")
            .description("The amount of memory in MB for the lambda that creates ingest jobs from submitted file ingest requests.")
            .defaultValue("1024")
            .propertyGroup(InstancePropertyGroup.INGEST)
            .runCDKDeployWhenChanged(true).build();
    UserDefinedInstanceProperty INGEST_BATCHER_JOB_CREATION_TIMEOUT_IN_SECONDS = Index.propertyBuilder("sleeper.ingest.batcher.job.creation.timeout.seconds")
            .description("The timeout in seconds for the lambda that creates ingest jobs from submitted file ingest requests.")
            .defaultValue("900")
            .propertyGroup(InstancePropertyGroup.INGEST)
            .runCDKDeployWhenChanged(true).build();
    UserDefinedInstanceProperty INGEST_BATCHER_JOB_CREATION_LAMBDA_PERIOD_IN_MINUTES = Index.propertyBuilder("sleeper.ingest.batcher.job.creation.period.minutes")
            .description("The rate at which the ingest batcher job creation lambda runs (in minutes, must be >=1).")
            .defaultValue("1")
            .validationPredicate(Utils::isPositiveInteger)
            .propertyGroup(InstancePropertyGroup.INGEST)
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
