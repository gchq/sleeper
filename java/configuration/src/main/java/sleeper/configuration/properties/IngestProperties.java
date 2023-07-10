package sleeper.configuration.properties;

import sleeper.configuration.Utils;

import java.util.List;

public interface IngestProperties {
    UserDefinedInstanceProperty ECR_INGEST_REPO = Index.propertyBuilder("sleeper.ingest.repo")
            .description("The name of the ECR repository for the ingest container. The Docker image from the ingest module should have been " +
                    "uploaded to an ECR repository of this name in this account.")
            .propertyGroup(InstancePropertyGroup.INGEST)
            .runCDKDeployWhenChanged(true).build();
    UserDefinedInstanceProperty MAXIMUM_CONCURRENT_INGEST_TASKS = Index.propertyBuilder("sleeper.ingest.max.concurrent.tasks")
            .description("The maximum number of concurrent ECS tasks to run.")
            .defaultValue("200")
            .propertyGroup(InstancePropertyGroup.INGEST).build();
    UserDefinedInstanceProperty INGEST_TASK_CREATION_PERIOD_IN_MINUTES = Index.propertyBuilder("sleeper.ingest.task.creation.period.minutes")
            .description("The frequency in minutes with which an EventBridge rule runs to trigger a lambda that, if necessary, runs more ECS " +
                    "tasks to perform ingest jobs.")
            .defaultValue("1")
            .validationPredicate(Utils::isPositiveInteger)
            .propertyGroup(InstancePropertyGroup.INGEST)
            .runCDKDeployWhenChanged(true).build();
    UserDefinedInstanceProperty INGEST_KEEP_ALIVE_PERIOD_IN_SECONDS = Index.propertyBuilder("sleeper.ingest.keepalive.period.seconds")
            .description("The frequency, in seconds, with which change message visibility requests are sent to extend the " +
                    "visibility of messages on the ingest queue so that they are not processed by other processes.\n" +
                    "This should be less than the value of sleeper.queue.visibility.timeout.seconds.")
            .defaultValue("300")
            .propertyGroup(InstancePropertyGroup.INGEST).build();
    UserDefinedInstanceProperty S3A_INPUT_FADVISE = Index.propertyBuilder("sleeper.ingest.fs.s3a.experimental.input.fadvise")
            .description("This sets the value of fs.s3a.experimental.input.fadvise on the Hadoop configuration used to read and write " +
                    "files to and from S3 in ingest jobs. Changing this value allows you to fine-tune how files are read. Possible " +
                    "values are \"normal\", \"sequential\" and \"random\". More information is available here:\n" +
                    "https://hadoop.apache.org/docs/current/hadoop-aws/tools/hadoop-aws/performance.html#fadvise.")
            .defaultValue("sequential")
            .validationPredicate(Utils::isValidFadvise)
            .propertyGroup(InstancePropertyGroup.INGEST).build();
    UserDefinedInstanceProperty INGEST_TASK_CPU = Index.propertyBuilder("sleeper.ingest.task.cpu")
            .description("The amount of CPU used by Fargate tasks that perform ingest jobs.\n" +
                    "Note that only certain combinations of CPU and memory are valid.\n" +
                    "See https://docs.aws.amazon.com/AmazonECS/latest/developerguide/task-cpu-memory-error.html for valid options.")
            .defaultValue("2048")
            .propertyGroup(InstancePropertyGroup.INGEST)
            .runCDKDeployWhenChanged(true).build();
    UserDefinedInstanceProperty INGEST_TASK_MEMORY = Index.propertyBuilder("sleeper.ingest.task.memory")
            .description("The amount of memory used by Fargate tasks that perform ingest jobs.\n" +
                    "Note that only certain combinations of CPU and memory are valid.\n" +
                    "See https://docs.aws.amazon.com/AmazonECS/latest/developerguide/task-cpu-memory-error.html for valid options.")
            .defaultValue("4096")
            .propertyGroup(InstancePropertyGroup.INGEST)
            .runCDKDeployWhenChanged(true).build();
    UserDefinedInstanceProperty INGEST_PARTITION_REFRESH_PERIOD_IN_SECONDS = Index.propertyBuilder("sleeper.ingest.partition.refresh.period")
            .description("The frequency in seconds with which ingest tasks refresh their view of the partitions.\n" +
                    "(NB Refreshes only happen once a batch of data has been written so this is a lower bound " +
                    "on the refresh frequency.)")
            .defaultValue("120")
            .propertyGroup(InstancePropertyGroup.INGEST).build();
    UserDefinedInstanceProperty INGEST_SOURCE_BUCKET = Index.propertyBuilder("sleeper.ingest.source.bucket")
            .description("A comma-separated list of buckets that contain files to be ingested via ingest jobs. The buckets should already " +
                    "exist, i.e. they will not be created as part of the cdk deployment of this instance of Sleeper. The ingest " +
                    "and bulk import stacks will be given read access to these buckets so that they can consume data from them.")
            .propertyGroup(InstancePropertyGroup.INGEST)
            .runCDKDeployWhenChanged(true).build();
    UserDefinedInstanceProperty INGEST_RECORD_BATCH_TYPE = Index.propertyBuilder("sleeper.ingest.record.batch.type")
            .description("The way in which records are held in memory before they are written to a local store.\n" +
                    "Valid values are 'arraylist' and 'arrow'.\n" +
                    "The arraylist method is simpler, but it is slower and requires careful tuning of the number of records in each batch.")
            .defaultValue("arrow")
            .propertyGroup(InstancePropertyGroup.INGEST).build();
    UserDefinedInstanceProperty INGEST_PARTITION_FILE_WRITER_TYPE = Index.propertyBuilder("sleeper.ingest.partition.file.writer.type")
            .description("The way in which partition files are written to the main Sleeper store.\n" +
                    "Valid values are 'direct' (which writes using the s3a Hadoop file system) and 'async' (which writes locally and then " +
                    "copies the completed Parquet file asynchronously into S3).\n" +
                    "The direct method is simpler but the async method should provide better performance when the number of partitions " +
                    "is large.")
            .defaultValue("async")
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
