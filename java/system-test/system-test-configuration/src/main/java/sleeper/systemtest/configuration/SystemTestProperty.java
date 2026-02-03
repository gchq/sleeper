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
package sleeper.systemtest.configuration;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.commons.lang3.EnumUtils;

import sleeper.core.properties.SleeperPropertyIndex;
import sleeper.core.properties.instance.InstanceProperty;
import sleeper.core.properties.model.IngestQueue;
import sleeper.core.properties.model.SleeperPropertyValueUtils;

import java.util.List;
import java.util.Objects;

import static sleeper.core.properties.model.SleeperPropertyValueUtils.describeEnumValuesInLowerCase;

// Suppress as this class will always be referenced before impl class, so initialization behaviour will be deterministic
@SuppressFBWarnings("IC_SUPERCLASS_USES_SUBCLASS_DURING_INITIALIZATION")
public interface SystemTestProperty extends InstanceProperty {
    int SYSTEM_TEST_ID_MAX_LEN = 13;
    SystemTestProperty SYSTEM_TEST_ID = Index.propertyBuilder("sleeper.systemtest.standalone.id")
            .description("The id of the deployment, if deploying standalone. This is also used as a base to generate " +
                    "Sleeper instance IDs, so must be short enough to leave room to define multiple instances.")
            .validationPredicate(value -> value == null || value.length() <= SYSTEM_TEST_ID_MAX_LEN)
            .editable(false).build();
    SystemTestProperty SYSTEM_TEST_ACCOUNT = Index.propertyBuilder("sleeper.systemtest.standalone.account")
            .description("The AWS account when deploying standalone.")
            .editable(false).build();
    SystemTestProperty SYSTEM_TEST_REGION = Index.propertyBuilder("sleeper.systemtest.standalone.region")
            .description("The AWS region when deploying standalone.")
            .editable(false).build();
    SystemTestProperty SYSTEM_TEST_VPC_ID = Index.propertyBuilder("sleeper.systemtest.standalone.vpc")
            .description("The id of the VPC to deploy to, when deploying standalone.")
            .editable(false).build();
    SystemTestProperty SYSTEM_TEST_JARS_BUCKET = Index.propertyBuilder("sleeper.systemtest.standalone.jars.bucket")
            .description("The S3 bucket containing the jar files of the Sleeper components, when deploying standalone.")
            .runCdkDeployWhenChanged(true).build();
    SystemTestProperty SYSTEM_TEST_LOG_RETENTION_DAYS = Index.propertyBuilder("sleeper.systemtest.standalone.log.retention.days")
            .description("The length of time in days that CloudWatch logs from lambda functions, ECS containers, etc., are retained.\n" +
                    "Used when deploying resources and Sleeper instances against a standalone system test deployment.\n" +
                    "See https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/aws-resource-logs-loggroup.html for valid options.\n" +
                    "Use -1 to indicate infinite retention.")
            .defaultValue("30")
            .validationPredicate(SleeperPropertyValueUtils::isValidLogRetention)
            .runCdkDeployWhenChanged(true).build();
    SystemTestProperty SYSTEM_TEST_REPO = Index.propertyBuilder("sleeper.systemtest.repo")
            .description("The image in ECR used for writing random data to the system")
            .validationPredicate(Objects::nonNull)
            .runCdkDeployWhenChanged(true).build();
    SystemTestProperty SYSTEM_TEST_CLUSTER_ENABLED = Index.propertyBuilder("sleeper.systemtest.cluster.enabled")
            .description("Whether to deploy the system test cluster for data generation")
            .defaultValue("true").validationPredicate(SleeperPropertyValueUtils::isTrueOrFalse)
            .runCdkDeployWhenChanged(true).build();
    SystemTestProperty SYSTEM_TEST_CLUSTER_NAME = Index.propertyBuilder("sleeper.systemtest.cluster")
            .description("The name of the ECS cluster where system test tasks will run")
            .setByCdk(true).build();
    SystemTestProperty SYSTEM_TEST_BUCKET_NAME = Index.propertyBuilder("sleeper.systemtest.bucket")
            .description("The name of the bucket where system test data will be stored")
            .setByCdk(true).build();
    SystemTestProperty SYSTEM_TEST_KEEP_ALIVE_PERIOD_IN_SECONDS = Index.propertyBuilder("sleeper.systemtest.keepalive.period.seconds")
            .description("The frequency, in seconds, with which change message visibility requests are sent to extend the " +
                    "visibility of messages on the system test queue so that they are not processed by other processes.\n" +
                    "This should be less than the value of sleeper.systemtest.queue.visibility.timeout.seconds.")
            .defaultValue("300").build();
    SystemTestProperty SYSTEM_TEST_QUEUE_VISIBILITY_TIMEOUT_IN_SECONDS = Index.propertyBuilder("sleeper.systemtest.queue.visibility.timeout.seconds")
            .description("The visibility timeout in seconds for the system test job queue. " +
                    "This should be greater than sleeper.systemtest.keepalive.period.seconds.")
            .defaultValue("900")
            .validationPredicate(SleeperPropertyValueUtils::isPositiveInteger)
            .runCdkDeployWhenChanged(true)
            .build();
    SystemTestProperty WRITE_DATA_TASK_DEFINITION_FAMILY = Index.propertyBuilder("sleeper.systemtest.task.definition")
            .description("The name of the family of task definitions used for writing data")
            .setByCdk(true).build();
    SystemTestProperty SYSTEM_TEST_TASK_CPU = Index.propertyBuilder("sleeper.systemtest.task.cpu")
            .description("The number of CPU units for the containers that write random data, where 1024 is 1 vCPU.\n" +
                    "For valid values, see: " +
                    "https://docs.aws.amazon.com/AmazonECS/latest/userguide/fargate-task-defs.html")
            .defaultValue("1024").runCdkDeployWhenChanged(true).build();
    SystemTestProperty SYSTEM_TEST_TASK_MEMORY = Index.propertyBuilder("sleeper.systemtest.task.memory.mb")
            .description("The memory for the containers that write random data, in MiB.\n" +
                    "For valid values, see: " +
                    "https://docs.aws.amazon.com/AmazonECS/latest/userguide/fargate-task-defs.html")
            .defaultValue("4096").runCdkDeployWhenChanged(true).build();
    SystemTestProperty INGEST_MODE = Index.propertyBuilder("sleeper.systemtest.ingest.mode")
            .description("The ingest mode to write random data. This should be either 'direct', 'queue', 'batcher', " +
                    "or 'generate_only'.\n" +
                    "Direct means that the data is written directly using an ingest coordinator.\n" +
                    "Queue means that the data is written to a Parquet file and an ingest job is created " +
                    "and posted to an ingest queue. The queue used is set by the property " +
                    "`sleeper.systemtest.ingest.queue`.\n" +
                    "Batcher means that the data is written to a Parquet file and posted to the ingest batcher. " +
                    "This will be processed based on the table properties under `sleeper.table.ingest.batcher`. " +
                    "These are defaulted based on instance properties under `sleeper.default.ingest.batcher`.\n" +
                    "Generate only means that the data is written to a Parquet file in the system test bucket, " +
                    "but the file is not ingested. The ingest will need to be performed manually in a separate step.")
            .defaultValue(SystemTestIngestMode.DIRECT.toString())
            .validationPredicate(s -> EnumUtils.isValidEnumIgnoreCase(SystemTestIngestMode.class, s)).build();
    SystemTestProperty INGEST_QUEUE = Index.propertyBuilder("sleeper.systemtest.ingest.queue")
            .description("Which queue to use when using the 'queue' ingest mode.\n" +
                    "Valid values: " + describeEnumValuesInLowerCase(IngestQueue.class))
            .defaultValue(IngestQueue.STANDARD_INGEST.toString())
            .validationPredicate(s -> EnumUtils.isValidEnumIgnoreCase(IngestQueue.class, s))
            .build();
    SystemTestProperty NUMBER_OF_WRITERS = Index.propertyBuilder("sleeper.systemtest.writers")
            .description("The number of containers that write random data")
            .defaultValue("1").validationPredicate(SleeperPropertyValueUtils::isNonNegativeInteger).build();
    SystemTestProperty NUMBER_OF_INGESTS_PER_WRITER = Index.propertyBuilder("sleeper.systemtest.ingests.per.writer")
            .description("The number of ingests to run for each writer")
            .defaultValue("1").validationPredicate(SleeperPropertyValueUtils::isNonNegativeInteger).build();
    SystemTestProperty NUMBER_OF_ROWS_PER_INGEST = Index.propertyBuilder("sleeper.systemtest.rows.per.ingest")
            .description("The number of random rows that each ingest should write")
            .defaultValue("100").validationPredicate(SleeperPropertyValueUtils::isNonNegativeInteger).build();
    SystemTestProperty MIN_RANDOM_INT = Index.propertyBuilder("sleeper.systemtest.random.int.min")
            .description("The minimum value of integers generated randomly during random row generation")
            .defaultValue("0").validationPredicate(SleeperPropertyValueUtils::isInteger).build();
    SystemTestProperty MAX_RANDOM_INT = Index.propertyBuilder("sleeper.systemtest.random.int.max")
            .description("The maximum value of integers generated randomly during random row generation")
            .defaultValue("100000000").validationPredicate(SleeperPropertyValueUtils::isInteger).build();
    SystemTestProperty MIN_RANDOM_LONG = Index.propertyBuilder("sleeper.systemtest.random.long.min")
            .description("The minimum value of longs generated randomly during random row generation")
            .defaultValue("0").validationPredicate(SleeperPropertyValueUtils::isLong).build();
    SystemTestProperty MAX_RANDOM_LONG = Index.propertyBuilder("sleeper.systemtest.random.long.max")
            .description("The maximum value of longs generated randomly during random row generation")
            .defaultValue("10000000000").validationPredicate(SleeperPropertyValueUtils::isLong).build();
    SystemTestProperty RANDOM_STRING_LENGTH = Index.propertyBuilder("sleeper.systemtest.random.string.length")
            .description("The length of strings generated randomly during random row generation")
            .defaultValue("10").validationPredicate(SleeperPropertyValueUtils::isNonNegativeInteger).build();
    SystemTestProperty RANDOM_BYTE_ARRAY_LENGTH = Index.propertyBuilder("sleeper.systemtest.random.bytearray.length")
            .description("The length of byte arrays generated randomly during random row generation")
            .defaultValue("10").validationPredicate(SleeperPropertyValueUtils::isNonNegativeInteger).build();
    SystemTestProperty MAX_ENTRIES_RANDOM_MAP = Index.propertyBuilder("sleeper.systemtest.random.map.length")
            .description("The maximum number of entries in maps generated randomly during random row generation\n" +
                    "(the number of entries in the map will range randomly from 0 to this number)")
            .defaultValue("10").validationPredicate(SleeperPropertyValueUtils::isNonNegativeInteger).build();
    SystemTestProperty MAX_ENTRIES_RANDOM_LIST = Index.propertyBuilder("sleeper.systemtest.random.list.length")
            .description("The maximum number of entries in lists generated randomly during random row generation\n" +
                    "(the number of entries in the list will range randomly from 0 to this number)")
            .defaultValue("10").validationPredicate(SleeperPropertyValueUtils::isNonNegativeInteger).build();

    static List<SystemTestProperty> getAll() {
        return Index.INSTANCE.getAll();
    }

    class Index {
        private Index() {
        }

        static final SleeperPropertyIndex<SystemTestProperty> INSTANCE = new SleeperPropertyIndex<>();

        private static SystemTestPropertyImpl.Builder propertyBuilder(String propertyName) {
            return SystemTestPropertyImpl.named(propertyName)
                    .addToIndex(INSTANCE::add);
        }
    }
}
