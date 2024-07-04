/*
 * Copyright 2022-2024 Crown Copyright
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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import sleeper.configuration.properties.instance.InstanceProperties;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static java.nio.file.Files.createTempDirectory;
import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.configuration.properties.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.configuration.properties.PropertiesUtils.loadProperties;
import static sleeper.configuration.properties.instance.ArrayListIngestProperty.MAX_IN_MEMORY_BATCH_SIZE;
import static sleeper.configuration.properties.instance.ArrayListIngestProperty.MAX_RECORDS_TO_WRITE_LOCALLY;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.COMPACTION_AUTO_SCALING_GROUP;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.COMPACTION_CLUSTER;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.COMPACTION_JOB_DLQ_URL;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.COMPACTION_JOB_QUEUE_URL;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.INGEST_CLUSTER;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.INGEST_JOB_DLQ_URL;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.INGEST_JOB_QUEUE_URL;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.PARTITION_SPLITTING_JOB_DLQ_URL;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.PARTITION_SPLITTING_JOB_QUEUE_URL;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.QUERY_DLQ_URL;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.QUERY_LAMBDA_ROLE;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.QUERY_QUEUE_URL;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.QUERY_RESULTS_QUEUE_URL;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.VERSION;
import static sleeper.configuration.properties.instance.CommonProperty.ACCOUNT;
import static sleeper.configuration.properties.instance.CommonProperty.EMAIL_ADDRESS_FOR_ERROR_NOTIFICATION;
import static sleeper.configuration.properties.instance.CommonProperty.FARGATE_VERSION;
import static sleeper.configuration.properties.instance.CommonProperty.FILE_SYSTEM;
import static sleeper.configuration.properties.instance.CommonProperty.ID;
import static sleeper.configuration.properties.instance.CommonProperty.JARS_BUCKET;
import static sleeper.configuration.properties.instance.CommonProperty.LOG_RETENTION_IN_DAYS;
import static sleeper.configuration.properties.instance.CommonProperty.MAXIMUM_CONNECTIONS_TO_S3;
import static sleeper.configuration.properties.instance.CommonProperty.QUEUE_VISIBILITY_TIMEOUT_IN_SECONDS;
import static sleeper.configuration.properties.instance.CommonProperty.REGION;
import static sleeper.configuration.properties.instance.CommonProperty.SUBNETS;
import static sleeper.configuration.properties.instance.CommonProperty.TASK_RUNNER_LAMBDA_MEMORY_IN_MB;
import static sleeper.configuration.properties.instance.CommonProperty.TASK_RUNNER_LAMBDA_TIMEOUT_IN_SECONDS;
import static sleeper.configuration.properties.instance.CommonProperty.VPC_ID;
import static sleeper.configuration.properties.instance.CompactionProperty.COMPACTION_EC2_POOL_DESIRED;
import static sleeper.configuration.properties.instance.CompactionProperty.COMPACTION_EC2_POOL_MAXIMUM;
import static sleeper.configuration.properties.instance.CompactionProperty.COMPACTION_EC2_POOL_MINIMUM;
import static sleeper.configuration.properties.instance.CompactionProperty.COMPACTION_EC2_ROOT_SIZE;
import static sleeper.configuration.properties.instance.CompactionProperty.COMPACTION_EC2_TYPE;
import static sleeper.configuration.properties.instance.CompactionProperty.COMPACTION_ECS_LAUNCHTYPE;
import static sleeper.configuration.properties.instance.CompactionProperty.COMPACTION_GPU_ENABLED;
import static sleeper.configuration.properties.instance.CompactionProperty.COMPACTION_JOB_CREATION_LAMBDA_MEMORY_IN_MB;
import static sleeper.configuration.properties.instance.CompactionProperty.COMPACTION_JOB_CREATION_LAMBDA_PERIOD_IN_MINUTES;
import static sleeper.configuration.properties.instance.CompactionProperty.COMPACTION_JOB_CREATION_LAMBDA_TIMEOUT_IN_SECONDS;
import static sleeper.configuration.properties.instance.CompactionProperty.COMPACTION_KEEP_ALIVE_PERIOD_IN_SECONDS;
import static sleeper.configuration.properties.instance.CompactionProperty.COMPACTION_TASK_ARM_CPU;
import static sleeper.configuration.properties.instance.CompactionProperty.COMPACTION_TASK_ARM_MEMORY;
import static sleeper.configuration.properties.instance.CompactionProperty.COMPACTION_TASK_CPU_ARCHITECTURE;
import static sleeper.configuration.properties.instance.CompactionProperty.COMPACTION_TASK_CREATION_PERIOD_IN_MINUTES;
import static sleeper.configuration.properties.instance.CompactionProperty.ECR_COMPACTION_GPU_REPO;
import static sleeper.configuration.properties.instance.CompactionProperty.ECR_COMPACTION_REPO;
import static sleeper.configuration.properties.instance.CompactionProperty.MAXIMUM_CONCURRENT_COMPACTION_TASKS;
import static sleeper.configuration.properties.instance.GarbageCollectionProperty.GARBAGE_COLLECTOR_LAMBDA_MEMORY_IN_MB;
import static sleeper.configuration.properties.instance.GarbageCollectionProperty.GARBAGE_COLLECTOR_PERIOD_IN_MINUTES;
import static sleeper.configuration.properties.instance.IngestProperty.ECR_INGEST_REPO;
import static sleeper.configuration.properties.instance.IngestProperty.INGEST_KEEP_ALIVE_PERIOD_IN_SECONDS;
import static sleeper.configuration.properties.instance.IngestProperty.INGEST_PARTITION_REFRESH_PERIOD_IN_SECONDS;
import static sleeper.configuration.properties.instance.IngestProperty.INGEST_TASK_CREATION_PERIOD_IN_MINUTES;
import static sleeper.configuration.properties.instance.IngestProperty.MAXIMUM_CONCURRENT_INGEST_TASKS;
import static sleeper.configuration.properties.instance.IngestProperty.S3A_INPUT_FADVISE;
import static sleeper.configuration.properties.instance.PartitionSplittingProperty.FIND_PARTITIONS_TO_SPLIT_LAMBDA_MEMORY_IN_MB;
import static sleeper.configuration.properties.instance.PartitionSplittingProperty.FIND_PARTITIONS_TO_SPLIT_TIMEOUT_IN_SECONDS;
import static sleeper.configuration.properties.instance.PartitionSplittingProperty.SPLIT_PARTITIONS_LAMBDA_MEMORY_IN_MB;
import static sleeper.configuration.properties.instance.PartitionSplittingProperty.SPLIT_PARTITIONS_TIMEOUT_IN_SECONDS;
import static sleeper.configuration.properties.instance.QueryProperty.QUERY_PROCESSING_LAMBDA_RESULTS_BATCH_SIZE;
import static sleeper.configuration.properties.instance.QueryProperty.QUERY_PROCESSING_LAMBDA_STATE_REFRESHING_PERIOD_IN_SECONDS;
import static sleeper.configuration.properties.instance.QueryProperty.QUERY_PROCESSOR_LAMBDA_MEMORY_IN_MB;
import static sleeper.configuration.properties.instance.QueryProperty.QUERY_PROCESSOR_LAMBDA_TIMEOUT_IN_SECONDS;

class InstancePropertiesTest {
    @TempDir
    public Path folder;

    @Test
    void shouldCreateFromFile() throws IOException {
        // Given
        InstanceProperties instanceProperties = getSleeperProperties();

        // When
        File file = new File(createTempDirectory(folder, null).toString() + "/props");
        instanceProperties.save(file);
        InstanceProperties loaded = new InstanceProperties(loadProperties(file));

        // Then
        assertThat(loaded).isEqualTo(instanceProperties);
    }

    @Test
    void shouldLoadAndSaveFromString() {
        // Given
        InstanceProperties instanceProperties = getSleeperProperties();

        // When
        String string = instanceProperties.saveAsString();
        InstanceProperties loaded = new InstanceProperties(loadProperties(string));

        // Then
        assertThat(loaded).isEqualTo(instanceProperties);
    }

    @Test
    void shouldBeAbleToUseStandardGetMethod() {
        InstanceProperties instanceProperties = getSleeperProperties();
        String expectedAccount = "1234567890";
        assertThat(instanceProperties.get(ACCOUNT)).isEqualTo(expectedAccount);
    }

    @Test
    void shouldBeAbleToUseStandardSetMethod() {
        InstanceProperties instanceProperties = new InstanceProperties();

        instanceProperties.set(FILE_SYSTEM, "file://");

        assertThat(instanceProperties.get(FILE_SYSTEM)).isEqualTo("file://");
    }

    @Test
    void shouldTranslatePropertyNamesIntoCompliantEnvironmentVariables() {
        assertThat(ID.toEnvironmentVariable()).isEqualTo("SLEEPER_ID");
    }

    @Test
    void shouldBeAbleToDetermineClassAtRuntime() {
        // Given
        InstanceProperties instanceProperties = getSleeperProperties();

        // When
        String pageSizeString = instanceProperties.get(QUERY_PROCESSING_LAMBDA_RESULTS_BATCH_SIZE);
        Long pageSizeLong = instanceProperties.getLong(QUERY_PROCESSING_LAMBDA_RESULTS_BATCH_SIZE);
        Integer pageSizeInt = instanceProperties.getInt(QUERY_PROCESSING_LAMBDA_RESULTS_BATCH_SIZE);

        // Then
        assertThat(pageSizeString).isEqualTo("100");
        assertThat(pageSizeInt).isEqualTo(Integer.valueOf(100));
        assertThat(pageSizeLong).isEqualTo(Long.valueOf(100L));
    }

    @Test
    void shouldLoadTagsFromProperties() {
        // Given
        Properties tags = new Properties();
        tags.setProperty("tag-1", "value-1");
        tags.setProperty("tag-2", "value-2");

        InstanceProperties properties = new InstanceProperties();
        properties.loadTags(tags);

        assertThat(properties.getTags()).isEqualTo(Map.of(
                "tag-1", "value-1",
                "tag-2", "value-2"));
    }

    @Test
    void shouldLoadNoTagsFromProperties() {
        // Given
        Properties tags = new Properties();

        InstanceProperties properties = new InstanceProperties();
        properties.loadTags(tags);

        assertThat(properties.getTags()).isEmpty();
    }

    @Test
    void shouldDetectNoSystemTestPropertySetWhenNoPropertiesSet() {
        // Given
        InstanceProperties properties = new InstanceProperties();

        assertThat(properties.isAnyPropertySetStartingWith("sleeper.systemtest")).isFalse();
    }

    @Test
    void shouldDetectNoSystemTestPropertySetWhenValidPropertiesSet() {
        // Given
        InstanceProperties properties = createTestInstanceProperties();

        assertThat(properties.isAnyPropertySetStartingWith("sleeper.systemtest")).isFalse();
    }

    @Test
    void shouldDetectSystemTestPropertySetWhenValidPropertiesAlsoSet() {
        // Given
        InstanceProperties properties = new InstanceProperties(loadProperties(
                createTestInstanceProperties().saveAsString() + "\n" +
                        "sleeper.systemtest.writers=123"));

        assertThat(properties.isAnyPropertySetStartingWith("sleeper.systemtest")).isTrue();
    }

    @Test
    void shouldGetUnknownPropertyValues() {
        // Given
        InstanceProperties properties = new InstanceProperties(loadProperties(
                createTestInstanceProperties().saveAsString() + "\n" +
                        "unknown.property=123"));

        // When / Then
        assertThat(properties.getUnknownProperties())
                .containsExactly(Map.entry("unknown.property", "123"));
    }

    private static InstanceProperties getSleeperProperties() {
        InstanceProperties instanceProperties = new InstanceProperties();
        instanceProperties.set(ACCOUNT, "1234567890");
        instanceProperties.set(REGION, "eu-west-2");
        instanceProperties.set(VERSION, "0.1");
        instanceProperties.set(ID, "test");
        instanceProperties.set(QUERY_LAMBDA_ROLE, "arn:aws:iam::1234567890:role/sleeper-QueryRoleABCDEF-GHIJKLMOP");
        Map<String, String> tags = new HashMap<>();
        tags.put("name", "abc");
        tags.put("project", "test");
        instanceProperties.setTags(tags);
        instanceProperties.setNumber(MAXIMUM_CONCURRENT_COMPACTION_TASKS, 100);
        instanceProperties.setNumber(MAXIMUM_CONCURRENT_INGEST_TASKS, 200);
        instanceProperties.set(VPC_ID, "aVPC");
        instanceProperties.set(SUBNETS, "subnet1");
        instanceProperties.setNumber(GARBAGE_COLLECTOR_PERIOD_IN_MINUTES, 20);
        instanceProperties.setNumber(QUEUE_VISIBILITY_TIMEOUT_IN_SECONDS, 600);
        instanceProperties.setNumber(COMPACTION_KEEP_ALIVE_PERIOD_IN_SECONDS, 700);
        instanceProperties.setNumber(INGEST_KEEP_ALIVE_PERIOD_IN_SECONDS, 800);
        instanceProperties.set(JARS_BUCKET, "bucket");
        instanceProperties.set(ECR_COMPACTION_GPU_REPO, "sleeper-compaction-gpu");
        instanceProperties.set(ECR_COMPACTION_REPO, "sleeper-compaction");
        instanceProperties.set(ECR_INGEST_REPO, "sleeper-ingest");
        instanceProperties.set(PARTITION_SPLITTING_JOB_QUEUE_URL, "url");
        instanceProperties.set(PARTITION_SPLITTING_JOB_DLQ_URL, "url2");
        instanceProperties.set(COMPACTION_JOB_QUEUE_URL, "url3");
        instanceProperties.set(COMPACTION_JOB_DLQ_URL, "url4");
        instanceProperties.set(COMPACTION_CLUSTER, "ecsCluster1");
        instanceProperties.set(COMPACTION_AUTO_SCALING_GROUP, "autoScalingGroup1");
        instanceProperties.set(EMAIL_ADDRESS_FOR_ERROR_NOTIFICATION, "user@domain");
        instanceProperties.set(QUERY_QUEUE_URL, "url7");
        instanceProperties.set(QUERY_DLQ_URL, "url8");
        instanceProperties.set(QUERY_RESULTS_QUEUE_URL, "url9");
        instanceProperties.set(INGEST_JOB_QUEUE_URL, "url10");
        instanceProperties.set(INGEST_JOB_DLQ_URL, "url11");
        instanceProperties.set(INGEST_CLUSTER, "ecsCluster3");
        instanceProperties.set(COMPACTION_GPU_ENABLED, "false");
        instanceProperties.setNumber(COMPACTION_JOB_CREATION_LAMBDA_PERIOD_IN_MINUTES, 5);
        instanceProperties.setNumber(COMPACTION_TASK_CREATION_PERIOD_IN_MINUTES, 6);
        instanceProperties.setNumber(INGEST_TASK_CREATION_PERIOD_IN_MINUTES, 7);
        instanceProperties.setNumber(COMPACTION_JOB_CREATION_LAMBDA_MEMORY_IN_MB, 1024);
        instanceProperties.setNumber(COMPACTION_JOB_CREATION_LAMBDA_TIMEOUT_IN_SECONDS, 600);
        instanceProperties.set(COMPACTION_EC2_TYPE, "t3.xlarge");
        instanceProperties.setNumber(COMPACTION_EC2_POOL_MINIMUM, 0);
        instanceProperties.setNumber(COMPACTION_EC2_POOL_DESIRED, 0);
        instanceProperties.setNumber(COMPACTION_EC2_POOL_MAXIMUM, 3);
        instanceProperties.setNumber(COMPACTION_EC2_ROOT_SIZE, 50);
        instanceProperties.set(COMPACTION_ECS_LAUNCHTYPE, "FARGATE");
        instanceProperties.setNumber(TASK_RUNNER_LAMBDA_MEMORY_IN_MB, 2048);
        instanceProperties.setNumber(TASK_RUNNER_LAMBDA_TIMEOUT_IN_SECONDS, 600);
        instanceProperties.setNumber(GARBAGE_COLLECTOR_LAMBDA_MEMORY_IN_MB, 2048);
        instanceProperties.setNumber(FIND_PARTITIONS_TO_SPLIT_LAMBDA_MEMORY_IN_MB, 4096);
        instanceProperties.setNumber(FIND_PARTITIONS_TO_SPLIT_TIMEOUT_IN_SECONDS, 600);
        instanceProperties.setNumber(SPLIT_PARTITIONS_LAMBDA_MEMORY_IN_MB, 1024);
        instanceProperties.setNumber(SPLIT_PARTITIONS_TIMEOUT_IN_SECONDS, 600);
        instanceProperties.setNumber(QUERY_PROCESSOR_LAMBDA_MEMORY_IN_MB, 4096);
        instanceProperties.setNumber(QUERY_PROCESSOR_LAMBDA_TIMEOUT_IN_SECONDS, 600);
        instanceProperties.setNumber(MAXIMUM_CONNECTIONS_TO_S3, 25);
        instanceProperties.setNumber(QUERY_PROCESSING_LAMBDA_STATE_REFRESHING_PERIOD_IN_SECONDS, 300);
        instanceProperties.setNumber(QUERY_PROCESSING_LAMBDA_RESULTS_BATCH_SIZE, 100);
        instanceProperties.set(FARGATE_VERSION, "1.4.0");
        instanceProperties.setNumber(INGEST_PARTITION_REFRESH_PERIOD_IN_SECONDS, 300);
        instanceProperties.setNumber(MAX_IN_MEMORY_BATCH_SIZE, 1_000_000L);
        instanceProperties.set(FILE_SYSTEM, "s3a://");
        instanceProperties.setNumber(LOG_RETENTION_IN_DAYS, 1);
        instanceProperties.set(COMPACTION_TASK_CPU_ARCHITECTURE, "ARM64");
        instanceProperties.setNumber(COMPACTION_TASK_ARM_CPU, 2048);
        instanceProperties.setNumber(COMPACTION_TASK_ARM_MEMORY, 4096);
        instanceProperties.setNumber(MAX_RECORDS_TO_WRITE_LOCALLY, 100_000_000L);
        instanceProperties.setNumber(MAX_IN_MEMORY_BATCH_SIZE, 1_000_000L);
        instanceProperties.set(S3A_INPUT_FADVISE, "normal");

        return instanceProperties;
    }
}
