/*
 * Copyright 2022 Crown Copyright
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

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import sleeper.core.CommonTestConstants;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.COMPACTION_CLUSTER;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.COMPACTION_JOB_DLQ_URL;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.COMPACTION_JOB_QUEUE_URL;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.INGEST_CLUSTER;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.INGEST_JOB_DLQ_URL;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.INGEST_JOB_QUEUE_URL;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.PARTITION_SPLITTING_DLQ_URL;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.PARTITION_SPLITTING_QUEUE_URL;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.QUERY_DLQ_URL;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.QUERY_LAMBDA_ROLE;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.QUERY_QUEUE_URL;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.QUERY_RESULTS_QUEUE_URL;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.SPLITTING_COMPACTION_CLUSTER;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.SPLITTING_COMPACTION_JOB_DLQ_URL;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.SPLITTING_COMPACTION_JOB_QUEUE_URL;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.ACCOUNT;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.COMPACTION_JOB_CREATION_LAMBDA_MEMORY_IN_MB;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.COMPACTION_JOB_CREATION_LAMBDA_PERIOD_IN_MINUTES;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.COMPACTION_JOB_CREATION_LAMBDA_TIMEOUT_IN_SECONDS;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.COMPACTION_KEEP_ALIVE_PERIOD_IN_SECONDS;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.COMPACTION_TASK_CPU;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.COMPACTION_TASK_CREATION_PERIOD_IN_MINUTES;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.COMPACTION_TASK_MEMORY;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.ECR_COMPACTION_REPO;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.ECR_INGEST_REPO;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.EMAIL_ADDRESS_FOR_ERROR_NOTIFICATION;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.FARGATE_VERSION;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.FILE_SYSTEM;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.FIND_PARTITIONS_TO_SPLIT_LAMBDA_MEMORY_IN_MB;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.FIND_PARTITIONS_TO_SPLIT_TIMEOUT_IN_SECONDS;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.GARBAGE_COLLECTOR_LAMBDA_MEMORY_IN_MB;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.GARBAGE_COLLECTOR_PERIOD_IN_MINUTES;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.ID;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.INGEST_KEEP_ALIVE_PERIOD_IN_SECONDS;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.INGEST_PARTITION_REFRESH_PERIOD_IN_SECONDS;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.INGEST_TASK_CREATION_PERIOD_IN_MINUTES;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.JARS_BUCKET;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.LOG_RETENTION_IN_DAYS;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.MAXIMUM_CONCURRENT_COMPACTION_TASKS;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.MAXIMUM_CONCURRENT_INGEST_TASKS;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.MAXIMUM_CONNECTIONS_TO_S3;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.MAX_IN_MEMORY_BATCH_SIZE;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.MAX_RECORDS_TO_WRITE_LOCALLY;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.QUERY_PROCESSING_LAMBDA_RESULTS_BATCH_SIZE;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.QUERY_PROCESSING_LAMBDA_STATE_REFRESHING_PERIOD_IN_SECONDS;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.QUERY_PROCESSOR_LAMBDA_MEMORY_IN_MB;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.QUERY_PROCESSOR_LAMBDA_TIMEOUT_IN_SECONDS;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.QUEUE_VISIBILITY_TIMEOUT_IN_SECONDS;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.REGION;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.S3A_INPUT_FADVISE;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.SPLIT_PARTITIONS_LAMBDA_MEMORY_IN_MB;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.SPLIT_PARTITIONS_TIMEOUT_IN_SECONDS;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.SUBNET;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.TABLE_PROPERTIES;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.TASK_RUNNER_LAMBDA_MEMORY_IN_MB;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.TASK_RUNNER_LAMBDA_TIMEOUT_IN_SECONDS;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.VERSION;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.VPC_ID;

public class InstancePropertiesTest {
    @Rule
    public TemporaryFolder folder = new TemporaryFolder(CommonTestConstants.TMP_DIRECTORY);

    @Test
    public void shouldCreateFromFile() throws IOException {
        // Given
        InstanceProperties instanceProperties = getSleeperProperties();

        // When
        File file = new File(folder.newFolder() + "/props");
        instanceProperties.save(file);
        InstanceProperties loaded = new InstanceProperties();
        loaded.load(file);

        // Then
        assertEquals(instanceProperties, loaded);
    }

    @Test
    public void shouldLoadAndSaveFromString() throws IOException {
        // Given
        InstanceProperties instanceProperties = getSleeperProperties();

        // When
        String string = instanceProperties.saveAsString();
        InstanceProperties loaded = new InstanceProperties();
        loaded.loadFromString(string);

        // Then
        assertEquals(instanceProperties, loaded);
    }

    @Test
    public void shouldBeAbleToUseStandardGetMethod() {
        InstanceProperties instanceProperties = getSleeperProperties();
        String expectedAccount = "1234567890";
        assertEquals(expectedAccount, instanceProperties.get(ACCOUNT));
    }

    @Test
    public void shouldBeAbleToUseStandardSetMethod() {
        InstanceProperties instanceProperties = new InstanceProperties();

        instanceProperties.set(FILE_SYSTEM, "file://");

        assertEquals("file://", instanceProperties.get(FILE_SYSTEM));
    }

    @Test
    public void shouldTranslatePropertyNamesIntoCompliantEnvironmentVariables() {
        assertEquals("SLEEPER_ID", ID.toEnvironmentVariable());
    }

    @Test
    public void shouldBeAbleToDetermineClassAtRuntime() {
        // Given
        InstanceProperties instanceProperties = getSleeperProperties();

        // When
        String pageSizeString = instanceProperties.get(QUERY_PROCESSING_LAMBDA_RESULTS_BATCH_SIZE);
        Long pageSizeLong = instanceProperties.getLong(QUERY_PROCESSING_LAMBDA_RESULTS_BATCH_SIZE);
        Integer pageSizeInt = instanceProperties.getInt(QUERY_PROCESSING_LAMBDA_RESULTS_BATCH_SIZE);

        // Then
        assertEquals("100", pageSizeString);
        assertEquals(new Integer(100), pageSizeInt);
        assertEquals(new Long(100), pageSizeLong);
    }

    @Test
    public void shouldThrowExceptionOnLoadIfRequiredPropertyIsMissing() throws IOException {
        // Given - no account set
        InstanceProperties instanceProperties = new InstanceProperties();
        instanceProperties.set(REGION, "eu-west-2");
        instanceProperties.set(JARS_BUCKET, "jars");
        instanceProperties.set(VERSION, "0.1");
        instanceProperties.set(ID, "test");
        instanceProperties.set(VPC_ID, "aVPC");
        instanceProperties.set(SUBNET, "subnet1");
        instanceProperties.set(TABLE_PROPERTIES, "/path/to/table.properties");

        // When
        String serialised = instanceProperties.saveAsString();

        // Then
        try {
            new InstanceProperties().loadFromString(serialised);
            fail("Exception expected");
        } catch (Exception e) {
            assertNotNull(e.getMessage());
        }
    }

    @Test
    public void shouldThrowExceptionOnLoadIfPropertyIsInvalid() throws IOException {
        // Given
        InstanceProperties instanceProperties = new InstanceProperties();
        instanceProperties.set(ACCOUNT, "12345");
        instanceProperties.set(REGION, "eu-west-2");
        instanceProperties.set(JARS_BUCKET, "jars");
        instanceProperties.set(VERSION, "0.1");
        instanceProperties.set(ID, "test");
        instanceProperties.set(VPC_ID, "aVPC");
        instanceProperties.set(SUBNET, "subnet1");
        instanceProperties.set(TABLE_PROPERTIES, "/path/to/table.properties");

        // When
        instanceProperties.set(MAXIMUM_CONNECTIONS_TO_S3, "-1");
        String serialised = instanceProperties.saveAsString();

        // Then
        try {
            new InstanceProperties().loadFromString(serialised);
            fail("Exception expected");
        } catch (Exception e) {
            assertNotNull(e.getMessage());
        }
    }

    private static InstanceProperties getSleeperProperties() {
        InstanceProperties instanceProperties = new InstanceProperties();
        instanceProperties.set(ACCOUNT, "1234567890");
        instanceProperties.set(REGION, "eu-west-2");
        instanceProperties.set(VERSION, "0.1");
        instanceProperties.set(ID, "test");
        instanceProperties.set(TABLE_PROPERTIES, "/path/to/table.properties");
        instanceProperties.set(QUERY_LAMBDA_ROLE, "arn:aws:iam::1234567890:role/sleeper-QueryRoleABCDEF-GHIJKLMOP");
        Map<String, String> tags = new HashMap<>();
        tags.put("name", "abc");
        tags.put("project", "test");
        instanceProperties.setTags(tags);
        instanceProperties.setNumber(MAXIMUM_CONCURRENT_COMPACTION_TASKS, 100);
        instanceProperties.setNumber(MAXIMUM_CONCURRENT_INGEST_TASKS, 200);
        instanceProperties.set(VPC_ID, "aVPC");
        instanceProperties.set(SUBNET, "subnet1");
        instanceProperties.setNumber(GARBAGE_COLLECTOR_PERIOD_IN_MINUTES, 20);
        instanceProperties.setNumber(QUEUE_VISIBILITY_TIMEOUT_IN_SECONDS, 600);
        instanceProperties.setNumber(COMPACTION_KEEP_ALIVE_PERIOD_IN_SECONDS, 700);
        instanceProperties.setNumber(INGEST_KEEP_ALIVE_PERIOD_IN_SECONDS, 800);
        instanceProperties.set(JARS_BUCKET, "bucket");
        instanceProperties.set(ECR_COMPACTION_REPO, "sleeper-compaction");
        instanceProperties.set(ECR_INGEST_REPO, "sleeper-ingest");
        instanceProperties.set(PARTITION_SPLITTING_QUEUE_URL, "url");
        instanceProperties.set(PARTITION_SPLITTING_DLQ_URL, "url2");
        instanceProperties.set(COMPACTION_JOB_QUEUE_URL, "url3");
        instanceProperties.set(COMPACTION_JOB_DLQ_URL, "url4");
        instanceProperties.set(SPLITTING_COMPACTION_JOB_QUEUE_URL, "url5");
        instanceProperties.set(SPLITTING_COMPACTION_JOB_DLQ_URL, "url6");
        instanceProperties.set(COMPACTION_CLUSTER, "ecsCluster1");
        instanceProperties.set(SPLITTING_COMPACTION_CLUSTER, "ecsCluster2");
        instanceProperties.set(EMAIL_ADDRESS_FOR_ERROR_NOTIFICATION, "user@domain");
        instanceProperties.set(QUERY_QUEUE_URL, "url7");
        instanceProperties.set(QUERY_DLQ_URL, "url8");
        instanceProperties.set(QUERY_RESULTS_QUEUE_URL, "url9");
        instanceProperties.set(INGEST_JOB_QUEUE_URL, "url10");
        instanceProperties.set(INGEST_JOB_DLQ_URL, "url11");
        instanceProperties.set(INGEST_CLUSTER, "ecsCluster3");
        instanceProperties.setNumber(COMPACTION_JOB_CREATION_LAMBDA_PERIOD_IN_MINUTES, 5);
        instanceProperties.setNumber(COMPACTION_TASK_CREATION_PERIOD_IN_MINUTES, 6);
        instanceProperties.setNumber(INGEST_TASK_CREATION_PERIOD_IN_MINUTES, 7);
        instanceProperties.setNumber(COMPACTION_JOB_CREATION_LAMBDA_MEMORY_IN_MB, 1024);
        instanceProperties.setNumber(COMPACTION_JOB_CREATION_LAMBDA_TIMEOUT_IN_SECONDS, 600);
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
        instanceProperties.setNumber(COMPACTION_TASK_CPU, 2048);
        instanceProperties.setNumber(COMPACTION_TASK_MEMORY, 4096);
        instanceProperties.setNumber(MAX_RECORDS_TO_WRITE_LOCALLY, 100_000_000L);
        instanceProperties.setNumber(MAX_IN_MEMORY_BATCH_SIZE, 1_000_000L);
        instanceProperties.set(S3A_INPUT_FADVISE, "normal");

        return instanceProperties;
    }
}
