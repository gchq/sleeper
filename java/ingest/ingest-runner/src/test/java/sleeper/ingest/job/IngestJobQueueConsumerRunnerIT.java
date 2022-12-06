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

package sleeper.ingest.job;

import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.testcontainers.containers.localstack.LocalStackContainer;
import sleeper.configuration.jars.ObjectFactory;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.core.CommonTestConstants;
import sleeper.ingest.task.IngestTaskStatus;
import sleeper.ingest.task.IngestTaskStatusStore;
import sleeper.ingest.task.status.DynamoDBIngestTaskStatusStore;
import sleeper.ingest.task.status.DynamoDBIngestTaskStatusStoreCreator;
import sleeper.ingest.testutils.AwsExternalResource;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.INGEST_JOB_QUEUE_URL;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.FILE_SYSTEM;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.ID;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.INGEST_PARTITION_FILE_WRITER_TYPE;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.INGEST_RECORD_BATCH_TYPE;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.INGEST_STATUS_STORE_ENABLED;

public class IngestJobQueueConsumerRunnerIT {
    @ClassRule
    public static final AwsExternalResource AWS_EXTERNAL_RESOURCE = new AwsExternalResource(
            LocalStackContainer.Service.S3,
            LocalStackContainer.Service.SQS,
            LocalStackContainer.Service.DYNAMODB,
            LocalStackContainer.Service.CLOUDWATCH);
    private static final String TEST_INSTANCE_NAME = "myinstance";
    private static final String TEST_TABLE_NAME = "mytable";
    private static final String INGEST_QUEUE_NAME = TEST_INSTANCE_NAME + "-ingestqueue";
    private static final String CONFIG_BUCKET_NAME = TEST_INSTANCE_NAME + "-configbucket";
    private static final String INGEST_DATA_BUCKET_NAME = TEST_INSTANCE_NAME + "-" + TEST_TABLE_NAME + "-ingestdata";
    private static final String TABLE_DATA_BUCKET_NAME = TEST_INSTANCE_NAME + "-" + TEST_TABLE_NAME + "-tabledata";
    private static final String FILE_SYSTEM_PREFIX = "s3a://";
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder(CommonTestConstants.TMP_DIRECTORY);

    @Before
    public void before() throws IOException {
        AWS_EXTERNAL_RESOURCE.getS3Client().createBucket(CONFIG_BUCKET_NAME);
        AWS_EXTERNAL_RESOURCE.getS3Client().createBucket(TABLE_DATA_BUCKET_NAME);
        AWS_EXTERNAL_RESOURCE.getS3Client().createBucket(INGEST_DATA_BUCKET_NAME);
        AWS_EXTERNAL_RESOURCE.getSqsClient().createQueue(INGEST_QUEUE_NAME);
    }

    @After
    public void after() {
        AWS_EXTERNAL_RESOURCE.clear();
    }

    private InstanceProperties getInstanceProperties() {
        InstanceProperties instanceProperties = new InstanceProperties();
        instanceProperties.set(ID, TEST_INSTANCE_NAME);
        instanceProperties.set(CONFIG_BUCKET, CONFIG_BUCKET_NAME);
        instanceProperties.set(INGEST_JOB_QUEUE_URL, AWS_EXTERNAL_RESOURCE.getSqsClient().getQueueUrl(INGEST_QUEUE_NAME).getQueueUrl());
        instanceProperties.set(FILE_SYSTEM, FILE_SYSTEM_PREFIX);
        instanceProperties.set(INGEST_RECORD_BATCH_TYPE, "arraylist");
        instanceProperties.set(INGEST_PARTITION_FILE_WRITER_TYPE, "direct");
        instanceProperties.set(INGEST_STATUS_STORE_ENABLED, "true");
        return instanceProperties;
    }

    @Test
    public void shouldRecordIngestTaskFinishedWhenIngestCompleteWithNoJobRuns() throws Exception {
        // Given
        InstanceProperties instanceProperties = getInstanceProperties();
        DynamoDBIngestTaskStatusStoreCreator.create(instanceProperties, AWS_EXTERNAL_RESOURCE.getDynamoDBClient());
        IngestTaskStatusStore taskStatusStore = DynamoDBIngestTaskStatusStore.from(AWS_EXTERNAL_RESOURCE.getDynamoDBClient(),
                instanceProperties);

        // When
        IngestJobQueueConsumerRunner jobRunner = new IngestJobQueueConsumerRunner(
                ObjectFactory.noUserJars(),
                instanceProperties,
                "/mnt/scratch",
                AWS_EXTERNAL_RESOURCE.getSqsClient(),
                AWS_EXTERNAL_RESOURCE.getCloudWatchClient(),
                AWS_EXTERNAL_RESOURCE.getS3Client(),
                AWS_EXTERNAL_RESOURCE.getDynamoDBClient());
        String testTaskId = "test-task";
        jobRunner.run(testTaskId);

        // Then
        assertThat(taskStatusStore.getTask(testTaskId))
                .extracting(IngestTaskStatus::isFinished, IngestTaskStatus::getJobRuns)
                .containsExactly(true, 0);
    }
}
