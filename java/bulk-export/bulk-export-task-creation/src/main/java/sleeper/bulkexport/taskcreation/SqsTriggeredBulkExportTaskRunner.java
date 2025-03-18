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
package sleeper.bulkexport.taskcreation;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.ecs.EcsClient;

import sleeper.configuration.properties.S3InstanceProperties;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.task.common.QueueMessageCount;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;

/**
 * Lambda function triggered by an SQS message from a CloudWatch schedule.
 * Initiates bulk export ECS tasks based on the count of leaf partition bulk export jobs in the queue.
 */
@SuppressWarnings("unused")
public class SqsTriggeredBulkExportTaskRunner {
    private static final Logger LOGGER = LoggerFactory.getLogger(SqsTriggeredBulkExportTaskRunner.class);

    private final QueueMessageCount.Client queueMessageCount;
    private final RunLeafPartitionBulkExportTasks runLeafPartitionBulkExportTasks;

    public SqsTriggeredBulkExportTaskRunner() {
        String s3Bucket = validateParameter(CONFIG_BUCKET.toEnvironmentVariable());
        AmazonSQS sqsClient = AmazonSQSClientBuilder.defaultClient();
        AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();
        EcsClient ecsClient = EcsClient.create();
        InstanceProperties instanceProperties = S3InstanceProperties.loadFromBucket(s3Client, s3Bucket);
        runLeafPartitionBulkExportTasks = new RunLeafPartitionBulkExportTasks(instanceProperties, ecsClient);
        queueMessageCount = QueueMessageCount.withSqsClient(sqsClient);
    }

    /**
     * Constructs an instance of SqsTriggeredBulkExportTaskRunner using
     * the provided clients for Amazon SQS, Amazon S3, and Amazon DynamoDB.
     *
     * @param input   the SQS event
     * @param context the lambda context
     */
    public void handleRequest(SQSEvent input, Context context) {
        runLeafPartitionBulkExportTasks.run(queueMessageCount);
    }

    private static String validateParameter(String parameterName) {
        String parameter = System.getenv(parameterName);
        if (null == parameter || parameter.isEmpty()) {
            throw new IllegalArgumentException("Missing environment variable: " + parameter);
        }
        return parameter;
    }
}
