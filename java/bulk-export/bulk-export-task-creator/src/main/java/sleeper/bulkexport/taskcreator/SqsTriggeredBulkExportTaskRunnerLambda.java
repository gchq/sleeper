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
package sleeper.bulkexport.taskcreator;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.ecs.EcsClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sqs.SqsClient;

import sleeper.common.task.QueueMessageCount;
import sleeper.configurationv2.properties.S3InstanceProperties;
import sleeper.core.properties.instance.InstanceProperties;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;

/**
 * Lambda function triggered by an SQS message from a CloudWatch schedule.
 * Initiates bulk export ECS tasks based on the count of leaf partition bulk export jobs in the queue.
 */
@SuppressWarnings("unused")
public class SqsTriggeredBulkExportTaskRunnerLambda {
    private static final Logger LOGGER = LoggerFactory.getLogger(SqsTriggeredBulkExportTaskRunnerLambda.class);

    private final QueueMessageCount.Client queueMessageCount;
    private final RunLeafPartitionBulkExportTasks runLeafPartitionBulkExportTasks;

    public SqsTriggeredBulkExportTaskRunnerLambda() {
        String s3Bucket = validateParameter(CONFIG_BUCKET.toEnvironmentVariable());
        SqsClient sqsClient = SqsClient.create();
        S3Client s3Client = S3Client.create();
        EcsClient ecsClient = EcsClient.create();
        InstanceProperties instanceProperties = S3InstanceProperties.loadFromBucket(s3Client, s3Bucket);
        runLeafPartitionBulkExportTasks = new RunLeafPartitionBulkExportTasks(instanceProperties, ecsClient);
        queueMessageCount = QueueMessageCount.withSqsClient(sqsClient);
    }

    /**
     * Initiates bulk export ECS tasks based on the count of leaf partition bulk
     * export jobs in the queue.
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
