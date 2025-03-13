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
package sleeper.bulkexport.runner;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.ecs.EcsClient;

import sleeper.bulkexport.core.model.BulkExportLeafPartitionQuerySerDe;
import sleeper.bulkexport.core.recordretrieval.RunLeafPartitionBulkExportTasks;
import sleeper.configuration.properties.S3InstanceProperties;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.task.common.QueueMessageCount;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;

/**
 * A lambda that is triggered when a serialised leaf partition export query
 * arrives on an SQS
 * queue. A processor executes the request and publishes the results to S3.
 */
@SuppressWarnings("unused")
public class RunSqsLeafPartitionBulkExportLambda {
    private static final Logger LOGGER = LoggerFactory.getLogger(RunSqsLeafPartitionBulkExportLambda.class);

    private final AmazonSQS sqsClient;
    private final AmazonS3 s3Client;
    private final EcsClient ecsClient;
    private BulkExportLeafPartitionQuerySerDe querySerDe;
    private final QueueMessageCount.Client queueMessageCount;
    private final RunLeafPartitionBulkExportTasks runLeafPartitionBulkExportTasks;

    /**
     * Constructs an instance of RunSqsLeafPartitionBulkExportLambda using
     * default clients for Amazon SQS, Amazon S3, and Amazon DynamoDB.
     */
    public RunSqsLeafPartitionBulkExportLambda() {
        String s3Bucket = validateParameter(CONFIG_BUCKET.toEnvironmentVariable());
        sqsClient = AmazonSQSClientBuilder.defaultClient();
        ecsClient = EcsClient.create();
        s3Client = AmazonS3ClientBuilder.defaultClient();
        InstanceProperties instanceProperties = S3InstanceProperties.loadFromBucket(s3Client, s3Bucket);
        runLeafPartitionBulkExportTasks = new RunLeafPartitionBulkExportTasks(instanceProperties, ecsClient);
        queueMessageCount = QueueMessageCount.withSqsClient(sqsClient);
    }

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
