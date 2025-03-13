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
package sleeper.compaction.task.creation;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.events.ScheduledEvent;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import software.amazon.awssdk.services.autoscaling.AutoScalingClient;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ecs.EcsClient;

import sleeper.configuration.properties.S3InstanceProperties;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.task.common.QueueMessageCount;
import sleeper.task.common.RunCompactionTasks;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;

/**
 * A lambda function to start compaction tasks based on the number of queued compaction jobs.
 */
public class RunCompactionTasksLambda {
    private final RunCompactionTasks runTasks;
    private final QueueMessageCount.Client queueMessageCount;

    public RunCompactionTasksLambda() {
        String s3Bucket = validateParameter(CONFIG_BUCKET.toEnvironmentVariable());
        AmazonSQS sqsClient = AmazonSQSClientBuilder.defaultClient();
        EcsClient ecsClient = EcsClient.create();
        AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();
        AutoScalingClient asClient = AutoScalingClient.create();
        Ec2Client ec2Client = Ec2Client.create();
        InstanceProperties instanceProperties = S3InstanceProperties.loadFromBucket(s3Client, s3Bucket);
        this.runTasks = new RunCompactionTasks(instanceProperties, ecsClient, asClient, ec2Client);
        this.queueMessageCount = QueueMessageCount.withSqsClient(sqsClient);
    }

    public void eventHandler(ScheduledEvent event, Context context) {
        runTasks.run(queueMessageCount);
    }

    private static String validateParameter(String parameterName) {
        String parameter = System.getenv(parameterName);
        if (null == parameter || parameter.isEmpty()) {
            throw new IllegalArgumentException("Missing environment variable: " + parameter);
        }
        return parameter;
    }
}
