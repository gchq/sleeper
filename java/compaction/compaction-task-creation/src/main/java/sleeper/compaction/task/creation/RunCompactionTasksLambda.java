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
import software.amazon.awssdk.services.autoscaling.AutoScalingClient;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ecs.EcsClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sqs.SqsClient;

import sleeper.common.task.QueueMessageCount;
import sleeper.common.task.RunCompactionTasks;
import sleeper.configurationv2.properties.S3InstanceProperties;
import sleeper.core.properties.instance.InstanceProperties;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;

/**
 * A lambda function to start compaction tasks based on the number of queued compaction jobs.
 */
public class RunCompactionTasksLambda {
    private final RunCompactionTasks runTasks;
    private final QueueMessageCount.Client queueMessageCount;

    public RunCompactionTasksLambda() {
        String s3Bucket = validateParameter(CONFIG_BUCKET.toEnvironmentVariable());
        SqsClient sqsClient = SqsClient.create();
        EcsClient ecsClient = EcsClient.create();
        S3Client s3Client = S3Client.create();
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
