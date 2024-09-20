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
package sleeper.ingest.starter;

import com.amazonaws.services.ecs.AmazonECS;
import com.amazonaws.services.ecs.AmazonECSClientBuilder;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.events.ScheduledEvent;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;

import sleeper.configuration.properties.PropertiesReloader;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.instance.S3InstanceProperties;
import sleeper.core.ContainerConstants;
import sleeper.task.common.RunIngestTasks;

import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;

/**
 * A lambda function to run ingest tasks.
 */
@SuppressWarnings("unused")
public class RunIngestTasksLambda {
    private final RunIngestTasks runTasks;

    public RunIngestTasksLambda() {
        AmazonSQS sqsClient = AmazonSQSClientBuilder.defaultClient();
        AmazonECS ecsClient = AmazonECSClientBuilder.defaultClient();
        AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();

        String s3Bucket = validateParameter(CONFIG_BUCKET.toEnvironmentVariable());
        InstanceProperties instanceProperties = S3InstanceProperties.loadFromBucket(s3Client, s3Bucket);

        this.runTasks = new RunIngestTasks(sqsClient,
                ecsClient, instanceProperties,
                PropertiesReloader.ifConfigured(s3Client, instanceProperties),
                ContainerConstants.INGEST_CONTAINER_NAME);
    }

    public void eventHandler(ScheduledEvent event, Context context) {
        runTasks.run();
    }

    private String validateParameter(String parameterName) {
        String parameter = System.getenv(parameterName);
        if (null == parameter || "".equals(parameter)) {
            throw new IllegalArgumentException("RunTasksLambda can't get parameter " + parameterName);
        }
        return parameter;
    }
}
