/*
 * Copyright 2022-2023 Crown Copyright
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
package sleeper.compaction.taskcreation;

import com.amazonaws.services.autoscaling.AmazonAutoScaling;
import com.amazonaws.services.autoscaling.AmazonAutoScalingClientBuilder;
import com.amazonaws.services.ecs.AmazonECS;
import com.amazonaws.services.ecs.AmazonECSClientBuilder;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.events.ScheduledEvent;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;

import java.io.IOException;

import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.CONFIG_BUCKET;

/**
 * A lambda function to execute {@link RunTasks}.
 */
@SuppressWarnings("unused")
public class RunTasksLambda {
    private final AmazonSQS sqsClient;
    private final AmazonECS ecsClient;
    private final AmazonS3 s3Client;
    private final AmazonAutoScaling asClient;
    private final String s3Bucket;
    private final String type;
    private final RunTasks runTasks;

    public RunTasksLambda() throws IOException {

        this.s3Bucket = validateParameter(CONFIG_BUCKET.toEnvironmentVariable());
        this.type = validateParameter("type");
        this.sqsClient = AmazonSQSClientBuilder.defaultClient();
        this.ecsClient = AmazonECSClientBuilder.defaultClient();
        this.s3Client = AmazonS3ClientBuilder.defaultClient();
        this.asClient = AmazonAutoScalingClientBuilder.defaultClient();
        this.runTasks = new RunTasks(sqsClient, ecsClient, s3Client, asClient, s3Bucket, type);
    }

    public void eventHandler(ScheduledEvent event, Context context) throws InterruptedException {
        runTasks.run();
    }

    private String validateParameter(String parameterName) {
        String parameter = System.getenv(parameterName);
        if (null == parameter || "".equals(parameter)) {
            throw new IllegalArgumentException("Missing environment variable: " + parameter);
        }
        return parameter;
    }
}
