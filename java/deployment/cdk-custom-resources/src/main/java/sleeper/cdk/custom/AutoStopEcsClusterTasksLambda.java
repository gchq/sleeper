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
package sleeper.cdk.custom;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.events.CloudFormationCustomResourceEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.ecs.EcsClient;

import sleeper.core.util.ThreadSleep;

import java.util.Map;
import java.util.function.Consumer;

import static sleeper.core.util.RateLimitUtils.sleepForSustainedRatePerSecond;

/** Deletes an ECS cluster. */
public class AutoStopEcsClusterTasksLambda {
    public static final Logger LOGGER = LoggerFactory.getLogger(AutoStopEcsClusterTasksLambda.class);

    private final EcsClient ecsClient;
    private final ThreadSleep sleep;
    private final int maxResults;

    public AutoStopEcsClusterTasksLambda() {
        this(EcsClient.create(), Thread::sleep, 100);
    }

    public AutoStopEcsClusterTasksLambda(EcsClient ecsClient, ThreadSleep sleep, int maxResults) {
        this.ecsClient = ecsClient;
        this.sleep = sleep;
        this.maxResults = maxResults;
    }

    /**
     * Handles an event triggered by CloudFormation.
     *
     * @param event   the event to handle
     * @param context the context
     */
    public void handleEvent(
            CloudFormationCustomResourceEvent event, Context context) {
        Map<String, Object> resourceProperties = event.getResourceProperties();
        String clusterName = (String) resourceProperties.get("cluster");

        switch (event.getRequestType()) {
            case "Create":
                break;
            case "Update":
                break;
            case "Delete":
                stopTasks(ecsClient, clusterName, maxResults, sleep);
                break;
            default:
                throw new IllegalArgumentException("Invalid request type: " + event.getRequestType());
        }
    }

    private static void stopTasks(EcsClient ecs, String clusterName, int maxResults, ThreadSleep sleep) {
        LOGGER.info("Stopping tasks for ECS cluster {}", clusterName);
        forEachTaskArn(ecs, clusterName, maxResults, taskArn -> {
            // Rate limit for ECS StopTask is 100 burst, 40 sustained:
            // https://docs.aws.amazon.com/AmazonECS/latest/APIReference/request-throttling.html
            sleepForSustainedRatePerSecond(30, sleep);
            ecs.stopTask(builder -> builder.cluster(clusterName).task(taskArn)
                    .reason("Cleaning up before cdk destroy"));
            LOGGER.info("Stopped task {} in ECS cluster {}", taskArn, clusterName);
        });
    }

    private static void forEachTaskArn(EcsClient ecs, String clusterName, int maxResults, Consumer<String> consumer) {
        ecs.listTasksPaginator(builder -> builder.cluster(clusterName).maxResults(maxResults))
                .stream()
                .peek(response -> LOGGER.info("Found {} tasks", response.taskArns().size()))
                .flatMap(response -> response.taskArns().stream())
                .forEach(consumer);
    }

}
