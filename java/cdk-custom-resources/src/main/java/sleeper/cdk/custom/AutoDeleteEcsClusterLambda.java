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
import software.amazon.awssdk.services.ecs.model.ClusterNotFoundException;
import software.amazon.awssdk.services.ecs.model.ListContainerInstancesResponse;

import java.util.Map;

import static sleeper.core.util.RateLimitUtils.sleepForSustainedRatePerSecond;

/**
 * Deletes an ECS cluster.
 */
public class AutoDeleteEcsClusterLambda {
    public static final Logger LOGGER = LoggerFactory.getLogger(AutoDeleteEcsClusterLambda.class);

    private final EcsClient ecsClient;

    public AutoDeleteEcsClusterLambda() {
        this(EcsClient.create());
    }

    public AutoDeleteEcsClusterLambda(EcsClient ecsClient) {
        this.ecsClient = ecsClient;
    }

    /**
     * Handles an event triggered by CloudFormation.
     *
     * @param event   the event to handle
     * @param context the context
     */
    public void handleEvent(CloudFormationCustomResourceEvent event, Context context) {
        Map<String, Object> resourceProperties = event.getResourceProperties();
        String clusterName = (String) resourceProperties.get("cluster");

        switch (event.getRequestType()) {
            case "Create":
                break;
            case "Update":
                break;
            case "Delete":
                deleteCluster(clusterName);
                break;
            default:
                throw new IllegalArgumentException("Invalid request type: " + event.getRequestType());
        }
    }

    private void deleteCluster(String clusterName) {
        try {
            ecsClient.listContainerInstancesPaginator(builder -> builder.cluster(clusterName))
                    .stream()
                    .parallel()
                    .forEach(response -> {
                        deregisterContainer(clusterName, response);
                    });
            LOGGER.info("Deleting cluster {}", clusterName);
            ecsClient.deleteCluster(request -> request.cluster(clusterName));
        } catch (ClusterNotFoundException e) {
            LOGGER.info("Cluster not found: {}", clusterName);
        }
    }

    private void deregisterContainer(String clusterName, ListContainerInstancesResponse response) {
        if (!response.containerInstanceArns().isEmpty()) {
            response.containerInstanceArns().forEach(container -> {
                stopContainerTasks(clusterName, container);
                LOGGER.info("De-registering {} containers", response.containerInstanceArns().size());
                ecsClient.deregisterContainerInstance(builder -> builder.cluster(clusterName)
                        .containerInstance(container));
            });
        }
    }

    private void stopContainerTasks(String clusterName, String containerName) {
        if (!containerName.isEmpty()) {
            ecsClient.listTasks(builder -> builder.cluster(clusterName)
                    .containerInstance(containerName)).taskArns().forEach(task -> {
                        LOGGER.info("Stopping task {} in container {} ", task, containerName);
                        // Rate limit for ECS StopTask is 100 burst, 40 sustained:
                        // https://docs.aws.amazon.com/AmazonECS/latest/APIReference/request-throttling.html
                        sleepForSustainedRatePerSecond(30);
                        ecsClient.stopTask(builder -> builder.cluster(clusterName)
                                .task(task));
                    });
        }
    }

}
