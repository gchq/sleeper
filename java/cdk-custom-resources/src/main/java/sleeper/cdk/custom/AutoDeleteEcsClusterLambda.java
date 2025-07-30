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

import java.util.Map;

public class AutoDeleteEcsClusterLambda {
    public static final Logger LOGGER = LoggerFactory.getLogger(AutoDeleteEcsClusterLambda.class);

    private final EcsClient ecsClient;

    public AutoDeleteEcsClusterLambda() {
        this(EcsClient.create());
    }

    public AutoDeleteEcsClusterLambda(EcsClient ecsClient) {
        this.ecsClient = ecsClient;
    }

    public void handleEvent(CloudFormationCustomResourceEvent event, Context context) {
        Map<String, Object> resourceProperties = event.getResourceProperties();
        String clusterName = (String) resourceProperties.get("cluster");

        switch (event.getRequestType()) {
            case "Create":
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
        // Need to de-register containers from cluster
        ecsClient.deleteCluster(request -> request.cluster(clusterName));
    }

}
