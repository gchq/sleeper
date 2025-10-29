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
import software.amazon.awssdk.services.emr.EmrClient;
import software.amazon.awssdk.services.emr.model.ClusterState;

import sleeper.core.util.PollWithRetries;

import java.time.Duration;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

/**
 * Delete an EMR Non-Persistent cluster.
 */
public class AutoStopEmrNonPersistentClusterLambda {
    public static final Logger LOGGER = LoggerFactory.getLogger(AutoStopEmrNonPersistentClusterLambda.class);

    private final PollWithRetries poll;
    private final EmrClient emrClient;

    public AutoStopEmrNonPersistentClusterLambda() {
        this(EmrClient.create(), PollWithRetries
                .intervalAndPollingTimeout(Duration.ofSeconds(30), Duration.ofMinutes(15)));
    }

    public AutoStopEmrNonPersistentClusterLambda(EmrClient emrClient, PollWithRetries poll) {
        this.emrClient = emrClient;
        this.poll = poll;
    }

    /**
     * Handles an event triggered by CloudFormation.
     *
     * @param event   the event to handle
     * @param context the context
     */
    public void handleEvent(
            CloudFormationCustomResourceEvent event, Context context) throws InterruptedException {

        Map<String, Object> resourceProperties = event.getResourceProperties();
        String clusterId = (String) resourceProperties.get("clusterId");

        switch (event.getRequestType()) {
            case "Create":
                break;
            case "Update":
                break;
            case "Delete":
                stopCluster(clusterId);
                break;
            default:
                throw new IllegalArgumentException("Invalid request type: " + event.getRequestType());
        }
    }

    private void stopCluster(String clusterId) throws InterruptedException {

        if (!isClusterStopped(clusterId)) {
            LOGGER.info("Terminating running cluster: {} ", clusterId);
            emrClient.terminateJobFlows(request -> request.jobFlowIds(clusterId));
            LOGGER.info("Waiting for cluster to stop");
            poll.pollUntil("all EMR Non-Persistent clusters stopped", () -> isClusterStopped(clusterId));
        }

        LOGGER.info("Terminated cluster {}", clusterId);
    }

    private boolean isClusterStopped(String clusterId) {

        ClusterState currentState = emrClient.describeCluster(request -> request.clusterId(clusterId)).cluster().status().state();

        Set<ClusterState> stoppedCluster = EnumSet.of(ClusterState.TERMINATED, ClusterState.TERMINATED_WITH_ERRORS);

        if (stoppedCluster.contains(currentState)) {
            return true;
        }
        return false;

    }

}
