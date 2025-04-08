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
package sleeper.bulkimport.starter.executor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.emr.EmrClient;
import software.amazon.awssdk.services.emr.model.ActionOnFailure;
import software.amazon.awssdk.services.emr.model.AddJobFlowStepsRequest;
import software.amazon.awssdk.services.emr.model.ClusterState;
import software.amazon.awssdk.services.emr.model.ClusterSummary;
import software.amazon.awssdk.services.emr.model.EmrException;
import software.amazon.awssdk.services.emr.model.ListClustersResponse;
import software.amazon.awssdk.services.emr.model.StepConfig;

import sleeper.bulkimport.starter.retry.ReturnBulkImportJobToQueue;
import sleeper.core.properties.instance.InstanceProperties;

import java.util.regex.Pattern;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.BULK_IMPORT_PERSISTENT_EMR_CLUSTER_NAME;

public class PersistentEmrPlatformExecutor implements PlatformExecutor {
    private static final Logger LOGGER = LoggerFactory.getLogger(PersistentEmrPlatformExecutor.class);

    private final EmrClient emrClient;
    private final ReturnBulkImportJobToQueue returnToQueueWhenClusterIsFull;
    private final InstanceProperties instanceProperties;
    private final String clusterId;
    private final String clusterName;

    public PersistentEmrPlatformExecutor(
            EmrClient emrClient,
            ReturnBulkImportJobToQueue returnToQueueWhenClusterIsFull,
            InstanceProperties instanceProperties) {
        this.emrClient = emrClient;
        this.returnToQueueWhenClusterIsFull = returnToQueueWhenClusterIsFull;
        this.instanceProperties = instanceProperties;
        this.clusterName = instanceProperties.get(BULK_IMPORT_PERSISTENT_EMR_CLUSTER_NAME);
        this.clusterId = getClusterIdFromName(emrClient, clusterName);
    }

    @Override
    public void runJobOnPlatform(BulkImportArguments arguments) {
        StepConfig stepConfig = StepConfig.builder()
                .name("Bulk Load (job id " + arguments.getBulkImportJob().getId() + ")")
                .actionOnFailure(ActionOnFailure.CONTINUE)
                .hadoopJarStep(step -> step
                        .jar("command-runner.jar")
                        .args(arguments.sparkSubmitCommandForEMRCluster(
                                clusterName, EmrJarLocation.getJarLocation(instanceProperties))))
                .build();
        AddJobFlowStepsRequest addJobFlowStepsRequest = AddJobFlowStepsRequest.builder()
                .jobFlowId(clusterId)
                .steps(stepConfig)
                .build();

        LOGGER.info("Adding job flow step {}", addJobFlowStepsRequest);
        try {
            emrClient.addJobFlowSteps(addJobFlowStepsRequest);
        } catch (EmrException e) {
            String message = e.awsErrorDetails().errorMessage();
            if (Pattern.matches("Maximum number of active steps.+ for cluster exceeded\\.", message)) {
                returnToQueueWhenClusterIsFull.send(arguments.getBulkImportJob());
            } else {
                throw e;
            }
        }
    }

    private static String getClusterIdFromName(EmrClient emrClient, String clusterName) {
        LOGGER.debug("Searching for id of cluster with name {}", clusterName);
        ListClustersResponse response = emrClient.listClusters(request -> request
                .clusterStates(ClusterState.BOOTSTRAPPING, ClusterState.RUNNING, ClusterState.STARTING, ClusterState.WAITING));
        String clusterId = null;
        for (ClusterSummary cs : response.clusters()) {
            LOGGER.debug("Found cluster with name {}", cs.name());
            if (cs.name().equals(clusterName)) {
                clusterId = cs.id();
                break;
            }
        }
        if (null != clusterId) {
            LOGGER.info("Found cluster of name {} with id {}", clusterName, clusterId);
        } else {
            throw new IllegalArgumentException("Found no cluster with name " + clusterName);
        }
        return clusterId;
    }
}
