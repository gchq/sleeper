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
package sleeper.bulkimport.starter.executor;

import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.model.ActionOnFailure;
import com.amazonaws.services.elasticmapreduce.model.AddJobFlowStepsRequest;
import com.amazonaws.services.elasticmapreduce.model.ClusterState;
import com.amazonaws.services.elasticmapreduce.model.ClusterSummary;
import com.amazonaws.services.elasticmapreduce.model.HadoopJarStepConfig;
import com.amazonaws.services.elasticmapreduce.model.ListClustersRequest;
import com.amazonaws.services.elasticmapreduce.model.ListClustersResult;
import com.amazonaws.services.elasticmapreduce.model.StepConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.configuration.properties.instance.InstanceProperties;

import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.BULK_IMPORT_PERSISTENT_EMR_CLUSTER_NAME;

public class PersistentEmrPlatformExecutor implements PlatformExecutor {
    private static final Logger LOGGER = LoggerFactory.getLogger(PersistentEmrPlatformExecutor.class);

    private final AmazonElasticMapReduce emrClient;
    private final InstanceProperties instanceProperties;
    private final String clusterId;
    private final String clusterName;

    public PersistentEmrPlatformExecutor(
            AmazonElasticMapReduce emrClient,
            InstanceProperties instanceProperties) {
        this.emrClient = emrClient;
        this.instanceProperties = instanceProperties;
        this.clusterName = instanceProperties.get(BULK_IMPORT_PERSISTENT_EMR_CLUSTER_NAME);
        this.clusterId = getClusterIdFromName(emrClient, clusterName);
    }

    @Override
    public void runJobOnPlatform(BulkImportArguments arguments) {
        StepConfig stepConfig = new StepConfig()
                .withName("Bulk Load (job id " + arguments.getBulkImportJob().getId() + ")")
                .withActionOnFailure(ActionOnFailure.CONTINUE)
                .withHadoopJarStep(new HadoopJarStepConfig().withJar("command-runner.jar")
                        .withArgs(arguments.sparkSubmitCommandForEMRCluster(
                                clusterName, EmrJarLocation.getJarLocation(instanceProperties))));
        AddJobFlowStepsRequest addJobFlowStepsRequest = new AddJobFlowStepsRequest()
                .withJobFlowId(clusterId)
                .withSteps(stepConfig);

        LOGGER.info("Adding job flow step {}", addJobFlowStepsRequest);
        emrClient.addJobFlowSteps(addJobFlowStepsRequest);
    }

    private static String getClusterIdFromName(AmazonElasticMapReduce emrClient, String clusterName) {
        ListClustersRequest listClustersRequest = new ListClustersRequest()
                .withClusterStates(ClusterState.BOOTSTRAPPING.name(), ClusterState.RUNNING.name(), ClusterState.STARTING.name(), ClusterState.WAITING.name());
        ListClustersResult result = emrClient.listClusters(listClustersRequest);
        String clusterId = null;
        LOGGER.debug("Searching for id of cluster with name {}", clusterName);
        for (ClusterSummary cs : result.getClusters()) {
            LOGGER.debug("Found cluster with name {}", cs.getName());
            if (cs.getName().equals(clusterName)) {
                clusterId = cs.getId();
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
