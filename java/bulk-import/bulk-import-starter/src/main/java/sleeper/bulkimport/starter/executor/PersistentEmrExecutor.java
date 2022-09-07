/*
 * Copyright 2022 Crown Copyright
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
import com.amazonaws.services.s3.AmazonS3;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sleeper.bulkimport.job.BulkImportJob;
import sleeper.bulkimport.job.BulkImportJobSerDe;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.UserDefinedInstanceProperty;
import sleeper.configuration.properties.table.TablePropertiesProvider;

import java.util.UUID;

import static sleeper.configuration.properties.SystemDefinedInstanceProperty.CONFIG_BUCKET;

/**
 *
 */
public class PersistentEmrExecutor extends AbstractEmrExecutor {
    private static final Logger LOGGER = LoggerFactory.getLogger(PersistentEmrExecutor.class);

    private final AmazonElasticMapReduce emrClient;
    private String clusterId;

    public PersistentEmrExecutor(
            AmazonElasticMapReduce emrClient,
            InstanceProperties instancePropeties,
            TablePropertiesProvider tablePropertiesProvider,
            AmazonS3 amazonS3) {
        super(instancePropeties, tablePropertiesProvider, amazonS3);
        this.emrClient = emrClient;
        this.clusterId = getClusterIdFromName(emrClient, instancePropeties.get(UserDefinedInstanceProperty.ID));
    }

    @Override
    public void runJobOnPlatform(BulkImportJob bulkImportJob) {
        String configBucket = instanceProperties.get(CONFIG_BUCKET);
        String key = "bulk_import/" + bulkImportJob.getId() + ".json";
        String bulkImportJobJSON = new BulkImportJobSerDe().toJson(bulkImportJob);
        s3Client.putObject(configBucket, key, bulkImportJobJSON);
        LOGGER.info("Put object for job {} to key {} in bucket {}", bulkImportJob.getId(), key, configBucket);

        StepConfig stepConfig = new StepConfig()
                .withName("BulkLoad-" + UUID.randomUUID().toString().substring(0, 5))
                .withActionOnFailure(ActionOnFailure.CONTINUE)
                .withHadoopJarStep(new HadoopJarStepConfig().withJar("command-runner.jar").withArgs(constructArgs(bulkImportJob)));
        AddJobFlowStepsRequest addJobFlowStepsRequest = new AddJobFlowStepsRequest()
                .withJobFlowId(clusterId)
                .withSteps(stepConfig);

        LOGGER.info("Adding job flow step {}", addJobFlowStepsRequest.toString());
        emrClient.addJobFlowSteps(addJobFlowStepsRequest);
    }

    private static String getClusterIdFromName(AmazonElasticMapReduce emrClient, String instanceId) {
        ListClustersRequest listClustersRequest = new ListClustersRequest()
                .withClusterStates(ClusterState.BOOTSTRAPPING.name(), ClusterState.RUNNING.name(), ClusterState.STARTING.name(), ClusterState.WAITING.name());
        ListClustersResult result = emrClient.listClusters(listClustersRequest);
        String clusterId = null;
        String clusterName = String.join("-", "sleeper", instanceId, "persistentEMR");
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
