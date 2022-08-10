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
import com.amazonaws.services.elasticmapreduce.model.*;
import com.amazonaws.services.s3.AmazonS3;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.slf4j.LoggerFactory;
import sleeper.bulkimport.job.BulkImportJob;
import sleeper.bulkimport.job.BulkImportJobSerDe;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.UserDefinedInstanceProperty;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.CONFIG_BUCKET;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.configuration.properties.table.TableProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class PersistentEmrExecutor extends Executor {
    private static final Logger LOGGER = LoggerFactory.getLogger(PersistentEmrExecutor.class);

    private final AmazonElasticMapReduce emrClient;
    private String clusterId;

    public PersistentEmrExecutor(AmazonElasticMapReduce emrClient,
                       InstanceProperties instancePropeties,
                       TablePropertiesProvider tablePropertiesProvider,
                       AmazonS3 amazonS3) {
        super(instancePropeties, tablePropertiesProvider, amazonS3);
        this.emrClient = emrClient;
        this.clusterId = getClusterIdFromName(instancePropeties.get(UserDefinedInstanceProperty.ID));
    }
    
    private String getClusterIdFromName(String instanceId) {
        ListClustersRequest listClustersRequest = new ListClustersRequest()
            .withClusterStates(ClusterState.BOOTSTRAPPING.name(), ClusterState.RUNNING.name(), ClusterState.STARTING.name());
        ListClustersResult result = emrClient.listClusters(listClustersRequest);
        String clusterId = null;
        String clusterName = String.join("-", "sleeper", instanceId, "persistentEMR");
        for (ClusterSummary cs : result.getClusters()) {
            if (cs.getName().equals(clusterName)) {
                clusterId = cs.getId();
                break;
            }
        }
        if (null != clusterId) {
            LOGGER.info("Found cluster of name {} with id {}", clusterName, clusterId);
        } else {
            LOGGER.error("Found no cluster with name {}", clusterName);
        }
        return clusterId;
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
    
    @Override
    protected Map<String, String> getDefaultSparkConfig(BulkImportJob bulkImportJob, Map<String, String> platformSpec, TableProperties tableProperties) {
        Map<String, String> defaultConfig = new HashMap<>();
        defaultConfig.put("spark.shuffle.mapStatus.compression.codec", getFromPlatformSpec(TableProperty.BULK_IMPORT_SPARK_SHUFFLE_MAPSTATUS_COMPRESSION_CODEC, platformSpec, tableProperties));
        defaultConfig.put("spark.speculation", getFromPlatformSpec(TableProperty.BULK_IMPORT_SPARK_SPECULATION, platformSpec, tableProperties));
        defaultConfig.put("spark.speculation.quantile", getFromPlatformSpec(TableProperty.BULK_IMPORT_SPARK_SPECULATION_QUANTILE, platformSpec, tableProperties));
        defaultConfig.put("spark.hadoop.fs.s3a.connection.maximum", instanceProperties.get(UserDefinedInstanceProperty.MAXIMUM_CONNECTIONS_TO_S3));
        return defaultConfig;
    }
    
    @Override
    protected List<String> constructArgs(BulkImportJob bulkImportJob) {
        List<String> args = super.constructArgs(bulkImportJob);
        // EMR doesn't seem to handle json very well - it cuts off the final "}}" characters. However
        // Pretty printing it seems to force it to quote the job and handle it properly
        args.set(args.size() - 2, bulkImportJob.getId()); // Changed to this as for persistent EMR we are putting the job in a JSON file in S3
        return args;
    }
    
    @Override
    protected String getJarLocation() {
        return "s3a://"
                + instanceProperties.get(UserDefinedInstanceProperty.JARS_BUCKET)
                + "/bulk-import-runner-"
                + instanceProperties.get(UserDefinedInstanceProperty.VERSION) + ".jar";
    }
}
