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
package sleeper.bulkimport.starter.executor;

import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.model.Application;
import com.amazonaws.services.elasticmapreduce.model.EbsBlockDeviceConfig;
import com.amazonaws.services.elasticmapreduce.model.EbsConfiguration;
import com.amazonaws.services.elasticmapreduce.model.HadoopJarStepConfig;
import com.amazonaws.services.elasticmapreduce.model.JobFlowInstancesConfig;
import com.amazonaws.services.elasticmapreduce.model.ManagedScalingPolicy;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowRequest;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowResult;
import com.amazonaws.services.elasticmapreduce.model.ScaleDownBehavior;
import com.amazonaws.services.elasticmapreduce.model.StepConfig;
import com.amazonaws.services.elasticmapreduce.model.Tag;
import com.amazonaws.services.elasticmapreduce.model.VolumeSpecification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.bulkimport.configuration.BulkImportPlatformSpec;
import sleeper.bulkimport.job.BulkImportJob;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;

import java.util.Collections;
import java.util.stream.Collectors;

import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.BULK_IMPORT_BUCKET;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.BULK_IMPORT_EMR_CLUSTER_ROLE_NAME;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.BULK_IMPORT_EMR_EC2_ROLE_NAME;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.BULK_IMPORT_EMR_SECURITY_CONF_NAME;
import static sleeper.configuration.properties.instance.CommonProperty.ID;
import static sleeper.configuration.properties.instance.EMRProperty.BULK_IMPORT_EMR_EBS_VOLUMES_PER_INSTANCE;
import static sleeper.configuration.properties.instance.EMRProperty.BULK_IMPORT_EMR_EBS_VOLUME_SIZE_IN_GB;
import static sleeper.configuration.properties.instance.EMRProperty.BULK_IMPORT_EMR_EBS_VOLUME_TYPE;
import static sleeper.configuration.properties.instance.EMRProperty.BULK_IMPORT_EMR_EC2_KEYPAIR_NAME;
import static sleeper.configuration.properties.instance.EMRProperty.BULK_IMPORT_EMR_MASTER_ADDITIONAL_SECURITY_GROUP;
import static sleeper.configuration.properties.table.TableProperty.BULK_IMPORT_EMR_RELEASE_LABEL;

/**
 * An {@link PlatformExecutor} which runs a bulk import job on an EMR cluster.
 */
public class EmrPlatformExecutor implements PlatformExecutor {
    private static final Logger LOGGER = LoggerFactory.getLogger(EmrPlatformExecutor.class);
    private final AmazonElasticMapReduce emrClient;
    private final EmrInstanceConfiguration instanceConfiguration;
    private final InstanceProperties instanceProperties;
    private final TablePropertiesProvider tablePropertiesProvider;

    public EmrPlatformExecutor(AmazonElasticMapReduce emrClient,
                               InstanceProperties instanceProperties,
                               TablePropertiesProvider tablePropertiesProvider) {
        this(emrClient, instanceProperties, tablePropertiesProvider, new EmrInstanceFleets(instanceProperties));
    }

    public EmrPlatformExecutor(AmazonElasticMapReduce emrClient,
                               InstanceProperties instanceProperties,
                               TablePropertiesProvider tablePropertiesProvider,
                               EmrInstanceConfiguration instanceConfiguration) {
        this.instanceProperties = instanceProperties;
        this.tablePropertiesProvider = tablePropertiesProvider;
        this.emrClient = emrClient;
        this.instanceConfiguration = instanceConfiguration;
    }

    @Override
    public void runJobOnPlatform(BulkImportArguments arguments) {
        BulkImportJob bulkImportJob = arguments.getBulkImportJob();
        TableProperties tableProperties = tablePropertiesProvider.getByName(bulkImportJob.getTableName());
        String bulkImportBucket = instanceProperties.get(BULK_IMPORT_BUCKET);
        String logUri = null == bulkImportBucket ? null : "s3://" + bulkImportBucket + "/logs";
        BulkImportPlatformSpec platformSpec = new BulkImportPlatformSpec(tableProperties, bulkImportJob);

        String clusterName = String.join("-", "sleeper",
                instanceProperties.get(ID),
                bulkImportJob.getTableName(),
                bulkImportJob.getId());
        if (clusterName.length() > 64) {
            clusterName = clusterName.substring(0, 64);
        }

        RunJobFlowResult response = emrClient.runJobFlow(new RunJobFlowRequest()
                .withName(clusterName)
                .withInstances(createJobFlowInstancesConfig(platformSpec))
                .withVisibleToAllUsers(true)
                .withSecurityConfiguration(instanceProperties.get(BULK_IMPORT_EMR_SECURITY_CONF_NAME))
                .withManagedScalingPolicy(new ManagedScalingPolicy()
                        .withComputeLimits(instanceConfiguration.createComputeLimits(platformSpec)))
                .withScaleDownBehavior(ScaleDownBehavior.TERMINATE_AT_TASK_COMPLETION)
                .withReleaseLabel(platformSpec.get(BULK_IMPORT_EMR_RELEASE_LABEL))
                .withApplications(new Application().withName("Spark"))
                .withLogUri(logUri)
                .withServiceRole(instanceProperties.get(BULK_IMPORT_EMR_CLUSTER_ROLE_NAME))
                .withJobFlowRole(instanceProperties.get(BULK_IMPORT_EMR_EC2_ROLE_NAME))
                .withSteps(new StepConfig()
                        .withName("Bulk Load (job id " + bulkImportJob.getId() + ")")
                        .withHadoopJarStep(new HadoopJarStepConfig().withJar("command-runner.jar")
                                .withArgs(arguments.sparkSubmitCommandForCluster(
                                        clusterName + "-EMR",
                                        EmrJarLocation.getJarLocation(instanceProperties)))))
                .withTags(instanceProperties.getTags().entrySet().stream()
                        .map(entry -> new Tag(entry.getKey(), entry.getValue()))
                        .collect(Collectors.toList())));

        LOGGER.info("Cluster created with ARN {}", response.getClusterArn());
    }

    private JobFlowInstancesConfig createJobFlowInstancesConfig(BulkImportPlatformSpec platformSpec) {

        VolumeSpecification volumeSpecification = new VolumeSpecification()
//                .withIops(null) // TODO Add property to control this
                .withSizeInGB(instanceProperties.getInt(BULK_IMPORT_EMR_EBS_VOLUME_SIZE_IN_GB))
                .withVolumeType(instanceProperties.get(BULK_IMPORT_EMR_EBS_VOLUME_TYPE));
        EbsBlockDeviceConfig ebsBlockDeviceConfig = new EbsBlockDeviceConfig()
                .withVolumeSpecification(volumeSpecification)
                .withVolumesPerInstance(instanceProperties.getInt(BULK_IMPORT_EMR_EBS_VOLUMES_PER_INSTANCE));
        EbsConfiguration ebsConfiguration = new EbsConfiguration()
                .withEbsBlockDeviceConfigs(ebsBlockDeviceConfig)
                .withEbsOptimized(true);

        JobFlowInstancesConfig config = instanceConfiguration.createJobFlowInstancesConfig(ebsConfiguration, platformSpec);

        String ec2KeyName = instanceProperties.get(BULK_IMPORT_EMR_EC2_KEYPAIR_NAME);
        if (null != ec2KeyName && !ec2KeyName.isEmpty()) {
            config.setEc2KeyName(ec2KeyName);
        }
        String additionalSecurityGroup = instanceProperties.get(BULK_IMPORT_EMR_MASTER_ADDITIONAL_SECURITY_GROUP);
        if (null != additionalSecurityGroup && !additionalSecurityGroup.isEmpty()) {
            config.setAdditionalMasterSecurityGroups(Collections.singletonList(additionalSecurityGroup));
        }
        return config;
    }
}
