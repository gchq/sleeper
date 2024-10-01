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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.emr.EmrClient;
import software.amazon.awssdk.services.emr.model.Application;
import software.amazon.awssdk.services.emr.model.EbsBlockDeviceConfig;
import software.amazon.awssdk.services.emr.model.EbsConfiguration;
import software.amazon.awssdk.services.emr.model.JobFlowInstancesConfig;
import software.amazon.awssdk.services.emr.model.RunJobFlowRequest;
import software.amazon.awssdk.services.emr.model.RunJobFlowResponse;
import software.amazon.awssdk.services.emr.model.ScaleDownBehavior;
import software.amazon.awssdk.services.emr.model.StepConfig;
import software.amazon.awssdk.services.emr.model.Tag;
import software.amazon.awssdk.services.emr.model.VolumeSpecification;

import sleeper.bulkimport.configuration.BulkImportPlatformSpec;
import sleeper.bulkimport.job.BulkImportJob;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertiesProvider;

import java.util.stream.Collectors;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.BULK_IMPORT_BUCKET;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.BULK_IMPORT_EMR_CLUSTER_ROLE_NAME;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.BULK_IMPORT_EMR_EC2_ROLE_NAME;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.BULK_IMPORT_EMR_SECURITY_CONF_NAME;
import static sleeper.core.properties.instance.CommonProperty.ID;
import static sleeper.core.properties.instance.EMRProperty.BULK_IMPORT_EMR_EBS_VOLUMES_PER_INSTANCE;
import static sleeper.core.properties.instance.EMRProperty.BULK_IMPORT_EMR_EBS_VOLUME_SIZE_IN_GB;
import static sleeper.core.properties.instance.EMRProperty.BULK_IMPORT_EMR_EBS_VOLUME_TYPE;
import static sleeper.core.properties.instance.EMRProperty.BULK_IMPORT_EMR_EC2_KEYPAIR_NAME;
import static sleeper.core.properties.instance.EMRProperty.BULK_IMPORT_EMR_MASTER_ADDITIONAL_SECURITY_GROUP;
import static sleeper.core.properties.table.TableProperty.BULK_IMPORT_EMR_RELEASE_LABEL;

/**
 * Starts an EMR cluster to run a single bulk import job and then terminate.
 */
public class EmrPlatformExecutor implements PlatformExecutor {
    private static final Logger LOGGER = LoggerFactory.getLogger(EmrPlatformExecutor.class);
    private final EmrClient emrClient;
    private final EmrInstanceConfiguration instanceConfiguration;
    private final InstanceProperties instanceProperties;
    private final TablePropertiesProvider tablePropertiesProvider;

    public EmrPlatformExecutor(EmrClient emrClient,
            InstanceProperties instanceProperties, TablePropertiesProvider tablePropertiesProvider) {
        this(emrClient, instanceProperties, tablePropertiesProvider, new EmrInstanceFleets(instanceProperties));
    }

    public EmrPlatformExecutor(EmrClient emrClient,
            InstanceProperties instanceProperties, TablePropertiesProvider tablePropertiesProvider,
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
                bulkImportJob.getTableId(),
                bulkImportJob.getId());

        RunJobFlowResponse response = emrClient.runJobFlow(RunJobFlowRequest.builder()
                .name(clusterName)
                .instances(createJobFlowInstancesConfig(platformSpec))
                .visibleToAllUsers(true)
                .securityConfiguration(instanceProperties.get(BULK_IMPORT_EMR_SECURITY_CONF_NAME))
                .managedScalingPolicy(policy -> policy
                        .computeLimits(instanceConfiguration.createComputeLimits(platformSpec)))
                .scaleDownBehavior(ScaleDownBehavior.TERMINATE_AT_TASK_COMPLETION)
                .releaseLabel(platformSpec.get(BULK_IMPORT_EMR_RELEASE_LABEL))
                .applications(Application.builder().name("Spark").build())
                .logUri(logUri)
                .serviceRole(instanceProperties.get(BULK_IMPORT_EMR_CLUSTER_ROLE_NAME))
                .jobFlowRole(instanceProperties.get(BULK_IMPORT_EMR_EC2_ROLE_NAME))
                .steps(StepConfig.builder()
                        .name("Bulk Load (job id " + bulkImportJob.getId() + ")")
                        .hadoopJarStep(jarStep -> jarStep
                                .jar("command-runner.jar")
                                .args(arguments.sparkSubmitCommandForEMRCluster(
                                        clusterName + "-EMR",
                                        EmrJarLocation.getJarLocation(instanceProperties))))
                        .build())
                .tags(instanceProperties.getTags().entrySet().stream()
                        .map(entry -> Tag.builder().key(entry.getKey()).value(entry.getValue()).build())
                        .collect(Collectors.toList()))
                .build());

        LOGGER.info("Cluster created with ARN {}", response.clusterArn());
    }

    private JobFlowInstancesConfig createJobFlowInstancesConfig(BulkImportPlatformSpec platformSpec) {

        VolumeSpecification volumeSpecification = VolumeSpecification.builder()
                // We could add a property to control the Iops setting here
                .sizeInGB(instanceProperties.getInt(BULK_IMPORT_EMR_EBS_VOLUME_SIZE_IN_GB))
                .volumeType(instanceProperties.get(BULK_IMPORT_EMR_EBS_VOLUME_TYPE))
                .build();
        EbsBlockDeviceConfig ebsBlockDeviceConfig = EbsBlockDeviceConfig.builder()
                .volumeSpecification(volumeSpecification)
                .volumesPerInstance(instanceProperties.getInt(BULK_IMPORT_EMR_EBS_VOLUMES_PER_INSTANCE))
                .build();
        EbsConfiguration ebsConfiguration = EbsConfiguration.builder()
                .ebsBlockDeviceConfigs(ebsBlockDeviceConfig)
                .ebsOptimized(true)
                .build();

        JobFlowInstancesConfig config = instanceConfiguration.createJobFlowInstancesConfig(ebsConfiguration, platformSpec);

        String ec2KeyName = instanceProperties.get(BULK_IMPORT_EMR_EC2_KEYPAIR_NAME);
        if (null != ec2KeyName && !ec2KeyName.isEmpty()) {
            config = config.toBuilder().ec2KeyName(ec2KeyName).build();
        }
        String additionalSecurityGroup = instanceProperties.get(BULK_IMPORT_EMR_MASTER_ADDITIONAL_SECURITY_GROUP);
        if (null != additionalSecurityGroup && !additionalSecurityGroup.isEmpty()) {
            config = config.toBuilder().additionalMasterSecurityGroups(additionalSecurityGroup).build();
        }
        return config;
    }
}
