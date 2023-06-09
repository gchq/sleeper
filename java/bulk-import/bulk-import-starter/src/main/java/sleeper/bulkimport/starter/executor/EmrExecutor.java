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
import com.amazonaws.services.elasticmapreduce.model.ComputeLimits;
import com.amazonaws.services.elasticmapreduce.model.ComputeLimitsUnitType;
import com.amazonaws.services.elasticmapreduce.model.Configuration;
import com.amazonaws.services.elasticmapreduce.model.EbsBlockDeviceConfig;
import com.amazonaws.services.elasticmapreduce.model.EbsConfiguration;
import com.amazonaws.services.elasticmapreduce.model.HadoopJarStepConfig;
import com.amazonaws.services.elasticmapreduce.model.InstanceGroupConfig;
import com.amazonaws.services.elasticmapreduce.model.InstanceRoleType;
import com.amazonaws.services.elasticmapreduce.model.JobFlowInstancesConfig;
import com.amazonaws.services.elasticmapreduce.model.ManagedScalingPolicy;
import com.amazonaws.services.elasticmapreduce.model.MarketType;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowRequest;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowResult;
import com.amazonaws.services.elasticmapreduce.model.ScaleDownBehavior;
import com.amazonaws.services.elasticmapreduce.model.StepConfig;
import com.amazonaws.services.elasticmapreduce.model.Tag;
import com.amazonaws.services.elasticmapreduce.model.VolumeSpecification;
import com.amazonaws.services.s3.AmazonS3;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.bulkimport.configuration.ConfigurationUtils;
import sleeper.bulkimport.job.BulkImportJob;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.SystemDefinedInstanceProperty;
import sleeper.configuration.properties.UserDefinedInstanceProperty;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.configuration.properties.table.TableProperty;
import sleeper.ingest.job.status.IngestJobStatusStore;
import sleeper.statestore.StateStoreProvider;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static sleeper.configuration.properties.SystemDefinedInstanceProperty.BULK_IMPORT_BUCKET;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.BULK_IMPORT_EMR_EBS_VOLUMES_PER_INSTANCE;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.BULK_IMPORT_EMR_EBS_VOLUME_SIZE_IN_GB;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.BULK_IMPORT_EMR_EBS_VOLUME_TYPE;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.BULK_IMPORT_EMR_EC2_KEYPAIR_NAME;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.BULK_IMPORT_EMR_MASTER_ADDITIONAL_SECURITY_GROUP;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.ID;

/**
 * An {@link Executor} which runs a bulk import job on an EMR cluster.
 */
public class EmrExecutor extends AbstractEmrExecutor {
    private static final Logger LOGGER = LoggerFactory.getLogger(EmrExecutor.class);
    private final AmazonElasticMapReduce emrClient;

    public EmrExecutor(AmazonElasticMapReduce emrClient,
                       InstanceProperties instanceProperties,
                       TablePropertiesProvider tablePropertiesProvider,
                       StateStoreProvider stateStoreProvider,
                       IngestJobStatusStore ingestJobStatusStore,
                       AmazonS3 amazonS3, String taskId,
                       Supplier<Instant> validationTimeSupplier) {
        super(instanceProperties, tablePropertiesProvider, stateStoreProvider, ingestJobStatusStore, amazonS3,
                taskId, validationTimeSupplier);
        this.emrClient = emrClient;
    }

    @Override
    public void runJobOnPlatform(BulkImportJob bulkImportJob) {
        Map<String, String> platformSpec = bulkImportJob.getPlatformSpec();
        TableProperties tableProperties = tablePropertiesProvider.getTableProperties(bulkImportJob.getTableName());
        String bulkImportBucket = instanceProperties.get(BULK_IMPORT_BUCKET);
        String logUri = null == bulkImportBucket ? null : "s3://" + bulkImportBucket + "/logs";

        Integer maxNumberOfExecutors = Integer.max(
                Integer.parseInt(getFromPlatformSpec(TableProperty.BULK_IMPORT_EMR_INITIAL_NUMBER_OF_EXECUTORS, platformSpec, tableProperties)),
                Integer.parseInt(getFromPlatformSpec(TableProperty.BULK_IMPORT_EMR_MAX_NUMBER_OF_EXECUTORS, platformSpec, tableProperties))
        );

        String clusterName = String.join("-", "sleeper",
                instanceProperties.get(ID),
                bulkImportJob.getTableName(),
                bulkImportJob.getId());
        if (clusterName.length() > 64) {
            clusterName = clusterName.substring(0, 64);
        }

        RunJobFlowResult response = emrClient.runJobFlow(new RunJobFlowRequest()
                .withName(clusterName)
                .withInstances(createJobFlowInstancesConfig(bulkImportJob, tableProperties))
                .withVisibleToAllUsers(true)
                .withSecurityConfiguration(instanceProperties.get(SystemDefinedInstanceProperty.BULK_IMPORT_EMR_SECURITY_CONF_NAME))
                .withManagedScalingPolicy(new ManagedScalingPolicy()
                        .withComputeLimits(new ComputeLimits()
                                .withUnitType(ComputeLimitsUnitType.Instances)
                                .withMinimumCapacityUnits(1)
                                .withMaximumCapacityUnits(maxNumberOfExecutors)
                                .withMaximumCoreCapacityUnits(3)))
                .withScaleDownBehavior(ScaleDownBehavior.TERMINATE_AT_TASK_COMPLETION)
                .withReleaseLabel(getFromPlatformSpec(TableProperty.BULK_IMPORT_EMR_RELEASE_LABEL, platformSpec, tableProperties))
                .withApplications(new Application().withName("Spark"))
                .withLogUri(logUri)
                .withServiceRole(instanceProperties.get(SystemDefinedInstanceProperty.BULK_IMPORT_EMR_CLUSTER_ROLE_NAME))
                .withJobFlowRole(instanceProperties.get(SystemDefinedInstanceProperty.BULK_IMPORT_EMR_EC2_ROLE_NAME))
                .withConfigurations(getConfigurations())
                .withSteps(new StepConfig()
                        .withName("Bulk Load (job id " + bulkImportJob.getId() + ")")
                        .withHadoopJarStep(new HadoopJarStepConfig().withJar("command-runner.jar")
                                .withArgs(constructArgs(bulkImportJob, clusterName + "-EMR"))))
                .withTags(instanceProperties.getTags().entrySet().stream()
                        .map(entry -> new Tag(entry.getKey(), entry.getValue()))
                        .collect(Collectors.toList())));

        LOGGER.info("Cluster created with ARN {}", response.getClusterArn());
    }

    private JobFlowInstancesConfig createJobFlowInstancesConfig(BulkImportJob bulkImportJob, TableProperties tableProperties) {
        JobFlowInstancesConfig config = new JobFlowInstancesConfig()
                .withEc2SubnetId(instanceProperties.get(UserDefinedInstanceProperty.SUBNET));

        Map<String, String> platformSpec = bulkImportJob.getPlatformSpec();
        String driverInstanceType = getFromPlatformSpec(TableProperty.BULK_IMPORT_EMR_MASTER_INSTANCE_TYPE, platformSpec, tableProperties);
        String executorInstanceType = getFromPlatformSpec(TableProperty.BULK_IMPORT_EMR_EXECUTOR_INSTANCE_TYPE, platformSpec, tableProperties);
        Integer initialNumberOfExecutors = Integer.parseInt(getFromPlatformSpec(TableProperty.BULK_IMPORT_EMR_INITIAL_NUMBER_OF_EXECUTORS, platformSpec, tableProperties));

        String marketTypeOfExecutors = getFromPlatformSpec(TableProperty.BULK_IMPORT_EMR_EXECUTOR_MARKET_TYPE, platformSpec, tableProperties);
        if (marketTypeOfExecutors == null) {
            marketTypeOfExecutors = "SPOT";
        }

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

        config.setInstanceGroups(Lists.newArrayList(
                new InstanceGroupConfig()
                        .withName("Executors")
                        .withInstanceType(executorInstanceType)
                        .withInstanceRole(InstanceRoleType.CORE)
                        .withInstanceCount(initialNumberOfExecutors)
                        .withEbsConfiguration(ebsConfiguration)
                        .withMarket(MarketType.fromValue(marketTypeOfExecutors)),
                new InstanceGroupConfig()
                        .withName("Driver")
                        .withInstanceType(driverInstanceType)
                        .withInstanceRole(InstanceRoleType.MASTER)
                        .withInstanceCount(1)
                        .withEbsConfiguration(ebsConfiguration)
        ));

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

    private List<Configuration> getConfigurations() {
        List<Configuration> configurations = new ArrayList<>();

        Map<String, String> emrSparkProps = ConfigurationUtils.getSparkEMRConfiguration();
        Configuration emrConfiguration = new Configuration()
                .withClassification("spark")
                .withProperties(emrSparkProps);
        configurations.add(emrConfiguration);

        Map<String, String> yarnConf = ConfigurationUtils.getYarnConfiguration();
        Configuration yarnConfiguration = new Configuration()
                .withClassification("yarn-site")
                .withProperties(yarnConf);
        configurations.add(yarnConfiguration);

        Map<String, String> sparkConf = ConfigurationUtils.getSparkConfigurationFromInstanceProperties(instanceProperties);
        Configuration sparkDefaultsConfigurations = new Configuration()
                .withClassification("spark-defaults")
                .withProperties(sparkConf);
        configurations.add(sparkDefaultsConfigurations);

        Map<String, String> sparkExecutorJavaHome = new HashMap<>();
        sparkExecutorJavaHome.put("JAVA_HOME", ConfigurationUtils.getJavaHome());
        Configuration sparkEnvExportConfigurations = new Configuration()
                .withClassification("export")
                .withProperties(sparkExecutorJavaHome);
        Configuration sparkEnvConfigurations = new Configuration()
                .withClassification("spark-env")
                .withConfigurations(sparkEnvExportConfigurations);
        configurations.add(sparkEnvConfigurations);

        Map<String, String> mapReduceSiteConf = ConfigurationUtils.getMapRedSiteConfiguration();
        Configuration mapRedSiteConfigurations = new Configuration()
                .withClassification("mapred-site")
                .withProperties(mapReduceSiteConf);
        configurations.add(mapRedSiteConfigurations);

        Map<String, String> javaHomeConf = ConfigurationUtils.getJavaHomeConfiguration();

        Configuration hadoopEnvExportConfigurations = new Configuration()
                .withClassification("export")
                .withProperties(javaHomeConf);
        Configuration hadoopEnvConfigurations = new Configuration()
                .withClassification("hadoop-env")
                .withConfigurations(hadoopEnvExportConfigurations);
        configurations.add(hadoopEnvConfigurations);

        return configurations;
    }

    protected String getFromPlatformSpec(TableProperty tableProperty, Map<String, String> platformSpec, TableProperties tableProperties) {
        if (null == platformSpec) {
            return tableProperties.get(tableProperty);
        }
        return platformSpec.getOrDefault(tableProperty.getPropertyName(), tableProperties.get(tableProperty));
    }
}
