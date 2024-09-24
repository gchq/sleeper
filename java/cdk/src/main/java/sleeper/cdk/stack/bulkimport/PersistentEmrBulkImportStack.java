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
package sleeper.cdk.stack.bulkimport;

import com.google.common.collect.Lists;
import software.amazon.awscdk.CfnTag;
import software.amazon.awscdk.NestedStack;
import software.amazon.awscdk.services.cloudwatch.IMetric;
import software.amazon.awscdk.services.emr.CfnCluster;
import software.amazon.awscdk.services.emr.CfnCluster.EbsBlockDeviceConfigProperty;
import software.amazon.awscdk.services.emr.CfnCluster.EbsConfigurationProperty;
import software.amazon.awscdk.services.emr.CfnCluster.JobFlowInstancesConfigProperty;
import software.amazon.awscdk.services.emr.CfnCluster.ManagedScalingPolicyProperty;
import software.amazon.awscdk.services.emr.CfnCluster.VolumeSpecificationProperty;
import software.amazon.awscdk.services.emr.CfnClusterProps;
import software.amazon.awscdk.services.iam.Effect;
import software.amazon.awscdk.services.iam.PolicyStatement;
import software.amazon.awscdk.services.lambda.IFunction;
import software.amazon.awscdk.services.s3.IBucket;
import software.amazon.awscdk.services.sns.Topic;
import software.amazon.awscdk.services.sqs.Queue;
import software.constructs.Construct;

import sleeper.bulkimport.configuration.ConfigurationUtils;
import sleeper.cdk.Utils;
import sleeper.cdk.jars.BuiltJars;
import sleeper.cdk.stack.CoreStacks;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.core.properties.validation.EmrInstanceArchitecture;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.BULK_IMPORT_PERSISTENT_EMR_CLUSTER_NAME;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.BULK_IMPORT_PERSISTENT_EMR_JOB_QUEUE_ARN;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.BULK_IMPORT_PERSISTENT_EMR_JOB_QUEUE_URL;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.BULK_IMPORT_PERSISTENT_EMR_MASTER_DNS;
import static sleeper.configuration.properties.instance.CommonProperty.SUBNETS;
import static sleeper.configuration.properties.instance.EMRProperty.BULK_IMPORT_EMR_EBS_VOLUMES_PER_INSTANCE;
import static sleeper.configuration.properties.instance.EMRProperty.BULK_IMPORT_EMR_EBS_VOLUME_SIZE_IN_GB;
import static sleeper.configuration.properties.instance.EMRProperty.BULK_IMPORT_EMR_EBS_VOLUME_TYPE;
import static sleeper.configuration.properties.instance.EMRProperty.BULK_IMPORT_EMR_EC2_KEYPAIR_NAME;
import static sleeper.configuration.properties.instance.EMRProperty.BULK_IMPORT_EMR_MASTER_ADDITIONAL_SECURITY_GROUP;
import static sleeper.configuration.properties.instance.PersistentEMRProperty.BULK_IMPORT_PERSISTENT_EMR_EXECUTOR_ARM_INSTANCE_TYPES;
import static sleeper.configuration.properties.instance.PersistentEMRProperty.BULK_IMPORT_PERSISTENT_EMR_EXECUTOR_X86_INSTANCE_TYPES;
import static sleeper.configuration.properties.instance.PersistentEMRProperty.BULK_IMPORT_PERSISTENT_EMR_INSTANCE_ARCHITECTURE;
import static sleeper.configuration.properties.instance.PersistentEMRProperty.BULK_IMPORT_PERSISTENT_EMR_MASTER_ARM_INSTANCE_TYPES;
import static sleeper.configuration.properties.instance.PersistentEMRProperty.BULK_IMPORT_PERSISTENT_EMR_MASTER_X86_INSTANCE_TYPES;
import static sleeper.configuration.properties.instance.PersistentEMRProperty.BULK_IMPORT_PERSISTENT_EMR_MAX_CAPACITY;
import static sleeper.configuration.properties.instance.PersistentEMRProperty.BULK_IMPORT_PERSISTENT_EMR_MIN_CAPACITY;
import static sleeper.configuration.properties.instance.PersistentEMRProperty.BULK_IMPORT_PERSISTENT_EMR_RELEASE_LABEL;
import static sleeper.configuration.properties.instance.PersistentEMRProperty.BULK_IMPORT_PERSISTENT_EMR_STEP_CONCURRENCY_LEVEL;
import static sleeper.configuration.properties.instance.PersistentEMRProperty.BULK_IMPORT_PERSISTENT_EMR_USE_MANAGED_SCALING;
import static sleeper.core.properties.validation.EmrInstanceTypeConfig.readInstanceTypes;

/**
 * Deploys a persistent EMR cluster to perform bulk import jobs. Bulk import jobs are sent to a queue. This triggers
 * a lambda which then adds a step to the EMR cluster to run the bulk import job.
 */
public class PersistentEmrBulkImportStack extends NestedStack {
    private final Queue bulkImportJobQueue;

    public PersistentEmrBulkImportStack(
            Construct scope,
            String id,
            InstanceProperties instanceProperties,
            BuiltJars jars,
            Topic errorsTopic,
            BulkImportBucketStack importBucketStack,
            CommonEmrBulkImportStack commonEmrStack,
            CoreStacks coreStacks,
            List<IMetric> errorMetrics) {
        super(scope, id);
        CommonEmrBulkImportHelper commonHelper = new CommonEmrBulkImportHelper(
                this, "PersistentEMR", instanceProperties, coreStacks, errorMetrics);
        bulkImportJobQueue = commonHelper.createJobQueue(
                BULK_IMPORT_PERSISTENT_EMR_JOB_QUEUE_URL, BULK_IMPORT_PERSISTENT_EMR_JOB_QUEUE_ARN,
                errorsTopic);
        IFunction jobStarter = commonHelper.createJobStarterFunction(
                "PersistentEMR", bulkImportJobQueue, jars, importBucketStack.getImportBucket(), commonEmrStack);
        configureJobStarterFunction(jobStarter);
        createCluster(this, instanceProperties, importBucketStack.getImportBucket(), commonEmrStack);
        Utils.addStackTagIfSet(this, instanceProperties);
    }

    private static void createCluster(Construct scope,
            InstanceProperties instanceProperties,
            IBucket importBucket,
            CommonEmrBulkImportStack commonStack) {

        // EMR cluster
        String logUri = "s3://" + importBucket.getBucketName() + "/logs";

        VolumeSpecificationProperty volumeSpecificationProperty = VolumeSpecificationProperty.builder()
                //                .iops() // TODO Add property to control this
                .sizeInGb(instanceProperties.getInt(BULK_IMPORT_EMR_EBS_VOLUME_SIZE_IN_GB))
                .volumeType(instanceProperties.get(BULK_IMPORT_EMR_EBS_VOLUME_TYPE))
                .build();
        EbsBlockDeviceConfigProperty ebsBlockDeviceConfig = EbsBlockDeviceConfigProperty.builder()
                .volumeSpecification(volumeSpecificationProperty)
                .volumesPerInstance(instanceProperties.getInt(BULK_IMPORT_EMR_EBS_VOLUMES_PER_INSTANCE))
                .build();
        EbsConfigurationProperty ebsConf = EbsConfigurationProperty.builder()
                .ebsBlockDeviceConfigs(List.of(ebsBlockDeviceConfig))
                .ebsOptimized(true)
                .build();
        CfnCluster.InstanceFleetConfigProperty masterInstanceFleetConfigProperty = CfnCluster.InstanceFleetConfigProperty.builder()
                .name("Driver")
                .instanceTypeConfigs(readMasterInstanceTypes(instanceProperties, ebsConf))
                .targetOnDemandCapacity(1)
                .build();
        CfnCluster.InstanceFleetConfigProperty coreInstanceFleetConfigProperty = CfnCluster.InstanceFleetConfigProperty.builder()
                .name("Executors")
                .instanceTypeConfigs(readExecutorInstanceTypes(instanceProperties, ebsConf))
                .targetOnDemandCapacity(instanceProperties.getInt(BULK_IMPORT_PERSISTENT_EMR_MIN_CAPACITY))
                .build();

        JobFlowInstancesConfigProperty.Builder jobFlowInstancesConfigPropertyBuilder = JobFlowInstancesConfigProperty.builder()
                .ec2SubnetIds(instanceProperties.getList(SUBNETS))
                .masterInstanceFleet(masterInstanceFleetConfigProperty)
                .coreInstanceFleet(coreInstanceFleetConfigProperty);

        String ec2KeyName = instanceProperties.get(BULK_IMPORT_EMR_EC2_KEYPAIR_NAME);
        if (null != ec2KeyName && !ec2KeyName.isEmpty()) {
            jobFlowInstancesConfigPropertyBuilder.ec2KeyName(ec2KeyName);
        }
        String additionalSecurityGroup = instanceProperties.get(BULK_IMPORT_EMR_MASTER_ADDITIONAL_SECURITY_GROUP);
        if (null != additionalSecurityGroup && !additionalSecurityGroup.isEmpty()) {
            jobFlowInstancesConfigPropertyBuilder.additionalMasterSecurityGroups(Collections.singletonList(
                    additionalSecurityGroup));
        }
        JobFlowInstancesConfigProperty jobFlowInstancesConfigProperty = jobFlowInstancesConfigPropertyBuilder.build();

        CfnClusterProps.Builder propsBuilder = CfnClusterProps.builder()
                .name(String.join("-", "sleeper",
                        Utils.cleanInstanceId(instanceProperties), "bulk-import-persistent-emr"))
                .visibleToAllUsers(true)
                .securityConfiguration(commonStack.getSecurityConfiguration().getName())
                .releaseLabel(instanceProperties.get(BULK_IMPORT_PERSISTENT_EMR_RELEASE_LABEL))
                .applications(List.of(CfnCluster.ApplicationProperty.builder()
                        .name("Spark")
                        .build()))
                .stepConcurrencyLevel(instanceProperties.getInt(BULK_IMPORT_PERSISTENT_EMR_STEP_CONCURRENCY_LEVEL))
                .instances(jobFlowInstancesConfigProperty)
                .logUri(logUri)
                .serviceRole(commonStack.getEmrRole().getRoleName())
                .jobFlowRole(commonStack.getEc2Role().getRoleName())
                .tags(instanceProperties.getTags().entrySet().stream()
                        .map(entry -> CfnTag.builder().key(entry.getKey()).value(entry.getValue()).build())
                        .collect(Collectors.toList()));

        if (instanceProperties.getBoolean(BULK_IMPORT_PERSISTENT_EMR_USE_MANAGED_SCALING)) {
            ManagedScalingPolicyProperty scalingPolicy = ManagedScalingPolicyProperty.builder()
                    .computeLimits(CfnCluster.ComputeLimitsProperty.builder()
                            .unitType("InstanceFleetUnits")
                            .minimumCapacityUnits(instanceProperties.getInt(BULK_IMPORT_PERSISTENT_EMR_MIN_CAPACITY))
                            .maximumCapacityUnits(instanceProperties.getInt(BULK_IMPORT_PERSISTENT_EMR_MAX_CAPACITY))
                            .maximumCoreCapacityUnits(3)
                            .build())
                    .build();
            propsBuilder.managedScalingPolicy(scalingPolicy);
        }

        CfnClusterProps emrClusterProps = propsBuilder.build();
        CfnCluster emrCluster = new CfnCluster(scope, "PersistentEMRCluster", emrClusterProps);
        instanceProperties.set(BULK_IMPORT_PERSISTENT_EMR_CLUSTER_NAME, emrCluster.getName());
        instanceProperties.set(BULK_IMPORT_PERSISTENT_EMR_MASTER_DNS, emrCluster.getAttrMasterPublicDns());
    }

    private static List<CfnCluster.InstanceTypeConfigProperty> readExecutorInstanceTypes(
            InstanceProperties instanceProperties, EbsConfigurationProperty ebsConf) {
        return readInstanceTypes(instanceProperties, BULK_IMPORT_PERSISTENT_EMR_INSTANCE_ARCHITECTURE,
                BULK_IMPORT_PERSISTENT_EMR_EXECUTOR_X86_INSTANCE_TYPES, BULK_IMPORT_PERSISTENT_EMR_EXECUTOR_ARM_INSTANCE_TYPES)
                .map(config -> new CfnCluster.InstanceTypeConfigProperty.Builder()
                        .instanceType(config.getInstanceType())
                        .weightedCapacity(config.getWeightedCapacity())
                        .ebsConfiguration(ebsConf)
                        .configurations(getConfigurations(instanceProperties, config.getArchitecture()))
                        .build())
                .collect(Collectors.toList());
    }

    private static List<CfnCluster.InstanceTypeConfigProperty> readMasterInstanceTypes(
            InstanceProperties instanceProperties, EbsConfigurationProperty ebsConf) {
        return readInstanceTypes(instanceProperties, BULK_IMPORT_PERSISTENT_EMR_INSTANCE_ARCHITECTURE,
                BULK_IMPORT_PERSISTENT_EMR_MASTER_X86_INSTANCE_TYPES, BULK_IMPORT_PERSISTENT_EMR_MASTER_ARM_INSTANCE_TYPES)
                .map(config -> new CfnCluster.InstanceTypeConfigProperty.Builder()
                        .instanceType(config.getInstanceType())
                        .weightedCapacity(config.getWeightedCapacity())
                        .ebsConfiguration(ebsConf)
                        .configurations(getConfigurations(instanceProperties, config.getArchitecture()))
                        .build())
                .collect(Collectors.toList());
    }

    private static void configureJobStarterFunction(IFunction bulkImportJobStarter) {

        bulkImportJobStarter.addToRolePolicy(PolicyStatement.Builder.create()
                .actions(Lists.newArrayList("elasticmapreduce:*", "elasticmapreduce:ListClusters"))
                .effect(Effect.ALLOW)
                .resources(Lists.newArrayList("*"))
                .build());
    }

    private static List<CfnCluster.ConfigurationProperty> getConfigurations(
            InstanceProperties instanceProperties, EmrInstanceArchitecture architecture) {
        List<CfnCluster.ConfigurationProperty> configurations = new ArrayList<>();

        Map<String, String> emrSparkProps = ConfigurationUtils.getSparkEMRConfiguration();
        CfnCluster.ConfigurationProperty emrConfigurations = CfnCluster.ConfigurationProperty.builder()
                .classification("spark")
                .configurationProperties(emrSparkProps)
                .build();
        configurations.add(emrConfigurations);

        Map<String, String> yarnConf = ConfigurationUtils.getYarnConfiguration();
        CfnCluster.ConfigurationProperty yarnConfigurations = CfnCluster.ConfigurationProperty.builder()
                .classification("yarn-site")
                .configurationProperties(yarnConf)
                .build();
        configurations.add(yarnConfigurations);

        Map<String, String> sparkConf = ConfigurationUtils.getSparkConfigurationFromInstanceProperties(instanceProperties, architecture);
        CfnCluster.ConfigurationProperty sparkDefaultsConfigurations = CfnCluster.ConfigurationProperty.builder()
                .classification("spark-defaults")
                .configurationProperties(sparkConf)
                .build();
        configurations.add(sparkDefaultsConfigurations);

        Map<String, String> mapReduceSiteConf = ConfigurationUtils.getMapRedSiteConfiguration();
        CfnCluster.ConfigurationProperty mapRedSiteConfigurations = CfnCluster.ConfigurationProperty.builder()
                .classification("mapred-site")
                .configurationProperties(mapReduceSiteConf)
                .build();
        configurations.add(mapRedSiteConfigurations);

        Map<String, String> javaHomeConf = ConfigurationUtils.getJavaHomeConfiguration(architecture);
        CfnCluster.ConfigurationProperty hadoopEnvExportConfigurations = CfnCluster.ConfigurationProperty.builder()
                .classification("export")
                .configurationProperties(javaHomeConf)
                .build();
        CfnCluster.ConfigurationProperty hadoopEnvConfigurations = CfnCluster.ConfigurationProperty.builder()
                .classification("hadoop-env")
                .configurations(Collections.singletonList(hadoopEnvExportConfigurations))
                .build();
        configurations.add(hadoopEnvConfigurations);

        CfnCluster.ConfigurationProperty sparkEnvExportConfigurations = CfnCluster.ConfigurationProperty.builder()
                .classification("export")
                .configurationProperties(javaHomeConf)
                .build();
        CfnCluster.ConfigurationProperty sparkEnvConfigurations = CfnCluster.ConfigurationProperty.builder()
                .classification("spark-env")
                .configurations(Collections.singletonList(sparkEnvExportConfigurations))
                .build();
        configurations.add(sparkEnvConfigurations);

        return configurations;
    }

    public Queue getBulkImportJobQueue() {
        return bulkImportJobQueue;
    }
}
