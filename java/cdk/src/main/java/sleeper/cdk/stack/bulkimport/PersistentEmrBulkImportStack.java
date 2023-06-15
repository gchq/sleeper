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
package sleeper.cdk.stack.bulkimport;

import com.google.common.collect.Lists;
import software.amazon.awscdk.CfnTag;
import software.amazon.awscdk.NestedStack;
import software.amazon.awscdk.services.emr.CfnCluster;
import software.amazon.awscdk.services.emr.CfnCluster.EbsBlockDeviceConfigProperty;
import software.amazon.awscdk.services.emr.CfnCluster.EbsConfigurationProperty;
import software.amazon.awscdk.services.emr.CfnCluster.InstanceGroupConfigProperty;
import software.amazon.awscdk.services.emr.CfnCluster.JobFlowInstancesConfigProperty;
import software.amazon.awscdk.services.emr.CfnCluster.ManagedScalingPolicyProperty;
import software.amazon.awscdk.services.emr.CfnCluster.VolumeSpecificationProperty;
import software.amazon.awscdk.services.emr.CfnClusterProps;
import software.amazon.awscdk.services.iam.Effect;
import software.amazon.awscdk.services.iam.PolicyStatement;
import software.amazon.awscdk.services.lambda.IFunction;
import software.amazon.awscdk.services.s3.IBucket;
import software.amazon.awscdk.services.sqs.Queue;
import software.constructs.Construct;

import sleeper.bulkimport.configuration.ConfigurationUtils;
import sleeper.cdk.Utils;
import sleeper.cdk.jars.BuiltJars;
import sleeper.cdk.stack.StateStoreStack;
import sleeper.cdk.stack.TopicStack;
import sleeper.configuration.properties.InstanceProperties;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static sleeper.configuration.properties.SystemDefinedInstanceProperty.BULK_IMPORT_PERSISTENT_EMR_CLUSTER_NAME;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.BULK_IMPORT_PERSISTENT_EMR_JOB_QUEUE_URL;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.BULK_IMPORT_PERSISTENT_EMR_MASTER_DNS;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.BULK_IMPORT_EMR_EBS_VOLUMES_PER_INSTANCE;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.BULK_IMPORT_EMR_EBS_VOLUME_SIZE_IN_GB;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.BULK_IMPORT_EMR_EBS_VOLUME_TYPE;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.BULK_IMPORT_EMR_EC2_KEYPAIR_NAME;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.BULK_IMPORT_EMR_MASTER_ADDITIONAL_SECURITY_GROUP;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.BULK_IMPORT_PERSISTENT_EMR_EXECUTOR_INSTANCE_TYPE;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.BULK_IMPORT_PERSISTENT_EMR_MASTER_INSTANCE_TYPE;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.BULK_IMPORT_PERSISTENT_EMR_MAX_NUMBER_OF_INSTANCES;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.BULK_IMPORT_PERSISTENT_EMR_MIN_NUMBER_OF_INSTANCES;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.BULK_IMPORT_PERSISTENT_EMR_RELEASE_LABEL;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.BULK_IMPORT_PERSISTENT_EMR_STEP_CONCURRENCY_LEVEL;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.BULK_IMPORT_PERSISTENT_EMR_USE_MANAGED_SCALING;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.ID;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.SUBNETS;


/**
 * A {@link PersistentEmrBulkImportStack} creates an SQS queue, a lambda and
 * a persistent EMR cluster. Bulk import jobs are sent to the queue. This triggers
 * the lambda which then adds a step to the EMR cluster to run the bulk import
 * job.
 */
public class PersistentEmrBulkImportStack extends NestedStack {
    private final Queue bulkImportJobQueue;

    public PersistentEmrBulkImportStack(
            Construct scope,
            String id,
            InstanceProperties instanceProperties,
            BuiltJars jars,
            BulkImportBucketStack importBucketStack,
            CommonEmrBulkImportStack commonEmrStack,
            TopicStack errorsTopicStack,
            List<StateStoreStack> stateStoreStacks) {
        super(scope, id);
        CommonEmrBulkImportHelper commonHelper = new CommonEmrBulkImportHelper(
                this, "PersistentEMR", instanceProperties);
        bulkImportJobQueue = commonHelper.createJobQueue(BULK_IMPORT_PERSISTENT_EMR_JOB_QUEUE_URL, errorsTopicStack.getTopic());
        IFunction jobStarter = commonHelper.createJobStarterFunction(
                "PersistentEMR", bulkImportJobQueue, jars, importBucketStack.getImportBucket(), commonEmrStack);
        configureJobStarterFunction(jobStarter);
        stateStoreStacks.forEach(sss -> sss.grantReadPartitionMetadata(jobStarter));
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

        InstanceGroupConfigProperty masterInstanceGroupConfigProperty = InstanceGroupConfigProperty.builder()
                .instanceCount(1)
                .instanceType(instanceProperties.get(BULK_IMPORT_PERSISTENT_EMR_MASTER_INSTANCE_TYPE))
                .ebsConfiguration(ebsConf)
                .build();
        InstanceGroupConfigProperty coreInstanceGroupConfigProperty = InstanceGroupConfigProperty.builder()
                .instanceCount(instanceProperties.getInt(BULK_IMPORT_PERSISTENT_EMR_MIN_NUMBER_OF_INSTANCES))
                .instanceType(instanceProperties.get(BULK_IMPORT_PERSISTENT_EMR_EXECUTOR_INSTANCE_TYPE))
                .ebsConfiguration(ebsConf)
                .build();

        JobFlowInstancesConfigProperty.Builder jobFlowInstancesConfigPropertyBuilder = JobFlowInstancesConfigProperty.builder()
                .ec2SubnetIds(instanceProperties.getList(SUBNETS))
                .masterInstanceGroup(masterInstanceGroupConfigProperty)
                .coreInstanceGroup(coreInstanceGroupConfigProperty);

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
                .name(String.join("-", "sleeper", instanceProperties.get(ID), "persistentEMR"))
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
                .configurations(getConfigurations(instanceProperties))
                .tags(instanceProperties.getTags().entrySet().stream()
                        .map(entry -> CfnTag.builder().key(entry.getKey()).value(entry.getValue()).build())
                        .collect(Collectors.toList()));

        if (instanceProperties.getBoolean(BULK_IMPORT_PERSISTENT_EMR_USE_MANAGED_SCALING)) {
            ManagedScalingPolicyProperty scalingPolicy = ManagedScalingPolicyProperty.builder()
                    .computeLimits(CfnCluster.ComputeLimitsProperty.builder()
                            .unitType("Instances")
                            .minimumCapacityUnits(instanceProperties.getInt(BULK_IMPORT_PERSISTENT_EMR_MIN_NUMBER_OF_INSTANCES))
                            .maximumCapacityUnits(instanceProperties.getInt(BULK_IMPORT_PERSISTENT_EMR_MAX_NUMBER_OF_INSTANCES))
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

    private static void configureJobStarterFunction(IFunction bulkImportJobStarter) {

        bulkImportJobStarter.addToRolePolicy(PolicyStatement.Builder.create()
                .actions(Lists.newArrayList("elasticmapreduce:*", "elasticmapreduce:ListClusters"))
                .effect(Effect.ALLOW)
                .resources(Lists.newArrayList("*"))
                .build());
    }

    private static List<CfnCluster.ConfigurationProperty> getConfigurations(InstanceProperties instanceProperties) {
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

        Map<String, String> sparkConf = ConfigurationUtils.getSparkConfigurationFromInstanceProperties(instanceProperties);
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

        Map<String, String> javaHomeConf = ConfigurationUtils.getJavaHomeConfiguration();
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
