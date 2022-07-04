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
package sleeper.cdk.stack.bulkimport;

import static sleeper.configuration.properties.SystemDefinedInstanceProperty.BULK_IMPORT_PERSISTENT_EMR_JOB_QUEUE_URL;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.BULK_IMPORT_PERSISTENT_EMR_MASTER_DNS;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.BULK_IMPORT_PERSISTENT_EMR_EXECUTOR_INSTANCE_TYPE;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.BULK_IMPORT_PERSISTENT_EMR_MASTER_INSTANCE_TYPE;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.BULK_IMPORT_PERSISTENT_EMR_MAX_NUMBER_OF_EXECUTORS;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.BULK_IMPORT_PERSISTENT_EMR_MIN_NUMBER_OF_EXECUTORS;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.BULK_IMPORT_PERSISTENT_EMR_RELEASE_LABEL;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.BULK_IMPORT_PERSISTENT_EMR_USE_MANAGED_SCALING;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.JARS_BUCKET;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.VERSION;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import sleeper.cdk.stack.StateStoreStack;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.SystemDefinedInstanceProperty;
import sleeper.configuration.properties.UserDefinedInstanceProperty;
import software.amazon.awscdk.CfnTag;
import software.amazon.awscdk.services.emr.CfnCluster;
import software.amazon.awscdk.services.emr.CfnCluster.BootstrapActionConfigProperty;
import software.amazon.awscdk.services.emr.CfnCluster.ConfigurationProperty;
import software.amazon.awscdk.services.emr.CfnCluster.HadoopJarStepConfigProperty;
import software.amazon.awscdk.services.emr.CfnCluster.InstanceGroupConfigProperty;
import software.amazon.awscdk.services.emr.CfnCluster.JobFlowInstancesConfigProperty;
import software.amazon.awscdk.services.emr.CfnCluster.ManagedScalingPolicyProperty;
import software.amazon.awscdk.services.emr.CfnCluster.ScriptBootstrapActionConfigProperty;
import software.amazon.awscdk.services.emr.CfnCluster.StepConfigProperty;
import software.amazon.awscdk.services.emr.CfnClusterProps;
import software.amazon.awscdk.services.s3.IBucket;
import software.amazon.awscdk.services.sns.ITopic;
import software.constructs.Construct;

/**
 * A {@link PersistentEmrBulkImportStack} creates an SQS queue and a persistent
 * EMR cluster. A script runs on the EMR cluster that polls the SQS queue for
 * messages. When a message is received, a Spark job is submitted to the cluster
 * to execute that job.
 */
public class PersistentEmrBulkImportStack extends AbstractEmrBulkImportStack {

    public PersistentEmrBulkImportStack(Construct scope,
            String id,
            List<IBucket> dataBuckets,
            List<StateStoreStack> stateStoreStacks,
            InstanceProperties instanceProperties,
            ITopic errorsTopic) {
        super(scope, id, "PersistentEMR", "PersistentEMR",
                BULK_IMPORT_PERSISTENT_EMR_JOB_QUEUE_URL, dataBuckets, stateStoreStacks,
                instanceProperties, errorsTopic);
        
        // Create security configuration
        createSecurityConfiguration();
        
        // EMR cluster
        String bulkImportBucket = instanceProperties.get(UserDefinedInstanceProperty.BULK_IMPORT_EMR_BUCKET);
        String logUri = null == bulkImportBucket ? null : "s3://" + bulkImportBucket + "/logs";

        ScriptBootstrapActionConfigProperty scriptBootstrapActionConfig = ScriptBootstrapActionConfigProperty.builder()
                .path("s3://" + instanceProperties.get(JARS_BUCKET) + "/bulk_import_job_starter_file_copy_" + instanceProperties.get(VERSION) + ".sh")
                .args(Arrays.asList(instanceProperties.get(JARS_BUCKET), instanceProperties.get(VERSION)))
                .build();        
        BootstrapActionConfigProperty bootstrapActionConfigProperty = BootstrapActionConfigProperty.builder()
                .name(String.join("-", "sleeper", "copy_script_bootstrap_action"))
                .scriptBootstrapAction(scriptBootstrapActionConfig)
                .build();
        
        HadoopJarStepConfigProperty hadoopJarStepConfigProperty = HadoopJarStepConfigProperty.builder()
                .jar("command-runner.jar")
                .args(Arrays.asList("/home/hadoop/bulk_import_job_starter.sh",
                        instanceProperties.get(SystemDefinedInstanceProperty.CONFIG_BUCKET),
                        instanceProperties.get(UserDefinedInstanceProperty.JARS_BUCKET),
                        instanceProperties.get(UserDefinedInstanceProperty.VERSION),
                        instanceProperties.get(SystemDefinedInstanceProperty.BULK_IMPORT_PERSISTENT_EMR_JOB_QUEUE_URL)))
                .build();
        StepConfigProperty stepConfigProperty = StepConfigProperty.builder()
                .name("Persistent EMR queue poller")
                .actionOnFailure("CONTINUE")
                .hadoopJarStep(hadoopJarStepConfigProperty)
                .build();
        
        InstanceGroupConfigProperty masterInstanceGroupConfigProperty = InstanceGroupConfigProperty.builder()
                .instanceCount(1)
                .instanceType(instanceProperties.get(BULK_IMPORT_PERSISTENT_EMR_MASTER_INSTANCE_TYPE))
                .build();
        InstanceGroupConfigProperty coreInstanceGroupConfigProperty = InstanceGroupConfigProperty.builder()
                .instanceCount(instanceProperties.getInt(BULK_IMPORT_PERSISTENT_EMR_MIN_NUMBER_OF_EXECUTORS))
                .instanceType(instanceProperties.get(BULK_IMPORT_PERSISTENT_EMR_EXECUTOR_INSTANCE_TYPE))
                .build();
        JobFlowInstancesConfigProperty.Builder jobFlowInstancesConfigPropertyBuilder = JobFlowInstancesConfigProperty.builder()
                .ec2SubnetId(subnet)
                .masterInstanceGroup(masterInstanceGroupConfigProperty)
                .coreInstanceGroup(coreInstanceGroupConfigProperty);
        
        String ec2KeyName = instanceProperties.get(UserDefinedInstanceProperty.BULK_IMPORT_EC2_KEY_NAME);
        if (null != ec2KeyName && !ec2KeyName.isEmpty()) {
            jobFlowInstancesConfigPropertyBuilder = jobFlowInstancesConfigPropertyBuilder.ec2KeyName(ec2KeyName);
        }
        String emrMasterAdditionalSecurityGroup = instanceProperties
                .get(UserDefinedInstanceProperty.BULK_IMPORT_EMR_MASTER_ADDITIONAL_SECURITY_GROUP);
        if (null != emrMasterAdditionalSecurityGroup && !emrMasterAdditionalSecurityGroup.isEmpty()) {
            jobFlowInstancesConfigPropertyBuilder = jobFlowInstancesConfigPropertyBuilder
                    .additionalMasterSecurityGroups(Collections.singletonList(emrMasterAdditionalSecurityGroup));
        }
        JobFlowInstancesConfigProperty jobFlowInstancesConfigProperty = jobFlowInstancesConfigPropertyBuilder.build();
        
        Map<String, String> props = new HashMap<>();
        props.put("maximizeResourceAllocation", "true");
        ConfigurationProperty configuration = ConfigurationProperty.builder()
                .classification("spark")
                .configurationProperties(props)
                .build();
        
        CfnClusterProps.Builder propsBuilder = CfnClusterProps.builder()
                .name(String.join("-", "sleeper", instanceId))
                .visibleToAllUsers(true)
                .securityConfiguration(instanceProperties.get(SystemDefinedInstanceProperty.BULK_IMPORT_EMR_SECURITY_CONF_NAME))
                .releaseLabel(instanceProperties.get(BULK_IMPORT_PERSISTENT_EMR_RELEASE_LABEL))
                .applications(Arrays.asList(CfnCluster.ApplicationProperty.builder()
                    .name("Spark")
                    .build()))
                .instances(jobFlowInstancesConfigProperty)
                .logUri(logUri)
                .serviceRole(instanceProperties.get(SystemDefinedInstanceProperty.BULK_IMPORT_EMR_CLUSTER_ROLE_NAME))
                .jobFlowRole(instanceProperties.get(SystemDefinedInstanceProperty.BULK_IMPORT_EMR_EC2_ROLE_NAME))
                .bootstrapActions(Arrays.asList(bootstrapActionConfigProperty))
                .configurations(Arrays.asList(configuration))
                .steps(Arrays.asList(stepConfigProperty))
                .tags(instanceProperties.getTags().entrySet().stream()
                        .map(entry -> CfnTag.builder().key(entry.getKey()).value(entry.getValue()).build())
                        .collect(Collectors.toList()));
        
        if (instanceProperties.getBoolean(BULK_IMPORT_PERSISTENT_EMR_USE_MANAGED_SCALING)) {
            ManagedScalingPolicyProperty scalingPolicy = ManagedScalingPolicyProperty.builder()
                .computeLimits(CfnCluster.ComputeLimitsProperty.builder()
                        .unitType("Instances")
                        .minimumCapacityUnits(instanceProperties.getInt(BULK_IMPORT_PERSISTENT_EMR_MIN_NUMBER_OF_EXECUTORS))
                        .maximumCapacityUnits(instanceProperties.getInt(BULK_IMPORT_PERSISTENT_EMR_MAX_NUMBER_OF_EXECUTORS))
                        .maximumCoreCapacityUnits(3)
                    .build())
                .build();
            propsBuilder = propsBuilder.managedScalingPolicy(scalingPolicy);
        }
        
        CfnClusterProps emrClusterProps = propsBuilder.build();
        CfnCluster emrCluster = new CfnCluster(this, id, emrClusterProps);
        
        bulkImportJobQueue.grantConsumeMessages(ec2Role);
        
        instanceProperties.set(BULK_IMPORT_PERSISTENT_EMR_MASTER_DNS, emrCluster.getAttrMasterPublicDns());
    }
}
