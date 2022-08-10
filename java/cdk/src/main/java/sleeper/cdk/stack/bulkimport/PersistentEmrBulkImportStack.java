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

import static sleeper.configuration.properties.SystemDefinedInstanceProperty.*;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.*;

import com.google.common.collect.Lists;
import com.google.common.collect.ImmutableMap;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import sleeper.cdk.Utils;

import sleeper.cdk.stack.StateStoreStack;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.UserDefinedInstanceProperty;
import software.amazon.awscdk.CfnTag;
import software.amazon.awscdk.Duration;
import software.amazon.awscdk.services.emr.CfnCluster;
import software.amazon.awscdk.services.emr.CfnCluster.InstanceGroupConfigProperty;
import software.amazon.awscdk.services.emr.CfnCluster.JobFlowInstancesConfigProperty;
import software.amazon.awscdk.services.emr.CfnCluster.ManagedScalingPolicyProperty;
import software.amazon.awscdk.services.emr.CfnClusterProps;
import software.amazon.awscdk.services.iam.Effect;
import software.amazon.awscdk.services.iam.PolicyStatement;
import software.amazon.awscdk.services.lambda.Code;
import software.amazon.awscdk.services.lambda.Function;
import software.amazon.awscdk.services.lambda.S3Code;
import software.amazon.awscdk.services.lambda.eventsources.SqsEventSource;
import software.amazon.awscdk.services.s3.Bucket;
import software.amazon.awscdk.services.s3.IBucket;
import software.amazon.awscdk.services.sns.ITopic;
import software.amazon.awscdk.services.sqs.Queue;
import software.constructs.Construct;

/**
 * A {@link PersistentEmrBulkImportStack} creates an SQS queue, a lambda and
 * a persistent EMR cluster. Bulk import jobs are sent to the queue. This triggers
 * the lambda which then adds a step to the EMR cluster to run the bulk import
 * job.
 */
public class PersistentEmrBulkImportStack extends AbstractEmrBulkImportStack {
    protected Function bulkImportJobStarter;

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
        String bulkImportBucket = instanceProperties.get(BULK_IMPORT_EMR_BUCKET);
        String logUri = null == bulkImportBucket ? null : "s3://" + bulkImportBucket + "/logs";

        InstanceGroupConfigProperty masterInstanceGroupConfigProperty = InstanceGroupConfigProperty.builder()
                .instanceCount(1)
                .instanceType(instanceProperties.get(BULK_IMPORT_PERSISTENT_EMR_MASTER_INSTANCE_TYPE))
                .build();
        InstanceGroupConfigProperty coreInstanceGroupConfigProperty = InstanceGroupConfigProperty.builder()
                .instanceCount(instanceProperties.getInt(BULK_IMPORT_PERSISTENT_EMR_MIN_NUMBER_OF_INSTANCES))
                .instanceType(instanceProperties.get(BULK_IMPORT_PERSISTENT_EMR_EXECUTOR_INSTANCE_TYPE))
                .build();

        JobFlowInstancesConfigProperty.Builder jobFlowInstancesConfigPropertyBuilder = JobFlowInstancesConfigProperty.builder()
                .ec2KeyName(instanceProperties.get(BULK_IMPORT_EC2_KEY_NAME))
                .ec2SubnetId(subnet)
                .masterInstanceGroup(masterInstanceGroupConfigProperty)
                .coreInstanceGroup(coreInstanceGroupConfigProperty);
        if (null != instanceProperties.get(BULK_IMPORT_EMR_MASTER_ADDITIONAL_SECURITY_GROUP)
                        && !instanceProperties.get(BULK_IMPORT_EMR_MASTER_ADDITIONAL_SECURITY_GROUP).isEmpty()) {
                jobFlowInstancesConfigPropertyBuilder.additionalMasterSecurityGroups(Collections.singletonList(
                        instanceProperties.get(BULK_IMPORT_EMR_MASTER_ADDITIONAL_SECURITY_GROUP)));
        }
        JobFlowInstancesConfigProperty jobFlowInstancesConfigProperty = jobFlowInstancesConfigPropertyBuilder.build();
        
        CfnClusterProps.Builder propsBuilder = CfnClusterProps.builder()
                .name(String.join("-", "sleeper", instanceId, "persistentEMR"))
                .visibleToAllUsers(true)
                .securityConfiguration(instanceProperties.get(BULK_IMPORT_EMR_SECURITY_CONF_NAME))
                .releaseLabel(instanceProperties.get(BULK_IMPORT_PERSISTENT_EMR_RELEASE_LABEL))
                .applications(Arrays.asList(CfnCluster.ApplicationProperty.builder()
                    .name("Spark")
                    .build()))
                .stepConcurrencyLevel(instanceProperties.getInt(BULK_IMPORT_PERSISTENT_EMR_STEP_CONCURRENCY_LEVEL))
                .instances(jobFlowInstancesConfigProperty)
                .logUri(logUri)
                .serviceRole(instanceProperties.get(BULK_IMPORT_EMR_CLUSTER_ROLE_NAME))
                .jobFlowRole(instanceProperties.get(BULK_IMPORT_EMR_EC2_ROLE_NAME))
                .configurations(getConfigurations())
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
            propsBuilder = propsBuilder.managedScalingPolicy(scalingPolicy);
        }
        
        CfnClusterProps emrClusterProps = propsBuilder.build();
        CfnCluster emrCluster = new CfnCluster(this, id, emrClusterProps);
        instanceProperties.set(BULK_IMPORT_PERSISTENT_EMR_MASTER_DNS, emrCluster.getAttrMasterPublicDns());
        createBulkImportJobStarterFunction(shortId, "PersistentEMR", bulkImportJobQueue);
    }
    
    private void createBulkImportJobStarterFunction(String shortId, String bulkImportPlatform, Queue jobQueue) {
        Map<String, String> env = Utils.createDefaultEnvironment(instanceProperties);
        env.put("BULK_IMPORT_PLATFORM", bulkImportPlatform);
        S3Code code = Code.fromBucket(Bucket.fromBucketName(this, "CodeBucketEMR", instanceProperties.get(JARS_BUCKET)),
                "bulk-import-starter-" + instanceProperties.get(UserDefinedInstanceProperty.VERSION) + ".jar");

        IBucket configBucket = Bucket.fromBucketName(this, "ConfigBucket", instanceProperties.get(CONFIG_BUCKET));

        String functionName = Utils.truncateTo64Characters(String.join("-", "sleeper",
                instanceId.toLowerCase(), shortId, "bulk-import-job-starter"));

        bulkImportJobStarter = Function.Builder.create(this, "BulkImport" + shortId + "JobStarter")
                .code(code)
                .functionName(functionName)
                .description("Function to start " + shortId + " bulk import jobs")
                .memorySize(1024)
                .timeout(Duration.seconds(20))
                .environment(env)
                .runtime(software.amazon.awscdk.services.lambda.Runtime.JAVA_8)
                .handler("sleeper.bulkimport.starter.BulkImportStarter")
                .logRetention(Utils.getRetentionDays(instanceProperties.getInt(LOG_RETENTION_IN_DAYS)))
                .events(Lists.newArrayList(new SqsEventSource(jobQueue)))
                .build();

        configBucket.grantReadWrite(bulkImportJobStarter);
        if (importBucket != null) {
            importBucket.grantRead(bulkImportJobStarter);
        }
        if (ingestBucket != null) {
            ingestBucket.grantRead(bulkImportJobStarter);
        }
        
        Map<String, Map<String, String>> conditions = new HashMap<>();
        Map<String, String> tagKeyCondition = new HashMap<>();

        instanceProperties.getTags().entrySet().stream().forEach(entry -> {
            tagKeyCondition.put("elasticmapreduce:RequestTag/" + entry.getKey(), entry.getValue());
        });

        conditions.put("StringEquals", tagKeyCondition);

        bulkImportJobStarter.addToRolePolicy(PolicyStatement.Builder.create()
                .actions(Lists.newArrayList("elasticmapreduce:*", "elasticmapreduce:ListClusters"))
                .effect(Effect.ALLOW)
                .resources(Lists.newArrayList("*"))
                .build());

        String arnPrefix = "arn:aws:iam::" + instanceProperties.get(ACCOUNT) + ":role/";

        bulkImportJobStarter.addToRolePolicy(PolicyStatement.Builder.create()
                .effect(Effect.ALLOW)
                .actions(Lists.newArrayList("iam:PassRole"))
                .resources(Lists.newArrayList(
                        arnPrefix + instanceProperties.get(BULK_IMPORT_EMR_CLUSTER_ROLE_NAME),
                        arnPrefix + instanceProperties.get(BULK_IMPORT_EMR_EC2_ROLE_NAME)
                ))
                .build());

        bulkImportJobStarter.addToRolePolicy(PolicyStatement.Builder.create()
                .sid("CreateCleanupRole")
                .actions(Lists.newArrayList("iam:CreateServiceLinkedRole", "iam:PutRolePolicy"))
                .resources(Lists.newArrayList("arn:aws:iam::*:role/aws-service-role/elasticmapreduce.amazonaws.com*/AWSServiceRoleForEMRCleanup*"))
                .conditions(ImmutableMap.of("StringLike", ImmutableMap.of("iam:AWSServiceName",
                        Lists.newArrayList("elasticmapreduce.amazonaws.com",
                                "elasticmapreduce.amazonaws.com.cn"))))
                .build());
    }

    private List<CfnCluster.ConfigurationProperty> getConfigurations() {
        List<CfnCluster.ConfigurationProperty> configurations = new ArrayList<>();
        
        // The following YARN and Spark properties are as recommended in this blog:
        // https://aws.amazon.com/blogs/big-data/best-practices-for-successfully-managing-memory-for-apache-spark-applications-on-amazon-emr/
        Map<String, String> props = new HashMap<>();
        props.put("maximizeResourceAllocation", "false");
        CfnCluster.ConfigurationProperty emrConfigurations = CfnCluster.ConfigurationProperty.builder()
                .classification("spark")
                .configurationProperties(props)
                .build();
        configurations.add(emrConfigurations);

        Map<String, String> yarnProps = new HashMap<>();
        yarnProps.put("yarn.nodemanager.vmem-check-enabled", "false");
        yarnProps.put("yarn.nodemanager.pmem-check-enabled", "false");
        CfnCluster.ConfigurationProperty yarnConfigurations = CfnCluster.ConfigurationProperty.builder()
                .classification("yarn-site")
                .configurationProperties(yarnProps)
                .build();
        configurations.add(yarnConfigurations);

        Map<String, String> sparkProps = new HashMap<>();
        sparkProps.put("spark.executor.memory", instanceProperties.get(BULK_IMPORT_PERSISTENT_EMR_SPARK_EXECUTOR_MEMORY));
        sparkProps.put("spark.driver.memory", instanceProperties.get(BULK_IMPORT_PERSISTENT_EMR_SPARK_DRIVER_MEMORY));
        sparkProps.put("spark.executor.instances", instanceProperties.get(BULK_IMPORT_PERSISTENT_EMR_SPARK_EXECUTOR_INSTANCES));
        sparkProps.put("spark.yarn.executor.memoryOverhead", instanceProperties.get(BULK_IMPORT_PERSISTENT_EMR_SPARK_YARN_EXECUTOR_MEMORY_OVERHEAD));
        sparkProps.put("spark.yarn.driver.memoryOverhead", instanceProperties.get(BULK_IMPORT_PERSISTENT_EMR_SPARK_YARN_DRIVER_MEMORY_OVERHEAD));
        sparkProps.put("spark.default.parallelism", instanceProperties.get(BULK_IMPORT_PERSISTENT_EMR_SPARK_DEFAULT_PARALLELISM));
        sparkProps.put("spark.executor.cores", instanceProperties.get(BULK_IMPORT_PERSISTENT_EMR_SPARK_EXECUTOR_CORES));
        sparkProps.put("spark.driver.cores", instanceProperties.get(BULK_IMPORT_PERSISTENT_EMR_SPARK_DRIVER_CORES));
        sparkProps.put("spark.network.timeout", instanceProperties.get(BULK_IMPORT_PERSISTENT_EMR_SPARK_NETWORK_TIMEOUT));
        sparkProps.put("spark.executor.heartbeatInterval", instanceProperties.get(BULK_IMPORT_PERSISTENT_EMR_SPARK_EXECUTOR_HEARTBEAT_INTERVAL));
        sparkProps.put("spark.dynamicAllocation.enabled", instanceProperties.get(BULK_IMPORT_PERSISTENT_EMR_SPARK_DYNAMIC_ALLOCATION_ENABLED));
        sparkProps.put("spark.memory.fraction", instanceProperties.get(BULK_IMPORT_PERSISTENT_EMR_SPARK_MEMORY_FRACTION));
        sparkProps.put("spark.memory.storageFraction", instanceProperties.get(BULK_IMPORT_PERSISTENT_EMR_SPARK_MEMORY_STORAGE_FRACTION));
        sparkProps.put("spark.executor.extraJavaOptions", instanceProperties.get(BULK_IMPORT_PERSISTENT_EMR_SPARK_EXECUTOR_EXTRA_JAVA_OPTIONS));
        sparkProps.put("spark.driver.extraJavaOptions", instanceProperties.get(BULK_IMPORT_PERSISTENT_EMR_SPARK_DRIVER_EXTRA_JAVA_OPTIONS));
        sparkProps.put("spark.yarn.scheduler.reporterThread.maxFailures", instanceProperties.get(BULK_IMPORT_PERSISTENT_EMR_SPARK_YARN_SCHEDULER_REPORTER_THREAD_MAX_FAILURES));
        sparkProps.put("spark.storage.level", instanceProperties.get(BULK_IMPORT_PERSISTENT_EMR_SPARK_STORAGE_LEVEL));
        sparkProps.put("spark.rdd.compress", instanceProperties.get(BULK_IMPORT_PERSISTENT_EMR_SPARK_RDD_COMPRESS));
        sparkProps.put("spark.shuffle.compress", instanceProperties.get(BULK_IMPORT_PERSISTENT_EMR_SPARK_SHUFFLE_COMPRESS));
        sparkProps.put("spark.shuffle.spill.compress", instanceProperties.get(BULK_IMPORT_PERSISTENT_EMR_SPARK_SHUFFLE_SPILL_COMPRESS));
        CfnCluster.ConfigurationProperty sparkDefaultsConfigurations = CfnCluster.ConfigurationProperty.builder()
                .classification("spark-defaults")
                .configurationProperties(sparkProps)
                .build();
        configurations.add(sparkDefaultsConfigurations);

        Map<String, String> mapRedSiteProps = new HashMap<>();
        mapRedSiteProps.put("mapreduce.map.output.compress", "true");
        CfnCluster.ConfigurationProperty mapRedSiteConfigurations = CfnCluster.ConfigurationProperty.builder()
                .classification("mapred-site")
                .configurationProperties(mapRedSiteProps)
                .build();
        configurations.add(mapRedSiteConfigurations);

        Map<String, String> hadoopEnvExportProperties = new HashMap<>();
        hadoopEnvExportProperties.put("JAVA_HOME", "/usr/lib/jvm/java-1.8.0");
        CfnCluster.ConfigurationProperty hadoopEnvExportConfigurations = CfnCluster.ConfigurationProperty.builder()
                .classification("export")
                .configurationProperties(hadoopEnvExportProperties)
                .build();
        CfnCluster.ConfigurationProperty hadoopEnvConfigurations = CfnCluster.ConfigurationProperty.builder()
                .classification("hadoop-env")
                .configurations(Collections.singletonList(hadoopEnvExportConfigurations))
                .build();
        configurations.add(hadoopEnvConfigurations);

        Map<String, String> sparkEnvProperties = new HashMap<>();
        sparkEnvProperties.put("JAVA_HOME", "/usr/lib/jvm/java-1.8.0");
        CfnCluster.ConfigurationProperty sparkEnvExportConfigurations = CfnCluster.ConfigurationProperty.builder()
                .classification("export")
                .configurationProperties(sparkEnvProperties)
                .build();
        CfnCluster.ConfigurationProperty sparkEnvConfigurations = CfnCluster.ConfigurationProperty.builder()
                .classification("spark-env")
                .configurations(Collections.singletonList(sparkEnvExportConfigurations))
                .build();
        configurations.add(sparkEnvConfigurations);
        
        return configurations;
    }
}
