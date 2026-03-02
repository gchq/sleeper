/*
 * Copyright 2022-2025 Crown Copyright
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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import software.amazon.awscdk.NestedStack;
import software.amazon.awscdk.services.ec2.SecurityGroup;
import software.amazon.awscdk.services.emrserverless.CfnApplication;
import software.amazon.awscdk.services.emrserverless.CfnApplication.AutoStartConfigurationProperty;
import software.amazon.awscdk.services.emrserverless.CfnApplication.AutoStopConfigurationProperty;
import software.amazon.awscdk.services.emrserverless.CfnApplication.InitialCapacityConfigKeyValuePairProperty;
import software.amazon.awscdk.services.emrserverless.CfnApplication.InitialCapacityConfigProperty;
import software.amazon.awscdk.services.emrserverless.CfnApplication.NetworkConfigurationProperty;
import software.amazon.awscdk.services.emrserverless.CfnApplication.WorkerConfigurationProperty;
import software.amazon.awscdk.services.iam.Effect;
import software.amazon.awscdk.services.iam.IRole;
import software.amazon.awscdk.services.iam.ManagedPolicy;
import software.amazon.awscdk.services.iam.PolicyDocument;
import software.amazon.awscdk.services.iam.PolicyStatement;
import software.amazon.awscdk.services.iam.PolicyStatementProps;
import software.amazon.awscdk.services.iam.Role;
import software.amazon.awscdk.services.iam.RoleProps;
import software.amazon.awscdk.services.iam.ServicePrincipal;
import software.amazon.awscdk.services.lambda.IFunction;
import software.amazon.awscdk.services.s3.Bucket;
import software.amazon.awscdk.services.s3.IBucket;
import software.amazon.awscdk.services.sqs.Queue;
import software.constructs.Construct;

import sleeper.bulkimport.core.configuration.BulkImportPlatform;
import sleeper.cdk.artefacts.SleeperInstanceArtefacts;
import sleeper.cdk.lambda.SleeperLambdaCode;
import sleeper.cdk.stack.SleeperCoreStacks;
import sleeper.cdk.stack.core.AutoStopEmrServerlessApplicationStack;
import sleeper.cdk.stack.core.LoggingStack.LogGroupRef;
import sleeper.cdk.util.Utils;
import sleeper.core.properties.instance.InstanceProperties;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.BULK_IMPORT_EMR_SERVERLESS_APPLICATION_ID;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.BULK_IMPORT_EMR_SERVERLESS_CLUSTER_NAME;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.BULK_IMPORT_EMR_SERVERLESS_CLUSTER_ROLE_ARN;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.BULK_IMPORT_EMR_SERVERLESS_JOB_QUEUE_ARN;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.BULK_IMPORT_EMR_SERVERLESS_JOB_QUEUE_URL;
import static sleeper.core.properties.instance.CommonProperty.JARS_BUCKET;
import static sleeper.core.properties.instance.EMRServerlessProperty.BULK_IMPORT_EMR_SERVERLESS_ARCHITECTURE;
import static sleeper.core.properties.instance.EMRServerlessProperty.BULK_IMPORT_EMR_SERVERLESS_AUTOSTART;
import static sleeper.core.properties.instance.EMRServerlessProperty.BULK_IMPORT_EMR_SERVERLESS_AUTOSTOP;
import static sleeper.core.properties.instance.EMRServerlessProperty.BULK_IMPORT_EMR_SERVERLESS_AUTOSTOP_TIMEOUT_MINUTES;
import static sleeper.core.properties.instance.EMRServerlessProperty.BULK_IMPORT_EMR_SERVERLESS_INITIAL_CAPACITY_DRIVER_CORES;
import static sleeper.core.properties.instance.EMRServerlessProperty.BULK_IMPORT_EMR_SERVERLESS_INITIAL_CAPACITY_DRIVER_COUNT;
import static sleeper.core.properties.instance.EMRServerlessProperty.BULK_IMPORT_EMR_SERVERLESS_INITIAL_CAPACITY_DRIVER_DISK;
import static sleeper.core.properties.instance.EMRServerlessProperty.BULK_IMPORT_EMR_SERVERLESS_INITIAL_CAPACITY_DRIVER_MEMORY;
import static sleeper.core.properties.instance.EMRServerlessProperty.BULK_IMPORT_EMR_SERVERLESS_INITIAL_CAPACITY_ENABLED;
import static sleeper.core.properties.instance.EMRServerlessProperty.BULK_IMPORT_EMR_SERVERLESS_INITIAL_CAPACITY_EXECUTOR_CORES;
import static sleeper.core.properties.instance.EMRServerlessProperty.BULK_IMPORT_EMR_SERVERLESS_INITIAL_CAPACITY_EXECUTOR_COUNT;
import static sleeper.core.properties.instance.EMRServerlessProperty.BULK_IMPORT_EMR_SERVERLESS_INITIAL_CAPACITY_EXECUTOR_DISK;
import static sleeper.core.properties.instance.EMRServerlessProperty.BULK_IMPORT_EMR_SERVERLESS_INITIAL_CAPACITY_EXECUTOR_MEMORY;
import static sleeper.core.properties.instance.EMRServerlessProperty.BULK_IMPORT_EMR_SERVERLESS_RELEASE;

/**
 * Deploys resources to perform bulk import jobs on EMR Serverless. Creates an SQS queue that bulk import jobs can be
 * sent to. A message arriving on this queue triggers a lambda. That lambda creates an EMR Serverless job that
 * executes the bulk import job and then terminates.
 */
@SuppressFBWarnings("MC_OVERRIDABLE_METHOD_CALL_IN_CONSTRUCTOR")
public class EmrServerlessBulkImportStack extends NestedStack {
    private final Queue bulkImportJobQueue;

    public EmrServerlessBulkImportStack(
            Construct scope,
            String id,
            InstanceProperties instanceProperties,
            SleeperInstanceArtefacts artefacts,
            BulkImportBucketStack importBucketStack,
            SleeperCoreStacks coreStacks,
            AutoStopEmrServerlessApplicationStack autoStopEmrServerlessApplicationStack) {
        super(scope, id);
        IBucket jarsBucket = Bucket.fromBucketName(this, "JarsBucket", instanceProperties.get(JARS_BUCKET));
        SleeperLambdaCode lambdaCode = artefacts.lambdaCodeAtScope(this);
        createEmrServerlessApplication(instanceProperties, coreStacks, autoStopEmrServerlessApplicationStack);
        IRole emrRole = createEmrServerlessRole(
                instanceProperties, importBucketStack, coreStacks, jarsBucket);
        CommonEmrBulkImportHelper commonHelper = new CommonEmrBulkImportHelper(this,
                BulkImportPlatform.EMRServerless, instanceProperties, coreStacks);
        bulkImportJobQueue = commonHelper.createJobQueue(
                BULK_IMPORT_EMR_SERVERLESS_JOB_QUEUE_URL, BULK_IMPORT_EMR_SERVERLESS_JOB_QUEUE_ARN);

        IFunction jobStarter = commonHelper.createJobStarterFunction(
                bulkImportJobQueue, lambdaCode, importBucketStack.getImportBucket(),
                LogGroupRef.BULK_IMPORT_EMR_SERVERLESS_START, List.of(emrRole));
        configureJobStarterFunction(instanceProperties, jobStarter);
        Utils.addTags(this, instanceProperties);
    }

    private static void configureJobStarterFunction(InstanceProperties instanceProperties, IFunction bulkImportJobStarter) {
        Map<String, Map<String, String>> conditions = new HashMap<>();
        Map<String, String> tagKeyCondition = new HashMap<>();
        instanceProperties.getTags().forEach(
                (key, value) -> tagKeyCondition.put("elasticmapreduce:RequestTag/" + key, value));

        conditions.put("StringEquals", tagKeyCondition);

        bulkImportJobStarter.addToRolePolicy(PolicyStatement.Builder.create()
                .actions(List.of("elasticmapreduce:RunJobFlow")).effect(Effect.ALLOW)
                .resources(List.of("*")).conditions(conditions).build());

        bulkImportJobStarter.addToRolePolicy(PolicyStatement.Builder.create()
                .sid("EmrServerlessStartJobRun")
                .actions(List.of("emr-serverless:StartJobRun"))
                .resources(List.of("*"))
                .build());
    }

    public void createEmrServerlessApplication(
            InstanceProperties instanceProperties, SleeperCoreStacks coreStacks,
            AutoStopEmrServerlessApplicationStack autoStopEmrServerlessApplicationStack) {
        CfnApplication emrServerlessCluster = CfnApplication.Builder.create(this, "BulkImportEMRServerless")
                .name(String.join("-", "sleeper", Utils.cleanInstanceId(instanceProperties)))
                .releaseLabel(instanceProperties.get(BULK_IMPORT_EMR_SERVERLESS_RELEASE))
                .architecture(instanceProperties.get(BULK_IMPORT_EMR_SERVERLESS_ARCHITECTURE))
                .type("Spark")
                .initialCapacity(createInitialCapacity(instanceProperties))
                .autoStartConfiguration(AutoStartConfigurationProperty.builder()
                        .enabled(instanceProperties.getBoolean(BULK_IMPORT_EMR_SERVERLESS_AUTOSTART)).build())
                .autoStopConfiguration(AutoStopConfigurationProperty.builder()
                        .enabled(instanceProperties.getBoolean(BULK_IMPORT_EMR_SERVERLESS_AUTOSTOP))
                        .idleTimeoutMinutes(instanceProperties.getInt(BULK_IMPORT_EMR_SERVERLESS_AUTOSTOP_TIMEOUT_MINUTES)).build())
                .networkConfiguration(NetworkConfigurationProperty.builder()
                        .subnetIds(coreStacks.networking().subnetIds())
                        .securityGroupIds(List.of(createSecurityGroup(coreStacks)))
                        .build())
                .build();
        instanceProperties.set(BULK_IMPORT_EMR_SERVERLESS_CLUSTER_NAME,
                emrServerlessCluster.getName());
        instanceProperties.set(BULK_IMPORT_EMR_SERVERLESS_APPLICATION_ID,
                emrServerlessCluster.getAttrApplicationId());

        autoStopEmrServerlessApplicationStack.addAutoStopEmrServerlessApplication(this, emrServerlessCluster);
    }

    private String createSecurityGroup(SleeperCoreStacks coreStacks) {
        SecurityGroup securityGroup = SecurityGroup.Builder
                .create(this, "EMR-Serverless")
                .description("Security Group used by EMR Serverless")
                .vpc(coreStacks.getVpc())
                .build();
        return securityGroup.getSecurityGroupId();
    }

    private List<InitialCapacityConfigKeyValuePairProperty> createInitialCapacity(InstanceProperties instanceProperties) {
        List<InitialCapacityConfigKeyValuePairProperty> properties = instanceProperties.getBoolean(BULK_IMPORT_EMR_SERVERLESS_INITIAL_CAPACITY_ENABLED) ? List.of(
                InitialCapacityConfigKeyValuePairProperty.builder().key("EXECUTOR")
                        .value(InitialCapacityConfigProperty.builder()
                                .workerCount(instanceProperties.getInt(
                                        BULK_IMPORT_EMR_SERVERLESS_INITIAL_CAPACITY_EXECUTOR_COUNT))
                                .workerConfiguration(WorkerConfigurationProperty.builder()
                                        .cpu(instanceProperties.get(
                                                BULK_IMPORT_EMR_SERVERLESS_INITIAL_CAPACITY_EXECUTOR_CORES))
                                        .memory(instanceProperties.get(
                                                BULK_IMPORT_EMR_SERVERLESS_INITIAL_CAPACITY_EXECUTOR_MEMORY))
                                        .disk(instanceProperties.get(
                                                BULK_IMPORT_EMR_SERVERLESS_INITIAL_CAPACITY_EXECUTOR_DISK))
                                        .build())
                                .build())
                        .build(),
                InitialCapacityConfigKeyValuePairProperty.builder().key("DRIVER")
                        .value(InitialCapacityConfigProperty.builder()
                                .workerCount(instanceProperties.getInt(
                                        BULK_IMPORT_EMR_SERVERLESS_INITIAL_CAPACITY_DRIVER_COUNT))
                                .workerConfiguration(WorkerConfigurationProperty.builder()
                                        .cpu(instanceProperties.get(
                                                BULK_IMPORT_EMR_SERVERLESS_INITIAL_CAPACITY_DRIVER_CORES))
                                        .memory(instanceProperties.get(
                                                BULK_IMPORT_EMR_SERVERLESS_INITIAL_CAPACITY_DRIVER_MEMORY))
                                        .disk(instanceProperties.get(
                                                BULK_IMPORT_EMR_SERVERLESS_INITIAL_CAPACITY_DRIVER_DISK))
                                        .build())
                                .build())
                        .build())
                : Collections.emptyList();
        return properties;
    }

    private IRole createEmrServerlessRole(
            InstanceProperties instanceProperties,
            BulkImportBucketStack bulkImportBucketStack,
            SleeperCoreStacks coreStacks, IBucket jarsBucket) {
        Role role = new Role(this, "EmrServerlessRole", RoleProps.builder()
                .roleName(String.join("-", "sleeper",
                        Utils.cleanInstanceId(instanceProperties), "bulk-import-emr-serverless"))
                .description("The role assumed by the Bulk import EMR Serverless Application")
                .managedPolicies(
                        List.of(createEmrServerlessManagedPolicy(instanceProperties)))
                .assumedBy(new ServicePrincipal("emr-serverless.amazonaws.com")).build());

        instanceProperties.set(BULK_IMPORT_EMR_SERVERLESS_CLUSTER_ROLE_ARN, role.getRoleArn());

        bulkImportBucketStack.getImportBucket().grantReadWrite(role);
        coreStacks.grantIngest(role);
        coreStacks.grantReadWritePartitions(role); // The partition tree can be extended if there aren't enough partitions to do a bulk import
        jarsBucket.grantRead(role);
        return role;
    }

    private ManagedPolicy createEmrServerlessManagedPolicy(InstanceProperties instanceProperties) {
        // See https://docs.aws.amazon.com/emr/latest/EMR-Serverless-UserGuide/getting-started.html
        return ManagedPolicy.Builder.create(this, "CustomEMRServerlessServicePolicy")
                .managedPolicyName(
                        String.join("-", "sleeper",
                                Utils.cleanInstanceId(instanceProperties), "bulk-import-emr-serverless"))
                .description("Policy required for Sleeper Bulk import EMR Serverless cluster, " +
                        "based on the AmazonEMRServicePolicy_v2 policy")
                .document(PolicyDocument.Builder.create().statements(List.of(
                        new PolicyStatement(PolicyStatementProps.builder()
                                .sid("PolicyStatementProps").effect(Effect.ALLOW)
                                .actions(List.of("s3:GetObject", "s3:ListBucket"))
                                .resources(List.of("arn:aws:s3:::*.elasticmapreduce",
                                        "arn:aws:s3:::*.elasticmapreduce/*"))
                                .build()),
                        new PolicyStatement(PolicyStatementProps.builder()
                                .sid("GlueCreateAndReadDataCatalog").effect(Effect.ALLOW)
                                .actions(List.of("glue:GetDatabase", "glue:CreateDatabase",
                                        "glue:GetDataBases", "glue:CreateTable",
                                        "glue:GetTable", "glue:UpdateTable",
                                        "glue:DeleteTable", "glue:GetTables",
                                        "glue:GetPartition", "glue:GetPartitions",
                                        "glue:CreatePartition", "glue:BatchCreatePartition",
                                        "glue:GetUserDefinedFunctions"))
                                .resources(List.of("*")).build())))
                        .build())
                .build();
    }

    public Queue getBulkImportJobQueue() {
        return bulkImportJobQueue;
    }
}
