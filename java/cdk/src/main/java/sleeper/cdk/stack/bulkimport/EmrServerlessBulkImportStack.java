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
import software.amazon.awscdk.NestedStack;
import software.amazon.awscdk.services.ec2.IVpc;
import software.amazon.awscdk.services.ec2.SecurityGroup;
import software.amazon.awscdk.services.ec2.Vpc;
import software.amazon.awscdk.services.ec2.VpcLookupOptions;
import software.amazon.awscdk.services.emrserverless.CfnApplication;
import software.amazon.awscdk.services.emrserverless.CfnApplication.ImageConfigurationInputProperty;
import software.amazon.awscdk.services.emrserverless.CfnApplication.NetworkConfigurationProperty;
import software.amazon.awscdk.services.emrserverless.CfnApplicationProps;
import software.amazon.awscdk.services.iam.Effect;
import software.amazon.awscdk.services.iam.IRole;
import software.amazon.awscdk.services.iam.ManagedPolicy;
import software.amazon.awscdk.services.iam.ManagedPolicyProps;
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

import sleeper.cdk.Utils;
import sleeper.cdk.jars.BuiltJars;
import sleeper.cdk.stack.IngestStatusStoreResources;
import sleeper.cdk.stack.TableDataStack;
import sleeper.cdk.stack.TableStack;
import sleeper.cdk.stack.TopicStack;
import sleeper.configuration.properties.instance.InstanceProperties;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static sleeper.cdk.stack.IngestStack.addIngestSourceBucketReferences;
import static sleeper.configuration.properties.instance.CommonProperty.ACCOUNT;
import static sleeper.configuration.properties.instance.CommonProperty.ID;
import static sleeper.configuration.properties.instance.CommonProperty.REGION;
import static sleeper.configuration.properties.instance.CommonProperty.SUBNETS;
import static sleeper.configuration.properties.instance.CommonProperty.VPC_ID;
import static sleeper.configuration.properties.instance.EMRServerlessProperty.DEFAULT_BULK_IMPORT_EMR_SERVERLESS_ARCHITECTURE;
import static sleeper.configuration.properties.instance.EMRServerlessProperty.DEFAULT_BULK_IMPORT_EMR_SERVERLESS_CUSTOM_IMAGE_REPO;
import static sleeper.configuration.properties.instance.EMRServerlessProperty.DEFAULT_BULK_IMPORT_EMR_SERVERLESS_RELEASE;
import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.BULK_IMPORT_EMR_SERVERLESS_APPLICATION_ID;
import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.BULK_IMPORT_EMR_SERVERLESS_CLUSTER_NAME;
import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.BULK_IMPORT_EMR_SERVERLESS_CLUSTER_ROLE_ARN;
import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.BULK_IMPORT_EMR_SERVERLESS_JOB_QUEUE_ARN;
import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.BULK_IMPORT_EMR_SERVERLESS_JOB_QUEUE_URL;
import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.VERSION;

/**
 * An {@link EmrServerlessBulkImportStack} creates an SQS queue that bulk import jobs can be sent
 * to. A message arriving on this queue triggers a lambda. That lambda creates an EMR cluster that
 * executes the bulk import job and then terminates.
 */
public class EmrServerlessBulkImportStack extends NestedStack {
    private final Queue bulkImportJobQueue;

    public EmrServerlessBulkImportStack(
            Construct scope, String id,
            InstanceProperties instanceProperties, BuiltJars jars,
            BulkImportBucketStack importBucketStack, TopicStack errorsTopicStack,
            TableStack tableStack, TableDataStack dataStack,
            IngestStatusStoreResources statusStoreResources) {
        super(scope, id);
        createEmrServerlessApplication(instanceProperties);
        IBucket configBucket = Bucket.fromBucketName(scope, "ConfigBucket", instanceProperties.get(CONFIG_BUCKET));
        List<IBucket> ingestBuckets = addIngestSourceBucketReferences(this, "IngestBucket", instanceProperties);
        IRole emrRole = createEmrServerlessRole(
                instanceProperties, importBucketStack, statusStoreResources, tableStack, dataStack, configBucket, ingestBuckets);
        CommonEmrBulkImportHelper commonHelper = new CommonEmrBulkImportHelper(this,
                "EMRServerless", instanceProperties, statusStoreResources, configBucket, ingestBuckets);
        bulkImportJobQueue = commonHelper.createJobQueue(
                BULK_IMPORT_EMR_SERVERLESS_JOB_QUEUE_URL, BULK_IMPORT_EMR_SERVERLESS_JOB_QUEUE_ARN,
                errorsTopicStack.getTopic());

        IFunction jobStarter = commonHelper.createJobStarterFunction("EMRServerless",
                bulkImportJobQueue, jars, importBucketStack.getImportBucket(), List.of(emrRole));
        configureJobStarterFunction(instanceProperties, jobStarter);
        Utils.addStackTagIfSet(this, instanceProperties);
        tableStack.getStateStoreStacks().forEach(sss -> sss.grantReadPartitionMetadata(jobStarter));
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
                .actions(Lists.newArrayList("emr-serverless:StartJobRun"))
                .resources(Lists.newArrayList("*"))
                .build());
    }

    public void createEmrServerlessApplication(InstanceProperties instanceProperties) {
        String instanceId = instanceProperties.get(ID);
        String region = instanceProperties.get(REGION);
        String accountId = instanceProperties.get(ACCOUNT);
        String repo = instanceProperties.get(DEFAULT_BULK_IMPORT_EMR_SERVERLESS_CUSTOM_IMAGE_REPO);
        String version = instanceProperties.get(VERSION);
        String uri = accountId + ".dkr.ecr." + region + ".amazonaws.com/" + repo + ":" + version;

        CfnApplicationProps props = CfnApplicationProps.builder()
                .name(String.join("-", "sleeper", instanceId, "emr", "serverless"))
                .releaseLabel(instanceProperties.get(DEFAULT_BULK_IMPORT_EMR_SERVERLESS_RELEASE))
                .architecture(instanceProperties.get(DEFAULT_BULK_IMPORT_EMR_SERVERLESS_ARCHITECTURE))
                .type("Spark")
                .imageConfiguration(ImageConfigurationInputProperty.builder().imageUri(uri).build())
                .networkConfiguration(NetworkConfigurationProperty.builder()
                        .subnetIds(instanceProperties.getList(SUBNETS))
                        .securityGroupIds(List.of(createSecurityGroup(instanceProperties))).build())
                .build();

        CfnApplication emrServerlessCluster = new CfnApplication(this, getArtifactId(), props);
        instanceProperties.set(BULK_IMPORT_EMR_SERVERLESS_CLUSTER_NAME,
                emrServerlessCluster.getName());
        instanceProperties.set(BULK_IMPORT_EMR_SERVERLESS_APPLICATION_ID,
                emrServerlessCluster.getAttrApplicationId());
    }

    private String createSecurityGroup(InstanceProperties instanceProperties) {
        IVpc vpc = Vpc.fromLookup(this, "VPC",
                VpcLookupOptions.builder().vpcId(instanceProperties.get(VPC_ID)).build());

        SecurityGroup securityGroup = SecurityGroup.Builder
                .create(this, "EMR-Serverless")
                .description("Security Group used by EMR Serverless").vpc(vpc).build();
        return securityGroup.getSecurityGroupId();
    }

    private IRole createEmrServerlessRole(
            InstanceProperties instanceProperties,
            BulkImportBucketStack bulkImportBucketStack, IngestStatusStoreResources statusStoreResources,
            TableStack tableStack, TableDataStack dataStack, IBucket configBucket, List<IBucket> ingestBuckets) {
        String instanceId = instanceProperties.get(ID);
        Role role = new Role(this, "EmrServerlessRole", RoleProps.builder()
                .roleName(String.join("-", "sleeper", instanceId, "EMR-Serverless-Role"))
                .description("The role assumed by the Bulk import EMR Serverless Application")
                .managedPolicies(
                        List.of(createEmrServerlessManagedPolicy(instanceProperties)))
                .assumedBy(new ServicePrincipal("emr-serverless.amazonaws.com")).build());

        instanceProperties.set(BULK_IMPORT_EMR_SERVERLESS_CLUSTER_ROLE_ARN, role.getRoleArn());

        IBucket importBucket = bulkImportBucketStack.getImportBucket();
        configBucket.grantRead(role);
        importBucket.grantReadWrite(role);

        ingestBuckets.forEach(ingestBucket -> ingestBucket.grantRead(role));
        statusStoreResources.grantWriteJobEvent(role);
        tableStack.getStateStoreStacks().forEach(sss -> {
            sss.grantReadPartitionMetadata(role);
            sss.grantReadWriteActiveFileMetadata(role);
        });
        dataStack.getDataBucket().grantReadWrite(role);
        return role;
    }

    private ManagedPolicy createEmrServerlessManagedPolicy(InstanceProperties instanceProperties) {
        // See https://docs.aws.amazon.com/emr/latest/EMR-Serverless-UserGuide/getting-started.html
        String instanceId = instanceProperties.get(ID);

        return new ManagedPolicy(this,
                "CustomEMRServerlessServicePolicy",
                ManagedPolicyProps.builder()
                        .managedPolicyName(
                                String.join("-", "sleeper", instanceId, "EmrServerlessPolicy"))
                        .description(
                                "Policy required for Sleeper Bulk import EMR Serverless cluster, based on the AmazonEMRServicePolicy_v2 policy")
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
                        .build());
    }

    public Queue getBulkImportJobQueue() {
        return bulkImportJobQueue;
    }
}
