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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
import software.amazon.awscdk.services.sqs.Queue;
import software.constructs.Construct;

import sleeper.cdk.Utils;
import sleeper.cdk.jars.BuiltJars;
import sleeper.cdk.stack.IngestStatusStoreResources;
import sleeper.cdk.stack.StateStoreStack;
import sleeper.cdk.stack.TopicStack;
import sleeper.configuration.properties.instance.InstanceProperties;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static sleeper.configuration.properties.instance.CommonProperty.ACCOUNT;
import static sleeper.configuration.properties.instance.CommonProperty.ID;
import static sleeper.configuration.properties.instance.CommonProperty.REGION;
import static sleeper.configuration.properties.instance.CommonProperty.SUBNETS;
import static sleeper.configuration.properties.instance.CommonProperty.VPC_ID;
import static sleeper.configuration.properties.instance.EMRServerlessProperty.BULK_IMPORT_EMR_SERVERLESS_ARCHITECTURE;
import static sleeper.configuration.properties.instance.EMRServerlessProperty.BULK_IMPORT_EMR_SERVERLESS_CUSTOM_IMAGE_REPO;
import static sleeper.configuration.properties.instance.EMRServerlessProperty.BULK_IMPORT_EMR_SERVERLESS_RELEASE;
import static sleeper.configuration.properties.instance.IngestProperty.INGEST_SOURCE_BUCKET;
import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.BULK_IMPORT_BUCKET;
import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.BULK_IMPORT_EMR_SERVERLESS_APPLICATION_ID;
import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.BULK_IMPORT_EMR_SERVERLESS_CLUSTER_NAME;
import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.BULK_IMPORT_EMR_SERVERLESS_CLUSTER_ROLE_ARN;
import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.BULK_IMPORT_EMR_SERVERLESS_JOB_QUEUE_URL;
import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.VERSION;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;

/**
 * An {@link EmrServerlessBulkImportStack} creates an SQS queue that bulk import jobs can be sent
 * to. A message arriving on this queue triggers a lambda. That lambda creates an EMR cluster that
 * executes the bulk import job and then terminates.
 */
public class EmrServerlessBulkImportStack extends NestedStack {
    private static final Logger LOGGER = LoggerFactory
            .getLogger(EmrServerlessBulkImportStack.class);

    public EmrServerlessBulkImportStack(Construct scope, String id,
            InstanceProperties instanceProperties, BuiltJars jars,
            BulkImportBucketStack importBucketStack, TopicStack errorsTopicStack,
            List<StateStoreStack> stateStoreStacks,
            IngestStatusStoreResources statusStoreResources) {
        super(scope, id);
        LOGGER.info("Starting to create application.\nScope: {}", scope);
        createEmrServerlessApplication(instanceProperties);

        IRole emrRole = createEmrServerlessRole(scope, instanceProperties);

        CommonEmrBulkImportHelper commonHelper = new CommonEmrBulkImportHelper(this,
                "EMRServerless", instanceProperties, statusStoreResources);
        Queue bulkImportJobQueue = commonHelper.createJobQueue(
                BULK_IMPORT_EMR_SERVERLESS_JOB_QUEUE_URL, errorsTopicStack.getTopic());
        IFunction jobStarter = commonHelper.createJobStarterFunction("EMRServerless",
                bulkImportJobQueue, jars, importBucketStack.getImportBucket(), List.of(emrRole));
        stateStoreStacks.forEach(sss -> sss.grantReadPartitionMetadata(jobStarter));
        configureJobStarterFunction(instanceProperties, jobStarter);
        Utils.addStackTagIfSet(this, instanceProperties);
    }

    private static void configureJobStarterFunction(InstanceProperties instanceProperties,
            IFunction bulkImportJobStarter) {
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
        String repo = instanceProperties.get(BULK_IMPORT_EMR_SERVERLESS_CUSTOM_IMAGE_REPO);
        String version = instanceProperties.get(VERSION);
        String uri = accountId + ".dkr.ecr." + region + ".amazonaws.com/" + repo + ":" + version;

        CfnApplicationProps props = CfnApplicationProps.builder()
                .name(String.join("-", "sleeper", instanceId, "emr", "serverless"))
                .releaseLabel(instanceProperties.get(BULK_IMPORT_EMR_SERVERLESS_RELEASE))
                .architecture(instanceProperties.get(BULK_IMPORT_EMR_SERVERLESS_ARCHITECTURE))
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

    private IRole createEmrServerlessRole(Construct scope, InstanceProperties instanceProperties) {
        String instanceId = instanceProperties.get(ID);
        Role role = new Role(this, "EmrServerlessRole", RoleProps.builder()
                .roleName(String.join("-", "sleeper", instanceId, "EMR-Serverless-Role"))
                .description("The role assumed by the Bulk import EMR Serverless Application")
                .managedPolicies(
                        List.of(createEmrServerlessManagedPolicy(scope, instanceProperties)))
                .assumedBy(new ServicePrincipal("emr-serverless.amazonaws.com")).build());

        instanceProperties.set(BULK_IMPORT_EMR_SERVERLESS_CLUSTER_ROLE_ARN, role.getRoleArn());
        return role;
    }

    private ManagedPolicy createEmrServerlessManagedPolicy(Construct scope,
            InstanceProperties instanceProperties) {
        // See https://docs.aws.amazon.com/emr/latest/EMR-Serverless-UserGuide/getting-started.html
        String instanceId = instanceProperties.get(ID);
        String bulkImportBucket = instanceProperties.get(BULK_IMPORT_BUCKET);
        String ingestSourceBucket = instanceProperties.get(INGEST_SOURCE_BUCKET);
        String configBucket = String.join("-", "sleeper", instanceId, "config");

        List<String> buckets = Stream.of("arn:aws:s3:::" + bulkImportBucket,
                "arn:aws:s3:::" + bulkImportBucket + "/*",
                "arn:aws:s3:::" + ingestSourceBucket,
                "arn:aws:s3:::" + ingestSourceBucket + "/*",
                "arn:aws:s3:::" + configBucket,
                "arn:aws:s3:::" + configBucket + "/*").collect(Collectors.toList());

        List<String> tables = new ArrayList<>();
        String base = "arn:aws:dynamodb:" + instanceProperties.get(REGION) + ":" + instanceProperties.get(ACCOUNT) + ":table/";
        tables.add(base + String.join("-", "sleeper", instanceId, "ingest-job-status"));

        Utils.getAllTableProperties(instanceProperties, scope).forEach(tableProperties -> {
            String table = String.join("-", "sleeper", instanceId, "table", tableProperties.get(TABLE_NAME));
            String bucketArn = "arn:aws:s3:::" + table;
            buckets.add(bucketArn);
            buckets.add(bucketArn + "/*");
            tables.add(base + table + "-partitions");
            tables.add(base + table + "-active-files");
        });

        ManagedPolicy emrServerlessManagedPolicy = new ManagedPolicy(this,
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
                                        .sid("AccessToBuckets").effect(Effect.ALLOW)
                                        .actions(List.of("s3:PutObject", "s3:GetObject",
                                                "s3:ListBucket", "s3:DeleteObject"))
                                        .resources(buckets).build()),
                                new PolicyStatement(PolicyStatementProps.builder()
                                        .sid("GlueCreateAndReadDataCatalog").effect(Effect.ALLOW)
                                        .actions(List.of("glue:GetDatabase", "glue:CreateDatabase",
                                                "glue:GetDataBases", "glue:CreateTable",
                                                "glue:GetTable", "glue:UpdateTable",
                                                "glue:DeleteTable", "glue:GetTables",
                                                "glue:GetPartition", "glue:GetPartitions",
                                                "glue:CreatePartition", "glue:BatchCreatePartition",
                                                "glue:GetUserDefinedFunctions"))
                                        .resources(List.of("*")).build()),

                                new PolicyStatement(PolicyStatementProps.builder()
                                        .sid("AccessToDynamo").effect(Effect.ALLOW)
                                        .actions(List.of("dynamodb:Scan",
                                        "dynamodb:PutItem"))
                                        .resources(tables).build())))
                                .build())
                        .build());
        return emrServerlessManagedPolicy;
    }
}
