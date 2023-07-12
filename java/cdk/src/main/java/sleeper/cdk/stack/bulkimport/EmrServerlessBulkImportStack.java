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
import software.amazon.awscdk.CfnTag;
import software.amazon.awscdk.NestedStack;
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
import sleeper.configuration.properties.InstanceProperties;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static sleeper.configuration.properties.SystemDefinedInstanceProperty.BULK_IMPORT_BUCKET;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.BULK_IMPORT_EMR_JOB_QUEUE_URL;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.BULK_IMPORT_EMR_SERVERLESS_APPLICATION_ID;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.BULK_IMPORT_EMR_SERVERLESS_CLUSTER_NAME;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.BULK_IMPORT_EMR_SERVERLESS_CLUSTER_ROLE_ARN;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.BULK_IMPORT_EMR_SERVERLESS_ARCHITECTURE;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.BULK_IMPORT_EMR_SERVERLESS_CUSTOM_IMAGE_REPO;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.BULK_IMPORT_EMR_SERVERLESS_RELEASE;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.BULK_IMPORT_EMR_SERVERLESS_TYPE;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.ID;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.SUBNETS;

/**
 * An {@link EmrServerlessBulkImportStack} creates an SQS queue that bulk import jobs can be sent
 * to. A message arriving on this queue triggers a lambda. That lambda creates an EMR cluster that
 * executes the bulk import job and then terminates.
 */
public class EmrServerlessBulkImportStack extends NestedStack {
    private static final Logger LOGGER = LoggerFactory.getLogger(EmrServerlessBulkImportStack.class);

    public EmrServerlessBulkImportStack(Construct scope, String id,
                                        InstanceProperties instanceProperties, BuiltJars jars,
                                        BulkImportBucketStack importBucketStack,
                                        TopicStack errorsTopicStack, List<StateStoreStack> stateStoreStacks,
                                        IngestStatusStoreResources statusStoreResources) {
        super(scope, id);
        LOGGER.info("Starting to create application.\nScope: {}\nInstanceProperties: {}", scope,
                instanceProperties);
        createEmrServerlessApplication(instanceProperties);

        IRole emrRole = createEmrServerlessRole(instanceProperties);

        CommonEmrBulkImportHelper commonHelper = new CommonEmrBulkImportHelper(this,
                "EMRServerless", instanceProperties, statusStoreResources);
        Queue bulkImportJobQueue = commonHelper.createJobQueue(BULK_IMPORT_EMR_JOB_QUEUE_URL,
                errorsTopicStack.getTopic());
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
                .actions(Lists.newArrayList("elasticmapreduce:RunJobFlow")).effect(Effect.ALLOW)
                .resources(Lists.newArrayList("*")).conditions(conditions).build());
    }

    public void createEmrServerlessApplication(InstanceProperties instanceProperties) {
        CfnApplicationProps props = CfnApplicationProps.builder()
                .name(String.join("-", "sleeper", "emr", "serverless"))
                .releaseLabel(instanceProperties.get(BULK_IMPORT_EMR_SERVERLESS_RELEASE))
                .architecture(instanceProperties.get(BULK_IMPORT_EMR_SERVERLESS_ARCHITECTURE))
                .type(instanceProperties.get(BULK_IMPORT_EMR_SERVERLESS_TYPE))
                .imageConfiguration(ImageConfigurationInputProperty.builder()
                        .imageUri(instanceProperties.get(BULK_IMPORT_EMR_SERVERLESS_CUSTOM_IMAGE_REPO))
                        .build())
                .networkConfiguration(NetworkConfigurationProperty.builder()
                        .subnetIds(List.of(getSubnet(instanceProperties))).build())
                .tags(List.of(new CfnTag.Builder().key("DeploymentStack")
                        .value("EmrServerlessBulkImport").build()))
                .build();

        CfnApplication emrServerlessCluster = new CfnApplication(this, getArtifactId(), props);
        instanceProperties.set(BULK_IMPORT_EMR_SERVERLESS_CLUSTER_NAME,
                emrServerlessCluster.getName());
        instanceProperties.set(BULK_IMPORT_EMR_SERVERLESS_APPLICATION_ID,
                emrServerlessCluster.getAttrApplicationId());
    }

    // ToDo test on multiple subnets
    private String getSubnet(InstanceProperties instanceProperties) {
        List<String> subnets = instanceProperties.getList(SUBNETS);
        LOGGER.info("Subnets {}", subnets);
        return subnets.get(0);
    }

    private IRole createEmrServerlessRole(InstanceProperties instanceProperties) {
        String instanceId = instanceProperties.get(ID);
        Role role = new Role(this, "EmrServerlessRole", RoleProps.builder()
                .roleName(String.join("-", "sleeper", instanceId, "EMR-Role"))
                .description("The role assumed by the Bulk import EMR Serverless Application")
                .managedPolicies(Lists
                        .newArrayList(createEmrServerlessManagedPolicy(instanceProperties)))
                .assumedBy(new ServicePrincipal("elasticmapreduce.amazonaws.com")).build());

        instanceProperties.set(BULK_IMPORT_EMR_SERVERLESS_CLUSTER_ROLE_ARN, role.getRoleArn());
        return role;
    }

    private ManagedPolicy createEmrServerlessManagedPolicy(InstanceProperties instanceProperties) {
        // See https://docs.aws.amazon.com/emr/latest/EMR-Serverless-UserGuide/getting-started.html
        String instanceId = instanceProperties.get(ID);

        ManagedPolicy emrServerlessManagedPolicy = new ManagedPolicy(this,
                "CustomEMRServerlessServicePolicy",
                ManagedPolicyProps.builder()
                        .managedPolicyName("sleeper-" + instanceId + "-EmrServerlessPolicy")
                        .description(
                                "Policy required for Sleeper Bulk import EMR Serverless cluster, based on the AmazonEMRServicePolicy_v2 policy")
                        .document(PolicyDocument.Builder.create()
                                .statements(Lists.newArrayList(
                                        new PolicyStatement(PolicyStatementProps.builder()
                                                .sid("PolicyStatementProps").effect(Effect.ALLOW)
                                                .actions(Lists.newArrayList("s3:GetObject",
                                                        "s3:ListBucket"))
                                                .resources(Lists.newArrayList(
                                                        "arn:aws:s3:::*.elasticmapreduce",
                                                        "arn:aws:s3:::*.elasticmapreduce/*"))
                                                .build()),
                                        new PolicyStatement(PolicyStatementProps.builder()
                                                .sid("FullAccessToOutputBucket")
                                                .effect(Effect.ALLOW)
                                                .actions(Lists.newArrayList("s3:PutObject",
                                                        "s3:GetObject", "s3:ListBucket",
                                                        "s3:DeleteObject"))
                                                .resources(Lists.newArrayList(
                                                        "arn:aws:s3:::" + BULK_IMPORT_BUCKET,
                                                        "arn:aws:s3:::" + BULK_IMPORT_BUCKET
                                                                + "/*"))
                                                .build()),
                                        new PolicyStatement(PolicyStatementProps.builder()
                                                .sid("GlueCreateAndReadDataCatalog")
                                                .effect(Effect.ALLOW)
                                                .actions(Lists.newArrayList("glue:GetDatabase",
                                                        "glue:CreateDatabase", "glue:GetDataBases",
                                                        "glue:CreateTable", "glue:GetTable",
                                                        "glue:UpdateTable", "glue:DeleteTable",
                                                        "glue:GetTables", "glue:GetPartition",
                                                        "glue:GetPartitions",
                                                        "glue:CreatePartition",
                                                        "glue:BatchCreatePartition",
                                                        "glue:GetUserDefinedFunctions"))
                                                .resources(Lists.newArrayList("*")).build())))
                                .build())
                        .build());
        return emrServerlessManagedPolicy;
    }
}
