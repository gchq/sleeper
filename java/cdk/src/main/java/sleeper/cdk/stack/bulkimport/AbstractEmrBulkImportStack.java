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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import sleeper.cdk.stack.StateStoreStack;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.SystemDefinedInstanceProperty;
import sleeper.configuration.properties.UserDefinedInstanceProperty;
import software.amazon.awscdk.CfnJson;
import software.amazon.awscdk.CfnJsonProps;
import software.amazon.awscdk.Duration;
import software.amazon.awscdk.services.cloudwatch.ComparisonOperator;
import software.amazon.awscdk.services.cloudwatch.CreateAlarmOptions;
import software.amazon.awscdk.services.cloudwatch.MetricOptions;
import software.amazon.awscdk.services.cloudwatch.TreatMissingData;
import software.amazon.awscdk.services.cloudwatch.actions.SnsAction;
import software.amazon.awscdk.services.emr.CfnSecurityConfiguration;
import software.amazon.awscdk.services.emr.CfnSecurityConfigurationProps;
import software.amazon.awscdk.services.iam.CfnInstanceProfile;
import software.amazon.awscdk.services.iam.CfnInstanceProfileProps;
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
import software.amazon.awscdk.services.s3.Bucket;
import software.amazon.awscdk.services.s3.IBucket;
import software.amazon.awscdk.services.sns.ITopic;
import software.amazon.awscdk.services.sqs.DeadLetterQueue;
import software.amazon.awscdk.services.sqs.Queue;
import software.constructs.Construct;

import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static sleeper.configuration.properties.SystemDefinedInstanceProperty.BULK_IMPORT_EMR_EC2_ROLE_NAME;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.ID;

public abstract class AbstractEmrBulkImportStack extends AbstractBulkImportStack {
    protected final String shortId;
    protected final String bulkImportPlatform;
    private final ITopic errorsTopic;
    protected final IBucket ingestBucket;
    protected final String instanceId;
    protected final String account;
    protected final String region;
    protected final String vpc;
    protected final String subnet;

    protected final Queue bulkImportJobQueue;
    protected IRole ec2Role;

    @SuppressFBWarnings("MC_OVERRIDABLE_METHOD_CALL_IN_CONSTRUCTOR")
    public AbstractEmrBulkImportStack(
            Construct scope,
            String id,
            String shortId,
            String bulkImportPlatform,
            SystemDefinedInstanceProperty jobQueueUrl,
            List<IBucket> dataBuckets,
            List<StateStoreStack> stateStoreStacks,
            InstanceProperties instanceProperties,
            ITopic errorsTopic) {
        super(scope, id, instanceProperties);
        this.shortId = shortId;
        this.bulkImportPlatform = bulkImportPlatform;
        this.errorsTopic = errorsTopic;
        this.instanceId = this.instanceProperties.get(ID);
        this.account = instanceProperties.get(UserDefinedInstanceProperty.ACCOUNT);
        this.region = instanceProperties.get(UserDefinedInstanceProperty.REGION);
        this.subnet = instanceProperties.get(UserDefinedInstanceProperty.SUBNET);
        this.vpc = instanceProperties.get(UserDefinedInstanceProperty.VPC_ID);

        // Ingest bucket
        String ingestBucketName = instanceProperties.get(UserDefinedInstanceProperty.INGEST_SOURCE_BUCKET);
        if (null != ingestBucketName && !ingestBucketName.isEmpty()) {
            this.ingestBucket = Bucket.fromBucketName(this, "IngestBucket", ingestBucketName);
        } else {
            this.ingestBucket = null;
        }

        // Queue for messages to trigger jobs - note that each concrete substack
        // will have its own queue. The shortId is used to ensure the names of
        // the queues are different.
        this.bulkImportJobQueue = createQueues(shortId, jobQueueUrl);

        // Create roles
        createRoles(dataBuckets, stateStoreStacks, region, account, vpc, subnet);

        // Create security configuration
        createSecurityConfiguration();
    }

    private Queue createQueues(String shortId, SystemDefinedInstanceProperty jobQueueUrl) {
        Queue queueForDLs = Queue.Builder
                .create(this, "BulkImport" + shortId + "JobDeadLetterQueue")
                .queueName(String.join("-", "sleeper", instanceId, "BulkImport" + shortId + "DLQ"))
                .build();

        DeadLetterQueue deadLetterQueue = DeadLetterQueue.builder()
                .maxReceiveCount(1)
                .queue(queueForDLs)
                .build();

        queueForDLs.metricApproximateNumberOfMessagesVisible().with(MetricOptions.builder()
                        .period(Duration.seconds(60))
                        .statistic("Sum")
                        .build())
                .createAlarm(this, "BulkImport" + shortId + "UndeliveredJobsAlarm", CreateAlarmOptions.builder()
                        .alarmDescription("Alarms if there are any messages that have failed validation or failed to start a " + shortId + " EMR Spark job")
                        .evaluationPeriods(1)
                        .comparisonOperator(ComparisonOperator.GREATER_THAN_THRESHOLD)
                        .threshold(0)
                        .datapointsToAlarm(1)
                        .treatMissingData(TreatMissingData.IGNORE)
                        .build())
                .addAlarmAction(new SnsAction(errorsTopic));

        Queue emrBulkImportJobQueue = Queue.Builder
                .create(this, "BulkImport" + shortId + "JobQueue")
                .deadLetterQueue(deadLetterQueue)
                .queueName(instanceId + "-BulkImport" + shortId + "Q")
                .build();

        instanceProperties.set(jobQueueUrl, emrBulkImportJobQueue.getQueueUrl());

        return emrBulkImportJobQueue;
    }

    protected void createRoles(List<IBucket> dataBuckets, List<StateStoreStack> stateStoreStacks,
                               String region, String account, String vpc, String subnet) {
        // These roles are shared across all concrete substacks, so we need to
        // avoid creating them twice if more than one concrete substack is
        // deployed.

        if (null != instanceProperties.get(BULK_IMPORT_EMR_EC2_ROLE_NAME)) {
            ec2Role = Role.fromRoleName(this, "Ec2Role", instanceProperties.get(BULK_IMPORT_EMR_EC2_ROLE_NAME));
            return;
        }

        // The EC2 Role is the role assumed by the EC2 instances and is the one
        // we need to grant accesses to.
        ec2Role = new Role(this, "Ec2Role", RoleProps.builder()
                .roleName("Sleeper-" + instanceId + "-EMR-EC2-Role")
                .description("The role assumed by the EC2 instances in EMR bulk import clusters")
                .assumedBy(new ServicePrincipal("ec2.amazonaws.com"))
                .build());
        dataBuckets.forEach(bucket -> bucket.grantReadWrite(ec2Role));
        stateStoreStacks.forEach(sss -> {
            sss.grantReadWriteActiveFileMetadata(ec2Role);
            sss.grantReadPartitionMetadata(ec2Role);
        });

        // The role needs to be able to access user's jars
        IBucket jarsBucket = Bucket.fromBucketName(this, "JarsBucket", instanceProperties.get(UserDefinedInstanceProperty.JARS_BUCKET));
        jarsBucket.grantRead(ec2Role);

        // Required to enable debugging
        ec2Role.addToPrincipalPolicy(PolicyStatement.Builder.create()
                .actions(Lists.newArrayList("sqs:GetQueueUrl", "sqs:SendMessage"))
                .effect(Effect.ALLOW)
                .resources(Lists.newArrayList("arn:aws:sqs:" + region + ":" + account + ":AWS-ElasticMapReduce-*"))
                .build());

        ec2Role.addToPrincipalPolicy(PolicyStatement.Builder.create()
                .actions(Lists.newArrayList("ec2:Describe*",
                        "elasticmapreduce:Describe*",
                        "elasticmapreduce:ListBootstrapActions",
                        "elasticmapreduce:ListClusters",
                        "elasticmapreduce:ListInstanceGroups",
                        "elasticmapreduce:ListInstances",
                        "elasticmapreduce:ListSteps",
                        "cloudwatch:*",
                        "s3:GetObject*"))
                .effect(Effect.ALLOW)
                .resources(Lists.newArrayList("*"))
                .build());

        // Allow SSM access
        ec2Role.addManagedPolicy(ManagedPolicy.fromAwsManagedPolicyName("AmazonSSMManagedInstanceCore"));

        instanceProperties.set(BULK_IMPORT_EMR_EC2_ROLE_NAME, ec2Role.getRoleName());

        new CfnInstanceProfile(this, "EC2InstanceProfile", CfnInstanceProfileProps.builder()
                .instanceProfileName(ec2Role.getRoleName())
                .roles(Lists.newArrayList(ec2Role.getRoleName()))
                .build());

        // Use the policy which is derived from the AmazonEMRServicePolicy_v2 policy.
        PolicyDocument policyDoc = PolicyDocument.fromJson(new Gson().fromJson(new JsonReader(
                        new InputStreamReader(EmrBulkImportStack.class.getResourceAsStream("/iam/SleeperEMRPolicy.json"), StandardCharsets.UTF_8)),
                Map.class));

        ManagedPolicy customEmrManagedPolicy = new ManagedPolicy(this, "CustomEMRManagedPolicy", ManagedPolicyProps.builder()
                .description("Custom policy for EMR bulk import to operate in VPC")
                .managedPolicyName("sleeper-" + instanceId + "-VPCPolicy")
                .document(PolicyDocument.Builder.create().statements(Lists.newArrayList(
                                new PolicyStatement(PolicyStatementProps.builder()
                                        .sid("CreateSecurityGroupInVPC")
                                        .actions(Lists.newArrayList("ec2:CreateSecurityGroup"))
                                        .effect(Effect.ALLOW)
                                        .resources(Lists.newArrayList("arn:aws:ec2:" + region + ":" + account + ":vpc/" + vpc))
                                        .build()),
                                new PolicyStatement(PolicyStatementProps.builder()
                                        .sid("ManageResourcesInSubnet")
                                        .actions(Lists.newArrayList(
                                                "ec2:CreateNetworkInterface",
                                                "ec2:RunInstances",
                                                "ec2:CreateFleet",
                                                "ec2:CreateLaunchTemplate",
                                                "ec2:CreateLaunchTemplateVersion"))
                                        .effect(Effect.ALLOW)
                                        .resources(Lists.newArrayList("arn:aws:ec2:" + region + ":" + account + ":subnet/" + subnet))
                                        .build()),
                                new PolicyStatement(PolicyStatementProps.builder()
                                        .sid("PassEc2Role")
                                        .effect(Effect.ALLOW)
                                        .actions(Lists.newArrayList("iam:PassRole"))
                                        .resources(Lists.newArrayList(ec2Role.getRoleArn()))
                                        .conditions(ImmutableMap.of("StringLike", ImmutableMap.of("iam:PassedToService", "ec2.amazonaws.com*")))
                                        .build()
                                )))
                        .build())
                .build());

        ManagedPolicy emrManagedPolicy = new ManagedPolicy(this, "DefaultEMRServicePolicy", ManagedPolicyProps.builder()
                .managedPolicyName("Sleeper-" + instanceId + "-DefaultEMRPolicy")
                .description("Policy required for Sleeper Bulk import EMR cluster, based on the AmazonEMRServicePolicy_v2 policy")
                .document(policyDoc)
                .build());

        Role emrRole = new Role(this, "EmrRole", RoleProps.builder()
                .roleName(String.join("-", "sleeper", instanceId, "EMR-Role"))
                .description("The role assumed by the Bulk import clusters")
                .managedPolicies(Lists.newArrayList(emrManagedPolicy, customEmrManagedPolicy))
                .assumedBy(new ServicePrincipal("elasticmapreduce.amazonaws.com"))
                .build());

        instanceProperties.set(SystemDefinedInstanceProperty.BULK_IMPORT_EMR_CLUSTER_ROLE_NAME, emrRole.getRoleName());

        if (importBucket != null) {
            importBucket.grantReadWrite(ec2Role);
        }

        if (ingestBucket != null) {
            ingestBucket.grantRead(ec2Role);
        }
    }

    protected void createSecurityConfiguration() {
        if (null == instanceProperties.get(SystemDefinedInstanceProperty.BULK_IMPORT_EMR_SECURITY_CONF_NAME)) {
            // See https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-create-security-configuration.html
            String jsonSecurityConf = "{\n" +
                    "  \"InstanceMetadataServiceConfiguration\" : {\n" +
                    "      \"MinimumInstanceMetadataServiceVersion\": 2,\n" +
                    "      \"HttpPutResponseHopLimit\": 1\n" +
                    "   }\n" +
                    "}";
            CfnJsonProps jsonProps = CfnJsonProps.builder().value(jsonSecurityConf).build();
            CfnJson jsonObject = new CfnJson(this, "EMRSecurityConfigurationJSONObject", jsonProps);
            CfnSecurityConfigurationProps securityConfigurationProps = CfnSecurityConfigurationProps.builder()
                    .name(String.join("-", "sleeper", instanceId, "EMRSecurityConfigurationProps"))
                    .securityConfiguration(jsonObject)
                    .build();
            new CfnSecurityConfiguration(this, "EMRSecurityConfiguration", securityConfigurationProps);
            instanceProperties.set(SystemDefinedInstanceProperty.BULK_IMPORT_EMR_SECURITY_CONF_NAME, securityConfigurationProps.getName());
        }
    }

    public Queue getEmrBulkImportJobQueue() {
        return bulkImportJobQueue;
    }
}
