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
import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;
import software.amazon.awscdk.NestedStack;
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
import software.constructs.Construct;

import sleeper.cdk.stack.StateStoreStack;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.SystemDefinedInstanceProperty;
import sleeper.configuration.properties.UserDefinedInstanceProperty;

import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static sleeper.configuration.properties.SystemDefinedInstanceProperty.BULK_IMPORT_EMR_EC2_ROLE_NAME;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.ACCOUNT;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.ID;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.REGION;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.SUBNET;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.VPC_ID;

public class CommonEmrBulkImportStack extends NestedStack {
    private final IRole ec2Role;
    private final InstanceProperties instanceProperties;

    protected CommonEmrBulkImportStack(Construct scope,
                                       String id,
                                       InstanceProperties instanceProperties,
                                       IBucket ingestBucket,
                                       IBucket importBucket,
                                       List<IBucket> dataBuckets,
                                       List<StateStoreStack> stateStoreStacks) {
        super(scope, id);
        this.instanceProperties = instanceProperties;
        this.ec2Role = createEc2Role(ingestBucket, importBucket, dataBuckets, stateStoreStacks);
    }

    protected IRole createEc2Role(IBucket ingestBucket, IBucket importBucket, List<IBucket> dataBuckets, List<StateStoreStack> stateStoreStacks) {
        String instanceId = instanceProperties.get(ID);
        String region = instanceProperties.get(REGION);
        String account = instanceProperties.get(ACCOUNT);
        String vpc = instanceProperties.get(VPC_ID);
        String subnet = instanceProperties.get(SUBNET);
        // The EC2 Role is the role assumed by the EC2 instances and is the one
        // we need to grant accesses to.
        IRole ec2Role = new Role(this, "Ec2Role", RoleProps.builder()
                .roleName("Sleeper-" + instanceId + "-EMR-EC2-Role")
                .description("The role assumed by the EC2 instances in EMR bulk import clusters")
                .assumedBy(new ServicePrincipal("ec2.amazonaws.com"))
                .build());
        dataBuckets.forEach(bucket -> bucket.grantReadWrite(ec2Role));
        stateStoreStacks.forEach(sss -> {
            sss.grantReadWriteActiveFileMetadata(ec2Role);
            sss.grantReadPartitionMetadata(ec2Role);
        });

        // The role needs to be able to access the user's jars
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
                                        .conditions(Map.of("StringLike", Map.of("iam:PassedToService", "ec2.amazonaws.com*")))
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

        importBucket.grantReadWrite(ec2Role);

        if (ingestBucket != null) {
            ingestBucket.grantRead(ec2Role);
        }
        return ec2Role;
    }

    public IRole getEc2Role() {
        return ec2Role;
    }
}
