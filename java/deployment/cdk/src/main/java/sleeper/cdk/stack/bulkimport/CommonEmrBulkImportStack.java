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

import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;
import software.amazon.awscdk.CfnJson;
import software.amazon.awscdk.Duration;
import software.amazon.awscdk.NestedStack;
import software.amazon.awscdk.RemovalPolicy;
import software.amazon.awscdk.Stack;
import software.amazon.awscdk.services.emr.CfnSecurityConfiguration;
import software.amazon.awscdk.services.iam.CfnInstanceProfile;
import software.amazon.awscdk.services.iam.CfnInstanceProfileProps;
import software.amazon.awscdk.services.iam.Effect;
import software.amazon.awscdk.services.iam.IRole;
import software.amazon.awscdk.services.iam.ManagedPolicy;
import software.amazon.awscdk.services.iam.PolicyDocument;
import software.amazon.awscdk.services.iam.PolicyStatement;
import software.amazon.awscdk.services.iam.PolicyStatementProps;
import software.amazon.awscdk.services.iam.Role;
import software.amazon.awscdk.services.iam.RoleProps;
import software.amazon.awscdk.services.iam.ServicePrincipal;
import software.amazon.awscdk.services.kms.IKey;
import software.amazon.awscdk.services.kms.Key;
import software.amazon.awscdk.services.s3.Bucket;
import software.amazon.awscdk.services.s3.IBucket;
import software.constructs.Construct;

import sleeper.cdk.stack.SleeperCoreStacks;
import sleeper.cdk.util.Utils;
import sleeper.core.properties.instance.CdkDefinedInstanceProperty;
import sleeper.core.properties.instance.InstanceProperties;

import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.BULK_IMPORT_EMR_CLUSTER_ROLE_NAME;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.BULK_IMPORT_EMR_EC2_ROLE_NAME;
import static sleeper.core.properties.instance.CommonProperty.JARS_BUCKET;
import static sleeper.core.properties.instance.EMRProperty.BULK_IMPORT_EMR_EBS_ENCRYPTION_KEY_ARN;

public class CommonEmrBulkImportStack extends NestedStack {

    private static final String[] KMS_GRANTS = new String[]{
        "kms:Encrypt", "kms:Decrypt", "kms:ReEncrypt*", "kms:GenerateDataKey*", "kms:DescribeKey",
        "kms:CreateGrant", "kms:ListGrants", "kms:RevokeGrant"};

    private final IRole ec2Role;
    private final IRole emrRole;
    private final CfnSecurityConfiguration securityConfiguration;

    public CommonEmrBulkImportStack(
            Construct scope, String id, InstanceProperties instanceProperties,
            SleeperCoreStacks coreStacks, BulkImportBucketStack importBucketStack) {
        super(scope, id);
        IKey ebsKey = createEbsEncryptionKey(this, instanceProperties);
        ec2Role = createEc2Role(this, instanceProperties,
                importBucketStack.getImportBucket(), coreStacks, ebsKey);
        emrRole = createEmrRole(this, instanceProperties, coreStacks, ec2Role, ebsKey);
        securityConfiguration = createSecurityConfiguration(this, instanceProperties, ebsKey);
    }

    private static IRole createEc2Role(
            Stack scope, InstanceProperties instanceProperties, IBucket importBucket,
            SleeperCoreStacks coreStacks, IKey ebsKey) {

        // The EC2 Role is the role assumed by the EC2 instances and is the one
        // we need to grant accesses to.
        IRole role = new Role(scope, "Ec2Role", RoleProps.builder()
                .roleName(String.join("-", "sleeper", Utils.cleanInstanceId(instanceProperties), "bulk-import-emr-ec2"))
                .description("The role assumed by the EC2 instances in EMR bulk import clusters")
                .assumedBy(new ServicePrincipal("ec2.amazonaws.com"))
                .build());
        coreStacks.grantIngest(role);
        coreStacks.grantReadWritePartitions(role); // The partition tree can be extended if there aren't enough partitions to do a bulk import
        ebsKey.grant(role, KMS_GRANTS);

        // The role needs to be able to access the user's jars
        IBucket jarsBucket = Bucket.fromBucketName(scope, "JarsBucket", instanceProperties.get(JARS_BUCKET));
        jarsBucket.grantRead(role);

        // Required to enable debugging
        role.addToPrincipalPolicy(PolicyStatement.Builder.create()
                .actions(List.of("sqs:GetQueueUrl", "sqs:SendMessage"))
                .effect(Effect.ALLOW)
                .resources(List.of("arn:" + scope.getPartition() + ":sqs:" + scope.getRegion() + ":" + scope.getAccount()
                        + ":AWS-ElasticMapReduce-*"))
                .build());

        role.addToPrincipalPolicy(PolicyStatement.Builder.create()
                .actions(List.of("ec2:Describe*",
                        "elasticmapreduce:Describe*",
                        "elasticmapreduce:ListBootstrapActions",
                        "elasticmapreduce:ListClusters",
                        "elasticmapreduce:ListInstanceGroups",
                        "elasticmapreduce:ListInstances",
                        "elasticmapreduce:ListSteps",
                        "cloudwatch:*",
                        "s3:GetObject*"))
                .effect(Effect.ALLOW)
                .resources(List.of("*"))
                .build());

        // Allow SSM access
        role.addManagedPolicy(ManagedPolicy.fromAwsManagedPolicyName("AmazonSSMManagedInstanceCore"));

        instanceProperties.set(BULK_IMPORT_EMR_EC2_ROLE_NAME, role.getRoleName());

        new CfnInstanceProfile(scope, "EC2InstanceProfile", CfnInstanceProfileProps.builder()
                .instanceProfileName(role.getRoleName())
                .roles(List.of(role.getRoleName()))
                .build());

        importBucket.grantReadWrite(role);
        return role;
    }

    private static IRole createEmrRole(Construct scope, InstanceProperties instanceProperties, SleeperCoreStacks coreStacks, IRole ec2Role, IKey ebsKey) {
        String instanceId = Utils.cleanInstanceId(instanceProperties);

        // Use the policy which is derived from the AmazonEMRServicePolicy_v2 policy.
        PolicyDocument policyDoc = PolicyDocument.fromJson(new Gson().fromJson(new JsonReader(
                new InputStreamReader(CommonEmrBulkImportStack.class.getResourceAsStream("/iam/SleeperEMRPolicy.json"), StandardCharsets.UTF_8)),
                Map.class));

        ManagedPolicy customEmrManagedPolicy = ManagedPolicy.Builder.create(scope, "CustomEMRManagedPolicy")
                .description("Custom policy for EMR bulk import to operate in VPC")
                .managedPolicyName(String.join("-", "sleeper", instanceId, "bulk-import-emr-in-vpc"))
                .document(PolicyDocument.Builder.create().statements(List.of(
                        new PolicyStatement(PolicyStatementProps.builder()
                                .sid("CreateSecurityGroupInVPC")
                                .actions(List.of("ec2:CreateSecurityGroup"))
                                .effect(Effect.ALLOW)
                                .resources(List.of(coreStacks.networking().vpcArn()))
                                .build()),
                        new PolicyStatement(PolicyStatementProps.builder()
                                .sid("ManageResourcesInSubnet")
                                .actions(List.of(
                                        "ec2:CreateNetworkInterface",
                                        "ec2:RunInstances",
                                        "ec2:CreateFleet",
                                        "ec2:CreateLaunchTemplate",
                                        "ec2:CreateLaunchTemplateVersion"))
                                .effect(Effect.ALLOW)
                                .resources(coreStacks.networking().subnetArns())
                                .build()),
                        new PolicyStatement(PolicyStatementProps.builder()
                                .sid("PassEc2Role")
                                .effect(Effect.ALLOW)
                                .actions(List.of("iam:PassRole"))
                                .resources(List.of(ec2Role.getRoleArn()))
                                .conditions(Map.of("StringLike", Map.of("iam:PassedToService", "ec2.amazonaws.com*")))
                                .build())))
                        .build())
                .build();

        ManagedPolicy emrManagedPolicy = ManagedPolicy.Builder.create(scope, "DefaultEMRServicePolicy")
                .managedPolicyName(String.join("-", "sleeper", instanceId, "bulk-import-emr"))
                .description("Policy required for Sleeper Bulk import EMR cluster, based on the AmazonEMRServicePolicy_v2 policy")
                .document(policyDoc)
                .build();

        Role role = new Role(scope, "EmrRole", RoleProps.builder()
                .roleName(String.join("-", "sleeper", instanceId, "EMR-Role"))
                .description("The role assumed by the Bulk import clusters")
                .managedPolicies(List.of(emrManagedPolicy, customEmrManagedPolicy))
                .assumedBy(new ServicePrincipal("elasticmapreduce.amazonaws.com"))
                .build());
        ebsKey.grant(role, KMS_GRANTS);

        instanceProperties.set(BULK_IMPORT_EMR_CLUSTER_ROLE_NAME, role.getRoleName());
        return role;
    }

    private static CfnSecurityConfiguration createSecurityConfiguration(Construct scope, InstanceProperties instanceProperties, IKey ebsKey) {
        // See https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-create-security-configuration.html
        CfnJson jsonObject = CfnJson.Builder.create(scope, "EMRSecurityConfigurationJSONObject")
                .value("{\n" +
                        "  \"InstanceMetadataServiceConfiguration\": {\n" +
                        "    \"MinimumInstanceMetadataServiceVersion\": 2,\n" +
                        "    \"HttpPutResponseHopLimit\": 1\n" +
                        "  },\n" +
                        "  \"EncryptionConfiguration\": {\n" +
                        "    \"EnableInTransitEncryption\": false,\n" +
                        "    \"EnableAtRestEncryption\": true,\n" +
                        "    \"AtRestEncryptionConfiguration\": {\n" +
                        "      \"LocalDiskEncryptionConfiguration\": {\n" +
                        "        \"EnableEbsEncryption\": true,\n" +
                        "        \"EncryptionKeyProviderType\": \"AwsKms\",\n" +
                        "        \"AwsKmsKey\": \"" + ebsKey.getKeyArn() + "\"\n" +
                        "      }\n" +
                        "    }\n" +
                        "  }\n" +
                        "}")
                .build();
        CfnSecurityConfiguration conf = CfnSecurityConfiguration.Builder.create(scope, "EMRSecurityConfiguration")
                .name(String.join("-", "sleeper",
                        Utils.cleanInstanceId(instanceProperties), "EMRSecurityConfigurationProps"))
                .securityConfiguration(jsonObject)
                .build();
        instanceProperties.set(CdkDefinedInstanceProperty.BULK_IMPORT_EMR_SECURITY_CONF_NAME, conf.getName());
        return conf;
    }

    private static IKey createEbsEncryptionKey(Construct scope, InstanceProperties instanceProperties) {
        String ebsKeyArn = instanceProperties.get(BULK_IMPORT_EMR_EBS_ENCRYPTION_KEY_ARN);
        if (ebsKeyArn == null) {
            return Key.Builder.create(scope, "EbsKey")
                    .description("Key used to encrypt data at rest in the local filesystem in AWS EMR for Sleeper.")
                    .enableKeyRotation(true)
                    .removalPolicy(RemovalPolicy.DESTROY)
                    .pendingWindow(Duration.days(7))
                    .build();
        } else {
            return Key.fromKeyArn(scope, "EbsKey", ebsKeyArn);
        }
    }

    public IRole getEc2Role() {
        return ec2Role;
    }

    public IRole getEmrRole() {
        return emrRole;
    }

    public CfnSecurityConfiguration getSecurityConfiguration() {
        return securityConfiguration;
    }
}
