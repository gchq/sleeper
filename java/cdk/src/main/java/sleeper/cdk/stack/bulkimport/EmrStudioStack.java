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

import software.amazon.awscdk.NestedStack;
import software.amazon.awscdk.RemovalPolicy;
import software.amazon.awscdk.services.ec2.ISecurityGroup;
import software.amazon.awscdk.services.ec2.IVpc;
import software.amazon.awscdk.services.ec2.Peer;
import software.amazon.awscdk.services.ec2.Port;
import software.amazon.awscdk.services.ec2.SecurityGroup;
import software.amazon.awscdk.services.ec2.Vpc;
import software.amazon.awscdk.services.ec2.VpcLookupOptions;
import software.amazon.awscdk.services.emr.CfnStudio;
import software.amazon.awscdk.services.emr.CfnStudioProps;
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
import software.amazon.awscdk.services.s3.BlockPublicAccess;
import software.amazon.awscdk.services.s3.Bucket;
import software.amazon.awscdk.services.s3.BucketEncryption;
import software.amazon.awscdk.services.s3.IBucket;
import software.constructs.Construct;

import sleeper.cdk.Utils;
import sleeper.configuration.properties.instance.InstanceProperties;

import java.util.List;
import java.util.Locale;

import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.BULK_IMPORT_EMR_SERVERLESS_STUDIO_BUCKET;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.BULK_IMPORT_EMR_SERVERLESS_STUDIO_URL;
import static sleeper.configuration.properties.instance.CommonProperty.ID;
import static sleeper.configuration.properties.instance.CommonProperty.SUBNETS;
import static sleeper.configuration.properties.instance.CommonProperty.VPC_ID;

/**
 * An {@link EmrStudioStack} creates an SQS queue that bulk import jobs can be sent to. A message
 * arriving on this queue triggers a lambda. That lambda creates an EMR cluster that executes the
 * bulk import job and then terminates.
 */
public class EmrStudioStack extends NestedStack {
    private ISecurityGroup defaultEngineSecurityGroup;
    private ISecurityGroup workspaceSecurityGroup;
    private IBucket studioBucket;

    public EmrStudioStack(Construct scope, String id, InstanceProperties instanceProperties) {
        super(scope, id);
        String instanceId = instanceProperties.get(ID);

        studioBucket = Bucket.Builder.create(this, "EmrStudioBucket")
            .bucketName(String.join("-", "sleeper", instanceId, "emr-studio")
                    .toLowerCase(Locale.ROOT))
            .blockPublicAccess(BlockPublicAccess.BLOCK_ALL).versioned(false)
            .autoDeleteObjects(true).removalPolicy(RemovalPolicy.DESTROY)
            .encryption(BucketEncryption.S3_MANAGED).build();
        instanceProperties.set(BULK_IMPORT_EMR_SERVERLESS_STUDIO_BUCKET, studioBucket.getBucketArn());

        IVpc vpc = Vpc.fromLookup(this, "VPC",
            VpcLookupOptions.builder().vpcId(instanceProperties.get(VPC_ID)).build());
        createDefaultEngineSecurityGroup(vpc, instanceId);
        createWorkspaceSecurityGroup(vpc, instanceId);
        createEmrStudio(instanceProperties, vpc);
        Utils.addStackTagIfSet(this, instanceProperties);

    }

    public void createEmrStudio(InstanceProperties instanceProperties, IVpc vpc) {
        String instanceId = instanceProperties.get(ID);

        CfnStudioProps props = CfnStudioProps.builder()
                .name(String.join("-", "sleeper", instanceId, "emr", "studio"))
                .description("EMR Studio to be used to access EMR Serverless").authMode("IAM")
                .vpcId(vpc.getVpcId())
                .subnetIds(instanceProperties.getList(SUBNETS))
                .engineSecurityGroupId(defaultEngineSecurityGroup.getSecurityGroupId())
                .serviceRole(createEmrStudioServiceRole(instanceId))
                .workspaceSecurityGroupId(workspaceSecurityGroup.getSecurityGroupId())
                .defaultS3Location(studioBucket.s3UrlForObject())
                .build();

        CfnStudio studio = new CfnStudio(this, getArtifactId(), props);
        instanceProperties.set(BULK_IMPORT_EMR_SERVERLESS_STUDIO_URL, studio.getAttrUrl());

    }

    private String createEmrStudioServiceRole(String instanceId) {
        IRole studioRole = new Role(this, "EmrServerlessStudioServiceRole",
            RoleProps.builder()
                .roleName(String.join("-", "sleeper", instanceId, "EMR-Serverless-Studio-Role"))
                .description("The role assumed by the Bulk import EMR Serverless Application")
                .managedPolicies(
                    List.of(ManagedPolicy.fromAwsManagedPolicyName("AmazonS3FullAccess"),
                        new ManagedPolicy(this, "CustomEMRServerlessServicePolicy",
                        ManagedPolicyProps.builder()
                            .managedPolicyName(String.join("-", "sleeper", instanceId, "Emr-Serverless-Studio-Service-Actions"))
                            .description("A policy to be used by EMR Studio to access EMR Serverless")
                            .document(PolicyDocument.Builder.create().statements(List.of(
                                new PolicyStatement(PolicyStatementProps.builder()
                                        .sid("EmrServerlessStudioServiceActions")
                                        .effect(Effect.ALLOW)
                                        .actions(List.of(
                                            "elasticmapreduce:ListInstances",
                                            "elasticmapreduce:DescribeCluster",
                                            "elasticmapreduce:ListSteps"))
                                        .resources(List.of("arn:aws:s3:::*.elasticmapreduce",
                                                "arn:aws:s3:::*.elasticmapreduce/*"))
                                        .build())))
                                .build())
                            .build())))
                .assumedBy(new ServicePrincipal("elasticmapreduce.amazonaws.com")).build());

        return studioRole.getRoleArn();
    }

    private void createDefaultEngineSecurityGroup(IVpc vpc, String instanceId) {
        defaultEngineSecurityGroup = SecurityGroup.Builder
            .create(this, "EmrServerlessStudioDefaultEngineSecurityGroup")
            .securityGroupName(String.join("-", "sleeper", instanceId, "emr-serverless-studio-default-engine-security-group"))
            .description("Default Engine Security Group used by EMR Studio")
            .vpc(vpc)
            .build();

        defaultEngineSecurityGroup.addIngressRule(Peer.anyIpv4(), Port.tcp(18888));
    }

    private void createWorkspaceSecurityGroup(IVpc vpc, String instanceId)  {
        workspaceSecurityGroup = SecurityGroup.Builder
            .create(this, "EmrServerlessStudioWorkspaceSecurityGroup")
            .securityGroupName(String.join("-", "sleeper", instanceId, "emr-serverless-studio-workspace-security-group"))
            .description("Workspace Security Group used by EMR Studio")
            .vpc(vpc)
            .allowAllOutbound(false)
            .build();
        workspaceSecurityGroup.addEgressRule(Peer.anyIpv4(), Port.tcp(18888));
        workspaceSecurityGroup.addEgressRule(defaultEngineSecurityGroup, Port.tcp(18888));
    }
}
