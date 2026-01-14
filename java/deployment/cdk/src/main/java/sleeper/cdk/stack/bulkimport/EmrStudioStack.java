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

import software.amazon.awscdk.NestedStack;
import software.amazon.awscdk.services.ec2.ISecurityGroup;
import software.amazon.awscdk.services.ec2.IVpc;
import software.amazon.awscdk.services.ec2.Port;
import software.amazon.awscdk.services.ec2.SecurityGroup;
import software.amazon.awscdk.services.emr.CfnStudio;
import software.amazon.awscdk.services.emr.CfnStudioProps;
import software.amazon.awscdk.services.iam.Effect;
import software.amazon.awscdk.services.iam.IRole;
import software.amazon.awscdk.services.iam.ManagedPolicy;
import software.amazon.awscdk.services.iam.PolicyDocument;
import software.amazon.awscdk.services.iam.PolicyStatement;
import software.amazon.awscdk.services.iam.Role;
import software.amazon.awscdk.services.iam.ServicePrincipal;
import software.amazon.awscdk.services.s3.Bucket;
import software.amazon.awscdk.services.s3.IBucket;
import software.constructs.Construct;

import sleeper.cdk.stack.SleeperCoreStacks;
import sleeper.cdk.util.Utils;
import sleeper.core.properties.instance.InstanceProperties;

import java.util.List;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.BULK_IMPORT_BUCKET;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.BULK_IMPORT_EMR_SERVERLESS_STUDIO_URL;
import static sleeper.core.properties.instance.CommonProperty.ID;

/**
 * Deploys a studio configuration to view EMR Serverless jobs.
 */
public class EmrStudioStack extends NestedStack {
    private ISecurityGroup defaultEngineSecurityGroup;
    private ISecurityGroup workspaceSecurityGroup;
    private IBucket bucket;

    public EmrStudioStack(Construct scope, String id, InstanceProperties instanceProperties, SleeperCoreStacks coreStacks) {
        super(scope, id);
        String instanceId = instanceProperties.get(ID);
        bucket = Bucket.fromBucketName(this, "BulkImportBucket", instanceProperties.get(BULK_IMPORT_BUCKET));

        createDefaultEngineSecurityGroup(coreStacks.getVpc(), instanceId);
        createWorkspaceSecurityGroup(coreStacks.getVpc(), instanceId);
        createEmrStudio(instanceProperties, coreStacks);
        Utils.addTags(this, instanceProperties);

    }

    public void createEmrStudio(InstanceProperties instanceProperties, SleeperCoreStacks coreStacks) {
        String instanceId = instanceProperties.get(ID);

        CfnStudioProps props = CfnStudioProps.builder()
                .name(String.join("-", "sleeper", instanceId, "emr", "studio"))
                .description("EMR Studio to be used to access EMR Serverless").authMode("IAM")
                .vpcId(coreStacks.networking().vpcId())
                .subnetIds(coreStacks.networking().subnetIds())
                .engineSecurityGroupId(defaultEngineSecurityGroup.getSecurityGroupId())
                .serviceRole(createEmrStudioServiceRole(instanceId))
                .workspaceSecurityGroupId(workspaceSecurityGroup.getSecurityGroupId())
                .defaultS3Location(bucket.s3UrlForObject())
                .build();

        CfnStudio studio = new CfnStudio(this, getArtifactId(), props);
        instanceProperties.set(BULK_IMPORT_EMR_SERVERLESS_STUDIO_URL, studio.getAttrUrl());
    }

    private String createEmrStudioServiceRole(String instanceId) {
        IRole studioRole = Role.Builder.create(this, "EmrServerlessStudioServiceRole")
                .roleName(String.join("-", "sleeper", instanceId, "EMR-Serverless-Studio-Role"))
                .description("The role assumed by the Bulk import EMR Serverless Application")
                .managedPolicies(List.of(
                        ManagedPolicy.fromAwsManagedPolicyName("AmazonS3FullAccess"),
                        ManagedPolicy.Builder.create(this, "CustomEMRServerlessServicePolicy")
                                .managedPolicyName(String.join("-", "sleeper", instanceId, "Emr-Serverless-Studio-Service-Actions"))
                                .description("A policy to be used by EMR Studio to access EMR Serverless")
                                .document(PolicyDocument.Builder.create()
                                        .statements(List.of(PolicyStatement.Builder.create()
                                                .sid("EmrServerlessStudioServiceActions")
                                                .effect(Effect.ALLOW)
                                                .actions(List.of(
                                                        "elasticmapreduce:ListInstances",
                                                        "elasticmapreduce:DescribeCluster",
                                                        "elasticmapreduce:ListSteps"))
                                                .resources(List.of("arn:aws:s3:::*.elasticmapreduce",
                                                        "arn:aws:s3:::*.elasticmapreduce/*"))
                                                .build()))
                                        .build())
                                .build()))
                .assumedBy(new ServicePrincipal("elasticmapreduce.amazonaws.com"))
                .build();

        return studioRole.getRoleArn();
    }

    private void createDefaultEngineSecurityGroup(IVpc vpc, String instanceId) {
        defaultEngineSecurityGroup = SecurityGroup.Builder
                .create(this, "EmrServerlessStudioDefaultEngineSecurityGroup")
                .securityGroupName(String.join("-", "sleeper", instanceId, "emr-serverless-studio-default-engine-security-group"))
                .description("Default Engine Security Group used by EMR Studio")
                .vpc(vpc)
                .allowAllOutbound(false)
                .build();
    }

    private void createWorkspaceSecurityGroup(IVpc vpc, String instanceId) {
        workspaceSecurityGroup = SecurityGroup.Builder
                .create(this, "EmrServerlessStudioWorkspaceSecurityGroup")
                .securityGroupName(String.join("-", "sleeper", instanceId, "emr-serverless-studio-workspace-security-group"))
                .description("Workspace Security Group used by EMR Studio")
                .vpc(vpc)
                .allowAllOutbound(false)
                .build();
        workspaceSecurityGroup.addEgressRule(defaultEngineSecurityGroup, Port.tcp(18888));
    }
}
