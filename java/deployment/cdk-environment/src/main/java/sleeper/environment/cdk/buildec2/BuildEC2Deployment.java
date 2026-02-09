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
package sleeper.environment.cdk.buildec2;

import software.amazon.awscdk.CfnOutput;
import software.amazon.awscdk.services.ec2.IInstance;
import software.amazon.awscdk.services.ec2.IVpc;
import software.amazon.awscdk.services.ec2.Instance;
import software.amazon.awscdk.services.ec2.InstanceClass;
import software.amazon.awscdk.services.ec2.InstanceSize;
import software.amazon.awscdk.services.ec2.InstanceType;
import software.amazon.awscdk.services.ec2.SecurityGroup;
import software.amazon.awscdk.services.ec2.SubnetSelection;
import software.amazon.awscdk.services.ec2.SubnetType;
import software.amazon.awscdk.services.ec2.UserData;
import software.amazon.awscdk.services.iam.AccountRootPrincipal;
import software.amazon.awscdk.services.iam.Effect;
import software.amazon.awscdk.services.iam.ManagedPolicy;
import software.amazon.awscdk.services.iam.PolicyStatement;
import software.amazon.awscdk.services.iam.Role;
import software.constructs.Construct;

import sleeper.environment.cdk.config.AppContext;
import sleeper.environment.cdk.nightlytests.NightlyTestDeployment;

import java.util.Collections;
import java.util.List;

public class BuildEC2Deployment {

    private final IVpc vpc;
    private final Instance instance;

    public BuildEC2Deployment(Construct scope, IVpc vpc, NightlyTestDeployment nightlyTests) {
        AppContext context = AppContext.of(scope);
        BuildEC2Parameters params = BuildEC2Parameters.builder()
                .context(context)
                .testBucket(nightlyTests.getTestBucketName())
                .inheritVpc(vpc)
                .build();
        this.vpc = vpc;
        BuildEC2Image image = params.image();

        instance = Instance.Builder.create(scope, "BuildEC2")
                .vpc(vpc)
                .securityGroup(createSecurityGroup(scope))
                .machineImage(image.machineImage())
                .instanceType(InstanceType.of(InstanceClass.T3, InstanceSize.XLARGE2))
                .vpcSubnets(SubnetSelection.builder().subnetType(SubnetType.PRIVATE_WITH_EGRESS).build())
                .userData(UserData.custom(LoadUserDataUtil.userData(params)))
                .userDataCausesReplacement(true)
                .blockDevices(Collections.singletonList(image.rootBlockDevice()))
                .build();
        instance.getRole().addManagedPolicy(ManagedPolicy.fromAwsManagedPolicyName("AdministratorAccess"));

        Role restrictedRole = createRestrictedRole(scope);

        CfnOutput.Builder.create(scope, "BuildEC2LoginUser")
                .value(image.loginUser())
                .description("User to SSH into on build EC2 instance")
                .build();
        CfnOutput.Builder.create(scope, "BuildEC2Id")
                .value(instance.getInstanceId())
                .description("ID of the build EC2 instance")
                .build();
        CfnOutput.Builder.create(scope, "BuildEC2RestrictedRoleArn")
                .value(restrictedRole.getRoleArn())
                .description("Role with restricted access to deploy Sleeper instances. " +
                        "This can be assumed to test deploying a Sleeper instance with fewer permissions. " +
                        "We can aim to reduce the permissions for this role in the future.")
                .build();
    }

    private Role createRestrictedRole(Construct scope) {

        Role role = Role.Builder.create(scope, "RestrictedRole")
                .assumedBy(new AccountRootPrincipal())
                .build();
        ManagedPolicy policy = new ManagedPolicy(scope, "BuildEC2Policy");

        // Allow running CDK by assuming roles created by cdk bootstrap
        // Allow interacting with Sleeper by assuming admin role
        policy.addStatements(PolicyStatement.Builder.create()
                .effect(Effect.ALLOW)
                .actions(List.of("sts:AssumeRole"))
                .resources(List.of(
                        "arn:aws:iam::*:role/cdk-*",
                        "arn:aws:iam::*:role/sleeper-admin-*"))
                .build());

        // Allow creating jars bucket & Docker repositories, working with CloudFormation stacks
        policy.addStatements(PolicyStatement.Builder.create()
                .effect(Effect.ALLOW)
                .actions(List.of("s3:*", "ecr:*", "cloudformation:*"))
                .resources(List.of("*"))
                .build());

        // Allow running ECS tasks
        policy.addStatements(PolicyStatement.Builder.create()
                .effect(Effect.ALLOW)
                .actions(List.of("ecs:DescribeClusters", "ecs:RunTask", "iam:PassRole",
                        "ecs:DescribeContainerInstances", "ecs:DescribeTasks", "ecs:ListContainerInstances",
                        "autoscaling:SetDesiredCapacity", "autoscaling:DescribeAutoScalingGroups"))
                .resources(List.of("*"))
                .build());
        role.addManagedPolicy(ManagedPolicy.fromAwsManagedPolicyName("service-role/AmazonECSTaskExecutionRolePolicy"));

        role.addManagedPolicy(policy);
        return role;
    }

    private SecurityGroup createSecurityGroup(Construct scope) {
        return SecurityGroup.Builder.create(scope, "AllowOutbound")
                .vpc(vpc)
                .description("Allow outbound traffic")
                .allowAllOutbound(true)
                .build();
    }

    public IInstance getInstance() {
        return instance;
    }

}
