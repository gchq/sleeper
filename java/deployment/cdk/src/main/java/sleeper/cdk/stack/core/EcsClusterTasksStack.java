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
package sleeper.cdk.stack.core;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import software.amazon.awscdk.CustomResource;
import software.amazon.awscdk.Duration;
import software.amazon.awscdk.NestedStack;
import software.amazon.awscdk.customresources.Provider;
import software.amazon.awscdk.services.ec2.SecurityGroup;
import software.amazon.awscdk.services.ec2.Vpc;
import software.amazon.awscdk.services.ec2.VpcLookupOptions;
import software.amazon.awscdk.services.ecs.ICluster;
import software.amazon.awscdk.services.iam.PolicyStatement;
import software.amazon.awscdk.services.lambda.IFunction;
import software.amazon.awscdk.services.logs.ILogGroup;
import software.constructs.Construct;

import sleeper.cdk.artefacts.SleeperArtefacts;
import sleeper.cdk.lambda.SleeperLambdaCode;
import sleeper.cdk.stack.core.LoggingStack.LogGroupRef;
import sleeper.cdk.util.Utils;
import sleeper.core.deploy.LambdaHandler;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.util.EnvironmentUtils;

import java.util.List;
import java.util.Map;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.ECS_SECURITY_GROUP;
import static sleeper.core.properties.instance.CommonProperty.VPC_ID;

/**
 * Stops ECS Cluster tasks for the CloudFormation stack.
 */
public class EcsClusterTasksStack extends NestedStack {

    private IFunction lambda;
    private Provider provider;
    private SecurityGroup ecsSecurityGroup;

    public EcsClusterTasksStack(Construct scope, String id, InstanceProperties instanceProperties, SleeperArtefacts artefacts, LoggingStack loggingStack) {
        super(scope, id);
        createEcsSecurityGroup(this, instanceProperties);
        createAutoStopLambda(instanceProperties, artefacts, loggingStack.getLogGroup(LogGroupRef.AUTO_STOP_ECS_CLUSTER_TASKS),
                loggingStack.getLogGroup(LogGroupRef.AUTO_STOP_ECS_CLUSTER_TASKS_PROVIDER));
    }

    public EcsClusterTasksStack(Construct scope, String id, InstanceProperties instanceProperties, SleeperArtefacts artefacts) {
        super(scope, id);
        createEcsSecurityGroup(this, instanceProperties);
        ILogGroup logGroup = LoggingStack.createLogGroup(this, LogGroupRef.AUTO_STOP_ECS_CLUSTER_TASKS, instanceProperties);
        ILogGroup providerLogGroup = LoggingStack.createLogGroup(this, LogGroupRef.AUTO_STOP_ECS_CLUSTER_TASKS_PROVIDER, instanceProperties);
        createAutoStopLambda(instanceProperties, artefacts, logGroup, providerLogGroup);
    }

    @SuppressFBWarnings("NP_NULL_ON_SOME_PATH_FROM_RETURN_VALUE") // getRole is incorrectly labelled as nullable
    private void createAutoStopLambda(InstanceProperties instanceProperties, SleeperArtefacts artefacts, ILogGroup logGroup, ILogGroup providerLogGroup) {

        SleeperLambdaCode lambdaCode = artefacts.lambdaCodeAtScope(this);

        String functionName = String.join("-", "sleeper",
                Utils.cleanInstanceId(instanceProperties), "auto-stop-ecs-cluster-tasks");

        lambda = lambdaCode.buildFunction(this, LambdaHandler.AUTO_STOP_ECS_CLUSTER_TASKS, "Lambda", builder -> builder
                .functionName(functionName)
                .memorySize(2048)
                .environment(EnvironmentUtils.createDefaultEnvironmentNoConfigBucket(instanceProperties))
                .description("Lambda for auto-stopping ECS tasks")
                .logGroup(logGroup)
                .timeout(Duration.minutes(15)));

        lambda.getRole().addToPrincipalPolicy(PolicyStatement.Builder
                .create()
                .resources(List.of("*"))
                .actions(List.of("ecs:ListTasks", "ecs:StopTask"))
                .build());

        provider = Provider.Builder.create(this, "Provider")
                .onEventHandler(lambda)
                .logGroup(providerLogGroup)
                .build();

        Utils.addTags(this, instanceProperties);

    }

    /**
     * Adds a custom resource to stop tasks in an ECS cluster.
     *
     * @param scope       the stack to add the custom resource to
     * @param cluster     the ECS cluster
     * @param taskCreator the lambda function that starts tasks in the cluster
     */
    public void addAutoStopEcsClusterTasksAfterTaskCreatorIsDeleted(Construct scope, ICluster cluster, IFunction taskCreator) {
        CustomResource customResource = addAutoStopEcsClusterTasks(scope, cluster);

        // This dependency means that during teardown the task creator lambda will be deleted before ECS tasks are stopped.
        // This is important otherwise more tasks may be created as they are being stopped.
        taskCreator.getNode().addDependency(customResource);
    }

    /**
     * Adds a custom resource to stop tasks in an ECS cluster.
     *
     * @param  scope   the stack to add the custom resource to
     * @param  cluster the ECS cluster
     * @return         the custom resource that will stop tasks when it is deleted
     */
    public CustomResource addAutoStopEcsClusterTasks(Construct scope, ICluster cluster) {

        String id = cluster.getNode().getId() + "-AutoStop";

        CustomResource customResource = CustomResource.Builder.create(scope, id)
                .resourceType("Custom::AutoStopEcsClusterTasks")
                .properties(Map.of("cluster", cluster.getClusterName()))
                .serviceToken(provider.getServiceToken())
                .build();

        // This dependency means that ECS tasks will be stopped before the cluster is deleted.
        customResource.getNode().addDependency(cluster);

        return customResource;
    }

    public SecurityGroup getEcsSecurityGroup() {
        return ecsSecurityGroup;
    }

    private void createEcsSecurityGroup(Construct scope, InstanceProperties instanceProperties) {
        ecsSecurityGroup = SecurityGroup.Builder.create(scope, "ECS")
                .vpc(Vpc.fromLookup(scope, "vpc", VpcLookupOptions.builder().vpcId(instanceProperties.get(VPC_ID)).build()))
                .description("Security group for ECS tasks and services that do not need to serve incoming requests.")
                .allowAllOutbound(true)
                .build();
        instanceProperties.set(ECS_SECURITY_GROUP, ecsSecurityGroup.getSecurityGroupId());
    }
}
