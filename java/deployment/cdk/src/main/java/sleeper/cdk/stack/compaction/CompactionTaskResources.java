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
package sleeper.cdk.stack.compaction;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import software.amazon.awscdk.CfnOutput;
import software.amazon.awscdk.CfnOutputProps;
import software.amazon.awscdk.Duration;
import software.amazon.awscdk.Stack;
import software.amazon.awscdk.services.ecs.Cluster;
import software.amazon.awscdk.services.ecs.ContainerImage;
import software.amazon.awscdk.services.ecs.ContainerInsights;
import software.amazon.awscdk.services.ecs.Ec2TaskDefinition;
import software.amazon.awscdk.services.ecs.FargateTaskDefinition;
import software.amazon.awscdk.services.ecs.ITaskDefinition;
import software.amazon.awscdk.services.events.Rule;
import software.amazon.awscdk.services.events.Schedule;
import software.amazon.awscdk.services.events.targets.LambdaFunction;
import software.amazon.awscdk.services.iam.Effect;
import software.amazon.awscdk.services.iam.IRole;
import software.amazon.awscdk.services.iam.ManagedPolicy;
import software.amazon.awscdk.services.iam.PolicyStatement;
import software.amazon.awscdk.services.lambda.IFunction;
import software.amazon.awscdk.services.s3.IBucket;
import software.amazon.awscdk.services.sqs.Queue;

import sleeper.cdk.SleeperInstanceProps;
import sleeper.cdk.artefacts.SleeperEcsImages;
import sleeper.cdk.lambda.SleeperLambdaCode;
import sleeper.cdk.stack.SleeperCoreStacks;
import sleeper.cdk.stack.core.LoggingStack.LogGroupRef;
import sleeper.cdk.util.Utils;
import sleeper.core.deploy.DockerDeployment;
import sleeper.core.deploy.LambdaHandler;
import sleeper.core.deploy.SleeperScheduleRule;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.util.EnvironmentUtils;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.COMPACTION_CLUSTER;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.COMPACTION_TASK_CREATION_CLOUDWATCH_RULE;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.COMPACTION_TASK_CREATION_LAMBDA_FUNCTION;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.REGION;
import static sleeper.core.properties.instance.CommonProperty.TASK_RUNNER_LAMBDA_MEMORY_IN_MB;
import static sleeper.core.properties.instance.CommonProperty.TASK_RUNNER_LAMBDA_TIMEOUT_IN_SECONDS;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_ECS_LAUNCHTYPE;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_TASK_CREATION_PERIOD_IN_MINUTES;

/**
 * Deploys the resources to run compaction tasks that process compaction jobs. Specfically, there are the
 * following resources:
 * <p>
 * - An ECS {@link Cluster} and either a {@link FargateTaskDefinition} or a {@link Ec2TaskDefinition}
 * for tasks that will perform compaction jobs.
 * - A lambda, that is periodically triggered by a CloudWatch rule, to look at the
 * size of the compaction job queue and the number of running tasks and create more tasks if necessary.
 */
public class CompactionTaskResources {
    private static final String COMPACTION_CLUSTER_NAME = "CompactionClusterName";

    private final Stack stack;
    private final SleeperInstanceProps props;
    private final InstanceProperties instanceProperties;

    public CompactionTaskResources(Stack stack,
            SleeperInstanceProps props,
            SleeperEcsImages ecsImages,
            SleeperLambdaCode lambdaCode,
            IBucket jarsBucket,
            CompactionJobResources jobResources,
            SleeperCoreStacks coreStacks) {

        this.stack = stack;
        this.props = props;
        this.instanceProperties = props.getInstanceProperties();

        Cluster cluster = ecsClusterForCompactionTasks(coreStacks, jarsBucket, ecsImages, lambdaCode, jobResources);
        IFunction taskCreator = lambdaToCreateCompactionTasks(coreStacks, lambdaCode, jobResources.getCompactionJobsQueue());
        coreStacks.addAutoStopEcsClusterTasksAfterTaskCreatorIsDeleted(stack, cluster, taskCreator);

        // Allow running compaction tasks
        coreStacks.getInvokeCompactionPolicyForGrants().addStatements(runTasksPolicyStatement());
    }

    private Cluster ecsClusterForCompactionTasks(
            SleeperCoreStacks coreStacks, IBucket jarsBucket, SleeperEcsImages ecsImages, SleeperLambdaCode lambdaCode, CompactionJobResources jobResources) {
        String clusterName = String.join("-", "sleeper",
                Utils.cleanInstanceId(instanceProperties), "compaction-cluster");
        Cluster cluster = Cluster.Builder
                .create(stack, "CompactionCluster")
                .clusterName(clusterName)
                .containerInsightsV2(ContainerInsights.ENHANCED)
                .vpc(coreStacks.getVpc())
                .build();
        instanceProperties.set(COMPACTION_CLUSTER, cluster.getClusterName());

        ContainerImage containerImage = ecsImages.containerImage(DockerDeployment.COMPACTION);

        Map<String, String> environmentVariables = EnvironmentUtils.createDefaultEnvironment(instanceProperties);
        environmentVariables.put(Utils.AWS_REGION, instanceProperties.get(REGION));

        String launchType = instanceProperties.get(COMPACTION_ECS_LAUNCHTYPE);
        ITaskDefinition taskDefinition;
        if ("FARGATE".equalsIgnoreCase(launchType)) {
            taskDefinition = new CompactionOnFargateResources(instanceProperties, stack, coreStacks)
                    .createTaskDefinition(containerImage, environmentVariables);
        } else {
            taskDefinition = new CompactionOnEc2Resources(instanceProperties, stack, coreStacks)
                    .createTaskDefinition(cluster, coreStacks.getVpc(), lambdaCode, containerImage, environmentVariables);
        }
        coreStacks.grantRunCompactionJobs(taskDefinition.getTaskRole());
        jarsBucket.grantRead(taskDefinition.getTaskRole());

        taskDefinition.getTaskRole().addToPrincipalPolicy(PolicyStatement.Builder
                .create()
                .resources(Collections.singletonList("*"))
                .actions(List.of("ecs:DescribeContainerInstances"))
                .build());

        jobResources.getCompactionJobsQueue().grantConsumeMessages(taskDefinition.getTaskRole());
        jobResources.getCommitBatcherQueue().grantSendMessages(taskDefinition.getTaskRole());

        CfnOutputProps compactionClusterProps = new CfnOutputProps.Builder()
                .value(cluster.getClusterName())
                .build();
        new CfnOutput(stack, COMPACTION_CLUSTER_NAME, compactionClusterProps);

        return cluster;
    }

    @SuppressFBWarnings("NP_NULL_ON_SOME_PATH_FROM_RETURN_VALUE")
    private IFunction lambdaToCreateCompactionTasks(
            SleeperCoreStacks coreStacks, SleeperLambdaCode lambdaCode, Queue compactionJobsQueue) {
        String functionName = String.join("-", "sleeper",
                Utils.cleanInstanceId(instanceProperties), "compaction-tasks-creator");

        IFunction handler = lambdaCode.buildFunction(LambdaHandler.COMPACTION_TASK_CREATOR, "CompactionTasksCreator", builder -> builder
                .functionName(functionName)
                .description("If there are compaction jobs on queue create tasks to run them")
                .memorySize(instanceProperties.getInt(TASK_RUNNER_LAMBDA_MEMORY_IN_MB))
                .timeout(Duration.seconds(instanceProperties.getInt(TASK_RUNNER_LAMBDA_TIMEOUT_IN_SECONDS)))
                .environment(EnvironmentUtils.createDefaultEnvironment(instanceProperties))
                .reservedConcurrentExecutions(1)
                .logGroup(coreStacks.getLogGroup(LogGroupRef.COMPACTION_TASKS_CREATOR)));

        // Grant this function permission to read from the S3 bucket
        coreStacks.grantReadInstanceConfig(handler);

        // Grant this function permission to query the queue for number of messages
        compactionJobsQueue.grant(handler, "sqs:GetQueueAttributes");
        compactionJobsQueue.grantSendMessages(handler);
        compactionJobsQueue.grantSendMessages(coreStacks.getInvokeCompactionPolicyForGrants());
        Utils.grantInvokeOnPolicy(handler, coreStacks.getInvokeCompactionPolicyForGrants());
        coreStacks.grantInvokeScheduled(handler);

        // Grant this function permission to query ECS for the number of tasks, etc
        IRole role = Objects.requireNonNull(handler.getRole());
        role.addToPrincipalPolicy(runTasksPolicyStatement());
        role.addManagedPolicy(ManagedPolicy.fromAwsManagedPolicyName("service-role/AmazonECSTaskExecutionRolePolicy"));

        // Cloudwatch rule to trigger this lambda
        Rule rule = Rule.Builder
                .create(stack, "CompactionTasksCreationPeriodicTrigger")
                .ruleName(SleeperScheduleRule.COMPACTION_TASK_CREATION.buildRuleName(instanceProperties))
                .description(SleeperScheduleRule.COMPACTION_TASK_CREATION.getDescription())
                .enabled(!props.isDeployPaused())
                .schedule(Schedule.rate(Duration.minutes(instanceProperties.getInt(COMPACTION_TASK_CREATION_PERIOD_IN_MINUTES))))
                .targets(Collections.singletonList(new LambdaFunction(handler)))
                .build();
        instanceProperties.set(COMPACTION_TASK_CREATION_LAMBDA_FUNCTION, handler.getFunctionName());
        instanceProperties.set(COMPACTION_TASK_CREATION_CLOUDWATCH_RULE, rule.getRuleName());

        return handler;
    }

    private static PolicyStatement runTasksPolicyStatement() {
        return PolicyStatement.Builder.create()
                .effect(Effect.ALLOW)
                .actions(List.of("ecs:DescribeClusters", "ecs:RunTask", "iam:PassRole",
                        "ecs:DescribeContainerInstances", "ecs:DescribeTasks", "ecs:ListContainerInstances",
                        "autoscaling:SetDesiredCapacity", "autoscaling:DescribeAutoScalingGroups", "ec2:DescribeInstanceTypes"))
                .resources(List.of("*"))
                .build();
    }
}
