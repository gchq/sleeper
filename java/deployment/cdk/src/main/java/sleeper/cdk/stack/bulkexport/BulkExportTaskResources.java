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
package sleeper.cdk.stack.bulkexport;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import software.amazon.awscdk.CfnOutput;
import software.amazon.awscdk.CfnOutputProps;
import software.amazon.awscdk.Duration;
import software.amazon.awscdk.Stack;
import software.amazon.awscdk.services.ecr.IRepository;
import software.amazon.awscdk.services.ecr.Repository;
import software.amazon.awscdk.services.ecs.Cluster;
import software.amazon.awscdk.services.ecs.ContainerImage;
import software.amazon.awscdk.services.ecs.ContainerInsights;
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

import static sleeper.core.properties.instance.BulkExportProperty.BULK_EXPORT_TASK_CREATION_PERIOD_IN_MINUTES;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.BULK_EXPORT_CLUSTER;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.BULK_EXPORT_TASK_CREATION_CLOUDWATCH_RULE;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.BULK_EXPORT_TASK_CREATION_LAMBDA_FUNCTION;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.REGION;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.VERSION;
import static sleeper.core.properties.instance.CommonProperty.TASK_RUNNER_LAMBDA_MEMORY_IN_MB;
import static sleeper.core.properties.instance.CommonProperty.TASK_RUNNER_LAMBDA_TIMEOUT_IN_SECONDS;

@SuppressFBWarnings("NP_NULL_ON_SOME_PATH_FROM_RETURN_VALUE")
public class BulkExportTaskResources {
    private static final String BULK_EXPORT_CLUSTER_NAME = "BulkExportClusterName";

    private final Stack stack;
    private final SleeperInstanceProps props;
    private final InstanceProperties instanceProperties;
    private final Queue jobsQueue;

    public BulkExportTaskResources(
            Stack stack, SleeperInstanceProps props, SleeperCoreStacks coreStacks,
            SleeperLambdaCode lambdaCode, IBucket jarsBucket, Queue jobsQueue, IBucket resultsBucket) {
        this.stack = stack;
        this.props = props;
        this.instanceProperties = props.getInstanceProperties();
        this.jobsQueue = jobsQueue;
        Cluster cluster = ecsClusterForBulkExportTasks(coreStacks, jarsBucket, resultsBucket);
        IFunction taskCreator = lambdaToCreateTasks(coreStacks, lambdaCode);
        coreStacks.addAutoStopEcsClusterTasksAfterTaskCreatorIsDeleted(stack, cluster, taskCreator);
    }

    private IFunction lambdaToCreateTasks(SleeperCoreStacks coreStacks, SleeperLambdaCode lambdaCode) {
        String instanceId = Utils.cleanInstanceId(instanceProperties);
        String functionName = String.join("-", "sleeper",
                instanceId, "bulk-export-tasks-creator");

        IFunction handler = lambdaCode.buildFunction(
                stack, LambdaHandler.BULK_EXPORT_TASK_CREATOR, "BulkExportTasksCreator", builder -> builder
                        .functionName(functionName)
                        .description("If there are leaf partition bulk export jobs on queue create tasks to run them")
                        .memorySize(instanceProperties.getInt(TASK_RUNNER_LAMBDA_MEMORY_IN_MB))
                        .timeout(Duration.seconds(instanceProperties.getInt(TASK_RUNNER_LAMBDA_TIMEOUT_IN_SECONDS)))
                        .environment(EnvironmentUtils.createDefaultEnvironment(instanceProperties))
                        .reservedConcurrentExecutions(1)
                        .logGroup(coreStacks.getLogGroup(LogGroupRef.BULK_EXPORT_TASKS_CREATOR)));

        // Grant this function permission to read from the S3 bucket
        coreStacks.grantReadInstanceConfig(handler);

        // Grant this function permission to query the queue for number of messages
        jobsQueue.grant(handler, "sqs:GetQueueAttributes");
        jobsQueue.grantSendMessages(handler);
        jobsQueue.grantSendMessages(coreStacks.getInvokeCompactionPolicyForGrants());
        Utils.grantInvokeOnPolicy(handler, coreStacks.getInvokeCompactionPolicyForGrants());
        coreStacks.grantInvokeScheduled(handler);

        // Grant this function permission to query ECS for the number of tasks, etc
        IRole role = Objects.requireNonNull(handler.getRole());
        role.addToPrincipalPolicy(runTasksPolicyStatement());
        role.addManagedPolicy(ManagedPolicy.fromAwsManagedPolicyName("service-role/AmazonECSTaskExecutionRolePolicy"));

        // Cloudwatch rule to trigger this lambda
        Rule rule = Rule.Builder
                .create(stack, "BulkExportTasksCreationPeriodicTrigger")
                .ruleName(SleeperScheduleRule.BULK_EXPORT_TASK_CREATION.buildRuleName(instanceProperties))
                .description(SleeperScheduleRule.BULK_EXPORT_TASK_CREATION.getDescription())
                .enabled(!props.isDeployPaused())
                .schedule(Schedule
                        .rate(Duration.minutes(instanceProperties.getInt(BULK_EXPORT_TASK_CREATION_PERIOD_IN_MINUTES))))
                .targets(Collections.singletonList(new LambdaFunction(handler)))
                .build();
        instanceProperties.set(BULK_EXPORT_TASK_CREATION_LAMBDA_FUNCTION, handler.getFunctionName());
        instanceProperties.set(BULK_EXPORT_TASK_CREATION_CLOUDWATCH_RULE, rule.getRuleName());

        return handler;
    }

    private Cluster ecsClusterForBulkExportTasks(
            SleeperCoreStacks coreStacks, IBucket jarsBucket, IBucket resultsBucket) {
        String clusterName = String.join("-", "sleeper",
                Utils.cleanInstanceId(instanceProperties), "bulk-export-cluster");
        Cluster cluster = Cluster.Builder
                .create(stack, "BulkExportCluster")
                .clusterName(clusterName)
                .containerInsightsV2(ContainerInsights.ENHANCED)
                .vpc(coreStacks.getVpc())
                .build();
        instanceProperties.set(BULK_EXPORT_CLUSTER, cluster.getClusterName());

        IRepository repository = Repository.fromRepositoryName(stack, "BE-ECR1",
                DockerDeployment.BULK_EXPORT.getEcrRepositoryName(instanceProperties));
        ContainerImage containerImage = ContainerImage.fromEcrRepository(repository, instanceProperties.get(VERSION));

        Map<String, String> environmentVariables = EnvironmentUtils.createDefaultEnvironment(instanceProperties);
        environmentVariables.put(Utils.AWS_REGION, instanceProperties.get(REGION));

        ITaskDefinition taskDefinition = new BulkExportOnFargateResources(
                instanceProperties, stack, coreStacks).createTaskDefinition(containerImage, environmentVariables);

        coreStacks.grantRunCompactionJobs(taskDefinition.getTaskRole());
        jarsBucket.grantRead(taskDefinition.getTaskRole());
        resultsBucket.grantPut(taskDefinition.getTaskRole());
        resultsBucket.grantReadWrite(taskDefinition.getTaskRole());
        jobsQueue.grantConsumeMessages(taskDefinition.getTaskRole());

        CfnOutputProps bulkExportClusterProps = new CfnOutputProps.Builder()
                .value(cluster.getClusterName())
                .build();
        new CfnOutput(stack, BULK_EXPORT_CLUSTER_NAME, bulkExportClusterProps);

        return cluster;
    }

    private static PolicyStatement runTasksPolicyStatement() {
        return PolicyStatement.Builder.create()
                .effect(Effect.ALLOW)
                .actions(List.of("ecs:DescribeClusters", "ecs:RunTask", "iam:PassRole",
                        "ecs:DescribeContainerInstances", "ecs:DescribeTasks", "ecs:ListContainerInstances",
                        "autoscaling:SetDesiredCapacity", "autoscaling:DescribeAutoScalingGroups",
                        "ec2:DescribeInstanceTypes"))
                .resources(List.of("*"))
                .build();
    }
}
