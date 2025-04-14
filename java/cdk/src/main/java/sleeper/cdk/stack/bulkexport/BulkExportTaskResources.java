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
import software.amazon.awscdk.services.ec2.IVpc;
import software.amazon.awscdk.services.ec2.Vpc;
import software.amazon.awscdk.services.ec2.VpcLookupOptions;
import software.amazon.awscdk.services.ecr.IRepository;
import software.amazon.awscdk.services.ecr.Repository;
import software.amazon.awscdk.services.ecs.Cluster;
import software.amazon.awscdk.services.ecs.ContainerImage;
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

import sleeper.cdk.jars.LambdaCode;
import sleeper.cdk.stack.core.CoreStacks;
import sleeper.cdk.stack.core.LoggingStack.LogGroupRef;
import sleeper.cdk.util.Utils;
import sleeper.core.deploy.LambdaHandler;
import sleeper.core.deploy.SleeperScheduleRule;
import sleeper.core.properties.instance.InstanceProperties;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static sleeper.cdk.util.Utils.shouldDeployPaused;
import static sleeper.core.properties.instance.BulkExportProperty.BULK_EXPORT_ECR_REPO;
import static sleeper.core.properties.instance.BulkExportProperty.BULK_EXPORT_TASK_CREATION_PERIOD_IN_MINUTES;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.BULK_EXPORT_CLUSTER;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.BULK_EXPORT_TASK_CREATION_CLOUDWATCH_RULE;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.BULK_EXPORT_TASK_CREATION_LAMBDA_FUNCTION;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.VERSION;
import static sleeper.core.properties.instance.CommonProperty.ACCOUNT;
import static sleeper.core.properties.instance.CommonProperty.REGION;
import static sleeper.core.properties.instance.CommonProperty.TASK_RUNNER_LAMBDA_MEMORY_IN_MB;
import static sleeper.core.properties.instance.CommonProperty.TASK_RUNNER_LAMBDA_TIMEOUT_IN_SECONDS;
import static sleeper.core.properties.instance.CommonProperty.VPC_ID;

@SuppressFBWarnings("NP_NULL_ON_SOME_PATH_FROM_RETURN_VALUE")
public class BulkExportTaskResources {
    private static final String BULK_EXPORT_CLUSTER_NAME = "BulkExportClusterName";

    private final InstanceProperties instanceProperties;
    private final Stack stack;
    private final Queue jobsQueue;

    public BulkExportTaskResources(Stack stack, CoreStacks coreStacks, InstanceProperties instanceProperties,
            LambdaCode lambdaCode, IBucket jarsBucket, Queue jobsQueue, IBucket resultsBucket) {
        this.instanceProperties = instanceProperties;
        this.stack = stack;
        this.jobsQueue = jobsQueue;
        lambdaToCreateTasks(coreStacks, lambdaCode, instanceProperties);
        ecsClusterForBulkExportTasks(coreStacks, jarsBucket, lambdaCode, resultsBucket);
    }

    private void lambdaToCreateTasks(
            CoreStacks coreStacks, LambdaCode lambdaCode, InstanceProperties instanceProperties) {
        String instanceId = Utils.cleanInstanceId(instanceProperties);
        String functionName = String.join("-", "sleeper",
                instanceId, "bulk-export-tasks-creator");

        IFunction handler = lambdaCode.buildFunction(
                stack, LambdaHandler.BULK_EXPORT_TASK_CREATOR, "BulkExportTasksCreator", builder -> builder
                        .functionName(functionName)
                        .description("If there are leaf partition bulk export jobs on queue create tasks to run them")
                        .memorySize(instanceProperties.getInt(TASK_RUNNER_LAMBDA_MEMORY_IN_MB))
                        .timeout(Duration.seconds(instanceProperties.getInt(TASK_RUNNER_LAMBDA_TIMEOUT_IN_SECONDS)))
                        .environment(Utils.createDefaultEnvironment(instanceProperties))
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
                .enabled(!shouldDeployPaused(stack))
                .schedule(Schedule
                        .rate(Duration.minutes(instanceProperties.getInt(BULK_EXPORT_TASK_CREATION_PERIOD_IN_MINUTES))))
                .targets(Collections.singletonList(new LambdaFunction(handler)))
                .build();
        instanceProperties.set(BULK_EXPORT_TASK_CREATION_LAMBDA_FUNCTION, handler.getFunctionName());
        instanceProperties.set(BULK_EXPORT_TASK_CREATION_CLOUDWATCH_RULE, rule.getRuleName());
    }

    private void ecsClusterForBulkExportTasks(CoreStacks coreStacks, IBucket jarsBucket, LambdaCode taskCreatorJar,
            IBucket resultsBucket) {
        VpcLookupOptions vpcLookupOptions = VpcLookupOptions.builder()
                .vpcId(instanceProperties.get(VPC_ID))
                .build();
        IVpc vpc = Vpc.fromLookup(stack, "VPC1", vpcLookupOptions);
        String clusterName = String.join("-", "sleeper",
                Utils.cleanInstanceId(instanceProperties), "bulk-export-cluster");
        Cluster cluster = Cluster.Builder
                .create(stack, "BulkExportCluster")
                .clusterName(clusterName)
                .containerInsights(Boolean.TRUE)
                .vpc(vpc)
                .build();
        instanceProperties.set(BULK_EXPORT_CLUSTER, cluster.getClusterName());

        IRepository repository = Repository.fromRepositoryName(stack, "BE- ECR1",
                instanceProperties.get(BULK_EXPORT_ECR_REPO));
        ContainerImage containerImage = ContainerImage.fromEcrRepository(repository, instanceProperties.get(VERSION));

        Map<String, String> environmentVariables = Utils.createDefaultEnvironment(instanceProperties);
        environmentVariables.put(Utils.AWS_REGION, instanceProperties.get(REGION));

        ITaskDefinition taskDefinition = new BulkExportOnFargateResources(
                instanceProperties, stack, coreStacks).createTaskDefinition(containerImage, environmentVariables);

        coreStacks.grantRunCompactionJobs(taskDefinition.getTaskRole());
        jarsBucket.grantRead(taskDefinition.getTaskRole());
        resultsBucket.grantReadWrite(taskDefinition.getTaskRole());

        String queueName = jobsQueue.getQueueName();
        taskDefinition.getTaskRole().addToPrincipalPolicy(PolicyStatement.Builder
                .create()
                .resources(List.of(
                        String.format("arn:aws:ecs:%s:%s:cluster/%s", instanceProperties.get(REGION),
                                instanceProperties.get(ACCOUNT), instanceProperties.get(BULK_EXPORT_CLUSTER)),
                        String.format("arn:aws:sqs:%s:%s:%s", instanceProperties.get(REGION),
                                instanceProperties.get(ACCOUNT), queueName)))
                .actions(List.of(
                        "ecs:DescribeContainerInstances",
                        "sqs:ReceiveMessage",
                        "sqs:DeleteMessage",
                        "sqs:ChangeMessageVisibility"))
                .build());

        CfnOutputProps bulkExportClusterProps = new CfnOutputProps.Builder()
                .value(cluster.getClusterName())
                .build();
        new CfnOutput(stack, BULK_EXPORT_CLUSTER_NAME, bulkExportClusterProps);
    }

    private static PolicyStatement runTasksPolicyStatement() {
        return PolicyStatement.Builder.create()
                .effect(Effect.ALLOW)
                .actions(List.of("ecs:DescribeClusters", "ecs:RunTask", "iam:PassRole",
                        "ecs:DescribeContainerInstances", "ecs:DescribeTasks", "ecs:ListContainerInstances"))
                .resources(List.of("*"))
                .build();
    }
}
