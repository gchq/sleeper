/*
 * Copyright 2022-2024 Crown Copyright
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
import software.amazon.awscdk.services.cloudwatch.IMetric;
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
import software.amazon.awscdk.services.sns.Topic;
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
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.COMPACTION_CLUSTER;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.COMPACTION_TASK_CREATION_CLOUDWATCH_RULE;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.COMPACTION_TASK_CREATION_LAMBDA_FUNCTION;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.VERSION;
import static sleeper.core.properties.instance.CommonProperty.REGION;
import static sleeper.core.properties.instance.CommonProperty.TASK_RUNNER_LAMBDA_MEMORY_IN_MB;
import static sleeper.core.properties.instance.CommonProperty.TASK_RUNNER_LAMBDA_TIMEOUT_IN_SECONDS;
import static sleeper.core.properties.instance.CommonProperty.VPC_ID;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_ECS_LAUNCHTYPE;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_TASK_CREATION_PERIOD_IN_MINUTES;
import static sleeper.core.properties.instance.CompactionProperty.ECR_COMPACTION_REPO;

public class CompactionTaskResources {

    private static final String COMPACTION_CLUSTER_NAME = "CompactionClusterName";
    private final InstanceProperties instanceProperties;
    private final Stack stack;

    public CompactionTaskResources(Stack stack,
            InstanceProperties instanceProperties,
            LambdaCode lambdaCode,
            IBucket jarsBucket,
            Queue compactionJobsQueue,
            Topic topic,
            CoreStacks coreStacks,
            List<IMetric> errorMetrics) {

        this.instanceProperties = instanceProperties;
        this.stack = stack;

        ecsClusterForCompactionTasks(coreStacks, jarsBucket, lambdaCode, compactionJobsQueue);
        lambdaToCreateCompactionTasks(coreStacks, lambdaCode, compactionJobsQueue);

        // Allow running compaction tasks
        coreStacks.getInvokeCompactionPolicyForGrants().addStatements(runTasksPolicyStatement());

    }

    private void ecsClusterForCompactionTasks(CoreStacks coreStacks, IBucket jarsBucket, LambdaCode taskCreatorJar, Queue compactionJobsQueue) {
        VpcLookupOptions vpcLookupOptions = VpcLookupOptions.builder()
                .vpcId(instanceProperties.get(VPC_ID))
                .build();
        IVpc vpc = Vpc.fromLookup(stack, "VPC1", vpcLookupOptions);
        String clusterName = String.join("-", "sleeper",
                Utils.cleanInstanceId(instanceProperties), "compaction-cluster");
        Cluster cluster = Cluster.Builder
                .create(stack, "CompactionCluster")
                .clusterName(clusterName)
                .containerInsights(Boolean.TRUE)
                .vpc(vpc)
                .build();
        instanceProperties.set(COMPACTION_CLUSTER, cluster.getClusterName());

        IRepository repository = Repository.fromRepositoryName(stack, "ECR1",
                instanceProperties.get(ECR_COMPACTION_REPO));
        ContainerImage containerImage = ContainerImage.fromEcrRepository(repository, instanceProperties.get(VERSION));

        Map<String, String> environmentVariables = Utils.createDefaultEnvironment(instanceProperties);
        environmentVariables.put(Utils.AWS_REGION, instanceProperties.get(REGION));

        String launchType = instanceProperties.get(COMPACTION_ECS_LAUNCHTYPE);
        ITaskDefinition taskDefinition;
        if (launchType.equalsIgnoreCase("FARGATE")) {
            taskDefinition = new CompactionOnFargateResources(instanceProperties, stack, coreStacks)
                    .createTaskDefinition(containerImage, environmentVariables);
        } else {
            taskDefinition = new CompactionOnEc2Resources(instanceProperties, stack, coreStacks)
                    .createTaskDefinition(cluster, vpc, taskCreatorJar, containerImage, environmentVariables);
        }
        coreStacks.grantRunCompactionJobs(taskDefinition.getTaskRole());
        jarsBucket.grantRead(taskDefinition.getTaskRole());

        taskDefinition.getTaskRole().addToPrincipalPolicy(PolicyStatement.Builder
                .create()
                .resources(Collections.singletonList("*"))
                .actions(List.of("ecs:DescribeContainerInstances"))
                .build());

        compactionJobsQueue.grantConsumeMessages(taskDefinition.getTaskRole());

        CfnOutputProps compactionClusterProps = new CfnOutputProps.Builder()
                .value(cluster.getClusterName())
                .build();
        new CfnOutput(stack, COMPACTION_CLUSTER_NAME, compactionClusterProps);
    }

    @SuppressFBWarnings("NP_NULL_ON_SOME_PATH_FROM_RETURN_VALUE")
    private void lambdaToCreateCompactionTasks(
            CoreStacks coreStacks, LambdaCode lambdaCode, Queue compactionJobsQueue) {
        String functionName = String.join("-", "sleeper",
                Utils.cleanInstanceId(instanceProperties), "compaction-tasks-creator");

        IFunction handler = lambdaCode.buildFunction(stack, LambdaHandler.COMPACTION_TASK_CREATOR, "CompactionTasksCreator", builder -> builder
                .functionName(functionName)
                .description("If there are compaction jobs on queue create tasks to run them")
                .memorySize(instanceProperties.getInt(TASK_RUNNER_LAMBDA_MEMORY_IN_MB))
                .timeout(Duration.seconds(instanceProperties.getInt(TASK_RUNNER_LAMBDA_TIMEOUT_IN_SECONDS)))
                .environment(Utils.createDefaultEnvironment(instanceProperties))
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
                .description("A rule to periodically trigger the compaction task creation lambda")
                .enabled(!shouldDeployPaused(stack))
                .schedule(Schedule.rate(Duration.minutes(instanceProperties.getInt(COMPACTION_TASK_CREATION_PERIOD_IN_MINUTES))))
                .targets(Collections.singletonList(new LambdaFunction(handler)))
                .build();
        instanceProperties.set(COMPACTION_TASK_CREATION_LAMBDA_FUNCTION, handler.getFunctionName());
        instanceProperties.set(COMPACTION_TASK_CREATION_CLOUDWATCH_RULE, rule.getRuleName());
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
