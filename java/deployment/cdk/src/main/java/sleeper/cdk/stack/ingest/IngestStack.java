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
package sleeper.cdk.stack.ingest;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import software.amazon.awscdk.CfnOutput;
import software.amazon.awscdk.CfnOutputProps;
import software.amazon.awscdk.Duration;
import software.amazon.awscdk.NestedStack;
import software.amazon.awscdk.services.ecr.IRepository;
import software.amazon.awscdk.services.ecr.Repository;
import software.amazon.awscdk.services.ecs.Cluster;
import software.amazon.awscdk.services.ecs.ContainerDefinitionOptions;
import software.amazon.awscdk.services.ecs.ContainerImage;
import software.amazon.awscdk.services.ecs.ContainerInsights;
import software.amazon.awscdk.services.ecs.FargateTaskDefinition;
import software.amazon.awscdk.services.events.Rule;
import software.amazon.awscdk.services.events.Schedule;
import software.amazon.awscdk.services.events.targets.LambdaFunction;
import software.amazon.awscdk.services.iam.Effect;
import software.amazon.awscdk.services.iam.IRole;
import software.amazon.awscdk.services.iam.ManagedPolicy;
import software.amazon.awscdk.services.iam.PolicyStatement;
import software.amazon.awscdk.services.lambda.IFunction;
import software.amazon.awscdk.services.s3.IBucket;
import software.amazon.awscdk.services.sqs.DeadLetterQueue;
import software.amazon.awscdk.services.sqs.Queue;
import software.constructs.Construct;

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

import java.util.List;
import java.util.Map;
import java.util.Objects;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.INGEST_CLOUDWATCH_RULE;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.INGEST_CLUSTER;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.INGEST_JOB_DLQ_ARN;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.INGEST_JOB_DLQ_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.INGEST_JOB_QUEUE_ARN;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.INGEST_JOB_QUEUE_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.INGEST_LAMBDA_FUNCTION;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.INGEST_TASK_DEFINITION_FAMILY;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.VERSION;
import static sleeper.core.properties.instance.CommonProperty.ID;
import static sleeper.core.properties.instance.CommonProperty.TASK_RUNNER_LAMBDA_MEMORY_IN_MB;
import static sleeper.core.properties.instance.CommonProperty.TASK_RUNNER_LAMBDA_TIMEOUT_IN_SECONDS;
import static sleeper.core.properties.instance.IngestProperty.INGEST_QUEUE_VISIBILITY_TIMEOUT_IN_SECONDS;
import static sleeper.core.properties.instance.IngestProperty.INGEST_TASK_CPU;
import static sleeper.core.properties.instance.IngestProperty.INGEST_TASK_CREATION_PERIOD_IN_MINUTES;
import static sleeper.core.properties.instance.IngestProperty.INGEST_TASK_MEMORY;
import static sleeper.core.properties.instance.MetricsProperty.METRICS_NAMESPACE;

@SuppressFBWarnings("NP_NULL_ON_SOME_PATH_FROM_RETURN_VALUE")
public class IngestStack extends NestedStack {
    public static final String INGEST_STACK_QUEUE_NAME = "IngestStackQueueNameKey";
    public static final String INGEST_STACK_QUEUE_URL = "IngestStackQueueUrlKey";
    public static final String INGEST_STACK_DL_QUEUE_URL = "IngestStackDLQueueUrlKey";
    public static final String INGEST_CLUSTER_NAME = "IngestClusterName";
    public static final String INGEST_CONTAINER_ROLE_ARN = "IngestContainerRoleARN";

    private final SleeperInstanceProps props;
    private final InstanceProperties instanceProperties;
    private Queue ingestJobQueue;
    private Queue ingestDLQ;

    public IngestStack(
            Construct scope, String id,
            SleeperInstanceProps props,
            SleeperCoreStacks coreStacks) {
        super(scope, id);
        this.props = props;
        this.instanceProperties = props.getInstanceProperties();
        // The ingest stack consists of the following components:
        //  - An SQS queue for the ingest jobs.
        //  - An ECS cluster, task definition, etc., for ingest jobs.
        //  - A lambda that periodically checks the number of running ingest tasks
        //      and if there are not enough (i.e. there is a backlog on the queue
        //      then it creates more tasks).
        //  - A lambda that stops task when a delete cluster event is triggered.

        IBucket jarsBucket = props.getJars().createJarsBucketReference(this, "JarsBucket");
        SleeperLambdaCode lambdaCode = props.getJars().lambdaCode(jarsBucket);

        ingestJobQueue = sqsQueueForIngestJobs(coreStacks);
        Cluster cluster = ecsClusterForIngestTasks(jarsBucket, coreStacks, ingestJobQueue, lambdaCode);
        IFunction taskCreator = lambdaToCreateIngestTasks(coreStacks, ingestJobQueue, lambdaCode);
        coreStacks.addAutoStopEcsClusterTasksAfterTaskCreatorIsDeleted(this, cluster, taskCreator);

        Utils.addTags(this, instanceProperties);
    }

    private Queue sqsQueueForIngestJobs(SleeperCoreStacks coreStacks) {
        // Create queue for ingest job definitions
        String instanceId = Utils.cleanInstanceId(instanceProperties);
        String dlQueueName = String.join("-", "sleeper", instanceId, "IngestJobDLQ");

        ingestDLQ = Queue.Builder
                .create(this, "IngestJobDeadLetterQueue")
                .queueName(dlQueueName)
                .build();
        DeadLetterQueue ingestJobDeadLetterQueue = DeadLetterQueue.builder()
                .maxReceiveCount(1)
                .queue(ingestDLQ)
                .build();
        String queueName = String.join("-", "sleeper", instanceId, "IngestJobQ");
        Queue ingestJobQueue = Queue.Builder
                .create(this, "IngestJobQueue")
                .queueName(queueName)
                .deadLetterQueue(ingestJobDeadLetterQueue)
                .visibilityTimeout(Duration.seconds(instanceProperties.getInt(INGEST_QUEUE_VISIBILITY_TIMEOUT_IN_SECONDS)))
                .build();
        instanceProperties.set(INGEST_JOB_QUEUE_URL, ingestJobQueue.getQueueUrl());
        instanceProperties.set(INGEST_JOB_QUEUE_ARN, ingestJobQueue.getQueueArn());
        instanceProperties.set(INGEST_JOB_DLQ_URL, ingestJobDeadLetterQueue.getQueue().getQueueUrl());
        instanceProperties.set(INGEST_JOB_DLQ_ARN, ingestJobDeadLetterQueue.getQueue().getQueueArn());
        ingestJobQueue.grantSendMessages(coreStacks.getIngestByQueuePolicyForGrants());
        ingestJobQueue.grantPurge(coreStacks.getPurgeQueuesPolicyForGrants());
        coreStacks.alarmOnDeadLetters(this, "IngestAlarm", "ingest jobs", ingestDLQ);

        CfnOutputProps ingestJobQueueProps = new CfnOutputProps.Builder()
                .value(ingestJobQueue.getQueueUrl())
                .exportName(instanceProperties.get(ID) + "-" + INGEST_STACK_QUEUE_URL)
                .build();
        new CfnOutput(this, INGEST_STACK_QUEUE_URL, ingestJobQueueProps);

        CfnOutputProps ingestJobQueueNameProps = new CfnOutputProps.Builder()
                .value(ingestJobQueue.getQueueName())
                .exportName(instanceProperties.get(ID) + "-" + INGEST_STACK_QUEUE_NAME)
                .build();
        new CfnOutput(this, INGEST_STACK_QUEUE_NAME, ingestJobQueueNameProps);

        CfnOutputProps ingestJobDefinitionsDLQueueProps = new CfnOutputProps.Builder()
                .value(ingestJobDeadLetterQueue.getQueue().getQueueUrl())
                .build();
        new CfnOutput(this, INGEST_STACK_DL_QUEUE_URL, ingestJobDefinitionsDLQueueProps);

        return ingestJobQueue;
    }

    private Cluster ecsClusterForIngestTasks(
            IBucket jarsBucket,
            SleeperCoreStacks coreStacks,
            Queue ingestJobQueue,
            SleeperLambdaCode lambdaCode) {

        String instanceId = Utils.cleanInstanceId(instanceProperties);
        String clusterName = String.join("-", "sleeper", instanceId, "ingest-cluster");
        Cluster cluster = Cluster.Builder
                .create(this, "IngestCluster")
                .clusterName(clusterName)
                .containerInsightsV2(ContainerInsights.ENHANCED)
                .vpc(coreStacks.getVpc())
                .build();
        instanceProperties.set(INGEST_CLUSTER, cluster.getClusterName());

        FargateTaskDefinition taskDefinition = FargateTaskDefinition.Builder
                .create(this, "IngestTaskDefinition")
                .family(String.join("-", "sleeper", instanceId, "IngestTask"))
                .cpu(instanceProperties.getInt(INGEST_TASK_CPU))
                .memoryLimitMiB(instanceProperties.getInt(INGEST_TASK_MEMORY))
                .build();
        instanceProperties.set(INGEST_TASK_DEFINITION_FAMILY, taskDefinition.getFamily());

        IRepository repository = Repository.fromRepositoryName(this,
                "ECR-ingest", DockerDeployment.INGEST.getEcrRepositoryName(instanceProperties));
        ContainerImage containerImage = ContainerImage.fromEcrRepository(repository, instanceProperties.get(VERSION));

        ContainerDefinitionOptions containerDefinitionOptions = ContainerDefinitionOptions.builder()
                .image(containerImage)
                .logging(Utils.createECSContainerLogDriver(coreStacks.getLogGroup(LogGroupRef.INGEST_TASKS)))
                .environment(EnvironmentUtils.createDefaultEnvironment(instanceProperties))
                .build();
        taskDefinition.addContainer("IngestContainer", containerDefinitionOptions);

        coreStacks.grantIngest(taskDefinition.getTaskRole());
        jarsBucket.grantRead(taskDefinition.getTaskRole());
        ingestJobQueue.grantConsumeMessages(taskDefinition.getTaskRole());
        taskDefinition.getTaskRole().addToPrincipalPolicy(PolicyStatement.Builder.create()
                .effect(Effect.ALLOW)
                .actions(List.of("cloudwatch:PutMetricData"))
                .resources(List.of("*"))
                .conditions(Map.of("StringEquals", Map.of("cloudwatch:namespace", instanceProperties.get(METRICS_NAMESPACE))))
                .build());

        CfnOutputProps ingestClusterProps = new CfnOutputProps.Builder()
                .value(cluster.getClusterName())
                .build();
        new CfnOutput(this, INGEST_CLUSTER_NAME, ingestClusterProps);

        CfnOutputProps ingestRoleARNProps = new CfnOutputProps.Builder()
                .value(taskDefinition.getTaskRole().getRoleArn())
                .exportName(instanceProperties.get(ID) + "-" + INGEST_CONTAINER_ROLE_ARN)
                .build();
        new CfnOutput(this, INGEST_CONTAINER_ROLE_ARN, ingestRoleARNProps);

        return cluster;
    }

    private IFunction lambdaToCreateIngestTasks(SleeperCoreStacks coreStacks, Queue ingestJobQueue, SleeperLambdaCode lambdaCode) {

        // Run tasks function
        String functionName = String.join("-", "sleeper",
                Utils.cleanInstanceId(instanceProperties), "ingest-create-tasks");

        IFunction handler = lambdaCode.buildFunction(this, LambdaHandler.INGEST_TASK_CREATOR, "IngestTasksCreator", builder -> builder
                .functionName(functionName)
                .description("If there are ingest jobs on queue create tasks to run them")
                .memorySize(instanceProperties.getInt(TASK_RUNNER_LAMBDA_MEMORY_IN_MB))
                .timeout(Duration.seconds(instanceProperties.getInt(TASK_RUNNER_LAMBDA_TIMEOUT_IN_SECONDS)))
                .environment(EnvironmentUtils.createDefaultEnvironment(instanceProperties))
                .reservedConcurrentExecutions(1)
                .logGroup(coreStacks.getLogGroup(LogGroupRef.INGEST_CREATE_TASKS)));

        // Grant this function permission to read from the S3 bucket
        coreStacks.grantReadInstanceConfig(handler);

        // Grant this function permission to query the queue for number of messages
        ingestJobQueue.grantSendMessages(handler);
        ingestJobQueue.grant(handler, "sqs:GetQueueAttributes");
        coreStacks.grantInvokeScheduled(handler);

        // Grant this function permission to query ECS for the number of tasks, etc
        PolicyStatement policyStatement = PolicyStatement.Builder
                .create()
                .resources(List.of("*"))
                .actions(List.of("ecs:DescribeClusters", "ecs:RunTask", "iam:PassRole"))
                .build();
        IRole role = Objects.requireNonNull(handler.getRole());
        role.addToPrincipalPolicy(policyStatement);
        role.addManagedPolicy(ManagedPolicy.fromAwsManagedPolicyName("service-role/AmazonECSTaskExecutionRolePolicy"));

        // Cloudwatch rule to trigger this lambda
        Rule rule = Rule.Builder
                .create(this, "IngestTasksCreationPeriodicTrigger")
                .ruleName(SleeperScheduleRule.INGEST_TASK_CREATION.buildRuleName(instanceProperties))
                .description(SleeperScheduleRule.INGEST_TASK_CREATION.getDescription())
                .enabled(!props.isDeployPaused())
                .schedule(Schedule.rate(Duration.minutes(instanceProperties.getInt(INGEST_TASK_CREATION_PERIOD_IN_MINUTES))))
                .targets(List.of(new LambdaFunction(handler)))
                .build();
        instanceProperties.set(INGEST_LAMBDA_FUNCTION, handler.getFunctionName());
        instanceProperties.set(INGEST_CLOUDWATCH_RULE, rule.getRuleName());

        return handler;
    }

    public Queue getIngestJobQueue() {
        return ingestJobQueue;
    }
}
