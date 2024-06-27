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
package sleeper.cdk.stack;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import software.amazon.awscdk.CfnOutput;
import software.amazon.awscdk.CfnOutputProps;
import software.amazon.awscdk.Duration;
import software.amazon.awscdk.NestedStack;
import software.amazon.awscdk.services.cloudwatch.IMetric;
import software.amazon.awscdk.services.ec2.IVpc;
import software.amazon.awscdk.services.ec2.Vpc;
import software.amazon.awscdk.services.ec2.VpcLookupOptions;
import software.amazon.awscdk.services.ecr.IRepository;
import software.amazon.awscdk.services.ecr.Repository;
import software.amazon.awscdk.services.ecs.Cluster;
import software.amazon.awscdk.services.ecs.ContainerDefinitionOptions;
import software.amazon.awscdk.services.ecs.ContainerImage;
import software.amazon.awscdk.services.ecs.FargateTaskDefinition;
import software.amazon.awscdk.services.events.Rule;
import software.amazon.awscdk.services.events.Schedule;
import software.amazon.awscdk.services.events.targets.LambdaFunction;
import software.amazon.awscdk.services.iam.Effect;
import software.amazon.awscdk.services.iam.IRole;
import software.amazon.awscdk.services.iam.ManagedPolicy;
import software.amazon.awscdk.services.iam.PolicyStatement;
import software.amazon.awscdk.services.lambda.IFunction;
import software.amazon.awscdk.services.s3.Bucket;
import software.amazon.awscdk.services.s3.IBucket;
import software.amazon.awscdk.services.sns.Topic;
import software.amazon.awscdk.services.sqs.DeadLetterQueue;
import software.amazon.awscdk.services.sqs.Queue;
import software.constructs.Construct;

import sleeper.cdk.Utils;
import sleeper.cdk.jars.BuiltJar;
import sleeper.cdk.jars.BuiltJars;
import sleeper.cdk.jars.LambdaCode;
import sleeper.configuration.properties.SleeperScheduleRule;
import sleeper.configuration.properties.instance.InstanceProperties;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static sleeper.cdk.Utils.createAlarmForDlq;
import static sleeper.cdk.Utils.createLambdaLogGroup;
import static sleeper.cdk.Utils.shouldDeployPaused;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.INGEST_CLOUDWATCH_RULE;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.INGEST_CLUSTER;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.INGEST_JOB_DLQ_ARN;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.INGEST_JOB_DLQ_URL;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.INGEST_JOB_QUEUE_ARN;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.INGEST_JOB_QUEUE_URL;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.INGEST_LAMBDA_FUNCTION;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.INGEST_TASK_DEFINITION_FAMILY;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.VERSION;
import static sleeper.configuration.properties.instance.CommonProperty.ID;
import static sleeper.configuration.properties.instance.CommonProperty.METRICS_NAMESPACE;
import static sleeper.configuration.properties.instance.CommonProperty.QUEUE_VISIBILITY_TIMEOUT_IN_SECONDS;
import static sleeper.configuration.properties.instance.CommonProperty.TASK_RUNNER_LAMBDA_MEMORY_IN_MB;
import static sleeper.configuration.properties.instance.CommonProperty.TASK_RUNNER_LAMBDA_TIMEOUT_IN_SECONDS;
import static sleeper.configuration.properties.instance.CommonProperty.VPC_ID;
import static sleeper.configuration.properties.instance.IngestProperty.ECR_INGEST_REPO;
import static sleeper.configuration.properties.instance.IngestProperty.INGEST_TASK_CPU;
import static sleeper.configuration.properties.instance.IngestProperty.INGEST_TASK_CREATION_PERIOD_IN_MINUTES;
import static sleeper.configuration.properties.instance.IngestProperty.INGEST_TASK_MEMORY;

@SuppressFBWarnings("NP_NULL_ON_SOME_PATH_FROM_RETURN_VALUE")
public class IngestStack extends NestedStack {
    public static final String INGEST_STACK_QUEUE_NAME = "IngestStackQueueNameKey";
    public static final String INGEST_STACK_QUEUE_URL = "IngestStackQueueUrlKey";
    public static final String INGEST_STACK_DL_QUEUE_URL = "IngestStackDLQueueUrlKey";
    public static final String INGEST_CLUSTER_NAME = "IngestClusterName";
    public static final String INGEST_CONTAINER_ROLE_ARN = "IngestContainerRoleARN";

    private Queue ingestJobQueue;
    private Queue ingestDLQ;
    private final InstanceProperties instanceProperties;
    private final IngestStatusStoreResources statusStore;

    public IngestStack(
            Construct scope,
            String id,
            InstanceProperties instanceProperties,
            BuiltJars jars,
            Topic topic,
            CoreStacks coreStacks,
            IngestStatusStoreResources statusStore,
            List<IMetric> errorMetrics) {
        super(scope, id);
        this.instanceProperties = instanceProperties;
        this.statusStore = statusStore;
        // The ingest stack consists of the following components:
        //  - An SQS queue for the ingest jobs.
        //  - An ECS cluster, task definition, etc., for ingest jobs.
        //  - A lambda that periodically checks the number of running ingest tasks
        //      and if there are not enough (i.e. there is a backlog on the queue
        //      then it creates more tasks).

        // Jars bucket
        IBucket jarsBucket = Bucket.fromBucketName(this, "JarsBucket", jars.bucketName());

        // Job creation code
        LambdaCode taskCreatorJar = jars.lambdaCode(BuiltJar.INGEST_STARTER, jarsBucket);

        // SQS queue for ingest jobs
        sqsQueueForIngestJobs(coreStacks, topic, errorMetrics);

        // ECS cluster for ingest tasks
        ecsClusterForIngestTasks(jarsBucket, coreStacks, ingestJobQueue);

        // Lambda to create ingest tasks
        lambdaToCreateIngestTasks(coreStacks, ingestJobQueue, taskCreatorJar);

        Utils.addStackTagIfSet(this, instanceProperties);
    }

    private Queue sqsQueueForIngestJobs(CoreStacks coreStacks, Topic topic, List<IMetric> errorMetrics) {
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
        ingestJobQueue = Queue.Builder
                .create(this, "IngestJobQueue")
                .queueName(queueName)
                .deadLetterQueue(ingestJobDeadLetterQueue)
                .visibilityTimeout(Duration.seconds(instanceProperties.getInt(QUEUE_VISIBILITY_TIMEOUT_IN_SECONDS)))
                .build();
        instanceProperties.set(INGEST_JOB_QUEUE_URL, ingestJobQueue.getQueueUrl());
        instanceProperties.set(INGEST_JOB_QUEUE_ARN, ingestJobQueue.getQueueArn());
        instanceProperties.set(INGEST_JOB_DLQ_URL, ingestJobDeadLetterQueue.getQueue().getQueueUrl());
        instanceProperties.set(INGEST_JOB_DLQ_ARN, ingestJobDeadLetterQueue.getQueue().getQueueArn());
        ingestJobQueue.grantSendMessages(coreStacks.getIngestByQueuePolicyForGrants());
        ingestJobQueue.grantPurge(coreStacks.getPurgeQueuesPolicyForGrants());

        // Add alarm to send message to SNS if there are any messages on the dead letter queue
        createAlarmForDlq(this, "IngestAlarm",
                "Alarms if there are any messages on the dead letter queue for the ingest queue",
                ingestDLQ, topic);
        errorMetrics.add(Utils.createErrorMetric("Ingest Errors", ingestDLQ, instanceProperties));

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
            CoreStacks coreStacks,
            Queue ingestJobQueue) {
        VpcLookupOptions vpcLookupOptions = VpcLookupOptions.builder()
                .vpcId(instanceProperties.get(VPC_ID))
                .build();
        IVpc vpc = Vpc.fromLookup(this, "VPC1", vpcLookupOptions);
        String instanceId = Utils.cleanInstanceId(instanceProperties);
        String clusterName = String.join("-", "sleeper", instanceId, "ingest-cluster");
        Cluster cluster = Cluster.Builder
                .create(this, "IngestCluster")
                .clusterName(clusterName)
                .containerInsights(Boolean.TRUE)
                .vpc(vpc)
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
                "ECR-ingest",
                instanceProperties.get(ECR_INGEST_REPO));
        ContainerImage containerImage = ContainerImage.fromEcrRepository(repository, instanceProperties.get(VERSION));

        ContainerDefinitionOptions containerDefinitionOptions = ContainerDefinitionOptions.builder()
                .image(containerImage)
                .logging(Utils.createECSContainerLogDriver(this, instanceProperties, "IngestTasks"))
                .environment(Utils.createDefaultEnvironment(instanceProperties))
                .build();
        taskDefinition.addContainer("IngestContainer", containerDefinitionOptions);

        coreStacks.grantIngest(taskDefinition.getTaskRole());
        jarsBucket.grantRead(taskDefinition.getTaskRole());
        statusStore.grantWriteJobEvent(taskDefinition.getTaskRole());
        statusStore.grantWriteTaskEvent(taskDefinition.getTaskRole());
        ingestJobQueue.grantConsumeMessages(taskDefinition.getTaskRole());
        taskDefinition.getTaskRole().addToPrincipalPolicy(PolicyStatement.Builder.create()
                .effect(Effect.ALLOW)
                .actions(Collections.singletonList("cloudwatch:PutMetricData"))
                .resources(Collections.singletonList("*"))
                .conditions(Collections.singletonMap("StringEquals", Collections.singletonMap("cloudwatch:namespace", instanceProperties.get(METRICS_NAMESPACE))))
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

    private void lambdaToCreateIngestTasks(CoreStacks coreStacks, Queue ingestJobQueue, LambdaCode taskCreatorJar) {

        // Run tasks function
        String functionName = String.join("-", "sleeper",
                Utils.cleanInstanceId(instanceProperties), "ingest-create-tasks");

        IFunction handler = taskCreatorJar.buildFunction(this, "IngestTasksCreator", builder -> builder
                .functionName(functionName)
                .description("If there are ingest jobs on queue create tasks to run them")
                .runtime(software.amazon.awscdk.services.lambda.Runtime.JAVA_11)
                .memorySize(instanceProperties.getInt(TASK_RUNNER_LAMBDA_MEMORY_IN_MB))
                .timeout(Duration.seconds(instanceProperties.getInt(TASK_RUNNER_LAMBDA_TIMEOUT_IN_SECONDS)))
                .handler("sleeper.ingest.starter.RunIngestTasksLambda::eventHandler")
                .environment(Utils.createDefaultEnvironment(instanceProperties))
                .reservedConcurrentExecutions(1)
                .logGroup(createLambdaLogGroup(this, "IngestTasksCreatorLogGroup", functionName, instanceProperties)));

        // Grant this function permission to read from the S3 bucket
        coreStacks.grantReadInstanceConfig(handler);

        // Grant this function permission to query the queue for number of messages
        ingestJobQueue.grantSendMessages(handler);
        ingestJobQueue.grant(handler, "sqs:GetQueueAttributes");
        statusStore.grantWriteJobEvent(handler);
        statusStore.grantWriteTaskEvent(handler);
        coreStacks.grantInvokeScheduled(handler);

        // Grant this function permission to query ECS for the number of tasks, etc
        PolicyStatement policyStatement = PolicyStatement.Builder
                .create()
                .resources(Collections.singletonList("*"))
                .actions(Arrays.asList("ecs:DescribeClusters", "ecs:RunTask", "iam:PassRole"))
                .build();
        IRole role = Objects.requireNonNull(handler.getRole());
        role.addToPrincipalPolicy(policyStatement);
        role.addManagedPolicy(ManagedPolicy.fromAwsManagedPolicyName("service-role/AmazonECSTaskExecutionRolePolicy"));

        // Cloudwatch rule to trigger this lambda
        Rule rule = Rule.Builder
                .create(this, "IngestTasksCreationPeriodicTrigger")
                .ruleName(SleeperScheduleRule.INGEST.buildRuleName(instanceProperties))
                .description("A rule to periodically trigger the ingest tasks lambda")
                .enabled(!shouldDeployPaused(this))
                .schedule(Schedule.rate(Duration.minutes(instanceProperties.getInt(INGEST_TASK_CREATION_PERIOD_IN_MINUTES))))
                .targets(Collections.singletonList(new LambdaFunction(handler)))
                .build();
        instanceProperties.set(INGEST_LAMBDA_FUNCTION, handler.getFunctionName());
        instanceProperties.set(INGEST_CLOUDWATCH_RULE, rule.getRuleName());
    }

    public Queue getIngestJobQueue() {
        return ingestJobQueue;
    }
}
