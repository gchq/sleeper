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
import software.amazon.awscdk.Duration;
import software.amazon.awscdk.NestedStack;
import software.amazon.awscdk.services.autoscaling.AutoScalingGroup;
import software.amazon.awscdk.services.autoscaling.UpdatePolicy;
import software.amazon.awscdk.services.ec2.IVpc;
import software.amazon.awscdk.services.ec2.InstanceArchitecture;
import software.amazon.awscdk.services.ec2.InstanceType;
import software.amazon.awscdk.services.ec2.LaunchTemplate;
import software.amazon.awscdk.services.ec2.SecurityGroup;
import software.amazon.awscdk.services.ec2.UserData;
import software.amazon.awscdk.services.ec2.Vpc;
import software.amazon.awscdk.services.ec2.VpcLookupOptions;
import software.amazon.awscdk.services.ecr.IRepository;
import software.amazon.awscdk.services.ecr.Repository;
import software.amazon.awscdk.services.ecs.AmiHardwareType;
import software.amazon.awscdk.services.ecs.AsgCapacityProvider;
import software.amazon.awscdk.services.ecs.Cluster;
import software.amazon.awscdk.services.ecs.ContainerDefinitionOptions;
import software.amazon.awscdk.services.ecs.ContainerImage;
import software.amazon.awscdk.services.ecs.Ec2Service;
import software.amazon.awscdk.services.ecs.Ec2TaskDefinition;
import software.amazon.awscdk.services.ecs.EcsOptimizedImage;
import software.amazon.awscdk.services.iam.Effect;
import software.amazon.awscdk.services.iam.IGrantable;
import software.amazon.awscdk.services.iam.PolicyStatement;
import software.amazon.awscdk.services.iam.Role;
import software.amazon.awscdk.services.iam.ServicePrincipal;
import software.amazon.awscdk.services.lambda.IFunction;
import software.amazon.awscdk.services.lambda.eventsources.SqsEventSource;
import software.amazon.awscdk.services.logs.ILogGroup;
import software.amazon.awscdk.services.sqs.DeadLetterQueue;
import software.amazon.awscdk.services.sqs.DeduplicationScope;
import software.amazon.awscdk.services.sqs.FifoThroughputLimit;
import software.amazon.awscdk.services.sqs.Queue;
import software.constructs.Construct;

import sleeper.cdk.artefacts.SleeperArtefacts;
import sleeper.cdk.lambda.SleeperLambdaCode;
import sleeper.cdk.stack.compaction.CompactionTrackerResources;
import sleeper.cdk.stack.core.LoggingStack.LogGroupRef;
import sleeper.cdk.stack.ingest.IngestTrackerResources;
import sleeper.cdk.util.TrackDeadLetters;
import sleeper.cdk.util.Utils;
import sleeper.core.deploy.DockerDeployment;
import sleeper.core.deploy.LambdaHandler;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.model.StateStoreCommitterPlatform;
import sleeper.core.util.EnvironmentUtils;

import java.util.List;
import java.util.Map;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.REGION;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.STATESTORE_COMMITTER_DLQ_ARN;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.STATESTORE_COMMITTER_DLQ_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.STATESTORE_COMMITTER_EVENT_SOURCE_ID;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.STATESTORE_COMMITTER_LOG_GROUP;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.STATESTORE_COMMITTER_QUEUE_ARN;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.STATESTORE_COMMITTER_QUEUE_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.VERSION;
import static sleeper.core.properties.instance.CommonProperty.ID;
import static sleeper.core.properties.instance.CommonProperty.VPC_ID;
import static sleeper.core.properties.instance.TableStateProperty.STATESTORE_COMMITTER_BATCH_SIZE;
import static sleeper.core.properties.instance.TableStateProperty.STATESTORE_COMMITTER_EC2_INSTANCE_TYPE;
import static sleeper.core.properties.instance.TableStateProperty.STATESTORE_COMMITTER_LAMBDA_CONCURRENCY_MAXIMUM;
import static sleeper.core.properties.instance.TableStateProperty.STATESTORE_COMMITTER_LAMBDA_CONCURRENCY_RESERVED;
import static sleeper.core.properties.instance.TableStateProperty.STATESTORE_COMMITTER_LAMBDA_MEMORY_IN_MB;
import static sleeper.core.properties.instance.TableStateProperty.STATESTORE_COMMITTER_LAMBDA_TIMEOUT_IN_SECONDS;
import static sleeper.core.properties.instance.TableStateProperty.STATESTORE_COMMITTER_PLATFORM;

@SuppressFBWarnings("MC_OVERRIDABLE_METHOD_CALL_IN_CONSTRUCTOR")
public class StateStoreCommitterStack extends NestedStack {
    private final InstanceProperties instanceProperties;
    private final Queue commitQueue;

    @SuppressWarnings("checkstyle:ParameterNumberCheck")
    public StateStoreCommitterStack(
            Construct scope,
            String id,
            InstanceProperties instanceProperties,
            SleeperArtefacts artefacts,
            LoggingStack loggingStack,
            ConfigBucketStack configBucketStack,
            TableIndexStack tableIndexStack,
            StateStoreStacks stateStoreStacks,
            IngestTrackerResources ingestTracker,
            CompactionTrackerResources compactionTracker,
            ManagedPoliciesStack policiesStack,
            EcsClusterTasksStack ecsClusterTasksStack,
            TrackDeadLetters deadLetters) {
        super(scope, id);
        this.instanceProperties = instanceProperties;
        SleeperLambdaCode lambdaCode = artefacts.lambdaCodeAtScope(this);

        commitQueue = sqsQueueForStateStoreCommitter(policiesStack, deadLetters);

        if (instanceProperties.getEnumValue(STATESTORE_COMMITTER_PLATFORM, StateStoreCommitterPlatform.class).equals(StateStoreCommitterPlatform.EC2)) {
            ecsTaskToCommitStateStoreUpdates(loggingStack, configBucketStack, tableIndexStack, stateStoreStacks, ecsClusterTasksStack.getEcsSecurityGroup(), commitQueue);
        } else {
            lambdaToCommitStateStoreUpdates(
                    loggingStack, policiesStack, lambdaCode,
                    configBucketStack, tableIndexStack, stateStoreStacks,
                    compactionTracker, ingestTracker);
        }
        Utils.addTags(this, instanceProperties);
    }

    private Queue sqsQueueForStateStoreCommitter(ManagedPoliciesStack policiesStack, TrackDeadLetters deadLetters) {
        String instanceId = Utils.cleanInstanceId(instanceProperties);
        Queue deadLetterQueue = Queue.Builder
                .create(this, "StateStoreCommitterDLQ")
                .queueName(String.join("-", "sleeper", instanceId, "StateStoreCommitterDLQ.fifo"))
                .fifo(true)
                .build();
        Queue queue = Queue.Builder
                .create(this, "StateStoreCommitterQueue")
                .queueName(String.join("-", "sleeper", instanceId, "StateStoreCommitterQ.fifo"))
                .deadLetterQueue(DeadLetterQueue.builder()
                        .maxReceiveCount(1)
                        .queue(deadLetterQueue)
                        .build())
                .fifo(true)
                .fifoThroughputLimit(FifoThroughputLimit.PER_MESSAGE_GROUP_ID)
                .deduplicationScope(DeduplicationScope.MESSAGE_GROUP)
                .visibilityTimeout(
                        Duration.seconds(instanceProperties.getInt(STATESTORE_COMMITTER_LAMBDA_TIMEOUT_IN_SECONDS)))
                .build();
        instanceProperties.set(STATESTORE_COMMITTER_QUEUE_URL, queue.getQueueUrl());
        instanceProperties.set(STATESTORE_COMMITTER_QUEUE_ARN, queue.getQueueArn());
        instanceProperties.set(STATESTORE_COMMITTER_DLQ_URL, deadLetterQueue.getQueueUrl());
        instanceProperties.set(STATESTORE_COMMITTER_DLQ_ARN, deadLetterQueue.getQueueArn());

        queue.grantSendMessages(policiesStack.getDirectIngestPolicyForGrants());
        queue.grantPurge(policiesStack.getPurgeQueuesPolicyForGrants());
        deadLetters.alarmOnDeadLetters(this, "StateStoreCommitterAlarm", "the state store committer", deadLetterQueue);
        return queue;
    }

    private void ecsTaskToCommitStateStoreUpdates(LoggingStack loggingStack, ConfigBucketStack configBucketStack, TableIndexStack tableIndexStack,
            StateStoreStacks stateStoreStacks, SecurityGroup ecsSecurityGroup, Queue commitQueue) {
        String instanceId = instanceProperties.get(ID);

        IVpc vpc = Vpc.fromLookup(this, "vpc", VpcLookupOptions.builder()
                .vpcId(instanceProperties.get(VPC_ID))
                .build());
        String clusterName = String.join("-", "sleeper",
                Utils.cleanInstanceId(instanceProperties), "statestore-commit-cluster");
        Cluster cluster = Cluster.Builder.create(this, "cluster")
                .clusterName(clusterName)
                .vpc(vpc)
                .build();

        String ec2InstanceType = instanceProperties.get(STATESTORE_COMMITTER_EC2_INSTANCE_TYPE);
        InstanceType instanceType = new InstanceType(ec2InstanceType);

        cluster.addAsgCapacityProvider(AsgCapacityProvider.Builder.create(this, "capacity-provider")
                .autoScalingGroup(AutoScalingGroup.Builder.create(this, "asg")
                        .launchTemplate(LaunchTemplate.Builder.create(this, "instance-template")
                                .instanceType(instanceType)
                                .machineImage(EcsOptimizedImage.amazonLinux2023(lookupAmiHardwareType(instanceType.getArchitecture())))
                                .requireImdsv2(true)
                                .userData(UserData.forLinux())
                                .securityGroup(ecsSecurityGroup)
                                .role(Role.Builder.create(this, "role")
                                        .assumedBy(ServicePrincipal.fromStaticServicePrincipleName("ec2.amazonaws.com"))
                                        .build())
                                .build())
                        .vpc(vpc)
                        .minCapacity(0)
                        .maxCapacity(1)
                        .desiredCapacity(1)
                        .minHealthyPercentage(0)
                        .maxHealthyPercentage(100)
                        .defaultInstanceWarmup(Duration.seconds(30))
                        .newInstancesProtectedFromScaleIn(false)
                        .updatePolicy(UpdatePolicy.rollingUpdate())
                        .build())
                .instanceWarmupPeriod(30)
                .enableManagedTerminationProtection(false)
                .build());

        IRepository repository = Repository.fromRepositoryName(this, "ecr", DockerDeployment.STATESTORE_COMMITTER.getEcrRepositoryName(this.instanceProperties));
        ContainerImage containerImage = ContainerImage.fromEcrRepository(repository, instanceProperties.get(VERSION));

        ILogGroup logGroup = loggingStack.getLogGroup(LogGroupRef.STATESTORE_COMMITTER);

        Map<String, String> environmentVariables = EnvironmentUtils.createDefaultEnvironment(instanceProperties);
        environmentVariables.put(Utils.AWS_REGION, instanceProperties.get(REGION));

        Ec2TaskDefinition taskDefinition = Ec2TaskDefinition.Builder.create(this, "task-definition")
                .family(String.join("-", Utils.cleanInstanceId(instanceProperties), "StateStoreCommitterOnEC2"))
                .build();

        taskDefinition.addContainer("committer", ContainerDefinitionOptions.builder()
                .containerName("committer")
                .image(containerImage)
                .command(List.of(instanceId, commitQueue.getQueueUrl()))
                .environment(environmentVariables)
                .memoryReservationMiB(1024)
                .logging(Utils.createECSContainerLogDriver(logGroup))
                .build());

        commitQueue.grantConsumeMessages(taskDefinition.getTaskRole());
        configBucketStack.grantRead(taskDefinition.getTaskRole());
        tableIndexStack.grantRead(taskDefinition.getTaskRole());
        stateStoreStacks.grantReadWriteAllFilesAndPartitions(taskDefinition.getTaskRole());

        Ec2Service.Builder.create(this, "service")
                .cluster(cluster)
                .taskDefinition(taskDefinition)
                .desiredCount(1)
                .build();
    }

    private void lambdaToCommitStateStoreUpdates(
            LoggingStack loggingStack, ManagedPoliciesStack policiesStack, SleeperLambdaCode lambdaCode,
            ConfigBucketStack configBucketStack, TableIndexStack tableIndexStack, StateStoreStacks stateStoreStacks,
            CompactionTrackerResources compactionTracker,
            IngestTrackerResources ingestTracker) {

        String functionName = String.join("-", "sleeper",
                Utils.cleanInstanceId(instanceProperties), "statestore-committer");
        ILogGroup logGroup = loggingStack.getLogGroup(LogGroupRef.STATESTORE_COMMITTER);
        instanceProperties.set(STATESTORE_COMMITTER_LOG_GROUP, logGroup.getLogGroupName());

        IFunction handlerFunction = lambdaCode.buildFunction(LambdaHandler.STATESTORE_COMMITTER, "StateStoreCommitter", builder -> builder
                .functionName(functionName)
                .description("Commits updates to the state store. Used to commit compaction and ingest jobs asynchronously.")
                .memorySize(instanceProperties.getInt(STATESTORE_COMMITTER_LAMBDA_MEMORY_IN_MB))
                .timeout(Duration.seconds(instanceProperties.getInt(STATESTORE_COMMITTER_LAMBDA_TIMEOUT_IN_SECONDS)))
                .environment(EnvironmentUtils.createDefaultEnvironment(instanceProperties))
                .reservedConcurrentExecutions(instanceProperties.getIntOrNull(STATESTORE_COMMITTER_LAMBDA_CONCURRENCY_RESERVED))
                .logGroup(logGroup));

        SqsEventSource eventSource = SqsEventSource.Builder.create(commitQueue)
                .batchSize(instanceProperties.getInt(STATESTORE_COMMITTER_BATCH_SIZE))
                .maxConcurrency(instanceProperties.getIntOrNull(STATESTORE_COMMITTER_LAMBDA_CONCURRENCY_MAXIMUM))
                .build();
        handlerFunction.addEventSource(eventSource);
        instanceProperties.set(STATESTORE_COMMITTER_EVENT_SOURCE_ID, eventSource.getEventSourceMappingId());

        policiesStack.getEditStateStoreCommitterTriggerPolicyForGrants().addStatements(
                PolicyStatement.Builder.create()
                        .effect(Effect.ALLOW)
                        .actions(List.of("lambda:GetEventSourceMapping"))
                        .resources(List.of(eventSource.getEventSourceMappingArn()))
                        .build(),
                PolicyStatement.Builder.create()
                        .effect(Effect.ALLOW)
                        .actions(List.of("lambda:UpdateEventSourceMapping"))
                        .resources(List.of(eventSource.getEventSourceMappingArn()))
                        .build());
        logGroup.grantRead(policiesStack.getReportingPolicyForGrants());
        logGroup.grant(policiesStack.getReportingPolicyForGrants(), "logs:StartQuery", "logs:GetQueryResults");
        configBucketStack.grantRead(handlerFunction);
        tableIndexStack.grantRead(handlerFunction);
        stateStoreStacks.grantReadWriteAllFilesAndPartitions(handlerFunction);
        compactionTracker.grantWriteJobEvent(handlerFunction);
        ingestTracker.grantWriteJobEvent(handlerFunction);
    }

    public void grantSendCommits(IGrantable grantee) {
        commitQueue.grantSendMessages(grantee);
    }

    private static AmiHardwareType lookupAmiHardwareType(InstanceArchitecture architecture) {
        switch (architecture) {
            case ARM_64:
                return AmiHardwareType.ARM;
            case X86_64:
                return AmiHardwareType.STANDARD;
            default:
                throw new IllegalArgumentException("Unrecognised architecture: " + architecture);
        }
    }

}
