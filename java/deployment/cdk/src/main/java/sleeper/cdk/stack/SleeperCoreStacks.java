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

package sleeper.cdk.stack;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awscdk.Stack;
import software.amazon.awscdk.services.cloudwatch.IMetric;
import software.amazon.awscdk.services.ec2.ISubnet;
import software.amazon.awscdk.services.ec2.IVpc;
import software.amazon.awscdk.services.ecs.ICluster;
import software.amazon.awscdk.services.iam.IGrantable;
import software.amazon.awscdk.services.iam.IRole;
import software.amazon.awscdk.services.iam.ManagedPolicy;
import software.amazon.awscdk.services.lambda.IFunction;
import software.amazon.awscdk.services.logs.ILogGroup;
import software.amazon.awscdk.services.s3.IBucket;
import software.amazon.awscdk.services.sns.Topic;
import software.amazon.awscdk.services.sqs.IQueue;
import software.amazon.awscdk.services.sqs.Queue;
import software.constructs.Construct;

import sleeper.cdk.SleeperInstanceProps;
import sleeper.cdk.jars.SleeperJarsInBucket;
import sleeper.cdk.networking.SleeperNetworking;
import sleeper.cdk.stack.compaction.CompactionTrackerResources;
import sleeper.cdk.stack.core.AutoDeleteS3ObjectsStack;
import sleeper.cdk.stack.core.AutoStopEcsClusterTasksStack;
import sleeper.cdk.stack.core.ConfigBucketStack;
import sleeper.cdk.stack.core.LoggingStack;
import sleeper.cdk.stack.core.LoggingStack.LogGroupRef;
import sleeper.cdk.stack.core.ManagedPoliciesStack;
import sleeper.cdk.stack.core.SleeperInstanceRoles;
import sleeper.cdk.stack.core.StateStoreCommitterStack;
import sleeper.cdk.stack.core.StateStoreStacks;
import sleeper.cdk.stack.core.TableDataStack;
import sleeper.cdk.stack.core.TableIndexStack;
import sleeper.cdk.stack.core.TopicStack;
import sleeper.cdk.stack.core.TransactionLogSnapshotStack;
import sleeper.cdk.stack.core.TransactionLogStateStoreStack;
import sleeper.cdk.stack.core.TransactionLogTransactionStack;
import sleeper.cdk.stack.core.VpcCheckStack;
import sleeper.cdk.stack.ingest.IngestTrackerResources;
import sleeper.cdk.util.TrackDeadLetters;
import sleeper.core.properties.instance.InstanceProperties;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Objects;

import static sleeper.core.properties.instance.CommonProperty.VPC_ENDPOINT_CHECK;

public class SleeperCoreStacks {
    public static final Logger LOGGER = LoggerFactory.getLogger(SleeperCoreStacks.class);

    private final SleeperNetworking networking;
    private final LoggingStack loggingStack;
    private final TrackDeadLetters deadLetters;
    private final ConfigBucketStack configBucketStack;
    private final TableIndexStack tableIndexStack;
    private final ManagedPoliciesStack policiesStack;
    private final StateStoreStacks stateStoreStacks;
    private final TableDataStack dataStack;
    private final StateStoreCommitterStack stateStoreCommitterStack;
    private final IngestTrackerResources ingestTracker;
    private final CompactionTrackerResources compactionTracker;
    private final AutoDeleteS3ObjectsStack autoDeleteS3Stack;
    private final AutoStopEcsClusterTasksStack autoStopEcsStack;
    private SleeperInstanceRoles roles;

    @SuppressWarnings("checkstyle:ParameterNumberCheck")
    private SleeperCoreStacks(
            SleeperNetworking networking, LoggingStack loggingStack, TrackDeadLetters deadLetters,
            ConfigBucketStack configBucketStack, TableIndexStack tableIndexStack,
            ManagedPoliciesStack policiesStack, StateStoreStacks stateStoreStacks, TableDataStack dataStack,
            StateStoreCommitterStack stateStoreCommitterStack,
            IngestTrackerResources ingestTracker,
            CompactionTrackerResources compactionTracker,
            AutoDeleteS3ObjectsStack autoDeleteS3Stack,
            AutoStopEcsClusterTasksStack autoStopEcsStack) {
        this.networking = networking;
        this.loggingStack = loggingStack;
        this.deadLetters = deadLetters;
        this.configBucketStack = configBucketStack;
        this.tableIndexStack = tableIndexStack;
        this.policiesStack = policiesStack;
        this.stateStoreStacks = stateStoreStacks;
        this.dataStack = dataStack;
        this.stateStoreCommitterStack = stateStoreCommitterStack;
        this.ingestTracker = ingestTracker;
        this.compactionTracker = compactionTracker;
        this.autoDeleteS3Stack = autoDeleteS3Stack;
        this.autoStopEcsStack = autoStopEcsStack;
    }

    public static SleeperCoreStacks create(Stack stack, SleeperInstanceProps props) {
        SleeperNetworking networking = props.getNetworkingProvider().getNetworking(stack);
        props.prepareProperties(stack, networking);
        LoggingStack loggingStack = new LoggingStack(stack, "Logging", props.getInstanceProperties());
        AutoDeleteS3ObjectsStack autoDeleteS3Stack = new AutoDeleteS3ObjectsStack(stack, "AutoDeleteS3Objects", props.getInstanceProperties(), props.getJars(), loggingStack);
        return create(stack, props, networking, loggingStack, autoDeleteS3Stack);
    }

    public static SleeperCoreStacks create(
            Construct scope, SleeperInstanceProps props, SleeperNetworking networking, LoggingStack loggingStack, AutoDeleteS3ObjectsStack autoDeleteS3Stack) {
        InstanceProperties instanceProperties = props.getInstanceProperties();

        SleeperJarsInBucket jars = props.getJars();
        if (instanceProperties.getBoolean(VPC_ENDPOINT_CHECK)) {
            new VpcCheckStack(scope, "Vpc", instanceProperties, jars, networking, loggingStack);
        } else {
            LOGGER.warn("Skipping VPC check as requested by the user. Be aware that VPCs that don't have an S3 endpoint can result "
                    + "in very significant NAT charges.");
        }
        TrackDeadLetters deadLetters = new TrackDeadLetters(instanceProperties,
                new TopicStack(scope, "Topic", instanceProperties));

        // Custom resource providers
        AutoStopEcsClusterTasksStack autoStopEcsStack = new AutoStopEcsClusterTasksStack(scope, "AutoStopEcsClusterTasks", instanceProperties, jars, loggingStack);
        ManagedPoliciesStack policiesStack = new ManagedPoliciesStack(scope, "Policies", instanceProperties);

        // Stacks for tables
        TableDataStack dataStack = new TableDataStack(scope, "TableData", instanceProperties, loggingStack, policiesStack, autoDeleteS3Stack, jars);
        TransactionLogStateStoreStack transactionLogStateStoreStack = new TransactionLogStateStoreStack(
                scope, "TransactionLogStateStore", instanceProperties, dataStack);
        StateStoreStacks stateStoreStacks = new StateStoreStacks(transactionLogStateStoreStack, policiesStack);
        IngestTrackerResources ingestTracker = IngestTrackerResources.from(
                scope, "IngestTracker", instanceProperties, policiesStack);
        CompactionTrackerResources compactionTracker = CompactionTrackerResources.from(
                scope, "CompactionTracker", instanceProperties, policiesStack);
        ConfigBucketStack configBucketStack = new ConfigBucketStack(scope, "Configuration", instanceProperties, loggingStack, policiesStack, autoDeleteS3Stack, jars);
        TableIndexStack tableIndexStack = new TableIndexStack(scope, "TableIndex", instanceProperties, policiesStack);
        StateStoreCommitterStack stateStoreCommitterStack = new StateStoreCommitterStack(scope, "StateStoreCommitter",
                instanceProperties, jars,
                loggingStack, configBucketStack, tableIndexStack,
                stateStoreStacks, ingestTracker, compactionTracker,
                policiesStack, deadLetters);

        SleeperCoreStacks stacks = new SleeperCoreStacks(networking, loggingStack, deadLetters,
                configBucketStack, tableIndexStack, policiesStack, stateStoreStacks, dataStack,
                stateStoreCommitterStack, ingestTracker, compactionTracker, autoDeleteS3Stack, autoStopEcsStack);

        // Table state store maintenance
        new TransactionLogSnapshotStack(scope, "TransactionLogSnapshot",
                props, stacks, transactionLogStateStoreStack, deadLetters);
        new TransactionLogTransactionStack(scope, "TransactionLogTransaction",
                props, stacks, transactionLogStateStoreStack, deadLetters);
        return stacks;
    }

    public SleeperNetworking networking() {
        return networking;
    }

    public IVpc getVpc() {
        return networking.vpc();
    }

    public List<ISubnet> getSubnets() {
        return networking.subnets();
    }

    public Topic getAlertsTopic() {
        return deadLetters.getTopic();
    }

    public void alarmOnDeadLetters(Construct scope, String id, String description, Queue dlq) {
        deadLetters.alarmOnDeadLetters(scope, id, description, dlq);
    }

    public List<IMetric> getErrorMetrics() {
        return deadLetters.getErrorMetrics();
    }

    public LoggingStack getLoggingStack() {
        return loggingStack;
    }

    public ILogGroup getLogGroup(LogGroupRef logGroupRef) {
        return loggingStack.getLogGroup(logGroupRef);
    }

    public void grantReadInstanceConfig(IGrantable grantee) {
        configBucketStack.grantRead(grantee);
    }

    public void grantWriteInstanceConfig(IGrantable grantee) {
        configBucketStack.grantWrite(grantee);
    }

    public void grantReadTablesConfig(IGrantable grantee) {
        configBucketStack.grantRead(grantee);
        tableIndexStack.grantRead(grantee);
    }

    public void grantReadTableDataBucket(IGrantable grantee) {
        dataStack.grantRead(grantee);
    }

    public void grantReadTablesAndData(IGrantable grantee) {
        configBucketStack.grantRead(grantee);
        tableIndexStack.grantRead(grantee);
        stateStoreStacks.grantReadFileReferencesAndPartitions(grantee);
        dataStack.grantRead(grantee);
    }

    public void grantReadTablesMetadata(IGrantable grantee) {
        configBucketStack.grantRead(grantee);
        tableIndexStack.grantRead(grantee);
        stateStoreStacks.grantReadFileReferencesAndPartitions(grantee);
    }

    public void grantReadTablesStatus(IGrantable grantee) {
        configBucketStack.grantRead(grantee);
        tableIndexStack.grantRead(grantee);
    }

    public void addAutoDeleteS3Objects(Construct scope, IBucket bucket) {
        autoDeleteS3Stack.addAutoDeleteS3Objects(scope, bucket);
    }

    public void addAutoStopEcsClusterTasksAfterTaskCreatorIsDeleted(Construct scope, ICluster cluster, IFunction taskCreator) {
        autoStopEcsStack.addAutoStopEcsClusterTasksAfterTaskCreatorIsDeleted(scope, cluster, taskCreator);
    }

    public AutoStopEcsClusterTasksStack getAutoStopEcsStack() {
        return autoStopEcsStack;
    }

    // The Lambda IFunction.getRole method is annotated as nullable, even though it will never return null in practice.
    // This means SpotBugs complains if we pass that role into attachToRole.
    // The role parameter is marked as nullable to convince SpotBugs that it's fine to pass it into this method,
    // even though attachToRole really requires the role to be non-null.
    @SuppressFBWarnings("NP_PARAMETER_MUST_BE_NONNULL_BUT_MARKED_AS_NULLABLE")
    public void grantValidateBulkImport(@Nullable IRole nullableRole) {
        IRole grantee = Objects.requireNonNull(nullableRole);
        configBucketStack.grantRead(grantee);
        tableIndexStack.grantRead(grantee);
        stateStoreStacks.grantReadPartitions(grantee);
        policiesStack.grantReadIngestSources(grantee);
        ingestTracker.grantWriteJobEvent(grantee);
    }

    public void grantIngest(IRole grantee) {
        configBucketStack.grantRead(grantee);
        tableIndexStack.grantRead(grantee);
        stateStoreStacks.grantReadPartitionsReadWriteFileReferences(grantee);
        dataStack.grantReadWrite(grantee);
        policiesStack.grantReadIngestSources(grantee);
        stateStoreCommitterStack.grantSendCommits(grantee);
        ingestTracker.grantWriteJobEvent(grantee);
        ingestTracker.grantWriteTaskEvent(grantee);
    }

    public void grantGarbageCollection(IGrantable grantee) {
        configBucketStack.grantRead(grantee);
        tableIndexStack.grantRead(grantee);
        stateStoreStacks.grantReadWriteUnreferencedFiles(grantee);
        dataStack.grantReadDelete(grantee);
        stateStoreCommitterStack.grantSendCommits(grantee);
    }

    public void grantReadWritePartitions(IGrantable grantee) {
        stateStoreStacks.grantReadWritePartitions(grantee);
    }

    public void grantCreateCompactionJobs(IGrantable grantee) {
        configBucketStack.grantRead(grantee);
        tableIndexStack.grantRead(grantee);
        stateStoreStacks.grantReadPartitionsReadWriteFileReferences(grantee);
        compactionTracker.grantWriteJobEvent(grantee);
        stateStoreCommitterStack.grantSendCommits(grantee);
    }

    public void grantRunCompactionJobs(IGrantable grantee) {
        configBucketStack.grantRead(grantee);
        tableIndexStack.grantRead(grantee);
        stateStoreStacks.grantReadWriteFileReferencesAndUnreferenced(grantee);
        stateStoreStacks.grantReadPartitions(grantee);
        dataStack.grantReadWrite(grantee);
        stateStoreCommitterStack.grantSendCommits(grantee);
        compactionTracker.grantWriteJobEvent(grantee);
        compactionTracker.grantWriteTaskEvent(grantee);
    }

    public void grantSplitPartitions(IGrantable grantee) {
        configBucketStack.grantRead(grantee);
        tableIndexStack.grantRead(grantee);
        stateStoreStacks.grantReadWritePartitions(grantee);
        dataStack.grantRead(grantee);
        stateStoreCommitterStack.grantSendCommits(grantee);
    }

    // Needed to write transaction body to S3
    public void grantSendStateStoreCommits(IGrantable grantee) {
        configBucketStack.grantRead(grantee);
        stateStoreCommitterStack.grantSendCommits(grantee);
        dataStack.grantReadWrite(grantee); // Needed to write transaction body to S3
    }

    public void grantUpdateJobTrackersFromTransactionLog(IGrantable grantee) {
        configBucketStack.grantRead(grantee);
        tableIndexStack.grantRead(grantee);
        stateStoreStacks.grantReadFileReferencesAndPartitions(grantee);
        stateStoreStacks.grantReadAllSnapshotsTable(grantee);
        compactionTracker.grantWriteJobEvent(grantee);
        ingestTracker.grantWriteJobEvent(grantee);
    }

    // The Lambda IFunction.getRole method is annotated as nullable, even though it will never return null in practice.
    // This means SpotBugs complains if we pass that role into attachToRole.
    // The role parameter is marked as nullable to convince SpotBugs that it's fine to pass it into this method,
    // even though attachToRole really requires the role to be non-null.
    @SuppressFBWarnings("NP_PARAMETER_MUST_BE_NONNULL_BUT_MARKED_AS_NULLABLE")
    public void grantReadIngestSources(@Nullable IRole nullableRole) {
        IRole grantee = Objects.requireNonNull(nullableRole);
        policiesStack.grantReadIngestSources(grantee);
    }

    public IGrantable getIngestByQueuePolicyForGrants() {
        return policiesStack.getIngestByQueuePolicyForGrants();
    }

    public IGrantable getQueryPolicyForGrants() {
        return policiesStack.getQueryPolicyForGrants();
    }

    public ManagedPolicy getReportingPolicyForGrants() {
        return policiesStack.getReportingPolicyForGrants();
    }

    public void grantInvokeScheduled(IFunction function) {
        policiesStack.grantInvokeScheduled(function);
    }

    public void grantInvokeScheduled(IFunction triggerFunction, IQueue invokeQueue) {
        policiesStack.grantInvokeScheduled(triggerFunction, invokeQueue);
    }

    public ManagedPolicy getInvokeCompactionPolicyForGrants() {
        return policiesStack.getInvokeCompactionPolicyForGrants();
    }

    public IGrantable getPurgeQueuesPolicyForGrants() {
        return policiesStack.getPurgeQueuesPolicyForGrants();
    }

    public void createRoles() {
        roles = policiesStack.createRoles();
    }

    public SleeperInstanceRoles getRoles() {
        return roles;
    }
}
