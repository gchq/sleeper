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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awscdk.App;
import software.amazon.awscdk.Stack;
import software.amazon.awscdk.StackProps;
import software.amazon.awscdk.Tags;
import software.amazon.awscdk.services.cloudwatch.IMetric;
import software.constructs.Construct;

import sleeper.cdk.jars.SleeperJarsInBucket;
import sleeper.cdk.stack.compaction.CompactionTrackerResources;
import sleeper.cdk.stack.core.AutoDeleteS3ObjectsStack;
import sleeper.cdk.stack.core.AutoStopEcsClusterTasksStack;
import sleeper.cdk.stack.core.ConfigBucketStack;
import sleeper.cdk.stack.core.LoggingStack;
import sleeper.cdk.stack.core.ManagedPoliciesStack;
import sleeper.cdk.stack.core.PropertiesStack;
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
import sleeper.core.deploy.DeployInstanceConfiguration;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;

import java.util.ArrayList;
import java.util.List;

import static sleeper.core.properties.instance.CommonProperty.VPC_ENDPOINT_CHECK;

/**
 * Deploys an instance of Sleeper, including any configured optional stacks. Does not create Sleeper tables. If the
 * configuration for tables is provided then the optional DashboardStack will create individual dashboards for each
 * table.
 */
public class SleeperInstanceStack extends Stack {
    public static final Logger LOGGER = LoggerFactory.getLogger(SleeperInstanceStack.class);

    private final InstanceProperties instanceProperties;
    private final List<TableProperties> tableProperties;
    private final SleeperJarsInBucket jars;
    private final App app;
    private SleeperCoreStacks coreStacks;
    private AutoDeleteS3ObjectsStack autoDeleteS3ObjectsStack;
    private AutoStopEcsClusterTasksStack autoStopEcsClusterTasksStack;
    private LoggingStack loggingStack;

    // These flags are used to control when the stacks are deployed in the SystemTest CDK app.
    private boolean generateAutoDeleteS3ObjectsStack = true;
    private boolean generateLoggingStack = true;
    private boolean generateProperties = true;

    public SleeperInstanceStack(App app, String id, StackProps props, DeployInstanceConfiguration configuration, SleeperJarsInBucket jars) {
        super(app, id, props);
        this.instanceProperties = configuration.getInstanceProperties();
        this.tableProperties = configuration.getTableProperties();
        this.jars = jars;
        this.app = app;
    }

    @SuppressWarnings("checkstyle:methodlength")
    public void create() {

        List<IMetric> errorMetrics = new ArrayList<>();

        // Logging stack
        generateLoggingStack();

        // Stack for Checking VPC configuration
        if (instanceProperties.getBoolean(VPC_ENDPOINT_CHECK)) {
            new VpcCheckStack(this, "Vpc", instanceProperties, jars, loggingStack);
        } else {
            LOGGER.warn("Skipping VPC check as requested by the user. Be aware that VPCs that don't have an S3 endpoint can result "
                    + "in very significant NAT charges.");
        }

        // Auto delete s3 objects Stack
        generateAutoDeleteS3ObjectsStack();

        // Topic stack
        TopicStack topicStack = new TopicStack(this, "Topic", instanceProperties);

        // Auto stop ECS cluster tasks stack
        autoStopEcsClusterTasksStack = new AutoStopEcsClusterTasksStack(this, "AutoStopEcsClusterTasks", instanceProperties, jars, loggingStack);

        // Stacks for tables
        ManagedPoliciesStack policiesStack = new ManagedPoliciesStack(this, "Policies", instanceProperties);
        TableDataStack dataStack = new TableDataStack(this, "TableData", instanceProperties, loggingStack, policiesStack, autoDeleteS3ObjectsStack, jars);
        TransactionLogStateStoreStack transactionLogStateStoreStack = new TransactionLogStateStoreStack(
                this, "TransactionLogStateStore", instanceProperties, dataStack);
        StateStoreStacks stateStoreStacks = new StateStoreStacks(transactionLogStateStoreStack, policiesStack);
        IngestTrackerResources ingestTracker = IngestTrackerResources.from(
                this, "IngestTracker", instanceProperties, policiesStack);
        CompactionTrackerResources compactionTracker = CompactionTrackerResources.from(
                this, "CompactionTracker", instanceProperties, policiesStack);
        ConfigBucketStack configBucketStack = new ConfigBucketStack(this, "Configuration", instanceProperties, loggingStack, policiesStack, autoDeleteS3ObjectsStack, jars);
        TableIndexStack tableIndexStack = new TableIndexStack(this, "TableIndex", instanceProperties, policiesStack);
        StateStoreCommitterStack stateStoreCommitterStack = new StateStoreCommitterStack(this, "StateStoreCommitter",
                instanceProperties, jars,
                loggingStack, configBucketStack, tableIndexStack,
                stateStoreStacks, ingestTracker, compactionTracker,
                policiesStack, topicStack.getTopic(), errorMetrics);
        coreStacks = new SleeperCoreStacks(
                loggingStack, topicStack, errorMetrics, configBucketStack, tableIndexStack, policiesStack, stateStoreStacks, dataStack,
                stateStoreCommitterStack, ingestTracker, compactionTracker, autoDeleteS3ObjectsStack, autoStopEcsClusterTasksStack);

        new TransactionLogSnapshotStack(this, "TransactionLogSnapshot",
                instanceProperties, jars, coreStacks, transactionLogStateStoreStack, topicStack.getTopic(), errorMetrics);
        new TransactionLogTransactionStack(this, "TransactionLogTransaction",
                instanceProperties, jars, coreStacks, transactionLogStateStoreStack, topicStack.getTopic(), errorMetrics);

        SleeperOptionalStacks.create(this, instanceProperties, tableProperties, jars, coreStacks);

        // Only create roles after we know which policies are deployed in the instance
        policiesStack.createRoles();

        this.generateProperties();
        addTags(app);
    }

    private void addTags(Construct construct) {
        instanceProperties.getTags()
                .forEach((key, value) -> Tags.of(construct).add(key, value));
    }

    protected void generateProperties() {
        // Stack for writing properties
        if (generateProperties) {
            new PropertiesStack(this, "Properties", instanceProperties, jars, coreStacks);
        }
    }

    protected void generateAutoDeleteS3ObjectsStack() {
        if (generateAutoDeleteS3ObjectsStack) {
            autoDeleteS3ObjectsStack = new AutoDeleteS3ObjectsStack(this, "AutoDeleteS3Objects", instanceProperties, jars, loggingStack);
        }
    }

    protected void generateLoggingStack() {
        if (generateLoggingStack) {
            loggingStack = new LoggingStack(this, "Logging", instanceProperties);
        }
    }

    protected void setGenerateProperties(Boolean generateProperties) {
        this.generateProperties = generateProperties;
    }

    protected void setGenerateLoggingStack(Boolean generateLoggingStack) {
        this.generateLoggingStack = generateLoggingStack;
    }

    protected void setGenerateAutoDeleteS3ObjectsStack(Boolean generateAutoDeleteS3ObjectsStack) {
        this.generateAutoDeleteS3ObjectsStack = generateAutoDeleteS3ObjectsStack;
    }

    protected AutoDeleteS3ObjectsStack getAutoDeleteS3ObjectsStack() {
        return autoDeleteS3ObjectsStack;
    }

    protected AutoStopEcsClusterTasksStack getAutoStopEcsClusterTasksStack() {
        return autoStopEcsClusterTasksStack;
    }

}
