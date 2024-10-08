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
package sleeper.cdk;

import software.amazon.awscdk.App;
import software.amazon.awscdk.AppProps;
import software.amazon.awscdk.Environment;
import software.amazon.awscdk.Stack;
import software.amazon.awscdk.StackProps;
import software.amazon.awscdk.Tags;
import software.amazon.awscdk.services.cloudwatch.IMetric;
import software.amazon.awssdk.services.s3.S3Client;
import software.constructs.Construct;

import sleeper.cdk.jars.BuiltJars;
import sleeper.cdk.stack.AthenaStack;
import sleeper.cdk.stack.CompactionStack;
import sleeper.cdk.stack.CompactionStatusStoreResources;
import sleeper.cdk.stack.CompactionStatusStoreStack;
import sleeper.cdk.stack.ConfigBucketStack;
import sleeper.cdk.stack.CoreStacks;
import sleeper.cdk.stack.DashboardStack;
import sleeper.cdk.stack.DynamoDBStateStoreStack;
import sleeper.cdk.stack.GarbageCollectorStack;
import sleeper.cdk.stack.IngestBatcherStack;
import sleeper.cdk.stack.IngestStack;
import sleeper.cdk.stack.IngestStacks;
import sleeper.cdk.stack.IngestStatusStoreResources;
import sleeper.cdk.stack.IngestStatusStoreStack;
import sleeper.cdk.stack.InstanceRolesStack;
import sleeper.cdk.stack.KeepLambdaWarmStack;
import sleeper.cdk.stack.ManagedPoliciesStack;
import sleeper.cdk.stack.PartitionSplittingStack;
import sleeper.cdk.stack.PropertiesStack;
import sleeper.cdk.stack.QueryQueueStack;
import sleeper.cdk.stack.QueryStack;
import sleeper.cdk.stack.S3StateStoreStack;
import sleeper.cdk.stack.StateStoreCommitterStack;
import sleeper.cdk.stack.StateStoreStacks;
import sleeper.cdk.stack.TableDataStack;
import sleeper.cdk.stack.TableIndexStack;
import sleeper.cdk.stack.TableMetricsStack;
import sleeper.cdk.stack.TopicStack;
import sleeper.cdk.stack.TransactionLogSnapshotStack;
import sleeper.cdk.stack.TransactionLogStateStoreStack;
import sleeper.cdk.stack.TransactionLogTransactionStack;
import sleeper.cdk.stack.VpcStack;
import sleeper.cdk.stack.WebSocketQueryStack;
import sleeper.cdk.stack.bulkimport.BulkImportBucketStack;
import sleeper.cdk.stack.bulkimport.CommonEmrBulkImportStack;
import sleeper.cdk.stack.bulkimport.EksBulkImportStack;
import sleeper.cdk.stack.bulkimport.EmrBulkImportStack;
import sleeper.cdk.stack.bulkimport.EmrServerlessBulkImportStack;
import sleeper.cdk.stack.bulkimport.EmrStudioStack;
import sleeper.cdk.stack.bulkimport.PersistentEmrBulkImportStack;
import sleeper.cdk.util.Utils;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.validation.OptionalStack;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static java.util.stream.Collectors.toUnmodifiableSet;
import static sleeper.core.properties.instance.CommonProperty.ACCOUNT;
import static sleeper.core.properties.instance.CommonProperty.ID;
import static sleeper.core.properties.instance.CommonProperty.JARS_BUCKET;
import static sleeper.core.properties.instance.CommonProperty.OPTIONAL_STACKS;
import static sleeper.core.properties.instance.CommonProperty.REGION;

/**
 * Deploys an instance of Sleeper, including any configured optional stacks.
 */
public class SleeperCdkApp extends Stack {
    private final InstanceProperties instanceProperties;
    private final BuiltJars jars;
    private final App app;
    private CoreStacks coreStacks;
    private IngestStacks ingestStacks;
    private IngestStack ingestStack;
    private IngestBatcherStack ingestBatcherStack;
    private CompactionStack compactionStack;
    private PartitionSplittingStack partitionSplittingStack;
    private BulkImportBucketStack bulkImportBucketStack;
    private CommonEmrBulkImportStack emrBulkImportCommonStack;
    private EmrBulkImportStack emrBulkImportStack;
    private EmrServerlessBulkImportStack emrServerlessBulkImportStack;
    private PersistentEmrBulkImportStack persistentEmrBulkImportStack;
    private EksBulkImportStack eksBulkImportStack;
    private QueryQueueStack queryQueueStack;

    public SleeperCdkApp(App app, String id, StackProps props, InstanceProperties instanceProperties, BuiltJars jars) {
        super(app, id, props);
        this.app = app;
        this.instanceProperties = instanceProperties;
        this.jars = jars;
    }

    @SuppressWarnings("checkstyle:methodlength")
    public void create() {
        // Optional stacks to be included
        Set<OptionalStack> optionalStacks = instanceProperties
                .streamEnumList(OPTIONAL_STACKS, OptionalStack.class)
                .collect(toUnmodifiableSet());

        List<IMetric> errorMetrics = new ArrayList<>();
        // Stack for Checking VPC configuration
        new VpcStack(this, "Vpc", instanceProperties, jars);

        // Topic stack
        TopicStack topicStack = new TopicStack(this, "Topic", instanceProperties);

        // Stacks for tables
        ManagedPoliciesStack policiesStack = new ManagedPoliciesStack(this, "Policies", instanceProperties);
        TableDataStack dataStack = new TableDataStack(this, "TableData", instanceProperties, policiesStack, jars);
        TransactionLogStateStoreStack transactionLogStateStoreStack = new TransactionLogStateStoreStack(
                this, "TransactionLogStateStore", instanceProperties, dataStack);
        StateStoreStacks stateStoreStacks = new StateStoreStacks(
                new DynamoDBStateStoreStack(this, "DynamoDBStateStore", instanceProperties),
                new S3StateStoreStack(this, "S3StateStore", instanceProperties, dataStack),
                transactionLogStateStoreStack, policiesStack);
        IngestStatusStoreResources ingestStatusStore = new IngestStatusStoreStack(this, "IngestStatusStore",
                instanceProperties, policiesStack).getResources();
        CompactionStatusStoreResources compactionStatusStore = new CompactionStatusStoreStack(this, "CompactionStatusStore",
                instanceProperties, policiesStack).getResources();
        ConfigBucketStack configBucketStack = new ConfigBucketStack(this, "Configuration", instanceProperties, policiesStack, jars);
        TableIndexStack tableIndexStack = new TableIndexStack(this, "TableIndex", instanceProperties, policiesStack);
        StateStoreCommitterStack stateStoreCommitterStack = new StateStoreCommitterStack(this, "StateStoreCommitter",
                instanceProperties, jars,
                configBucketStack, tableIndexStack,
                stateStoreStacks, ingestStatusStore, compactionStatusStore,
                policiesStack, topicStack.getTopic(), errorMetrics);
        coreStacks = new CoreStacks(
                configBucketStack, tableIndexStack, policiesStack, stateStoreStacks, dataStack,
                stateStoreCommitterStack, ingestStatusStore, compactionStatusStore);

        new TransactionLogSnapshotStack(this, "TransactionLogSnapshot",
                instanceProperties, jars, coreStacks, transactionLogStateStoreStack, topicStack.getTopic(), errorMetrics);
        new TransactionLogTransactionStack(this, "TransactionLogTransaction",
                instanceProperties, jars, coreStacks, transactionLogStateStoreStack, topicStack.getTopic(), errorMetrics);
        if (optionalStacks.contains(OptionalStack.TableMetricsStack)) {
            new TableMetricsStack(this, "TableMetrics", instanceProperties, jars, topicStack.getTopic(), coreStacks, errorMetrics);
        }

        // Stack for Athena analytics
        if (optionalStacks.contains(OptionalStack.AthenaStack)) {
            new AthenaStack(this, "Athena", instanceProperties, jars, coreStacks);
        }

        if (OptionalStack.BULK_IMPORT_STACKS.stream().anyMatch(optionalStacks::contains)) {
            bulkImportBucketStack = new BulkImportBucketStack(this, "BulkImportBucket", instanceProperties, coreStacks, jars);
        }
        if (OptionalStack.EMR_BULK_IMPORT_STACKS.stream().anyMatch(optionalStacks::contains)) {
            emrBulkImportCommonStack = new CommonEmrBulkImportStack(this, "BulkImportEMRCommon",
                    instanceProperties, coreStacks, bulkImportBucketStack);
        }

        // Stack to run bulk import jobs via EMR Serverless
        if (optionalStacks.contains(OptionalStack.EmrServerlessBulkImportStack)) {
            emrServerlessBulkImportStack = new EmrServerlessBulkImportStack(this, "BulkImportEMRServerless",
                    instanceProperties, jars,
                    topicStack.getTopic(),
                    bulkImportBucketStack,
                    coreStacks,
                    errorMetrics);

            // Stack to created EMR studio to be used to access EMR Serverless
            if (optionalStacks.contains(OptionalStack.EmrStudioStack)) {
                new EmrStudioStack(this, "EmrStudio", instanceProperties);
            }
        }
        // Stack to run bulk import jobs via EMR (one cluster per bulk import job)
        if (optionalStacks.contains(OptionalStack.EmrBulkImportStack)) {
            emrBulkImportStack = new EmrBulkImportStack(this, "BulkImportEMR",
                    instanceProperties, jars,
                    topicStack.getTopic(),
                    bulkImportBucketStack,
                    emrBulkImportCommonStack,
                    coreStacks,
                    errorMetrics);
        }

        // Stack to run bulk import jobs via a persistent EMR cluster
        if (optionalStacks.contains(OptionalStack.PersistentEmrBulkImportStack)) {
            persistentEmrBulkImportStack = new PersistentEmrBulkImportStack(this, "BulkImportPersistentEMR",
                    instanceProperties, jars,
                    topicStack.getTopic(),
                    bulkImportBucketStack,
                    emrBulkImportCommonStack,
                    coreStacks,
                    errorMetrics);
        }

        // Stack to run bulk import jobs via EKS
        if (optionalStacks.contains(OptionalStack.EksBulkImportStack)) {
            eksBulkImportStack = new EksBulkImportStack(this, "BulkImportEKS",
                    instanceProperties, jars,
                    topicStack.getTopic(),
                    bulkImportBucketStack,
                    coreStacks,
                    errorMetrics);
        }

        // Stack to garbage collect old files
        if (optionalStacks.contains(OptionalStack.GarbageCollectorStack)) {
            new GarbageCollectorStack(this,
                    "GarbageCollector",
                    instanceProperties, jars,
                    topicStack.getTopic(),
                    coreStacks,
                    errorMetrics);
        }
        // Stack for containers for compactions and splitting compactions
        if (optionalStacks.contains(OptionalStack.CompactionStack)) {
            compactionStack = new CompactionStack(this,
                    "Compaction",
                    instanceProperties, jars,
                    topicStack.getTopic(),
                    coreStacks,
                    errorMetrics);
        }

        // Stack to split partitions
        if (optionalStacks.contains(OptionalStack.PartitionSplittingStack)) {
            partitionSplittingStack = new PartitionSplittingStack(this,
                    "PartitionSplitting",
                    instanceProperties, jars,
                    topicStack.getTopic(),
                    coreStacks,
                    errorMetrics);
        }

        QueryStack queryStack = null;
        // Stack to execute queries
        if (OptionalStack.QUERY_STACKS.stream().anyMatch(optionalStacks::contains)) {
            queryQueueStack = new QueryQueueStack(this, "QueryQueue",
                    instanceProperties,
                    topicStack.getTopic(), coreStacks,
                    errorMetrics);
            queryStack = new QueryStack(this,
                    "Query",
                    instanceProperties, jars,
                    topicStack.getTopic(),
                    coreStacks, queryQueueStack,
                    errorMetrics);
            // Stack to execute queries using the web socket API
            if (optionalStacks.contains(OptionalStack.WebSocketQueryStack)) {
                new WebSocketQueryStack(this,
                        "WebSocketQuery",
                        instanceProperties, jars,
                        coreStacks, queryQueueStack, queryStack);
            }
        }
        // Stack for ingest jobs
        if (optionalStacks.contains(OptionalStack.IngestStack)) {
            ingestStack = new IngestStack(this,
                    "Ingest",
                    instanceProperties, jars,
                    topicStack.getTopic(),
                    coreStacks,
                    errorMetrics);
        }

        // Aggregate ingest stacks
        ingestStacks = new IngestStacks(ingestStack, emrBulkImportStack, persistentEmrBulkImportStack, eksBulkImportStack, emrServerlessBulkImportStack);

        // Stack to batch up files to ingest and create jobs
        if (optionalStacks.contains(OptionalStack.IngestBatcherStack)) {
            ingestBatcherStack = new IngestBatcherStack(this, "IngestBatcher",
                    instanceProperties, jars,
                    topicStack.getTopic(),
                    coreStacks,
                    ingestStacks,
                    errorMetrics);
        }

        if (optionalStacks.contains(OptionalStack.DashboardStack)) {
            new DashboardStack(this,
                    "Dashboard",
                    ingestStack,
                    compactionStack,
                    partitionSplittingStack,
                    instanceProperties,
                    errorMetrics);
        }

        if (optionalStacks.contains(OptionalStack.KeepLambdaWarmStack)) {
            new KeepLambdaWarmStack(this,
                    "KeepLambdaWarmExecution",
                    instanceProperties,
                    jars,
                    coreStacks,
                    queryQueueStack);
        }

        // Only create instance admin role after we know which policies are deployed in the instance
        new InstanceRolesStack(this, "InstanceRoles", instanceProperties, policiesStack);

        this.generateProperties();
        addTags(app);
    }

    protected InstanceProperties getInstanceProperties() {
        return instanceProperties;
    }

    public CoreStacks getCoreStacks() {
        return coreStacks;
    }

    public IngestStacks getIngestStacks() {
        return ingestStacks;
    }

    public IngestBatcherStack getIngestBatcherStack() {
        return ingestBatcherStack;
    }

    private void addTags(Construct construct) {
        instanceProperties.getTags()
                .forEach((key, value) -> Tags.of(construct).add(key, value));
    }

    protected void generateProperties() {
        // Stack for writing properties
        new PropertiesStack(this, "Properties", instanceProperties, jars, coreStacks);
    }

    public static void main(String[] args) {
        App app = new App(AppProps.builder()
                .analyticsReporting(false)
                .build());

        InstanceProperties instanceProperties = Utils.loadInstanceProperties(InstanceProperties::createWithoutValidation, app);

        String id = instanceProperties.get(ID);
        Environment environment = Environment.builder()
                .account(instanceProperties.get(ACCOUNT))
                .region(instanceProperties.get(REGION))
                .build();
        try (S3Client s3Client = S3Client.create()) {
            BuiltJars jars = new BuiltJars(s3Client, instanceProperties.get(JARS_BUCKET));

            new SleeperCdkApp(app, id, StackProps.builder()
                    .stackName(id)
                    .env(environment)
                    .build(),
                    instanceProperties, jars).create();

            app.synth();
        }
    }
}
