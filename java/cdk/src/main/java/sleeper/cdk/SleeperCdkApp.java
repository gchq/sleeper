/*
 * Copyright 2022-2023 Crown Copyright
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

import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import software.amazon.awscdk.App;
import software.amazon.awscdk.Environment;
import software.amazon.awscdk.Stack;
import software.amazon.awscdk.StackProps;
import software.amazon.awscdk.Tags;
import software.constructs.Construct;

import sleeper.cdk.jars.BuiltJars;
import sleeper.cdk.stack.AthenaStack;
import sleeper.cdk.stack.CompactionStack;
import sleeper.cdk.stack.ConfigBucketStack;
import sleeper.cdk.stack.CoreStacks;
import sleeper.cdk.stack.DashboardStack;
import sleeper.cdk.stack.DynamoDBStateStoreStack;
import sleeper.cdk.stack.GarbageCollectorStack;
import sleeper.cdk.stack.IngestBatcherStack;
import sleeper.cdk.stack.IngestSourceBucketsStack;
import sleeper.cdk.stack.IngestStack;
import sleeper.cdk.stack.IngestStatusStoreStack;
import sleeper.cdk.stack.ManagedPoliciesStack;
import sleeper.cdk.stack.PartitionSplittingStack;
import sleeper.cdk.stack.PropertiesStack;
import sleeper.cdk.stack.QueryStack;
import sleeper.cdk.stack.S3StateStoreStack;
import sleeper.cdk.stack.StateStoreStacks;
import sleeper.cdk.stack.TableDataStack;
import sleeper.cdk.stack.TableIndexStack;
import sleeper.cdk.stack.TableMetricsStack;
import sleeper.cdk.stack.TopicStack;
import sleeper.cdk.stack.VpcStack;
import sleeper.cdk.stack.bulkimport.BulkImportBucketStack;
import sleeper.cdk.stack.bulkimport.CommonEmrBulkImportStack;
import sleeper.cdk.stack.bulkimport.EksBulkImportStack;
import sleeper.cdk.stack.bulkimport.EmrBulkImportStack;
import sleeper.cdk.stack.bulkimport.EmrServerlessBulkImportStack;
import sleeper.cdk.stack.bulkimport.EmrStudioStack;
import sleeper.cdk.stack.bulkimport.PersistentEmrBulkImportStack;
import sleeper.configuration.properties.instance.InstanceProperties;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static sleeper.configuration.properties.instance.CommonProperty.ACCOUNT;
import static sleeper.configuration.properties.instance.CommonProperty.ID;
import static sleeper.configuration.properties.instance.CommonProperty.JARS_BUCKET;
import static sleeper.configuration.properties.instance.CommonProperty.OPTIONAL_STACKS;
import static sleeper.configuration.properties.instance.CommonProperty.REGION;

/**
 * The {@link App} that deploys all the Sleeper stacks.
 */
public class SleeperCdkApp extends Stack {
    private final InstanceProperties instanceProperties;
    private final BuiltJars jars;
    private final App app;
    private CoreStacks coreStacks;
    private IngestStack ingestStack;
    private CompactionStack compactionStack;
    private PartitionSplittingStack partitionSplittingStack;
    private BulkImportBucketStack bulkImportBucketStack;
    private CommonEmrBulkImportStack emrBulkImportCommonStack;
    private EmrBulkImportStack emrBulkImportStack;
    private EmrServerlessBulkImportStack emrServerlessBulkImportStack;
    private EmrStudioStack emrStudioStack;
    private PersistentEmrBulkImportStack persistentEmrBulkImportStack;
    private EksBulkImportStack eksBulkImportStack;
    private IngestStatusStoreStack ingestStatusStoreStack;

    public SleeperCdkApp(App app, String id, StackProps props, InstanceProperties instanceProperties, BuiltJars jars) {
        super(app, id, props);
        this.app = app;
        this.instanceProperties = instanceProperties;
        this.jars = jars;
    }

    private static final List<String> BULK_IMPORT_STACK_NAMES = Stream.of(
                    EmrBulkImportStack.class,
                    EmrServerlessBulkImportStack.class,
                    PersistentEmrBulkImportStack.class,
                    EksBulkImportStack.class)
            .map(Class::getSimpleName).collect(Collectors.toList());

    private static final List<String> EMR_BULK_IMPORT_STACK_NAMES = Stream.of(
                    EmrBulkImportStack.class,
                    EmrServerlessBulkImportStack.class,
                    PersistentEmrBulkImportStack.class)
            .map(Class::getSimpleName).collect(Collectors.toList());

    public static final List<String> INGEST_STACK_NAMES = Stream.of(
                    IngestStack.class,
                    EmrBulkImportStack.class,
                    EmrServerlessBulkImportStack.class,
                    PersistentEmrBulkImportStack.class,
                    EksBulkImportStack.class)
            .map(Class::getSimpleName).collect(Collectors.toList());

    public void create() {
        // Optional stacks to be included
        List<String> optionalStacks = instanceProperties.getList(OPTIONAL_STACKS);

        // Stack for Checking VPC configuration
        new VpcStack(this, "Vpc", instanceProperties, jars);

        // Topic stack
        TopicStack topicStack = new TopicStack(this, "Topic", instanceProperties);

        // Stacks for tables
        ManagedPoliciesStack policiesStack = new ManagedPoliciesStack(this, "Policies", instanceProperties);
        TableDataStack dataStack = new TableDataStack(this, "TableData", instanceProperties, policiesStack);
        StateStoreStacks stateStoreStacks = new StateStoreStacks(
                new DynamoDBStateStoreStack(this, "DynamoDBStateStore", instanceProperties, policiesStack),
                new S3StateStoreStack(this, "S3StateStore", instanceProperties, dataStack, policiesStack));
        coreStacks = new CoreStacks(
                new ConfigBucketStack(this, "Configuration", instanceProperties, policiesStack),
                new TableIndexStack(this, "TableIndex", instanceProperties, policiesStack),
                new IngestSourceBucketsStack(this, "SourceBuckets", instanceProperties),
                policiesStack, stateStoreStacks, dataStack);
        new TableMetricsStack(this, "TableMetrics", instanceProperties, jars, coreStacks);

        // Stack for Athena analytics
        if (optionalStacks.contains(AthenaStack.class.getSimpleName())) {
            new AthenaStack(this, "Athena", instanceProperties, jars, coreStacks);
        }

        if (INGEST_STACK_NAMES.stream().anyMatch(optionalStacks::contains)) {
            ingestStatusStoreStack = new IngestStatusStoreStack(this, "IngestStatusStore", instanceProperties);
        }
        if (BULK_IMPORT_STACK_NAMES.stream().anyMatch(optionalStacks::contains)) {
            bulkImportBucketStack = new BulkImportBucketStack(this, "BulkImportBucket", instanceProperties);
        }
        if (EMR_BULK_IMPORT_STACK_NAMES.stream().anyMatch(optionalStacks::contains)) {
            emrBulkImportCommonStack = new CommonEmrBulkImportStack(this, "BulkImportEMRCommon",
                    instanceProperties, coreStacks, bulkImportBucketStack, ingestStatusStoreStack);
        }

        // Stack to run bulk import jobs via EMR Serverless
        if (optionalStacks.contains(EmrServerlessBulkImportStack.class.getSimpleName())) {
            emrServerlessBulkImportStack = new EmrServerlessBulkImportStack(this, "BulkImportEMRServerless",
                    instanceProperties, jars,
                    bulkImportBucketStack,
                    topicStack,
                    coreStacks,
                    ingestStatusStoreStack.getResources()
            );

            // Stack to created EMR studio to be used to access EMR Serverless
            if (optionalStacks.contains(EmrStudioStack.class.getSimpleName())) {
                emrStudioStack = new EmrStudioStack(this, "EmrStudio", instanceProperties);
            }
        }

        // Stack to run bulk import jobs via EMR (one cluster per bulk import job)
        if (optionalStacks.contains(EmrBulkImportStack.class.getSimpleName())) {
            emrBulkImportStack = new EmrBulkImportStack(this, "BulkImportEMR",
                    instanceProperties, jars,
                    bulkImportBucketStack,
                    emrBulkImportCommonStack,
                    topicStack,
                    coreStacks,
                    ingestStatusStoreStack.getResources()
            );
        }

        // Stack to run bulk import jobs via a persistent EMR cluster
        if (optionalStacks.contains(PersistentEmrBulkImportStack.class.getSimpleName())) {
            persistentEmrBulkImportStack = new PersistentEmrBulkImportStack(this, "BulkImportPersistentEMR",
                    instanceProperties,
                    jars,
                    bulkImportBucketStack,
                    emrBulkImportCommonStack,
                    topicStack,
                    coreStacks,
                    ingestStatusStoreStack.getResources()
            );
        }

        // Stack to run bulk import jobs via EKS
        if (optionalStacks.contains(EksBulkImportStack.class.getSimpleName())) {
            eksBulkImportStack = new EksBulkImportStack(this, "BulkImportEKS",
                    instanceProperties,
                    jars,
                    bulkImportBucketStack,
                    coreStacks,
                    topicStack,
                    ingestStatusStoreStack
            );
        }

        // Stack to garbage collect old files
        if (optionalStacks.contains(GarbageCollectorStack.class.getSimpleName())) {
            new GarbageCollectorStack(this,
                    "GarbageCollector",
                    instanceProperties, jars,
                    coreStacks);
        }

        // Stack for containers for compactions and splitting compactions
        if (optionalStacks.contains(CompactionStack.class.getSimpleName())) {
            compactionStack = new CompactionStack(this,
                    "Compaction",
                    instanceProperties, jars,
                    topicStack.getTopic(),
                    coreStacks);
        }

        // Stack to split partitions
        if (optionalStacks.contains(PartitionSplittingStack.class.getSimpleName())) {
            partitionSplittingStack = new PartitionSplittingStack(this,
                    "PartitionSplitting",
                    instanceProperties, jars,
                    coreStacks,
                    topicStack.getTopic());
        }

        // Stack to execute queries
        if (optionalStacks.contains(QueryStack.class.getSimpleName())) {
            new QueryStack(this,
                    "Query",
                    instanceProperties, jars,
                    coreStacks);
        }

        // Stack for ingest jobs
        if (optionalStacks.contains(IngestStack.class.getSimpleName())) {
            ingestStack = new IngestStack(this,
                    "Ingest",
                    instanceProperties, jars,
                    coreStacks,
                    topicStack.getTopic(),
                    ingestStatusStoreStack);
        }

        // Stack to batch up files to ingest and create jobs
        if (optionalStacks.contains(IngestBatcherStack.class.getSimpleName())) {
            new IngestBatcherStack(this, "IngestBatcher",
                    instanceProperties, jars, coreStacks,
                    ingestStack, emrBulkImportStack, persistentEmrBulkImportStack,
                    eksBulkImportStack, emrServerlessBulkImportStack);
        }

        if (optionalStacks.contains(DashboardStack.class.getSimpleName())) {
            new DashboardStack(this,
                    "Dashboard",
                    ingestStack,
                    compactionStack,
                    partitionSplittingStack,
                    instanceProperties
            );
        }

        this.generateProperties();
        addTags(app);
    }

    protected InstanceProperties getInstanceProperties() {
        return instanceProperties;
    }

    public IngestStack getIngestStack() {
        return ingestStack;
    }

    public CoreStacks getCoreStacks() {
        return coreStacks;
    }

    public EmrServerlessBulkImportStack getEmrServerlessBulkImportStack() {
        return emrServerlessBulkImportStack;
    }

    public EmrStudioStack gEmrStudioStack() {
        return emrStudioStack;
    }

    public EmrBulkImportStack getEmrBulkImportStack() {
        return emrBulkImportStack;
    }

    public PersistentEmrBulkImportStack getPersistentEmrBulkImportStack() {
        return persistentEmrBulkImportStack;
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
        App app = new App();

        InstanceProperties instanceProperties = Utils.loadInstanceProperties(InstanceProperties::new, app);

        String id = instanceProperties.get(ID);
        Environment environment = Environment.builder()
                .account(instanceProperties.get(ACCOUNT))
                .region(instanceProperties.get(REGION))
                .build();
        BuiltJars jars = new BuiltJars(AmazonS3ClientBuilder.defaultClient(), instanceProperties.get(JARS_BUCKET));

        new SleeperCdkApp(app, id, StackProps.builder()
                .stackName(id)
                .env(environment)
                .build(),
                instanceProperties, jars).create();

        app.synth();
    }
}
