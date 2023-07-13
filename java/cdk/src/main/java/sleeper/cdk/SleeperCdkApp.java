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
import sleeper.cdk.stack.ConfigurationStack;
import sleeper.cdk.stack.DashboardStack;
import sleeper.cdk.stack.GarbageCollectorStack;
import sleeper.cdk.stack.IngestBatcherStack;
import sleeper.cdk.stack.IngestStack;
import sleeper.cdk.stack.IngestStatusStoreStack;
import sleeper.cdk.stack.PartitionSplittingStack;
import sleeper.cdk.stack.PropertiesStack;
import sleeper.cdk.stack.QueryStack;
import sleeper.cdk.stack.TableStack;
import sleeper.cdk.stack.TopicStack;
import sleeper.cdk.stack.VpcStack;
import sleeper.cdk.stack.bulkimport.BulkImportBucketStack;
import sleeper.cdk.stack.bulkimport.CommonEmrBulkImportStack;
import sleeper.cdk.stack.bulkimport.EksBulkImportStack;
import sleeper.cdk.stack.bulkimport.EmrBulkImportStack;
import sleeper.cdk.stack.bulkimport.PersistentEmrBulkImportStack;
import sleeper.configuration.properties.InstanceProperties;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static sleeper.configuration.properties.CommonProperty.ACCOUNT;
import static sleeper.configuration.properties.CommonProperty.ID;
import static sleeper.configuration.properties.CommonProperty.JARS_BUCKET;
import static sleeper.configuration.properties.CommonProperty.OPTIONAL_STACKS;
import static sleeper.configuration.properties.CommonProperty.REGION;

/**
 * The {@link App} that deploys all the Sleeper stacks.
 */
public class SleeperCdkApp extends Stack {
    private final InstanceProperties instanceProperties;
    private final BuiltJars jars;
    private final App app;
    private IngestStack ingestStack;
    private TableStack tableStack;
    private CompactionStack compactionStack;
    private PartitionSplittingStack partitionSplittingStack;
    private BulkImportBucketStack bulkImportBucketStack;
    private CommonEmrBulkImportStack emrBulkImportCommonStack;
    private EmrBulkImportStack emrBulkImportStack;
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
                    PersistentEmrBulkImportStack.class,
                    EksBulkImportStack.class)
            .map(Class::getSimpleName).collect(Collectors.toList());

    private static final List<String> EMR_BULK_IMPORT_STACK_NAMES = Stream.of(
                    EmrBulkImportStack.class,
                    PersistentEmrBulkImportStack.class)
            .map(Class::getSimpleName).collect(Collectors.toList());

    public static final List<String> INGEST_STACK_NAMES = Stream.of(
                    IngestStack.class,
                    EmrBulkImportStack.class,
                    PersistentEmrBulkImportStack.class,
                    EksBulkImportStack.class)
            .map(Class::getSimpleName).collect(Collectors.toList());

    public void create() {
        // Optional stacks to be included
        List<String> optionalStacks = instanceProperties.getList(OPTIONAL_STACKS);

        // Stack for Checking VPC configuration
        new VpcStack(this, "Vpc", instanceProperties, jars);

        // Stack for instance configuration
        new ConfigurationStack(this, "Configuration", instanceProperties);

        // Topic stack
        TopicStack topicStack = new TopicStack(this, "Topic", instanceProperties);

        // Stack for tables
        tableStack = new TableStack(this, "Table", instanceProperties, jars);

        // Stack for Athena analytics
        if (optionalStacks.contains(AthenaStack.class.getSimpleName())) {
            new AthenaStack(this, "Athena", instanceProperties, jars, getTableStack().getStateStoreStacks(), getTableStack().getDataBuckets());
        }

        if (INGEST_STACK_NAMES.stream().anyMatch(optionalStacks::contains)) {
            ingestStatusStoreStack = new IngestStatusStoreStack(this, "IngestStatusStore", instanceProperties);
        }
        if (BULK_IMPORT_STACK_NAMES.stream().anyMatch(optionalStacks::contains)) {
            bulkImportBucketStack = new BulkImportBucketStack(this, "BulkImportBucket", instanceProperties);
        }
        if (EMR_BULK_IMPORT_STACK_NAMES.stream().anyMatch(optionalStacks::contains)) {
            emrBulkImportCommonStack = new CommonEmrBulkImportStack(this, "BulkImportEMRCommon",
                    instanceProperties, bulkImportBucketStack, tableStack, ingestStatusStoreStack);
        }

        // Stack to run bulk import jobs via EMR (one cluster per bulk import job)
        if (optionalStacks.contains(EmrBulkImportStack.class.getSimpleName())) {
            emrBulkImportStack = new EmrBulkImportStack(this, "BulkImportEMR",
                    instanceProperties, jars,
                    bulkImportBucketStack,
                    emrBulkImportCommonStack,
                    topicStack,
                    tableStack.getStateStoreStacks(),
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
                    tableStack.getStateStoreStacks(),
                    ingestStatusStoreStack.getResources()
            );
        }

        // Stack to run bulk import jobs via EKS
        if (optionalStacks.contains(EksBulkImportStack.class.getSimpleName())) {
            eksBulkImportStack = new EksBulkImportStack(this, "BulkImportEKS",
                    instanceProperties,
                    jars,
                    bulkImportBucketStack,
                    tableStack,
                    topicStack,
                    ingestStatusStoreStack,
                    tableStack.getStateStoreStacks()
            );
        }

        // Stack to garbage collect old files
        if (optionalStacks.contains(GarbageCollectorStack.class.getSimpleName())) {
            new GarbageCollectorStack(this,
                    "GarbageCollector",
                    instanceProperties, jars,
                    tableStack.getStateStoreStacks(),
                    tableStack.getDataBuckets());
        }

        // Stack for containers for compactions and splitting compactions
        if (optionalStacks.contains(CompactionStack.class.getSimpleName())) {
            compactionStack = new CompactionStack(this,
                    "Compaction",
                    instanceProperties, jars,
                    topicStack.getTopic(),
                    tableStack.getStateStoreStacks(),
                    tableStack.getDataBuckets());
        }

        // Stack to split partitions
        if (optionalStacks.contains(PartitionSplittingStack.class.getSimpleName())) {
            partitionSplittingStack = new PartitionSplittingStack(this,
                    "PartitionSplitting",
                    instanceProperties, jars,
                    tableStack.getDataBuckets(),
                    tableStack.getStateStoreStacks(),
                    topicStack.getTopic());
        }

        // Stack to execute queries
        if (optionalStacks.contains(QueryStack.class.getSimpleName())) {
            new QueryStack(this,
                    "Query",
                    instanceProperties, jars,
                    tableStack.getDataBuckets(),
                    tableStack.getStateStoreStacks());
        }

        // Stack for ingest jobs
        if (optionalStacks.contains(IngestStack.class.getSimpleName())) {
            ingestStack = new IngestStack(this,
                    "Ingest",
                    instanceProperties, jars,
                    tableStack.getStateStoreStacks(),
                    tableStack.getDataBuckets(),
                    topicStack.getTopic(),
                    ingestStatusStoreStack);
        }

        // Stack to batch up files to ingest and create jobs
        if (optionalStacks.contains(IngestBatcherStack.class.getSimpleName())) {
            new IngestBatcherStack(this, "IngestBatcher",
                    instanceProperties, jars,
                    ingestStack, emrBulkImportStack, persistentEmrBulkImportStack, eksBulkImportStack);
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

    public TableStack getTableStack() {
        return tableStack;
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
        new PropertiesStack(this, "Properties", instanceProperties, jars);
    }

    public static void main(String[] args) {
        App app = new App();

        InstanceProperties instanceProperties = Utils.loadInstanceProperties(new InstanceProperties(), app);

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
