/*
 * Copyright 2022 Crown Copyright
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

import static sleeper.configuration.properties.UserDefinedInstanceProperty.ACCOUNT;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.ID;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.OPTIONAL_STACKS;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.REGION;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.List;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

import sleeper.cdk.stack.AthenaStack;
import sleeper.cdk.stack.CompactionStack;
import sleeper.cdk.stack.ConfigurationStack;
import sleeper.cdk.stack.DashboardStack;
import sleeper.cdk.stack.GarbageCollectorStack;
import sleeper.cdk.stack.IngestStack;
import sleeper.cdk.stack.PartitionSplittingStack;
import sleeper.cdk.stack.PropertiesStack;
import sleeper.cdk.stack.QueryStack;
import sleeper.cdk.stack.TableStack;
import sleeper.cdk.stack.TopicStack;
import sleeper.cdk.stack.VpcStack;
import sleeper.cdk.stack.bulkimport.EksBulkImportStack;
import sleeper.cdk.stack.bulkimport.EmrBulkImportStack;
import sleeper.cdk.stack.bulkimport.PersistentEmrBulkImportStack;
import sleeper.configuration.properties.InstanceProperties;
import software.amazon.awscdk.App;
import software.amazon.awscdk.Environment;
import software.amazon.awscdk.Stack;
import software.amazon.awscdk.StackProps;
import software.amazon.awscdk.Tags;
import software.constructs.Construct;

/**
 * The {@link App} that deploys all the Sleeper stacks.
 */
public class SleeperCdkApp extends Stack {
    public final InstanceProperties instanceProperties;
    private final App app;
    private IngestStack ingestStack;
    private TableStack tableStack;
    private CompactionStack compactionStack;
    private PartitionSplittingStack partitionSplittingStack;
    private EmrBulkImportStack emrBulkImportStack;
    private PersistentEmrBulkImportStack persistentEmrBulkImportStack;

    public SleeperCdkApp(App app, String id, InstanceProperties instanceProperties, StackProps props) {
        super(app, id, props);
        this.app = app;
        this.instanceProperties = instanceProperties;
    }

    public void create() {
        // Optional stacks to be included
        List<String> optionalStacks = instanceProperties.getList(OPTIONAL_STACKS);

        // Stack for Checking VPC configuration
        new VpcStack(this, "Vpc", instanceProperties);

        // Stack for instance configuration
        new ConfigurationStack(this, "Configuration", instanceProperties);

        // Topic stack
        TopicStack topicStack = new TopicStack(this, "Topic", instanceProperties);

        // Stack for tables
        tableStack = new TableStack(this, "Table", instanceProperties);

        // Stack for Athena analytics
        if (optionalStacks.contains(AthenaStack.class.getSimpleName())) {
            new AthenaStack(this, "Athena", instanceProperties, getTableStack().getStateStoreStacks(), getTableStack().getDataBuckets());
        }
        
        // Stack to run bulk import jobs via EMR (one cluster per bulk import job)
        if (optionalStacks.contains(EmrBulkImportStack.class.getSimpleName())) {
            emrBulkImportStack = new EmrBulkImportStack(this, "BulkImportEMR",
                    tableStack.getDataBuckets(),
                    tableStack.getStateStoreStacks(),
                    instanceProperties,
                    topicStack.getTopic());
        }
        
        // Stack to run bulk import jobs via a persistent EMR cluster
        if (optionalStacks.contains(PersistentEmrBulkImportStack.class.getSimpleName())) {
            persistentEmrBulkImportStack = new PersistentEmrBulkImportStack(this, "BulkImportPersistentEMR",
                    tableStack.getDataBuckets(),
                    tableStack.getStateStoreStacks(),
                    instanceProperties,
                    topicStack.getTopic());
        }
        
        // Stack to run bulk import jobs via EKS
        if (optionalStacks.contains(EksBulkImportStack.class.getSimpleName())) {
            new EksBulkImportStack(this, "BulkImportEKS",
                    tableStack.getDataBuckets(),
                    tableStack.getStateStoreStacks(),
                    instanceProperties,
                    topicStack.getTopic());
        }

        // Stack to garbage collect old files
        if (optionalStacks.contains(GarbageCollectorStack.class.getSimpleName())) {
            new GarbageCollectorStack(this,
                    "GarbageCollector",
                    instanceProperties,
                    tableStack.getStateStoreStacks(),
                    tableStack.getDataBuckets());
        }

        // Stack for containers for compactions and splitting compactions
        if (optionalStacks.contains(CompactionStack.class.getSimpleName())) {
            compactionStack = new CompactionStack(this,
                    "Compaction",
                    topicStack.getTopic(),
                    tableStack.getStateStoreStacks(),
                    tableStack.getDataBuckets(),
                    instanceProperties);
        }

        // Stack to split partitions
        if (optionalStacks.contains(PartitionSplittingStack.class.getSimpleName())) {
            partitionSplittingStack = new PartitionSplittingStack(this,
                    "PartitionSplitting",
                    tableStack.getDataBuckets(),
                    tableStack.getStateStoreStacks(),
                    topicStack.getTopic(),
                    instanceProperties);
        }

        // Stack to execute queries
        if (optionalStacks.contains(QueryStack.class.getSimpleName())) {
            new QueryStack(this,
                    "Query",
                    tableStack.getDataBuckets(),
                    tableStack.getStateStoreStacks(),
                    instanceProperties);
        }

        // Stack for ingest jobs
        if (optionalStacks.contains(IngestStack.class.getSimpleName())) {
            ingestStack = new IngestStack(this,
                    "Ingest",
                    tableStack.getStateStoreStacks(),
                    tableStack.getDataBuckets(),
                    topicStack.getTopic(),
                    instanceProperties);
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
        new PropertiesStack(this, "Properties", instanceProperties);
    }

    public static void main(String[] args) throws FileNotFoundException {
        App app = new App();

        String propertiesFile = (String) app.getNode().tryGetContext("propertiesfile");
        String validate = (String) app.getNode().tryGetContext("validate");
        File inputPropertiesFile = new File(propertiesFile);
        InstanceProperties instanceProperties = new InstanceProperties();
        instanceProperties.load(inputPropertiesFile);

        if ("true".equalsIgnoreCase(validate)) {
            new ConfigValidator(AmazonS3ClientBuilder.defaultClient(),
                    AmazonDynamoDBClientBuilder.defaultClient()).validate(instanceProperties);
        }
        
        String id = instanceProperties.get(ID);
        Environment environment = Environment.builder()
                .account(instanceProperties.get(ACCOUNT))
                .region(instanceProperties.get(REGION))
                .build();

        new SleeperCdkApp(app, id, instanceProperties, StackProps.builder()
                .stackName(id)
                .env(environment)
                .build()).create();

        app.synth();
    }
}