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
package sleeper.cdk.stack;

import com.google.common.collect.Lists;
import software.amazon.awscdk.CustomResource;
import software.amazon.awscdk.Duration;
import software.amazon.awscdk.NestedStack;
import software.amazon.awscdk.customresources.Provider;
import software.amazon.awscdk.services.events.Rule;
import software.amazon.awscdk.services.events.RuleTargetInput;
import software.amazon.awscdk.services.events.Schedule;
import software.amazon.awscdk.services.events.targets.LambdaFunction;
import software.amazon.awscdk.services.iam.IRole;
import software.amazon.awscdk.services.lambda.IFunction;
import software.amazon.awscdk.services.lambda.Runtime;
import software.amazon.awscdk.services.s3.Bucket;
import software.amazon.awscdk.services.s3.IBucket;
import software.amazon.awscdk.services.s3.assets.AssetOptions;
import software.amazon.awscdk.services.s3.deployment.BucketDeployment;
import software.amazon.awscdk.services.s3.deployment.Source;
import software.constructs.Construct;

import sleeper.cdk.Utils;
import sleeper.cdk.jars.BuiltJar;
import sleeper.cdk.jars.BuiltJars;
import sleeper.cdk.jars.LambdaCode;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.statestore.dynamodb.DynamoDBStateStore;
import sleeper.statestore.s3.S3StateStore;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static sleeper.cdk.Utils.shouldDeployPaused;
import static sleeper.cdk.stack.IngestStack.addIngestSourceRoleReferences;
import static sleeper.configuration.properties.instance.CommonProperty.ID;
import static sleeper.configuration.properties.instance.CommonProperty.JARS_BUCKET;
import static sleeper.configuration.properties.instance.CommonProperty.LOG_RETENTION_IN_DAYS;
import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.TABLE_METRICS_RULES;
import static sleeper.configuration.properties.table.TableProperty.SPLIT_POINTS_FILE;
import static sleeper.configuration.properties.table.TableProperty.SPLIT_POINTS_KEY;
import static sleeper.configuration.properties.table.TableProperty.STATESTORE_CLASSNAME;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;

public class TableStack extends NestedStack {

    private final List<StateStoreStack> stateStoreStacks = new ArrayList<>();

    public TableStack(
            Construct scope,
            String id,
            InstanceProperties instanceProperties,
            BuiltJars jars,
            TableDataStack dataStack,
            NewDynamoDBStateStoreStack dynamoDBStateStoreStack) {
        super(scope, id);
        IBucket jarsBucket = Bucket.fromBucketName(this, "JarsBucket", instanceProperties.get(JARS_BUCKET));
        IBucket configBucket = Bucket.fromBucketName(this, "ConfigBucket", instanceProperties.get(CONFIG_BUCKET));
        LambdaCode jar = jars.lambdaCode(BuiltJar.CUSTOM_RESOURCES, jarsBucket);
        LambdaCode metricsJar = jars.lambdaCode(BuiltJar.METRICS, jarsBucket);

        String functionName = Utils.truncateTo64Characters(String.join("-", "sleeper",
                instanceProperties.get(ID).toLowerCase(Locale.ROOT), "sleeper-table"));

        IFunction sleeperTableLambda = jar.buildFunction(this, "SleeperTableLambda", builder -> builder
                .functionName(functionName)
                .handler("sleeper.cdk.custom.SleeperTableLambda::handleEvent")
                .memorySize(2048)
                .timeout(Duration.minutes(10))
                .environment(Utils.createDefaultEnvironment(instanceProperties))
                .description("Lambda for handling initialisation and teardown of Sleeper Tables")
                .logRetention(Utils.getRetentionDays(instanceProperties.getInt(LOG_RETENTION_IN_DAYS)))
                .runtime(Runtime.JAVA_11));

        configBucket.grantReadWrite(sleeperTableLambda);

        Provider sleeperTableProvider = Provider.Builder.create(this, "SleeperTableProvider")
                .onEventHandler(sleeperTableLambda)
                .logRetention(Utils.getRetentionDays(instanceProperties.getInt(LOG_RETENTION_IN_DAYS)))
                .build();

        dynamoDBStateStoreStack.grantReadWriteActiveFileMetadata(sleeperTableProvider.getOnEventHandler());
        dynamoDBStateStoreStack.grantReadWritePartitionMetadata(sleeperTableProvider.getOnEventHandler());
        stateStoreStacks.add(dynamoDBStateStoreStack);

        createTables(scope, instanceProperties, sleeperTableProvider, dataStack, dynamoDBStateStoreStack,
                configBucket, metricsJar);
        addIngestSourceRoleReferences(this, "TableWriterForIngest", instanceProperties)
                .forEach(role -> grantIngestSourceRole(role, stateStoreStacks));

        Utils.addStackTagIfSet(this, instanceProperties);
    }

    private static void grantIngestSourceRole(
            IRole role, List<StateStoreStack> stateStoreStacks) {
        stateStoreStacks.forEach(stateStoreStack -> {
            stateStoreStack.grantReadPartitionMetadata(role);
            stateStoreStack.grantReadWriteActiveFileMetadata(role);
        });
    }

    private void createTables(Construct scope,
                              InstanceProperties instanceProperties,
                              Provider tablesProvider,
                              TableDataStack dataStack,
                              NewDynamoDBStateStoreStack dynamoDBStateStoreStack,
                              IBucket configBucket,
                              LambdaCode metricsJar) {
        Utils.getAllTableProperties(instanceProperties, scope).forEach(tableProperties ->
                createTable(instanceProperties, tableProperties, tablesProvider, dataStack,
                        dynamoDBStateStoreStack, configBucket, metricsJar));
    }

    private void createTable(InstanceProperties instanceProperties,
                             TableProperties tableProperties,
                             Provider sleeperTablesProvider,
                             TableDataStack dataStack,
                             NewDynamoDBStateStoreStack dynamoDBStateStoreStack,
                             IBucket configBucket,
                             LambdaCode metricsJar) {
        String tableName = tableProperties.get(TABLE_NAME);

        StateStoreStack stateStoreStack;
        String stateStoreClassName = tableProperties.get(STATESTORE_CLASSNAME);
        if (stateStoreClassName.equals(DynamoDBStateStore.class.getName())) {
            stateStoreStack = dynamoDBStateStoreStack;
        } else if (stateStoreClassName.equals(S3StateStore.class.getName())) {
            stateStoreStack = createS3StateStore(instanceProperties, tableProperties, dataStack, sleeperTablesProvider);
            stateStoreStacks.add(stateStoreStack);
        } else {
            throw new RuntimeException("Unknown statestore class name");
        }

        BucketDeployment splitPoints = null;
        if (tableProperties.get(SPLIT_POINTS_FILE) != null) {
            File splitsFile = new File(tableProperties.get(SPLIT_POINTS_FILE));
            if (!splitsFile.exists()) {
                throw new RuntimeException("Unable to locate splits file: " + splitsFile.getAbsolutePath());
            }
            String fileName = splitsFile.getName();
            String directory = splitsFile.getParent();
            splitPoints = BucketDeployment.Builder.create(this, tableName + "SplitPoints")
                    .retainOnDelete(false)
                    .destinationKeyPrefix("splits/" + tableName)
                    .destinationBucket(configBucket)
                    .prune(false)
                    .sources(Lists.newArrayList(Source.asset(directory, AssetOptions.builder()
                            .exclude(Lists.newArrayList("*", "!" + fileName))
                            .build())))
                    .build();

            tableProperties.set(SPLIT_POINTS_KEY, "splits/" + tableName + "/" + fileName);
        }

        Map<String, String> properties = new HashMap<>();
        properties.put("instanceProperties", instanceProperties.saveAsString());
        properties.put("tableProperties", tableProperties.saveAsString());
        CustomResource tableInitialisation = CustomResource.Builder.create(this, tableName + "SleeperTable")
                .resourceType("Custom::SleeperTable")
                .properties(properties)
                .serviceToken(sleeperTablesProvider.getServiceToken())
                .build();

        if (splitPoints != null) {
            tableInitialisation.getNode().addDependency(splitPoints);
        }

        // Metrics generation and publishing
        IFunction tableMetricsPublisher = metricsJar.buildFunction(this, tableName + "MetricsPublisher", builder -> builder
                .description("Generates metrics for a Sleeper table based on info in its state store, and publishes them to CloudWatch")
                .runtime(Runtime.JAVA_11)
                .handler("sleeper.metrics.TableMetricsLambda::handleRequest")
                .memorySize(256)
                .timeout(Duration.seconds(60))
                .logRetention(Utils.getRetentionDays(instanceProperties.getInt(LOG_RETENTION_IN_DAYS))));

        configBucket.grantRead(tableMetricsPublisher);
        stateStoreStack.grantReadActiveFileMetadata(tableMetricsPublisher);
        stateStoreStack.grantReadPartitionMetadata(tableMetricsPublisher);

        Rule rule = Rule.Builder.create(this, tableName + "MetricsPublishSchedule")
                .schedule(Schedule.rate(Duration.minutes(1)))
                .targets(Collections.singletonList(
                        LambdaFunction.Builder.create(tableMetricsPublisher)
                                .event(RuleTargetInput.fromText(configBucket.getBucketName() + "|" + tableName))
                                .build()
                ))
                .enabled(!shouldDeployPaused(this))
                .build();
        if (null == instanceProperties.get(TABLE_METRICS_RULES) || instanceProperties.get(TABLE_METRICS_RULES).isEmpty()) {
            instanceProperties.set(TABLE_METRICS_RULES, rule.getRuleName());
        } else {
            String rulesList = instanceProperties.get(TABLE_METRICS_RULES);
            instanceProperties.set(TABLE_METRICS_RULES, rulesList + "," + rule.getRuleName());
        }
    }

    private StateStoreStack createS3StateStore(InstanceProperties instanceProperties,
                                               TableProperties tableProperties,
                                               TableDataStack dataStack,
                                               Provider sleeperTablesProvider) {
        return new S3StateStoreStack(this, dataStack, instanceProperties, tableProperties, sleeperTablesProvider);
    }

    public List<StateStoreStack> getStateStoreStacks() {
        return stateStoreStacks;
    }
}
