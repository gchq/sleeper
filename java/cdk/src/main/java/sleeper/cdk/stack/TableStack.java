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
import software.amazon.awscdk.RemovalPolicy;
import software.amazon.awscdk.customresources.Provider;
import software.amazon.awscdk.services.events.Rule;
import software.amazon.awscdk.services.events.RuleTargetInput;
import software.amazon.awscdk.services.events.Schedule;
import software.amazon.awscdk.services.events.targets.LambdaFunction;
import software.amazon.awscdk.services.lambda.IFunction;
import software.amazon.awscdk.services.lambda.Runtime;
import software.amazon.awscdk.services.s3.BlockPublicAccess;
import software.amazon.awscdk.services.s3.Bucket;
import software.amazon.awscdk.services.s3.BucketEncryption;
import software.amazon.awscdk.services.s3.IBucket;
import software.amazon.awscdk.services.s3.assets.AssetOptions;
import software.amazon.awscdk.services.s3.deployment.BucketDeployment;
import software.amazon.awscdk.services.s3.deployment.Source;
import software.constructs.Construct;

import sleeper.cdk.Utils;
import sleeper.cdk.jars.BuiltJar;
import sleeper.cdk.jars.BuiltJars;
import sleeper.cdk.jars.LambdaCode;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.statestore.dynamodb.DynamoDBStateStore;
import sleeper.statestore.s3.S3StateStore;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static sleeper.cdk.Utils.removalPolicy;
import static sleeper.cdk.Utils.shouldDeployPaused;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.TABLE_METRICS_RULES;
import static sleeper.configuration.properties.CommonProperties.ID;
import static sleeper.configuration.properties.CommonProperties.JARS_BUCKET;
import static sleeper.configuration.properties.CommonProperties.LOG_RETENTION_IN_DAYS;
import static sleeper.configuration.properties.table.TableProperty.DATA_BUCKET;
import static sleeper.configuration.properties.table.TableProperty.ENCRYPTED;
import static sleeper.configuration.properties.table.TableProperty.SPLIT_POINTS_FILE;
import static sleeper.configuration.properties.table.TableProperty.SPLIT_POINTS_KEY;
import static sleeper.configuration.properties.table.TableProperty.STATESTORE_CLASSNAME;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;

public class TableStack extends NestedStack {

    private final List<StateStoreStack> stateStoreStacks = new ArrayList<>();
    private final List<IBucket> dataBuckets = new ArrayList<>();

    public TableStack(
            Construct scope,
            String id,
            InstanceProperties instanceProperties,
            BuiltJars jars) {
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

        createTables(scope, instanceProperties, sleeperTableProvider, sleeperTableLambda, configBucket, metricsJar);

        Utils.addStackTagIfSet(this, instanceProperties);
    }

    private void createTables(Construct scope,
                              InstanceProperties instanceProperties,
                              Provider tablesProvider,
                              IFunction sleeperTableLambda,
                              IBucket configBucket,
                              LambdaCode metricsJar) {
        Utils.getAllTableProperties(instanceProperties, scope).forEach(tableProperties ->
                createTable(instanceProperties, tableProperties, tablesProvider, sleeperTableLambda, configBucket, metricsJar));
    }

    private void createTable(InstanceProperties instanceProperties,
                             TableProperties tableProperties,
                             Provider sleeperTablesProvider,
                             IFunction sleeperTableLambda,
                             IBucket configBucket,
                             LambdaCode metricsJar) {
        String instanceId = instanceProperties.get(ID);
        String tableName = tableProperties.get(TABLE_NAME);

        BucketEncryption encryption = tableProperties.getBoolean(ENCRYPTED) ? BucketEncryption.S3_MANAGED :
                BucketEncryption.UNENCRYPTED;

        RemovalPolicy removalPolicy = removalPolicy(instanceProperties);

        Bucket databucket = Bucket.Builder
                .create(this, tableName + "DataBucket")
                .bucketName(String.join("-", "sleeper", instanceId, "table", tableName).toLowerCase(Locale.ROOT))
                .versioned(false)
                .blockPublicAccess(BlockPublicAccess.BLOCK_ALL)
                .encryption(encryption)
                .removalPolicy(removalPolicy).autoDeleteObjects(removalPolicy == RemovalPolicy.DESTROY)
                .build();

        databucket.grantReadWrite(sleeperTableLambda);
        dataBuckets.add(databucket);

        tableProperties.set(DATA_BUCKET, databucket.getBucketName());

        StateStoreStack stateStoreStack;
        String stateStoreClassName = tableProperties.get(STATESTORE_CLASSNAME);
        if (stateStoreClassName.equals(DynamoDBStateStore.class.getName())) {
            stateStoreStack = createDynamoDBStateStore(instanceProperties, tableProperties, sleeperTablesProvider);
        } else if (stateStoreClassName.equals(S3StateStore.class.getName())) {
            stateStoreStack = createS3StateStore(instanceProperties, tableProperties, databucket, sleeperTablesProvider);
        } else {
            throw new RuntimeException("Unknown statestore class name");
        }
        stateStoreStacks.add(stateStoreStack);

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
        try {
            properties.put("instanceProperties", instanceProperties.saveAsString());
            properties.put("tableProperties", tableProperties.saveAsString());
        } catch (IOException e) {
            throw new RuntimeException("Failed to store instanceProperties or tableProperties in a string", e);
        }
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

    private StateStoreStack createDynamoDBStateStore(InstanceProperties instanceProperties,
                                                     TableProperties tableProperties,
                                                     Provider sleeperTablesProvider) {
        return new DynamoDBStateStoreStack(this, sleeperTablesProvider, instanceProperties, tableProperties);
    }

    private StateStoreStack createS3StateStore(InstanceProperties instanceProperties,
                                               TableProperties tableProperties,
                                               Bucket dataBucket,
                                               Provider sleeperTablesProvider) {
        return new S3StateStoreStack(this, dataBucket, instanceProperties, tableProperties, sleeperTablesProvider);
    }

    public List<StateStoreStack> getStateStoreStacks() {
        return stateStoreStacks;
    }

    public List<IBucket> getDataBuckets() {
        return dataBuckets;
    }
}
