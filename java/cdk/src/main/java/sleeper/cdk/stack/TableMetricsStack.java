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

import software.amazon.awscdk.Duration;
import software.amazon.awscdk.NestedStack;
import software.amazon.awscdk.services.events.Rule;
import software.amazon.awscdk.services.events.Schedule;
import software.amazon.awscdk.services.events.targets.LambdaFunction;
import software.amazon.awscdk.services.lambda.IFunction;
import software.amazon.awscdk.services.lambda.Runtime;
import software.amazon.awscdk.services.s3.Bucket;
import software.amazon.awscdk.services.s3.IBucket;
import software.constructs.Construct;

import sleeper.cdk.Utils;
import sleeper.cdk.jars.BuiltJar;
import sleeper.cdk.jars.BuiltJars;
import sleeper.cdk.jars.LambdaCode;
import sleeper.configuration.properties.SleeperScheduleRule;
import sleeper.configuration.properties.instance.InstanceProperties;

import java.util.Collections;
import java.util.Locale;

import static sleeper.cdk.Utils.createLambdaLogGroup;
import static sleeper.cdk.Utils.shouldDeployPaused;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.TABLE_METRICS_LAMBDA_FUNCTION;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.TABLE_METRICS_RULES;
import static sleeper.configuration.properties.instance.CommonProperty.ID;
import static sleeper.configuration.properties.instance.CommonProperty.JARS_BUCKET;

public class TableMetricsStack extends NestedStack {
    public TableMetricsStack(Construct scope,
                             String id,
                             InstanceProperties instanceProperties,
                             BuiltJars jars,
                             CoreStacks coreStacks) {
        super(scope, id);
        IBucket jarsBucket = Bucket.fromBucketName(this, "JarsBucket", instanceProperties.get(JARS_BUCKET));
        LambdaCode metricsJar = jars.lambdaCode(BuiltJar.METRICS, jarsBucket);
        String functionName = Utils.truncateTo64Characters(String.join("-", "sleeper",
                instanceProperties.get(ID).toLowerCase(Locale.ROOT), "metrics-publisher"));
        // Metrics generation and publishing
        IFunction tableMetricsPublisher = metricsJar.buildFunction(this, "MetricsPublisher", builder -> builder
                .functionName(functionName)
                .description("Generates metrics for a Sleeper table based on info in its state store, and publishes them to CloudWatch")
                .runtime(Runtime.JAVA_11)
                .handler("sleeper.metrics.TableMetricsLambda::handleRequest")
                .environment(Utils.createDefaultEnvironment(instanceProperties))
                .memorySize(1024)
                .timeout(Duration.seconds(60))
                .logGroup(createLambdaLogGroup(this, "MetricsPublisherLogGroup", functionName, instanceProperties)));

        coreStacks.grantReadTablesMetadata(tableMetricsPublisher);
        instanceProperties.set(TABLE_METRICS_LAMBDA_FUNCTION, tableMetricsPublisher.getFunctionName());

        Rule rule = Rule.Builder.create(this, "MetricsPublishSchedule")
                .ruleName(SleeperScheduleRule.TABLE_METRICS.buildRuleName(instanceProperties))
                .schedule(Schedule.rate(Duration.minutes(1)))
                .targets(Collections.singletonList(new LambdaFunction(tableMetricsPublisher)))
                .enabled(!shouldDeployPaused(this))
                .build();
        instanceProperties.set(TABLE_METRICS_RULES, rule.getRuleName());
    }
}
