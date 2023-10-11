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

import software.amazon.awscdk.Duration;
import software.amazon.awscdk.NestedStack;
import software.amazon.awscdk.services.events.Rule;
import software.amazon.awscdk.services.events.RuleTargetInput;
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
import sleeper.configuration.properties.instance.InstanceProperties;

import java.util.Collections;

import static sleeper.cdk.Utils.shouldDeployPaused;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.TABLE_METRICS_RULES;
import static sleeper.configuration.properties.instance.CommonProperty.JARS_BUCKET;
import static sleeper.configuration.properties.instance.CommonProperty.LOG_RETENTION_IN_DAYS;

public class TableMetricsStack extends NestedStack {
    public TableMetricsStack(Construct scope,
                             String id,
                             InstanceProperties instanceProperties,
                             BuiltJars jars,
                             StateStoreStacks stateStoreStacks) {
        super(scope, id);
        IBucket jarsBucket = Bucket.fromBucketName(this, "JarsBucket", instanceProperties.get(JARS_BUCKET));
        IBucket configBucket = Bucket.fromBucketName(this, "ConfigBucket", instanceProperties.get(CONFIG_BUCKET));
        LambdaCode metricsJar = jars.lambdaCode(BuiltJar.METRICS, jarsBucket);
        // Metrics generation and publishing
        IFunction tableMetricsPublisher = metricsJar.buildFunction(this, "MetricsPublisher", builder -> builder
                .description("Generates metrics for a Sleeper table based on info in its state store, and publishes them to CloudWatch")
                .runtime(Runtime.JAVA_11)
                .handler("sleeper.metrics.TableMetricsLambda::handleRequest")
                .memorySize(256)
                .timeout(Duration.seconds(60))
                .logRetention(Utils.getRetentionDays(instanceProperties.getInt(LOG_RETENTION_IN_DAYS))));

        configBucket.grantRead(tableMetricsPublisher);
        stateStoreStacks.grantReadActiveFilesAndPartitions(tableMetricsPublisher);

        Rule rule = Rule.Builder.create(this, "MetricsPublishSchedule")
                .schedule(Schedule.rate(Duration.minutes(1)))
                .targets(Collections.singletonList(
                        LambdaFunction.Builder.create(tableMetricsPublisher)
                                .event(RuleTargetInput.fromText(configBucket.getBucketName()))
                                .build()
                ))
                .enabled(!shouldDeployPaused(this))
                .build();
        instanceProperties.set(TABLE_METRICS_RULES, rule.getRuleName());
    }
}
