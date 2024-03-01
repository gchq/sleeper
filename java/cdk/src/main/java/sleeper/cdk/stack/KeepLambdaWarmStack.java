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

import software.amazon.awscdk.CfnOutput;
import software.amazon.awscdk.CfnOutputProps;
import software.amazon.awscdk.Duration;
import software.amazon.awscdk.NestedStack;
import software.amazon.awscdk.services.events.Rule;
import software.amazon.awscdk.services.events.Schedule;
import software.amazon.awscdk.services.events.targets.LambdaFunction;
import software.amazon.awscdk.services.lambda.Function;
import software.amazon.awscdk.services.lambda.IFunction;
import software.amazon.awscdk.services.lambda.Runtime;
import software.amazon.awscdk.services.s3.Bucket;
import software.amazon.awscdk.services.s3.IBucket;
import software.amazon.awscdk.services.sqs.Queue;
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
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.QUERY_WARM_LAMBDA_CLOUDWATCH_RULE;
import static sleeper.configuration.properties.instance.CommonProperty.ID;
import static sleeper.configuration.properties.instance.QueryProperty.DEFAULT_QUERY_WARM_LAMBDA_EXECUTION_PERIOD_IN_MINUTES;
import static sleeper.configuration.properties.instance.QueryProperty.QUERY_PROCESSOR_LAMBDA_MEMORY_IN_MB;
import static sleeper.configuration.properties.instance.QueryProperty.QUERY_PROCESSOR_LAMBDA_TIMEOUT_IN_SECONDS;

/**
 * A {@link NestedStack} to handle keeping lambdas warm. This consists of a {@link Rule} that runs periodically triggering
 * a lambda {@link Function} to create the queries that are placed on the Query {@link Queue} for processing.
 * This will trigger the query lambdas thus keeping them warm.
 */
public class KeepLambdaWarmStack extends NestedStack {

    public KeepLambdaWarmStack(Construct scope,
            String id,
            InstanceProperties instanceProperties,
            BuiltJars jars,
            CoreStacks coreStacks,
            QueryStack queryStack) {
        super(scope, id);

        String functionName = Utils.truncateTo64Characters(String.join("-", "sleeper",
                instanceProperties.get(ID).toLowerCase(Locale.ROOT), "warm-query-executor"));

        IBucket jarsBucket = Bucket.fromBucketName(this, "JarsBucket", jars.bucketName());
        LambdaCode queryJar = jars.lambdaCode(BuiltJar.QUERY, jarsBucket);

        // Keep lambda warm function
        IFunction handler = queryJar.buildFunction(this, "WarmQueryExecutorLambda", builder -> builder
                .functionName(functionName)
                .description("Sends a message to query-executor lambda in order for it to stay warm")
                .runtime(Runtime.JAVA_11)
                .memorySize(instanceProperties.getInt(QUERY_PROCESSOR_LAMBDA_MEMORY_IN_MB))
                .timeout(Duration.seconds(instanceProperties.getInt(QUERY_PROCESSOR_LAMBDA_TIMEOUT_IN_SECONDS)))
                .handler("sleeper.query.lambda.WarmQueryExecutorLambda::handleRequest")
                .environment(Utils.createDefaultEnvironment(instanceProperties))
                .reservedConcurrentExecutions(1)
                .logGroup(createLambdaLogGroup(this, id + "LogGroup", functionName, instanceProperties)));

        // Cloudwatch rule to trigger this lambda
        Rule rule = Rule.Builder
                .create(this, "QueryExecutionPeriodicTrigger")
                .ruleName(SleeperScheduleRule.QUERY_WARM_LAMBDA.buildRuleName(instanceProperties))
                .description("A rule to periodically trigger the query execution lambda")
                .enabled(!shouldDeployPaused(this))
                .schedule(Schedule.rate(Duration.minutes(instanceProperties
                        .getInt(DEFAULT_QUERY_WARM_LAMBDA_EXECUTION_PERIOD_IN_MINUTES))))
                .targets(Collections.singletonList(LambdaFunction.Builder
                        .create(handler)
                        .build()))
                .build();

        queryStack.grantSendMessages(handler);
        // IQueue queryExecutorQueue = Queue.fromQueueArn(scope, "WarmLambdaToQueryQueue", instanceProperties.get(QUERY_QUEUE_ARN));
        // squeryExecutorQueue.grantSendMessages(handler);

        coreStacks.grantReadInstanceConfig(handler);
        coreStacks.grantReadTablesAndData(handler);

        instanceProperties.set(QUERY_WARM_LAMBDA_CLOUDWATCH_RULE, rule.getRuleName());
        CfnOutputProps ruleArn = new CfnOutputProps.Builder()
                .value(rule.getRuleArn())
                .exportName(instanceProperties.get(ID) + "-" + "QueryExecutionPeriodicTriggerRuleARN")
                .build();
        new CfnOutput(this, "QueryExecutionPeriodicTriggerRuleARN", ruleArn);

        Utils.addStackTagIfSet(this, instanceProperties);
    }
}
