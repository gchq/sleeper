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
package sleeper.cdk.stack.query;

import software.amazon.awscdk.CfnOutput;
import software.amazon.awscdk.CfnOutputProps;
import software.amazon.awscdk.Duration;
import software.amazon.awscdk.NestedStack;
import software.amazon.awscdk.services.events.Rule;
import software.amazon.awscdk.services.events.Schedule;
import software.amazon.awscdk.services.events.targets.LambdaFunction;
import software.amazon.awscdk.services.lambda.IFunction;
import software.amazon.awscdk.services.s3.IBucket;
import software.constructs.Construct;

import sleeper.cdk.SleeperInstanceProps;
import sleeper.cdk.lambda.SleeperLambdaCode;
import sleeper.cdk.stack.SleeperCoreStacks;
import sleeper.cdk.stack.core.LoggingStack.LogGroupRef;
import sleeper.cdk.util.Utils;
import sleeper.core.deploy.LambdaHandler;
import sleeper.core.deploy.SleeperScheduleRule;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.util.EnvironmentUtils;

import java.util.List;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.QUERY_WARM_LAMBDA_CLOUDWATCH_RULE;
import static sleeper.core.properties.instance.CommonProperty.ID;
import static sleeper.core.properties.instance.QueryProperty.QUERY_PROCESSOR_LAMBDA_MEMORY_IN_MB;
import static sleeper.core.properties.instance.QueryProperty.QUERY_PROCESSOR_LAMBDA_TIMEOUT_IN_SECONDS;
import static sleeper.core.properties.instance.QueryProperty.QUERY_WARM_LAMBDA_EXECUTION_PERIOD_IN_MINUTES;

/*
 * A {@link NestedStack} to handle keeping lambdas warm. This consists of a {@link Rule} that runs periodically triggering
 * a lambda {@link Function} to create the queries that are placed on the Query {@link Queue} for processing.
 * This will trigger the query lambdas thus keeping them warm.
 */
public class KeepLambdaWarmStack extends NestedStack {

    public KeepLambdaWarmStack(
            Construct scope, String id,
            SleeperInstanceProps props,
            SleeperCoreStacks coreStacks,
            QueryQueueStack queryQueueStack) {
        super(scope, id);
        InstanceProperties instanceProperties = props.getInstanceProperties();

        String functionName = String.join("-", "sleeper",
                Utils.cleanInstanceId(instanceProperties), "query-keep-warm");

        IBucket jarsBucket = props.getJars().createJarsBucketReference(this, "JarsBucket");
        SleeperLambdaCode lambdaCode = props.getJars().lambdaCode(jarsBucket);

        // Keep lambda warm function
        IFunction handler = lambdaCode.buildFunction(this, LambdaHandler.KEEP_QUERY_WARM, "WarmQueryExecutorLambda", builder -> builder
                .functionName(functionName)
                .description("Sends a message to query-executor lambda in order for it to stay warm")
                .memorySize(instanceProperties.getInt(QUERY_PROCESSOR_LAMBDA_MEMORY_IN_MB))
                .timeout(Duration.seconds(instanceProperties.getInt(QUERY_PROCESSOR_LAMBDA_TIMEOUT_IN_SECONDS)))
                .environment(EnvironmentUtils.createDefaultEnvironment(instanceProperties))
                .reservedConcurrentExecutions(1)
                .logGroup(coreStacks.getLogGroup(LogGroupRef.QUERY_KEEP_WARM)));

        // Cloudwatch rule to trigger this lambda
        Rule rule = Rule.Builder
                .create(this, "QueryExecutionPeriodicTrigger")
                .ruleName(SleeperScheduleRule.QUERY_WARM_LAMBDA.buildRuleName(instanceProperties))
                .description(SleeperScheduleRule.QUERY_WARM_LAMBDA.getDescription())
                .enabled(!props.isDeployPaused())
                .schedule(Schedule.rate(Duration.minutes(instanceProperties
                        .getInt(QUERY_WARM_LAMBDA_EXECUTION_PERIOD_IN_MINUTES))))
                .targets(List.of(LambdaFunction.Builder
                        .create(handler)
                        .build()))
                .build();

        queryQueueStack.grantSendMessages(handler);

        coreStacks.grantReadInstanceConfig(handler);
        coreStacks.grantReadTablesAndData(handler);
        coreStacks.grantInvokeScheduled(handler);

        instanceProperties.set(QUERY_WARM_LAMBDA_CLOUDWATCH_RULE, rule.getRuleName());
        CfnOutputProps ruleArn = new CfnOutputProps.Builder()
                .value(rule.getRuleArn())
                .exportName(instanceProperties.get(ID) + "-" + "QueryExecutionPeriodicTriggerRuleARN")
                .build();
        new CfnOutput(this, "QueryExecutionPeriodicTriggerRuleARN", ruleArn);

        Utils.addTags(this, instanceProperties);
    }
}
