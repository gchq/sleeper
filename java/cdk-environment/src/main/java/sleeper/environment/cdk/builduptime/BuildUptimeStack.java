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
package sleeper.environment.cdk.builduptime;

import software.amazon.awscdk.Duration;
import software.amazon.awscdk.Stack;
import software.amazon.awscdk.StackProps;
import software.amazon.awscdk.services.ec2.IInstance;
import software.amazon.awscdk.services.events.CronOptions;
import software.amazon.awscdk.services.events.Rule;
import software.amazon.awscdk.services.events.RuleTargetInput;
import software.amazon.awscdk.services.events.Schedule;
import software.amazon.awscdk.services.events.targets.LambdaFunction;
import software.amazon.awscdk.services.lambda.Code;
import software.amazon.awscdk.services.lambda.Function;
import software.amazon.awscdk.services.lambda.IFunction;
import software.constructs.Construct;

import sleeper.environment.cdk.config.AppContext;
import sleeper.environment.cdk.config.AppParameters;
import sleeper.environment.cdk.config.OptionalStringParameter;
import sleeper.environment.cdk.config.StringListParameter;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static sleeper.environment.cdk.config.AppParameters.INSTANCE_ID;
import static software.amazon.awscdk.services.lambda.Runtime.JAVA_11;

public class BuildUptimeStack extends Stack {
    public static final OptionalStringParameter LAMBDA_JAR = AppParameters.BUILD_UPTIME_LAMBDA_JAR;
    public static final StringListParameter EXISTING_EC2_IDS = AppParameters.BUILD_UPTIME_EXISTING_EC2_IDS;

    public BuildUptimeStack(Construct scope, StackProps props, IInstance buildEc2) {
        super(scope, props.getStackName(), props);
        AppContext context = AppContext.of(this);
        String lambdaJarPath = context.get(LAMBDA_JAR)
                .orElseThrow(() -> new IllegalArgumentException("buildUptimeLambdaJar is required for BuildUptimeStack"));

        IFunction function = Function.Builder.create(this, "BuildUptimeLambda")
                .code(Code.fromAsset(lambdaJarPath))
                .functionName("sleeper-" + context.get(INSTANCE_ID) + "-build-uptime")
                .description("Create batches of tables and send requests to create compaction jobs for those batches")
                .runtime(JAVA_11)
                .memorySize(1024)
                .timeout(Duration.minutes(10))
                .handler("sleeper.build.uptime.lambda.BuildUptimeLambda::handleRequest")
                .environment(Map.of())
                .reservedConcurrentExecutions(1)
                .build().getCurrentVersion();

        scheduleNightlyTests(context, function, buildEc2);
        scheduleAutoShutdown(context, function, buildEc2);
    }

    private void scheduleNightlyTests(AppContext context, IFunction function, IInstance buildEc2) {

        String takeDownRuleName = "sleeper-" + context.get(INSTANCE_ID) + "-stop-nightly-tests";
        Rule.Builder.create(this, "StopAfterNightlyTests")
                .ruleName(takeDownRuleName)
                .description("Periodic trigger to take the build EC2 down when nightly tests finish")
                .schedule(Schedule.rate(Duration.minutes(10)))
                .targets(List.of(LambdaFunction.Builder.create(function)
                        .event(RuleTargetInput.fromObject(Map.of(
                                "operation", "stop",
                                "condition", "nightlyTestsFinished",
                                "ec2Ids", List.of(buildEc2.getInstanceId()),
                                "rules", List.of(takeDownRuleName))))
                        .build()))
                .enabled(false)
                .build();
        Rule.Builder.create(this, "StartForNightlyTests")
                .ruleName("sleeper-" + context.get(INSTANCE_ID) + "-start-for-nightly-tests")
                .description("Nightly invocation to start the build EC2 for nightly tests")
                .schedule(Schedule.cron(CronOptions.builder().hour("2").minute("50").build()))
                .targets(List.of(LambdaFunction.Builder.create(function)
                        .event(RuleTargetInput.fromObject(Map.of(
                                "operation", "start",
                                "ec2Ids", List.of(buildEc2.getInstanceId()),
                                "rules", List.of(takeDownRuleName))))
                        .build()))
                .build();
    }

    private void scheduleAutoShutdown(AppContext context, IFunction function, IInstance buildEc2) {
        List<String> ec2Ids = new ArrayList<>();
        ec2Ids.addAll(context.get(EXISTING_EC2_IDS));
        ec2Ids.add(buildEc2.getInstanceId());
        Rule.Builder.create(this, "AutoShutdown")
                .ruleName("sleeper-" + context.get(INSTANCE_ID) + "-auto-shutdown")
                .description("Daily invocation to shut down EC2s for the night")
                .schedule(Schedule.cron(CronOptions.builder().hour("19").minute("00").build()))
                .targets(List.of(LambdaFunction.Builder.create(function)
                        .event(RuleTargetInput.fromObject(Map.of(
                                "operation", "stop",
                                "ec2Ids", ec2Ids)))
                        .build()))
                .build();
    }
}
