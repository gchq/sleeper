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
package sleeper.environment.cdk.nightlytests;

import software.amazon.awscdk.Duration;
import software.amazon.awscdk.services.ec2.IInstance;
import software.amazon.awscdk.services.events.CronOptions;
import software.amazon.awscdk.services.events.IRule;
import software.amazon.awscdk.services.events.Rule;
import software.amazon.awscdk.services.events.RuleTargetInput;
import software.amazon.awscdk.services.events.Schedule;
import software.amazon.awscdk.services.events.targets.LambdaFunction;
import software.amazon.awscdk.services.lambda.IFunction;
import software.constructs.Construct;

import sleeper.environment.cdk.config.AppContext;
import sleeper.environment.cdk.config.AppParameters;
import sleeper.environment.cdk.config.IntParameter;

import java.util.List;
import java.util.Map;

import static sleeper.environment.cdk.config.AppParameters.INSTANCE_ID;

public class NightlyTestUptimeSchedules {
    public static final IntParameter NIGHTLY_TEST_RUN_HOUR_UTC = AppParameters.NIGHTLY_TEST_RUN_HOUR_UTC;

    private final IRule stopAfterTestsRule;

    public NightlyTestUptimeSchedules(
            Construct scope, IFunction buildUptimeFn, IInstance buildEc2, String testBucketName) {
        AppContext context = AppContext.of(scope);

        String stopAfterTestsRuleName = "sleeper-" + context.get(INSTANCE_ID) + "-stop-nightly-tests";
        stopAfterTestsRule = Rule.Builder.create(scope, "StopAfterNightlyTests")
                .ruleName(stopAfterTestsRuleName)
                .description("Periodic trigger to take the build EC2 down when nightly tests finish")
                .schedule(Schedule.rate(Duration.minutes(10)))
                .targets(List.of(LambdaFunction.Builder.create(buildUptimeFn)
                        .event(RuleTargetInput.fromObject(Map.of(
                                "operation", "stop",
                                "condition", "testFinishedFromToday",
                                "testBucket", testBucketName,
                                "ec2Ids", List.of(buildEc2.getInstanceId()),
                                "rules", List.of(stopAfterTestsRuleName))))
                        .build()))
                .enabled(false)
                .build();
        Rule.Builder.create(scope, "StartForNightlyTests")
                .ruleName("sleeper-" + context.get(INSTANCE_ID) + "-start-for-nightly-tests")
                .description("Nightly invocation to start the build EC2 for nightly tests")
                .schedule(Schedule.cron(CronOptions.builder()
                        .hour("" + (context.get(NIGHTLY_TEST_RUN_HOUR_UTC) - 1))
                        .minute("50")
                        .build()))
                .targets(List.of(LambdaFunction.Builder.create(buildUptimeFn)
                        .event(RuleTargetInput.fromObject(Map.of(
                                "operation", "start",
                                "ec2Ids", List.of(buildEc2.getInstanceId()),
                                "rules", List.of(stopAfterTestsRuleName))))
                        .build()))
                .build();
    }

    public IRule getStopAfterTestsRule() {
        return stopAfterTestsRule;
    }
}
