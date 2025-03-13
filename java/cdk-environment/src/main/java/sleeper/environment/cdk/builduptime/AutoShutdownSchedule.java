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
package sleeper.environment.cdk.builduptime;

import software.amazon.awscdk.services.events.CronOptions;
import software.amazon.awscdk.services.events.IRule;
import software.amazon.awscdk.services.events.Rule;
import software.amazon.awscdk.services.events.RuleTargetInput;
import software.amazon.awscdk.services.events.Schedule;
import software.amazon.awscdk.services.events.targets.LambdaFunction;
import software.constructs.Construct;

import sleeper.environment.cdk.buildec2.BuildEC2Deployment;
import sleeper.environment.cdk.config.AppContext;
import sleeper.environment.cdk.config.AppParameters;
import sleeper.environment.cdk.config.IntParameter;
import sleeper.environment.cdk.config.StringListParameter;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static java.util.stream.Collectors.toUnmodifiableList;
import static sleeper.environment.cdk.config.AppParameters.INSTANCE_ID;

public class AutoShutdownSchedule {

    public static final StringListParameter AUTO_SHUTDOWN_EXISTING_EC2_IDS = AppParameters.AUTO_SHUTDOWN_EXISTING_EC2_IDS;
    public static final IntParameter AUTO_SHUTDOWN_HOUR_UTC = AppParameters.AUTO_SHUTDOWN_HOUR_UTC;

    private AutoShutdownSchedule() {
    }

    public static void create(Construct scope, BuildUptimeDeployment buildUptime, BuildEC2Deployment buildEc2, List<IRule> autoStopRules) {
        AppContext context = AppContext.of(scope);

        List<String> ec2Ids = new ArrayList<>();
        ec2Ids.addAll(context.get(AUTO_SHUTDOWN_EXISTING_EC2_IDS));
        if (buildEc2 != null) {
            ec2Ids.add(buildEc2.getInstance().getInstanceId());
        }

        List<String> rules = autoStopRules.stream().map(IRule::getRuleName).collect(toUnmodifiableList());

        Rule.Builder.create(scope, "AutoShutdownSchedule")
                .ruleName("sleeper-" + context.get(INSTANCE_ID) + "-auto-shutdown")
                .description("Daily invocation to shut down EC2s for the night")
                .schedule(Schedule.cron(CronOptions.builder()
                        .hour("" + context.get(AUTO_SHUTDOWN_HOUR_UTC))
                        .minute("00")
                        .build()))
                .targets(List.of(LambdaFunction.Builder.create(buildUptime.getFunction())
                        .event(RuleTargetInput.fromObject(Map.of(
                                "operation", "stop",
                                "ec2Ids", ec2Ids,
                                "rules", rules)))
                        .build()))
                .build();
    }

}
