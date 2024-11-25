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
package sleeper.systemtest.drivers.instance;

import software.amazon.awssdk.services.cloudwatchevents.CloudWatchEventsClient;

import sleeper.core.deploy.SleeperScheduleRule;
import sleeper.core.deploy.SleeperScheduleRule.InstanceRule;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.systemtest.drivers.util.SystemTestClients;
import sleeper.systemtest.dsl.instance.ScheduleRulesDriver;

import java.util.Optional;

public class AwsScheduleRulesDriver implements ScheduleRulesDriver {

    private final CloudWatchEventsClient cw;

    public AwsScheduleRulesDriver(SystemTestClients clients) {
        cw = clients.getCloudWatchEvents();
    }

    @Override
    public void enableRuleIfDeployed(InstanceProperties instanceProperties, SleeperScheduleRule rule) {
        getRuleNameIfDeployed(instanceProperties, rule)
                .ifPresent(ruleName -> cw.enableRule(request -> request.name(ruleName)));
    }

    @Override
    public void disableRuleIfDeployed(InstanceProperties instanceProperties, SleeperScheduleRule rule) {
        getRuleNameIfDeployed(instanceProperties, rule)
                .ifPresent(ruleName -> cw.disableRule(request -> request.name(ruleName)));
    }

    private Optional<String> getRuleNameIfDeployed(InstanceProperties instanceProperties, SleeperScheduleRule rule) {
        InstanceRule instanceRule = rule.readValue(instanceProperties);
        if (instanceRule.isDeployed()) {
            return Optional.of(instanceRule.getRuleName());
        } else {
            return Optional.empty();
        }
    }

}
