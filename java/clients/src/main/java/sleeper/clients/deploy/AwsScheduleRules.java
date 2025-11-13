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
package sleeper.clients.deploy;

import software.amazon.awssdk.services.cloudwatchevents.CloudWatchEventsClient;
import software.amazon.awssdk.services.cloudwatchevents.model.ResourceNotFoundException;

import sleeper.core.deploy.SleeperScheduleRule;
import sleeper.core.deploy.SleeperScheduleRule.InstanceRule;
import sleeper.core.properties.instance.InstanceProperties;

import java.util.List;
import java.util.stream.Stream;

public class AwsScheduleRules {

    private final CloudWatchEventsClient cwClient;
    private final List<SleeperScheduleRule> rules;

    public AwsScheduleRules(CloudWatchEventsClient cwClient) {
        this(cwClient, SleeperScheduleRule.all());
    }

    public AwsScheduleRules(CloudWatchEventsClient cwClient, List<SleeperScheduleRule> rules) {
        this.cwClient = cwClient;
        this.rules = rules;
    }

    public void pauseInstance(InstanceProperties instanceProperties) {
        deployedRuleNames(instanceProperties).forEach(this::disableRule);
    }

    public void pauseInstance(String instanceId) {
        allRuleNames(instanceId).forEach(this::disableRule);
    }

    public void startInstance(InstanceProperties instanceProperties) {
        deployedRuleNames(instanceProperties).forEach(this::enableRule);
    }

    public void startInstance(String instanceId) {
        allRuleNames(instanceId).forEach(this::enableRule);
    }

    private Stream<String> deployedRuleNames(InstanceProperties instanceProperties) {
        return rules.stream()
                .map(rule -> rule.readValue(instanceProperties))
                .filter(InstanceRule::isDeployed)
                .map(InstanceRule::getRuleName);
    }

    private Stream<String> allRuleNames(String instanceId) {
        return rules.stream()
                .map(rule -> rule.buildRuleName(instanceId));
    }

    private void disableRule(String ruleName) {
        try {
            cwClient.disableRule(request -> request.name(ruleName));
            System.out.println("Disabled rule: " + ruleName);
        } catch (ResourceNotFoundException e) {
            System.out.println("Rule not found: " + ruleName);
        }
    }

    private void enableRule(String ruleName) {
        try {
            cwClient.enableRule(request -> request.name(ruleName));
            System.out.println("Enabled rule: " + ruleName);
        } catch (ResourceNotFoundException e) {
            System.out.println("Rule not found: " + ruleName);
        }
    }

}
