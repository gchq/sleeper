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
package sleeper.clients.status.update;

import com.amazonaws.services.cloudwatchevents.AmazonCloudWatchEvents;
import com.amazonaws.services.cloudwatchevents.AmazonCloudWatchEventsClientBuilder;
import com.amazonaws.services.cloudwatchevents.model.DisableRuleRequest;
import com.amazonaws.services.cloudwatchevents.model.ResourceNotFoundException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

import sleeper.clients.util.ClientUtils;
import sleeper.configuration.properties.SleeperScheduleRule;
import sleeper.configuration.properties.instance.InstanceProperties;

import java.util.List;

public class PauseSystem {

    private PauseSystem() {
    }

    public static void main(String[] args) {
        if (1 != args.length) {
            throw new IllegalArgumentException("Usage: <instance-id>");
        }
        String instanceId = args[0];

        AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();
        AmazonCloudWatchEvents cwClient = AmazonCloudWatchEventsClientBuilder.defaultClient();
        try {
            InstanceProperties instanceProperties = ClientUtils.getInstanceProperties(s3Client, instanceId);
            pause(cwClient, instanceProperties);
        } finally {
            s3Client.shutdown();
            cwClient.shutdown();
        }
    }

    public static void pause(AmazonCloudWatchEvents cwClient, InstanceProperties instanceProperties) {

        SleeperScheduleRule.getCloudWatchRules(instanceProperties)
                .forEach(rules -> disableRule(cwClient, rules));
    }

    private static void disableRule(AmazonCloudWatchEvents cwClient, SleeperScheduleRule.Value rules) {
        List<String> ruleNames = rules.getRuleNames();
        if (ruleNames.isEmpty()) {
            System.out.println("No rule found for property " + rules.getProperty() + ", not disabling");
        } else {
            ruleNames.forEach(ruleName -> disableRule(cwClient, ruleName));
        }
    }

    private static void disableRule(AmazonCloudWatchEvents cwClient, String ruleName) {
        try {
            cwClient.disableRule(new DisableRuleRequest()
                    .withName(ruleName));
            System.out.println("Disabled rule " + ruleName);
        } catch (ResourceNotFoundException e) {
            System.out.println("Rule not found: " + ruleName);
        }
    }
}
