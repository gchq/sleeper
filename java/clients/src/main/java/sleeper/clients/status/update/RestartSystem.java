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

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import software.amazon.awssdk.services.cloudwatchevents.CloudWatchEventsClient;
import software.amazon.awssdk.services.cloudwatchevents.model.ResourceNotFoundException;

import sleeper.clients.util.ClientUtils;
import sleeper.configuration.properties.SleeperScheduleRule;
import sleeper.configuration.properties.instance.InstanceProperties;

import java.util.List;

public class RestartSystem {

    private RestartSystem() {
    }

    public static void main(String[] args) {
        if (1 != args.length) {
            throw new IllegalArgumentException("Usage: <instance-id>");
        }

        AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();
        try (CloudWatchEventsClient cwClient = CloudWatchEventsClient.create()) {
            InstanceProperties instanceProperties = ClientUtils.getInstanceProperties(s3Client, args[0]);
            SleeperScheduleRule.getCloudWatchRules(instanceProperties)
                    .forEach(rules -> enableRule(cwClient, rules));
        } finally {
            s3Client.shutdown();
        }
    }

    private static void enableRule(CloudWatchEventsClient cwClient, SleeperScheduleRule.Value rules) {
        List<String> ruleNames = rules.getRuleNames();
        if (ruleNames.isEmpty()) {
            System.out.println("No rule found for property " + rules.getProperty() + ", not enabling");
        } else {
            ruleNames.forEach(ruleName -> enableRule(cwClient, ruleName));
        }
    }

    private static void enableRule(CloudWatchEventsClient cwClient, String ruleName) {
        try {
            cwClient.enableRule(request -> request.name(ruleName));
            System.out.println("Enabled rule " + ruleName);
        } catch (ResourceNotFoundException e) {
            System.out.println("Rule not found: " + ruleName);
        }
    }
}
