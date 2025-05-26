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
import software.amazon.awssdk.services.s3.S3Client;

import sleeper.configurationv2.properties.S3InstanceProperties;
import sleeper.core.deploy.SleeperScheduleRule;
import sleeper.core.properties.instance.InstanceProperties;

public class PauseSystem {

    private PauseSystem() {
    }

    public static void main(String[] args) {
        if (1 != args.length) {
            throw new IllegalArgumentException("Usage: <instance-id>");
        }
        String instanceId = args[0];

        try (S3Client s3Client = S3Client.create();
                CloudWatchEventsClient cwClient = CloudWatchEventsClient.create()) {
            InstanceProperties instanceProperties = S3InstanceProperties.loadGivenInstanceId(s3Client, instanceId);
            pause(cwClient, instanceProperties);
        }
    }

    public static void pause(CloudWatchEventsClient cwClient, InstanceProperties instanceProperties) {

        SleeperScheduleRule.getDeployedRules(instanceProperties)
                .forEach(rule -> disableRule(cwClient, rule.getRuleName()));
    }

    private static void disableRule(CloudWatchEventsClient cwClient, String ruleName) {
        try {
            cwClient.disableRule(request -> request.name(ruleName));
            System.out.println("Disabled rule: " + ruleName);
        } catch (ResourceNotFoundException e) {
            System.out.println("Rule not found: " + ruleName);
        }
    }
}
