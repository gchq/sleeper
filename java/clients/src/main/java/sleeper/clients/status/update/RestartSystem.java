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
package sleeper.clients.status.update;

import com.amazonaws.services.cloudwatchevents.AmazonCloudWatchEvents;
import com.amazonaws.services.cloudwatchevents.AmazonCloudWatchEventsClientBuilder;
import com.amazonaws.services.cloudwatchevents.model.EnableRuleRequest;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

import sleeper.clients.util.ClientUtils;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.InstanceProperty;

import java.io.IOException;

import static sleeper.configuration.properties.SystemDefinedInstanceProperty.COMPACTION_JOB_CREATION_CLOUDWATCH_RULE;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.COMPACTION_TASK_CREATION_CLOUDWATCH_RULE;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.GARBAGE_COLLECTOR_CLOUDWATCH_RULE;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.INGEST_BATCHER_JOB_CREATION_CLOUDWATCH_RULE;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.INGEST_CLOUDWATCH_RULE;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.PARTITION_SPLITTING_CLOUDWATCH_RULE;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.SPLITTING_COMPACTION_TASK_CREATION_CLOUDWATCH_RULE;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.TABLE_METRICS_RULES;

public class RestartSystem {

    private RestartSystem() {
    }

    public static void main(String[] args) throws IOException {
        if (1 != args.length) {
            throw new IllegalArgumentException("Usage: <instance id>");
        }

        AmazonS3 amazonS3 = AmazonS3ClientBuilder.defaultClient();
        InstanceProperties instanceProperties = ClientUtils.getInstanceProperties(amazonS3, args[0]);
        amazonS3.shutdown();

        AmazonCloudWatchEvents cwClient = AmazonCloudWatchEventsClientBuilder.defaultClient();

        // Rule that creates compaction jobs
        enableRule(cwClient, instanceProperties, COMPACTION_JOB_CREATION_CLOUDWATCH_RULE);

        // Rules that create compaction and splitting compaction tasks
        enableRule(cwClient, instanceProperties, COMPACTION_TASK_CREATION_CLOUDWATCH_RULE);
        enableRule(cwClient, instanceProperties, SPLITTING_COMPACTION_TASK_CREATION_CLOUDWATCH_RULE);

        // Rule that looks for partitions that need splitting
        enableRule(cwClient, instanceProperties, PARTITION_SPLITTING_CLOUDWATCH_RULE);

        // Rule that triggers garbage collector lambda
        enableRule(cwClient, instanceProperties, GARBAGE_COLLECTOR_CLOUDWATCH_RULE);

        // Rule that triggers creation of ingest tasks
        enableRule(cwClient, instanceProperties, INGEST_CLOUDWATCH_RULE);

        // Rule that batches up ingest jobs from file ingest requests
        enableRule(cwClient, instanceProperties, INGEST_BATCHER_JOB_CREATION_CLOUDWATCH_RULE);

        // Rules that trigger generation of metrics for tables
        String csvRules = instanceProperties.get(TABLE_METRICS_RULES);
        if (null != csvRules && !csvRules.isEmpty()) {
            String[] rules = csvRules.split(",");
            for (String rule : rules) {
                enableRule(cwClient, rule);
            }
        }

        cwClient.shutdown();
    }

    private static void enableRule(AmazonCloudWatchEvents cwClient,
                                   InstanceProperties instanceProperties,
                                   InstanceProperty ruleProperty) {
        String ruleName = instanceProperties.get(ruleProperty);
        if (null == ruleName) {
            System.out.println("Null rule name for property " + ruleProperty + ", not enabling");
        } else {
            enableRule(cwClient, ruleName);
        }
    }

    private static void enableRule(AmazonCloudWatchEvents cwClient, String ruleName) {
        EnableRuleRequest enableRuleRequest = new EnableRuleRequest()
                .withName(ruleName);
        cwClient.enableRule(enableRuleRequest);
        System.out.println("Enabled rule " + ruleName);
    }
}
