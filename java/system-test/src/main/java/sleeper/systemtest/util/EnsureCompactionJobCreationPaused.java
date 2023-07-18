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
package sleeper.systemtest.util;

import com.amazonaws.services.cloudwatchevents.AmazonCloudWatchEvents;
import com.amazonaws.services.cloudwatchevents.AmazonCloudWatchEventsClientBuilder;
import com.amazonaws.services.cloudwatchevents.model.DescribeRuleRequest;
import com.amazonaws.services.cloudwatchevents.model.DescribeRuleResult;
import com.amazonaws.services.cloudwatchevents.model.RuleState;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

import sleeper.clients.util.ClientUtils;
import sleeper.configuration.properties.instance.InstanceProperties;

import java.io.IOException;

import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.COMPACTION_JOB_CREATION_CLOUDWATCH_RULE;

public class EnsureCompactionJobCreationPaused {

    private EnsureCompactionJobCreationPaused() {
    }

    public static void main(String[] args) throws IOException {
        if (args.length != 1) {
            System.out.println("Usage: <instance id>");
            return;
        }

        AmazonS3 amazonS3 = AmazonS3ClientBuilder.defaultClient();
        InstanceProperties instanceProperties = ClientUtils.getInstanceProperties(amazonS3, args[0]);
        amazonS3.shutdown();

        AmazonCloudWatchEvents cwClient = AmazonCloudWatchEventsClientBuilder.defaultClient();

        String ruleName = instanceProperties.get(COMPACTION_JOB_CREATION_CLOUDWATCH_RULE);

        DescribeRuleResult result = cwClient.describeRule(new DescribeRuleRequest().withName(ruleName));
        RuleState state = RuleState.fromValue(result.getState());
        if (state != RuleState.DISABLED) {
            System.err.println("Compaction job creation CloudWatch rule is not paused");
            System.exit(1);
        }
    }
}
