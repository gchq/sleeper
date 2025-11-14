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
package sleeper.cdk.custom;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.events.CloudFormationCustomResourceEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.cloudwatchevents.CloudWatchEventsClient;

import java.util.Map;

/**
 * Pause a CloudWatch rule.
 */
public class PauseScheduledRuleLambda {
    public static final Logger LOGGER = LoggerFactory.getLogger(PauseScheduledRuleLambda.class);

    private final CloudWatchEventsClient cwClient;

    public PauseScheduledRuleLambda() {
        this(CloudWatchEventsClient.create());
    }

    public PauseScheduledRuleLambda(CloudWatchEventsClient cwClient) {
        this.cwClient = cwClient;
    }

    /**
     * Handles an event triggered by CloudFormation.
     *
     * @param event   the event to handle
     * @param context the context
     */
    public void handleEvent(
            CloudFormationCustomResourceEvent event, Context context) {
        Map<String, Object> resourceProperties = event.getResourceProperties();
        String scheduledRuleName = (String) resourceProperties.get("scheduledRuleName");

        switch (event.getRequestType()) {
            case "Create":
                break;
            case "Update":
                break;
            case "Delete":
                pauseRule(cwClient, scheduledRuleName);
                break;
            default:
                throw new IllegalArgumentException("Invalid request type: " + event.getRequestType());
        }
    }

    private static void pauseRule(CloudWatchEventsClient cwClient, String scheduledRuleName) {
        LOGGER.info("Pausing scheduled rule {}", scheduledRuleName);
        cwClient.disableRule(request -> request.name(scheduledRuleName));
        LOGGER.info("Paused scheduled rule {}", scheduledRuleName);
    }

}
