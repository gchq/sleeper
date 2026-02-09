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
package sleeper.cdk.stack.core;

import software.amazon.awscdk.NestedStack;
import software.amazon.awscdk.services.sns.Topic;
import software.amazon.awscdk.services.sns.subscriptions.EmailSubscription;
import software.constructs.Construct;

import sleeper.cdk.util.Utils;
import sleeper.core.properties.instance.InstanceProperties;

import static sleeper.core.properties.instance.CommonProperty.EMAIL_ADDRESS_FOR_ERROR_NOTIFICATION;

/**
 * Creates an SNS topic for alerts. This will email alerts if messages arrive on a dead-letter queue.
 */
public class TopicStack extends NestedStack {
    private final Topic topic;

    public TopicStack(
            Construct scope,
            String id,
            InstanceProperties instanceProperties) {
        super(scope, id);

        // SNS Topic for errors (triggered by things arriving on dead letter queues)
        // Add alarm to send message to SNS if there are any messages on the dead letter queue
        this.topic = Topic.Builder
                .create(this, "ErrorsTopic")
                .topicName(String.join("-", "sleeper",
                        Utils.cleanInstanceId(instanceProperties), "ErrorsTopic"))
                .build();
        String emailAddress = instanceProperties.get(EMAIL_ADDRESS_FOR_ERROR_NOTIFICATION);
        if (null != emailAddress && !emailAddress.isEmpty()) {
            topic.addSubscription(new EmailSubscription(emailAddress));
        }

        Utils.addTags(this, instanceProperties);
    }

    public Topic getTopic() {
        return topic;
    }
}
