/*
 * Copyright 2022-2026 Crown Copyright
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

import software.amazon.awssdk.services.sqs.model.Message;

import sleeper.core.properties.instance.InstanceProperty;
import sleeper.systemtest.drivers.util.SystemTestClients;
import sleeper.systemtest.drivers.util.sqs.AwsDrainSqsQueue;
import sleeper.systemtest.dsl.instance.DeadLettersDriver;
import sleeper.systemtest.dsl.instance.SystemTestInstanceContext;

import java.util.List;

public class AwsDeadLettersDriver implements DeadLettersDriver {

    private final SystemTestInstanceContext instance;
    private final AwsDrainSqsQueue drainQueue;

    public AwsDeadLettersDriver(SystemTestInstanceContext instance, SystemTestClients clients) {
        this.instance = instance;
        this.drainQueue = AwsDrainSqsQueue.forSystemTests(clients.getSqs());
    }

    @Override
    public List<String> drainQueueForWholeInstance(int expectedMessages, InstanceProperty queueUrlProperty) {
        String queueUrl = instance.getInstanceProperties().get(queueUrlProperty);
        return drainQueue.drainExpectingMessagesWithRetriesWhenEmpty(expectedMessages, 5, queueUrl)
                .map(Message::body)
                .toList();
    }

}
