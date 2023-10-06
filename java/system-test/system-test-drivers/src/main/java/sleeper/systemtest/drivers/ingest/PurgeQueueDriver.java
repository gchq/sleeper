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

package sleeper.systemtest.drivers.ingest;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.PurgeQueueRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.configuration.properties.instance.InstanceProperty;
import sleeper.core.util.RunAndWaitIfNeeded;
import sleeper.systemtest.drivers.instance.SleeperInstanceContext;

public class PurgeQueueDriver {
    private static final Logger LOGGER = LoggerFactory.getLogger(PurgeQueueDriver.class);
    private static final RunAndWaitIfNeeded RUNNER = new RunAndWaitIfNeeded(61000L);
    private final SleeperInstanceContext instance;
    private final AmazonSQS sqsClient;

    public PurgeQueueDriver(SleeperInstanceContext instance, AmazonSQS sqsClient) {
        this.instance = instance;
        this.sqsClient = sqsClient;
    }

    public void purgeQueue(InstanceProperty property) {
        String queueUrl = instance.getInstanceProperties().get(property);
        LOGGER.info("Purging SQS queue: {}", queueUrl);
        RUNNER.run(() -> sqsClient.purgeQueue(new PurgeQueueRequest(queueUrl)));
    }
}
