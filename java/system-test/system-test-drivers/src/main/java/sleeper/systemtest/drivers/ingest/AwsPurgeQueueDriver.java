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

package sleeper.systemtest.drivers.ingest;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.PurgeQueueRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.core.properties.instance.InstanceProperty;
import sleeper.systemtest.dsl.instance.SystemTestInstanceContext;
import sleeper.systemtest.dsl.util.PurgeQueueDriver;

import java.util.List;

public class AwsPurgeQueueDriver implements PurgeQueueDriver {
    private static final Logger LOGGER = LoggerFactory.getLogger(AwsPurgeQueueDriver.class);
    private final SystemTestInstanceContext instance;
    private final AmazonSQS sqsClient;

    public AwsPurgeQueueDriver(SystemTestInstanceContext instance, AmazonSQS sqsClient) {
        this.instance = instance;
        this.sqsClient = sqsClient;
    }

    public void purgeQueues(List<InstanceProperty> properties) {
        for (InstanceProperty property : properties) {
            String queueUrl = instance.getInstanceProperties().get(property);
            LOGGER.info("Purging queue: {}", queueUrl);
            sqsClient.purgeQueue(new PurgeQueueRequest(queueUrl));
        }
        LOGGER.info("Waiting 60s for purge");
        try {
            Thread.sleep(60000L);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }
}
