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

package sleeper.systemtest.suite.testutil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.configuration.properties.instance.InstanceProperty;
import sleeper.systemtest.dsl.util.PurgeQueueDriver;

import java.util.List;

public class AfterTestPurgeQueues {
    private static final Logger LOGGER = LoggerFactory.getLogger(AfterTestPurgeQueues.class);
    private final PurgeQueueDriver driver;
    private List<InstanceProperty> queueProperties = List.of();

    AfterTestPurgeQueues(PurgeQueueDriver driver) {
        this.driver = driver;
    }

    public void purgeIfTestFailed(InstanceProperty... queueProperties) {
        this.queueProperties = List.of(queueProperties);
    }

    void testPassed() {
        LOGGER.info("Test passed, not purging queue");
    }

    void testFailed() {
        LOGGER.info("Test failed, purging queues: {}", queueProperties);
        if (!queueProperties.isEmpty()) {
            driver.purgeQueues(queueProperties);
        }
    }
}
