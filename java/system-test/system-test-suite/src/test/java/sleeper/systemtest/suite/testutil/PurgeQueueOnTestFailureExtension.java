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

package sleeper.systemtest.suite.testutil;

import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.configuration.properties.instance.InstanceProperty;
import sleeper.systemtest.suite.dsl.ingest.SystemTestIngest;

import java.util.function.Consumer;

public class PurgeQueueOnTestFailureExtension implements AfterEachCallback {
    private static final Logger LOGGER = LoggerFactory.getLogger(PurgeQueueOnTestFailureExtension.class);
    private final InstanceProperty queueProperty;
    private final Consumer<InstanceProperty> purgeQueue;
    private final Runnable waitFn;

    PurgeQueueOnTestFailureExtension(InstanceProperty queueProperty, Consumer<InstanceProperty> purgeQueue, Runnable waitFn) {
        this.queueProperty = queueProperty;
        this.purgeQueue = purgeQueue;
        this.waitFn = waitFn;
    }

    public static PurgeQueueOnTestFailureExtension withQueue(InstanceProperty queueProperty, SystemTestIngest ingest) {
        return new PurgeQueueOnTestFailureExtension(queueProperty, ingest::purgeQueue, () -> {
            try {
                LOGGER.info("Waiting 60s for queue to purge");
                Thread.sleep(60000L);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Override
    public void afterEach(ExtensionContext testContext) {
        if (testContext.getExecutionException().isPresent()) {
            afterTestFailed();
        } else {
            afterTestPassed();
        }
    }

    public void afterTestFailed() {
        LOGGER.info("Test failed, purging queue: {}", queueProperty);
        purgeQueueAndWait();
    }

    public void afterTestPassed() {
        LOGGER.info("Test passed, not purging queue");
    }

    private void purgeQueueAndWait() {
        purgeQueue.accept(queueProperty);
        waitFn.run();
    }
}
